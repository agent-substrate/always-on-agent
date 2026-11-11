#!/usr/bin/env python3
"""T6, clock half - does a restored actor know what time it is.

The credential half of T6 is closed by the soak: 899 restores each followed by a
real model answer, which the actor cannot produce without its model credential.
The clock half has never been graded. Pass 1 recorded it ungraded, the plan says
in terms that it must not carry into pass 2 ungraded a second time, and the
reason it kept slipping is that the soak's own probe has nothing to measure. Its
gaps are tens of seconds, and no clock is wrong by tens of seconds.

So this is the spike, and the only thing it needs that the soak cannot give it
is a long sleep. Run it against agents that have been suspended overnight and
the question becomes answerable: a checkpointed sandbox that resumes with a
frozen clock is out by the whole length of the sleep, which is hours, and
nothing subtle is required to see it.

**The sandbox clock is the platform's answer, and the model's is not.** The
model is told the date in its system prompt, so asking "what day is it" grades
the gateway's prompt construction and tells you nothing about the restore. What
this asks for instead is the operating system clock inside the actor, read
through the agent's own code execution. That number is produced by the kernel
the checkpoint froze and the restore thawed, which is the thing under test.

Both are recorded, because the interesting failure is them disagreeing: an agent
whose OS clock is stale but whose prompt is current will answer date questions
correctly all day and get every duration, timeout and schedule wrong.

    ./t6_clock.py probe --run pass2      # after a long sleep
    ./t6_clock.py report --run pass2

Grading is on the OS clock: reconciled if it is within --tolerance of true UTC
at the moment the reply came back. The tolerance is generous on purpose. This
test is looking for a clock stuck by hours, not for NTP-grade accuracy, and a
tight bound would fail on model latency rather than on anything the platform
did.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import t15_baseline as t15  # noqa: E402
import t2_overnight as t2  # noqa: E402

CONTEXT = os.environ.get(
    "KUBECTL_CONTEXT", "REPLACE_WITH_KUBECTL_CONTEXT")
ATESPACE = os.environ.get("ATESPACE", "hermes-pass2")
RUN_DIR = os.environ.get("RUN_DIR", str(
    Path(__file__).resolve().parent.parent / "runs" / "pass2"))
GATEWAY = "http://127.0.0.1:18080"

# Asks for the OS clock and the model's own belief in one turn, in a shape that
# survives a chatty reply. Two tagged lines are greppable; a sentence is not.
PROMPT = (
    "Run this exact Python with your code execution tool and then answer:\n\n"
    "    import datetime\n"
    "    print(datetime.datetime.now(datetime.timezone.utc).isoformat())\n\n"
    "Reply with exactly two lines and nothing else:\n"
    "OSCLOCK: <the line the code printed, verbatim>\n"
    "BELIEF: <what you believe the current UTC date and time is, "
    "in ISO 8601, without running any code>"
)

ISO = re.compile(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?)"
                 r"(Z|[+-]\d{2}:?\d{2})?")


def ate(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run([t15.kubectl_ate_bin(), "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def actor_state(name: str) -> str:
    p = ate("get", "actor", name, "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        return "?"
    try:
        doc = json.loads(p.stdout or "{}")
        return ((doc.get("actors") or [doc])[0].get("status") or {}).get(
            "state", "?")
    except Exception:
        return "?"


def full_turn(base: str, session_key: str, prompt: str, timeout: float) -> dict:
    """Like t15.one_turn but keeps the whole reply.

    t15 truncates to 120 characters, which is the right call everywhere else and
    the wrong one here: the two timestamps are the entire measurement and the
    second of them falls off the end.
    """
    body = json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request(
        f"{base}/v1/chat/completions", data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + os.environ["GATEWAY_INGRESS_KEY"],
                 "X-Hermes-Session-Key": session_key})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    except Exception as e:
        return {"status": 0, "ok": False, "error": repr(e), "reply": "",
                "elapsed_s": round(time.monotonic() - t0, 2)}
    elapsed = round(time.monotonic() - t0, 2)
    try:
        d = json.loads(raw)
        reply = d["choices"][0]["message"]["content"] or ""
    except Exception:
        reply = raw.decode("utf-8", "replace")
    return {"status": status, "ok": 200 <= status < 300, "error": "",
            "reply": reply, "elapsed_s": elapsed}


def parse_line(reply: str, tag: str):
    """Pull the timestamp off a tagged line, falling back to the whole reply.

    A model that ignores the format and answers in prose still usually contains
    the right number, and throwing that away would report a platform result on
    the strength of the model's formatting.
    """
    for line in reply.splitlines():
        if line.strip().upper().startswith(tag):
            m = ISO.search(line)
            if m:
                return m.group(0), True
    m = ISO.search(reply)
    return (m.group(0) if m else None), False


def to_utc(s: str):
    if not s:
        return None
    s = s.strip().replace(" ", "T").replace("Z", "+00:00")
    if re.search(r"[+-]\d{4}$", s):
        s = s[:-5] + s[-5:-2] + ":" + s[-2:]
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def load_agents() -> list:
    """(session key, tier) per agent, read from the fleet file.

    Deliberately not t2.load_fleet, which returns the overnight harness's own
    tables keyed for its cold-fact probes. This needs two fields and reading
    them directly keeps a change to that harness from silently reshaping this
    one.
    """
    with open(t2.FLEET_FILE) as f:
        doc = json.load(f)
    return [(x["id"], x.get("idle_tier", "?")) for x in doc["agents"]]


def cmd_probe(a) -> int:
    agents = load_agents()
    out = []
    print(f"probing {len(agents)} agents via {a.gateway}")
    for key, tier in agents:
        name = t15.actor_name_for(key)
        before = actor_state(name)
        sent = datetime.now(timezone.utc)
        r = full_turn(a.gateway, key, PROMPT, a.timeout)
        back = datetime.now(timezone.utc)

        os_raw, os_tagged = parse_line(r["reply"], "OSCLOCK")
        be_raw, be_tagged = parse_line(r["reply"], "BELIEF")
        os_dt, be_dt = to_utc(os_raw), to_utc(be_raw)

        # Skew against the moment the reply came back. The turn takes seconds,
        # so anchoring at either end is within the tolerance either way; `back`
        # is the later bound and so the conservative one for a stale clock.
        os_skew = (os_dt - back).total_seconds() if os_dt else None
        # The model's belief is graded on the DATE, not the timestamp. The
        # gateway puts the date in the system prompt and not the time of day, so
        # the model answers midnight and a seconds-level skew reads as twenty
        # hours wrong. That number is an artefact of what it was told, and
        # putting it in a table invites someone to read it as a clock defect.
        be_date_ok = (be_dt.date() == back.date()) if be_dt else None

        rec = {
            "session_key": key, "tier": tier, "actor": name,
            "state_before": before,
            "crossed_restore": before == "ACTOR_STATE_SUSPENDED",
            "sent_utc": sent.isoformat(timespec="seconds"),
            "replied_utc": back.isoformat(timespec="seconds"),
            "status": r["status"], "ok": r["ok"], "error": r["error"],
            "elapsed_s": r["elapsed_s"],
            "os_clock_raw": os_raw, "os_clock_tagged": os_tagged,
            "os_skew_s": round(os_skew, 1) if os_skew is not None else None,
            "belief_raw": be_raw, "belief_tagged": be_tagged,
            "belief_date_ok": be_date_ok,
            "reply": r["reply"][:1500],
        }
        out.append(rec)
        v = ("no OS clock in the reply" if os_skew is None
             else f"skew {os_skew:+.1f}s")
        print(f"  {key:<26} {before.replace('ACTOR_STATE_',''):<10} "
              f"{r['status']} {r['elapsed_s']:6.2f}s  {v}", flush=True)

    p = Path(a.out) / f"t6-clock-{a.run}.json"
    p.write_text(json.dumps({"run": a.run, "at": t2.now(),
                             "build": os.environ.get("SUBSTRATE_BUILD", "c48b3a3c"),
                             "cluster": CONTEXT.rsplit("_", 1)[-1],
                             "atespace": ATESPACE,
                             "tolerance_s": a.tolerance,
                             "probes": out}, indent=2))
    print(f"\nwrote {p}")
    return 0


def cmd_report(a) -> int:
    p = Path(a.out) / f"t6-clock-{a.run}.json"
    doc = json.loads(p.read_text())
    probes = doc["probes"]
    tol = doc.get("tolerance_s", a.tolerance)

    graded = [r for r in probes if r["os_skew_s"] is not None]
    crossed = [r for r in graded if r["crossed_restore"]]
    good = [r for r in graded if abs(r["os_skew_s"]) <= tol]

    L = [f"# T6, clock half - run {doc['run']}",
         "",
         f"{doc['at']}. Build `{doc['build']}`, cluster `{doc['cluster']}`. "
         f"Each agent was asked for the operating system clock inside its own "
         f"sandbox, read through its code execution tool, and separately for "
         f"what it believed the time to be without running anything. Graded on "
         f"the first; tolerance {tol:.0f}s.",
         "",
         "| Agent | Tier | State at send | OS clock skew | Model's own date | Verdict |",
         "|---|---|---|---|---|---|"]
    for r in probes:
        st = r["state_before"].replace("ACTOR_STATE_", "")
        osk = "not answered" if r["os_skew_s"] is None else f"{r['os_skew_s']:+.1f}s"
        bek = {None: "not answered", True: "date correct",
               False: "**date wrong**"}[r["belief_date_ok"]]
        if r["os_skew_s"] is None:
            v = "ungraded"
        elif abs(r["os_skew_s"]) <= tol:
            v = "reconciled"
        else:
            v = "**OUT**"
        L += [f"| {r['session_key']} | {r['tier']} | {st} | {osk} | {bek} | {v} |"]

    L += ["",
          f"**{len(good)} of {len(graded)} graded agents had a sandbox clock "
          f"within {tol:.0f}s of true UTC.** {len(crossed)} of them were "
          f"SUSPENDED when the question arrived, so their answer was produced "
          f"by a restored sandbox rather than one that had been running all "
          f"along. An agent that was already awake evidences nothing here and "
          f"is reported rather than counted.",
          ""]
    if len(graded) < len(probes):
        L += [f"{len(probes) - len(graded)} agent(s) did not return a parseable "
              f"timestamp and are ungraded rather than failed. The raw replies "
              f"are in the JSON; a model that declines to run code is not a "
              f"clock defect.", ""]
    L += ["The model's own date is recorded beside the OS clock because the "
          "dangerous case is the two disagreeing. The gateway puts the date in "
          "the system prompt, so a stale sandbox clock is invisible to any "
          "question the model answers from its prompt, and shows up only in "
          "what the agent's code sees: durations, timeouts and anything it "
          "schedules for itself. The model is given the date and not the time "
          "of day, so only the date is graded here; it answers midnight and a "
          "seconds-level comparison would report a twenty-hour error that "
          "reflects the prompt rather than any clock.", ""]

    out = Path(a.out) / f"t6-clock-report-{a.run}.md"
    out.write_text("\n".join(L))
    print("\n".join(L))
    print(f"\nwrote {out}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=("probe", "report"))
    ap.add_argument("--run", required=True)
    ap.add_argument("--gateway", default=GATEWAY)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--tolerance", type=float, default=120.0,
                    help="seconds of skew still counted as reconciled")
    ap.add_argument("--out", default=RUN_DIR)
    a = ap.parse_args()
    return cmd_probe(a) if a.phase == "probe" else cmd_report(a)


if __name__ == "__main__":
    sys.exit(main())
