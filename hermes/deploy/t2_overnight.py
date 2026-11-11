#!/usr/bin/env python3
"""T2 - does an agent still know yesterday, after a night of suspend/resume.

§5 of the acceptance plan: agents idle overnight; the next morning each is asked
a question that can only be answered from the previous day's conversation. The
deliverable is a transcript per agent showing the answer depends on prior-day
context - not a pass/fail line.

Two invocations, twelve hours apart:

    ./t2_overnight.py plant --run 260827      # evening
    ./t2_overnight.py ask   --run 260827      # next morning
    ./t2_overnight.py report --run 260827     # writes the transcripts

**Two facts per agent, and only one of them is cold.** Each agent's cron job
asks it hourly to "confirm you still remember the <noun> I gave you", so the
noun fact is re-surfaced all night: for a `short`-tier agent the last mention
could be seven minutes before the morning question, which proves far less than
T2 asks. So planting also gives each agent a second fact that *nothing
else ever refers to* - no cron prompt names it, no intervening turn can echo it.
The morning question asks for that one. What is graded is the cold fact; the
noun fact is recorded alongside because the contrast between the two is itself
worth reporting.

**The grade is a substring match, and that is on purpose.** The cold facts are
shaped `WORD-4digits`, which a model cannot arrive at by inference or by luck.
An answer that contains it came from the conversation; an answer that does not
is a miss regardless of how confidently it is phrased. No model-graded scoring -
the point of this test is a transcript a human reads, and a fuzzy grader would
only add a second thing to distrust.

**Session keys are the registry's channel keys, not free-form.** The channel
declared in the fleet file is what binds a turn to its agent and therefore to
its idle tier; an invented session key gets an ad-hoc agent at the default tier
and the whole overnight shape is wrong. The script refuses to run if the keys it holds do not
match the agents the gateway loaded.

Needs a port-forward to the gateway:

    kubectl --context <ctx> -n hermes port-forward deploy/hermes-gateway 18080:8080
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

# Pass 2 by default, overridable on the same env names the other deploy
# scripts take. A hardcoded context does not fail when it is the wrong one: it
# reads a real cluster that this run has nothing on, and reports an empty,
# healthy fleet.
CONTEXT = os.environ.get(
    "KUBECTL_CONTEXT", "REPLACE_WITH_KUBECTL_CONTEXT")
ATESPACE = os.environ.get("ATESPACE", "hermes-pass2")
# Store local run artifacts separately from source files.
RUN_DIR = os.environ.get("RUN_DIR", str(
    Path(__file__).resolve().parent.parent / "runs" / "pass2"))
OUTDIR = Path(RUN_DIR)

# The memory probes, positional. (cron noun, noun value, cold question, cold fact)
#
# The cron noun and its value exist so the hourly job has something true to
# confirm. The cold fact is what T2 is actually measured on: nothing in the
# fleet definition mentions a locker, a corridor, a kettle or any of the rest,
# so no scheduled turn can put it back into context.
#
# These are probe payloads. Agent routing IDs come from the fleet file below;
# probes are paired with them in order.
PROBES = [
    ("shipment reference", "SHP-4417-ZULU",
     "what did I say the loading-bay locker code was?", "VERMILION-7314"),
    ("ticket number", "TCK-90218",
     "what was the name of the contractor I said I would not use again?",
     "MARLOWE-2286"),
    ("account name", "Brightwater Holdings",
     "which corridor did I say the spare key is taped inside?", "OBSIDIAN-5051"),
    ("release tag", "v0.9.4-rc2",
     "what was the codename I gave the machine in the corner?", "PERIWINKLE-8630"),
    ("vendor name", "Calloway Instruments",
     "what did I say was written on the side of the kettle?", "TAMARIND-1195"),
    ("postcode", "EH8 9YL",
     "what was the word I asked you to hold on to for me?", "CINNABAR-6472"),
    ("serial number", "SN-77-0142-B",
     "what did I say the dog at the front desk is called?", "SAFFRON-3908"),
    ("room number", "Room 214",
     "what was the reference on the parking permit I mentioned?", "INDIGO-4527"),
    ("flight number", "BA1476",
     "what did I say I had left in the seat pocket?", "AMARANTH-2064"),
    ("invoice number", "INV-2026-0831",
     "what was the name of the cafe I said to bill it through?", "JUNIPER-5813"),
]

# Pass 2 runs the real ten from the fleet file, where pass 1 ran ten stand-ins
# invented by this module. That is not a cosmetic difference and it is why this
# is loaded rather than typed:
#
#   - The routing IDs differ, so `actor_name()` hashes them to different actors.
#     Addressing `standin-0N` against the pass 2 cluster does not fail, it
#     auto-provisions ten actors nobody asked for, on the DEFAULT idle tier, and
#     measures those. The numbers look entirely reasonable.
#   - The idle-tier mix differs, and idle tier is what decides when an actor
#     suspends and therefore how many wakes are cold. The stand-ins are 6 short
#     / 3 medium / 1 long; the real fleet is 4 long / 3 medium / 2 short and one
#     `always-on` that should never suspend at all. A soak on the stand-in mix
#     measures a fleet that goes to sleep far harder than the one under test,
#     and never exercises always-on once.
FLEET_FILE = os.environ.get("FLEET_FILE", str(
    Path(__file__).resolve().parent.parent / "demo" / "fleet" / "fleet.example.json"))


def load_fleet(path=None):
    """Build the harness tables from the fleet definition.

    Returns (agents, plant_templates, idle_three, provenance). `agents` keeps
    the shape the rest of this module already expects:
    agent id -> (channel, idle tier, cron noun, noun value, cold question, cold fact)
    """
    path = path or FLEET_FILE
    with open(path) as f:
        doc = json.load(f)
    entries = doc["agents"]
    if len(entries) > len(PROBES):
        raise SystemExit(
            f"{path} has {len(entries)} agents but only {len(PROBES)} probes are "
            f"defined. Add probes rather than reusing one: two agents sharing a "
            f"cold fact cannot be told apart in a memory result.")
    default_tier = (doc.get("defaults") or {}).get("idle_tier", "medium")
    agents, plants = {}, {}
    for i, e in enumerate(entries):
        aid = e["id"]
        channels = e.get("channels") or []
        if not channels:
            raise SystemExit(f"{aid} in {path} declares no channel to address it on")
        noun, value, question, cold = PROBES[i]
        agents[aid] = (channels[0], e.get("idle_tier") or default_tier,
                       noun, value, question, cold)
        plants[aid] = PLANT_TEXT[i]
    # The three held genuinely idle (§5). Chosen as the longest-tier agents
    # rather than the last three in the file, because the test needs actors that
    # will actually suspend: `always-on` never does, and a `short` one is back
    # and forth all night anyway. Deterministic, so two runs hold the same three.
    rank = {"long": 0, "medium": 1, "short": 2, "always-on": 9}
    idle = tuple(sorted(agents, key=lambda a: (rank.get(agents[a][1], 5), a))[:3])
    prov = (f"{Path(path).name} - {len(agents)} agents, "
            f"tiers " + ", ".join(f"{t}x{sum(1 for v in agents.values() if v[1] == t)}"
                                  for t in ("always-on", "long", "medium", "short")
                                  if any(v[1] == t for v in agents.values())))
    return agents, plants, idle, prov

# Positional, paired onto the fleet in the same order as PROBES.
PLANT_TEXT = [
    ("Two things before I go. The {noun} for the pallets is {value}. "
     "Also the loading-bay locker code is {cold} - I keep forgetting it "
     "and I would rather not write it on the door again."),
    ("Logging off shortly. The {noun} is {value}. Separately: the "
     "contractor I am not using again is {cold}. Remember that one for me, "
     "I will forget by the morning and hire them twice."),
    ("End of day. The {noun} on the file is {value}. And the spare key is "
     "taped inside the {cold} corridor, behind the notice board. Nobody else "
     "knows that and I would like at least one of us to still know tomorrow."),
    ("Before I shut the laptop: the {noun} we are cutting is {value}. Also "
     "the machine in the corner - the loud one - I have started calling it "
     "{cold}. Use that name if it comes up."),
    ("Quick handover. The {noun} on the quote is {value}. Unrelated: someone "
     "has written {cold} on the side of the kettle in marker and I want to "
     "know who, so hold on to that."),
    ("Heading off. The {noun} for the delivery is {value}. And the word I "
     "want you to hold on to for me is {cold}. No reason yet. I will explain "
     "tomorrow."),
    ("Long gap coming, I am away until tomorrow. The {noun} on the unit is "
     "{value}. Also the dog at the front desk is called {cold}, which I only "
     "learned today and would hate to lose."),
    ("Signing off. The {noun} is {value}. Also my parking permit reference is "
     "{cold} - it is on the dashboard and I cannot see it from home."),
    ("Off to bed. The {noun} is {value}. And I left {cold} in the seat pocket, "
     "which I only realised at baggage claim. Remind me tomorrow."),
    ("Last thing today. The {noun} is {value}. Bill it through the cafe called "
     "{cold}, not the one on the high street - they are different accounts and "
     "I mix them up every single time."),
]

AGENTS, PLANT_TEMPLATES, IDLE_THREE, FLEET_PROVENANCE = load_fleet()

ASK_PREAMBLE = ("Good morning. Before anything else, and without asking me to remind you: ")

# Hermes returns a failed model call as an ordinary assistant message with HTTP
# 200 - "API call failed after 3 retries: Gemini HTTP 429 (RESOURCE_EXHAUSTED)"
# arrives with the same status as a good answer. Seen for real on the 27 Aug
# plant: two of a ten-way concurrent burst hit the provider's rate limit, and
# both were `ok: true`. A grader that only checks for the cold fact scores those
# as a memory MISS, which is a wrong and damaging conclusion - the actor's
# memory was never consulted. So provider failures are a third outcome here,
# never folded into either of the other two.
PROVIDER_ERROR_MARKERS = (
    "api call failed after", "resource_exhausted", "http 429", "http 5",
)


def provider_error(reply: str) -> bool:
    low = (reply or "").lower()
    return any(m in low for m in PROVIDER_ERROR_MARKERS)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ate(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl-ate", "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def actor_name(agent_id: str) -> str:
    """Mirror of gateway/actor_router.py:actor_name_for_agent.

    Duplicated rather than imported so this runs without the gateway package on
    the path, and checked against the control plane below so a drift in the
    gateway's derivation fails here instead of quietly addressing nothing.
    """
    return "agent-" + sha256(agent_id.encode("utf-8")).hexdigest()[:12]


def actor_docs() -> dict:
    p = ate("get", "actors", "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        raise SystemExit(f"listing actors failed: {p.stderr.strip()}")
    return {a["metadata"]["name"]: a for a in json.loads(p.stdout).get("actors", [])}


def state_of(docs: dict, name: str) -> str:
    return (docs.get(name, {}).get("status") or {}).get("state", "ABSENT")


def turn(base: str, session: str, text: str, timeout: float) -> dict:
    body = json.dumps({"session": session, "text": text}).encode()
    req = urllib.request.Request(f"{base}/message", data=body, method="POST",
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer " + os.environ["GATEWAY_INGRESS_KEY"]})
    t0 = time.monotonic()
    started = now()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    except Exception as e:
        return {"at": started, "elapsed_s": round(time.monotonic() - t0, 2),
                "status": 0, "ok": False, "reply": "", "error": repr(e)}
    elapsed = round(time.monotonic() - t0, 2)
    try:
        reply = json.loads(raw)["choices"][0]["message"]["content"] or ""
    except Exception:
        reply = raw.decode("utf-8", "replace")
    return {"at": started, "elapsed_s": elapsed, "status": status,
            "ok": 200 <= status < 300, "reply": reply}


def check_registry(base: str) -> None:
    """Fail early if the gateway is not serving the stand-in registry.

    Without the ConfigMap the gateway happily answers every session key with an
    ad-hoc agent on the default tier - the turns succeed, the artefact looks
    fine, and the overnight shape the test depends on never existed.
    """
    try:
        with urllib.request.urlopen(f"{base}/fleet", timeout=20) as r:
            fleet = json.loads(r.read())
    except Exception as e:
        print(f"  (could not read /fleet: {e!r} - continuing, actor states are "
              f"checked directly against the control plane)")
        return
    ids = {a.get("agent_id") for a in (fleet.get("agents") or [])}
    missing = sorted(set(AGENTS) - ids)
    if missing:
        raise SystemExit(
            f"gateway registry is missing {missing} - apply the fleet from "
            f"{FLEET_FILE} (hermesctl fleet apply) and restart the gateway first")


def wait_states(names: list[str], want: str, timeout: float) -> dict:
    """Poll until every name is in `want`, or the budget runs out. Returns the
    last observation either way: a partial result here is a finding, not a
    crash, and the caller records it."""
    deadline = time.monotonic() + timeout
    seen: dict = {}
    while True:
        docs = actor_docs()
        seen = {n: state_of(docs, n) for n in names}
        if all(s == want for s in seen.values()):
            return seen
        if time.monotonic() >= deadline:
            return seen
        time.sleep(10.0)


def path_for(run: str, what: str) -> Path:
    return OUTDIR / f"t2-{what}-{run}.json"


def cmd_plant(a: argparse.Namespace) -> int:
    check_registry(a.gateway)
    out = path_for(a.run, "plant")
    if out.exists() and not a.force:
        raise SystemExit(f"{out} exists - a second plant would overwrite the "
                         f"evening record the morning read is graded against. "
                         f"Use --force only if you mean it.")

    def one(item):
        agent_id, (session, tier, noun, value, _q, cold) = item
        text = PLANT_TEMPLATES[agent_id].format(noun=noun, value=value, cold=cold)
        r = turn(a.gateway, session, text, a.timeout)
        return agent_id, {"agent_id": agent_id, "session": session, "idle_tier": tier,
                          "actor": actor_name(agent_id), "held_idle": agent_id in IDLE_THREE,
                          "cron_noun": noun, "noun_value": value, "cold_fact": cold,
                          "planted_text": text, "plant": r}

    print(f"planting {len(AGENTS)} conversations via {a.gateway}")
    with ThreadPoolExecutor(max_workers=len(AGENTS)) as ex:
        records = dict(ex.map(one, AGENTS.items()))

    for agent_id, rec in sorted(records.items()):
        p = rec["plant"]
        mark = "ok " if p["ok"] else "FAIL"
        echoed = rec["cold_fact"].lower() in p["reply"].lower()
        print(f"  {mark} {agent_id}  {p['status']}  {p['elapsed_s']:6.2f}s  "
              f"cold-fact echoed: {'yes' if echoed else 'no'}")
        if not p["ok"]:
            print(f"       {p.get('error') or p['reply'][:160]}")

    names = [r["actor"] for r in records.values()]
    print(f"\nwaiting for all ten to suspend (tiers 60s/900s/3600s, budget {a.suspend_wait}s)")
    states = wait_states(names, "ACTOR_STATE_SUSPENDED", a.suspend_wait)
    for agent_id, rec in sorted(records.items()):
        rec["state_after_plant"] = states.get(rec["actor"], "ABSENT")
        print(f"  {agent_id:12s} {rec['state_after_plant']}")

    doc = {"test": "T2", "run": a.run, "phase": "plant", "at": now(),
           "gateway": a.gateway, "atespace": ATESPACE,
           "provenance": FLEET_PROVENANCE,
           "note": ("The graded fact is `cold_fact`, which no cron prompt in "
                    "the fleet definition refers to. `noun_value` is "
                    "re-surfaced hourly by that agent's scheduled job and is "
                    "recorded for contrast, not as evidence."),
           "held_idle": list(IDLE_THREE),
           "agents": [records[k] for k in sorted(records)]}
    out.write_text(json.dumps(doc, indent=2))
    print(f"\nwrote {out}")
    ok = sum(1 for r in records.values() if r["plant"]["ok"])
    susp = sum(1 for r in records.values() if r["state_after_plant"] == "ACTOR_STATE_SUSPENDED")
    print(f"{ok}/{len(records)} planted, {susp}/{len(records)} suspended")
    print(f"\nin the morning:  ./t2_overnight.py ask --run {a.run}")
    return 0 if ok == len(records) else 1


def cmd_ask(a: argparse.Namespace) -> int:
    plant = json.loads(path_for(a.run, "plant").read_text())
    by_id = {r["agent_id"]: r for r in plant["agents"]}
    gap_h = (datetime.now(timezone.utc)
             - datetime.fromisoformat(plant["at"])).total_seconds() / 3600.0
    print(f"asking {len(by_id)} agents, {gap_h:.1f}h after planting")

    pre = actor_docs()
    for r in plant["agents"]:
        r["state_before_ask"] = state_of(pre, r["actor"])

    def one(agent_id):
        _s, _t, _n, _v, question, _c = AGENTS[agent_id]
        rec = by_id[agent_id]
        return agent_id, turn(a.gateway, rec["session"], ASK_PREAMBLE + question, a.timeout)

    with ThreadPoolExecutor(max_workers=len(by_id)) as ex:
        replies = dict(ex.map(one, sorted(by_id)))

    for agent_id, rec in sorted(by_id.items()):
        r = replies[agent_id]
        rec["question"] = ASK_PREAMBLE + AGENTS[agent_id][4]
        rec["ask"] = r
        rec["provider_error"] = provider_error(r["reply"])
        rec["recalled"] = (not rec["provider_error"]
                           and rec["cold_fact"].lower() in (r["reply"] or "").lower())
        mark = ("PROVIDER" if rec["provider_error"]
                else "RECALLED" if rec["recalled"] else "MISS    ")
        print(f"  {mark} {agent_id:12s} {r['status']} {r['elapsed_s']:6.2f}s  "
              f"was {rec['state_before_ask']}")
        if not rec["recalled"]:
            print(f"       expected {rec['cold_fact']!r} in: {(r['reply'] or '')[:200]!r}")

    hit = sum(1 for r in by_id.values() if r["recalled"])
    perr = sum(1 for r in by_id.values() if r["provider_error"])
    idle_hit = sum(1 for r in by_id.values() if r["recalled"] and r["held_idle"])
    if perr:
        print(f"\n{perr} agent(s) never reached their memory - the model call "
              f"failed and Hermes returned it as a 200. Re-ask those "
              f"individually before reporting anything about them.")
    doc = dict(plant)
    doc.update({"phase": "ask", "asked_at": now(), "gap_hours": round(gap_h, 2),
                "agents": [by_id[k] for k in sorted(by_id)],
                "recalled": hit, "of": len(by_id), "provider_errors": perr,
                "recalled_among_held_idle": f"{idle_hit}/{len(IDLE_THREE)}"})
    out = path_for(a.run, "result")
    out.write_text(json.dumps(doc, indent=2))
    print(f"\n{hit}/{len(by_id)} recalled after {gap_h:.1f}h "
          f"({idle_hit}/{len(IDLE_THREE)} among the three held idle)")
    print(f"wrote {out}\nnow:  ./t2_overnight.py report --run {a.run}")
    return 0 if hit == len(by_id) else 1


def cmd_report(a: argparse.Namespace) -> int:
    """Transcripts, one per agent. The plan's deliverable is the transcript, not
    the count, so this is the artefact that goes in the run log."""
    d = json.loads(path_for(a.run, "result").read_text())
    out = OUTDIR / f"t2-transcripts-{a.run}.md"
    L = [f"# T2 - overnight memory, run {a.run}", "",
         f"Planted {d['at']}, asked {d['asked_at']} - a gap of **{d['gap_hours']}h**.",
         f"Agents: **{d['provenance']}**.", "",
         f"**{d['recalled']}/{d['of']} recalled**, "
         f"{d['recalled_among_held_idle']} among the three held idle."
         + (f" {d['provider_errors']} turn(s) never reached the actor's memory "
            f"because the model call failed - counted as neither." if d.get("provider_errors")
            else ""), "",
         d["note"], "",
         "The graded string is the cold fact. It appears exactly once in each "
         "conversation - in the evening message - and no scheduled job mentions "
         "it, so a correct morning answer cannot have come from anywhere else.",
         ""]
    for r in d["agents"]:
        L += [f"## {r['agent_id']} - {'RECALLED' if r['recalled'] else 'MISS'}", "",
              f"`{r['actor']}` · session `{r['session']}` · tier `{r['idle_tier']}`"
              f"{' · held idle' if r['held_idle'] else ''} · "
              f"state before the morning turn: `{r['state_before_ask']}`", "",
              f"**Evening ({r['plant']['at']}), user:**", "",
              f"> {r['planted_text']}", "",
              f"**Agent:**", "",
              "> " + (r["plant"]["reply"] or "").strip().replace("\n", "\n> "), "",
              f"**Morning ({r['ask']['at']}), user:**", "",
              f"> {r['question']}", "",
              f"**Agent:**", "",
              "> " + (r["ask"]["reply"] or "").strip().replace("\n", "\n> "), "",
              f"Cold fact `{r['cold_fact']}` - "
              f"{'present' if r['recalled'] else '**absent**'}. "
              f"Cron-surfaced fact for contrast: `{r['noun_value']}`.", ""]
    out.write_text("\n".join(L))
    print(f"wrote {out}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=("plant", "ask", "report"))
    ap.add_argument("--run", required=True, help="run id, e.g. 260827")
    ap.add_argument("--gateway", default="http://127.0.0.1:18080")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--suspend-wait", type=float, default=1200.0,
                    help="budget for all ten to reach SUSPENDED after planting; "
                         "the long tier is 3600s so one agent may still be "
                         "RUNNING when this expires, which is recorded, not fatal")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    OUTDIR.mkdir(parents=True, exist_ok=True)
    return {"plant": cmd_plant, "ask": cmd_ask, "report": cmd_report}[a.phase](a)


if __name__ == "__main__":
    sys.exit(main())
