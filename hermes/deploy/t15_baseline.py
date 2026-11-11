#!/usr/bin/env python3
"""T15 - latency baseline, measured gateway-side.

Message-to-first-response across the lifecycle states an agent actually passes
through: created from nothing, answering while already awake, and woken from a
suspend. Plus a small concurrent burst, which is the shape T9 measures at fleet
scale.

**One clock, named.** Every `first_response_s` in the output is wall clock in
*this* process, from the moment the request body goes out to the moment the last
byte of the response comes back. It includes the gateway, the wake, the actor and
the model call, because that is what a user waits for.

Control-plane timings - restore duration, scheduler assignment, atenet parking -
are a *different quantity* and are recorded separately, as histogram deltas
scraped from the collector around each phase. They explain the gateway number;
they are never added to it or averaged with it. Mixing the two is how a report
ends up quoting a resume latency that no user ever experienced.

**Why this has to be re-run on 4c1b37d.** Before 25 Aug the gateway read the old
flat actor document, so `wait_running()` never recognised a RUNNING actor and
burned its full 90s budget on every resume, and the 504 guard re-sent turns that
had already been taken. Any figure taken before that fix carries polling delay
and duplicate work. This script is the replacement baseline, not a second opinion.

Standard library only, deliberately: this has to run on an operator's laptop and
on the partner's side without a virtualenv, and a baseline nobody can reproduce
is not a baseline.

Usage:
    ./t15_baseline.py --out t15-results
    ./t15_baseline.py --warm 10 --resume 6 --burst 4 --cold 2
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict, field
from pathlib import Path

# Histograms worth reading around a phase. Names are the collector's, and the
# script tolerates any of them being absent - a missing series is reported as
# such rather than silently counted as zero, because "no restores happened" and
# "the metric was renamed" look identical in a summary table.
HISTOGRAMS = (
    "ate_actor_restore_duration_seconds",
    "ate_actor_checkpoint_duration_seconds",
    "ate_actor_lifecycle_operation_duration_seconds",
    "ate_scheduler_assignment_duration_seconds",
    "atenet_router_route_duration_seconds",
    "atenet_router_parking_wait_duration_seconds",
)

# Labels to aggregate over rather than group by. `instance` is the reporting
# atelet: five of them, and which one served a wake is not interesting here.
DROP_LABELS = frozenset({
    "instance", "job", "otel_scope_name", "otel_scope_version",
    "otel_scope_schema_url", "ate_template_name", "ate_template_namespace",
    "ate_workerpool_name", "ate_workerpool_namespace", "ate_snapshot_scope",
})

# Labels whose presence means the observation is of a FAILED operation. These
# sit in the same histogram family as the successes, so summing a family blind
# mixes them together - and since a cancelled resume is short, that pulls the
# mean *down*. There are `error_type="Canceled"` observations on this cluster
# right now, left by the cold-node wedge; they must never reach a report.
FAILURE_LABELS = ("ate_failure_reason", "error_type")

# The stage decomposition. Each restore emits one observation per stage plus one
# for `total`, so the headline number is phase="total" and nothing else.
#
# The stages are a genuine PARTITION of the total, not an indicative sample of
# it: summed, they come to within a few milliseconds of the `total` series
# (1.284s against a measured 1.290s on the 25 Aug baseline, and the same to
# three digits in the other three phases). So a stage percentage is a real share
# of the wall clock and can be reported as one. Checkpoint is very slightly
# lossy the same way - about 40ms of its ~1.02s is outside the named stages -
# so its shares are quoted as approximate.
TOTAL_PHASE = "total"


def parse_labels(text: str) -> dict:
    return dict(re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"', text))


def series_key(labels: dict) -> str:
    kept = {k: v for k, v in labels.items() if k not in DROP_LABELS}
    return ",".join(f"{k}={v}" for k, v in sorted(kept.items())) or "-"


def actor_name_for(session_key: str) -> str:
    """Mirror of gateway/actor_router.py - agent-<first 12 hex of sha256>.

    Duplicated rather than imported so this can run against a cluster without the
    gateway package on the path. The run aborts if the names it derives do not
    show up in the control plane, so a change to the gateway's derivation is
    caught before this script suspends an actor belonging to something else.
    """
    return "agent-" + hashlib.sha256(session_key.encode("utf-8")).hexdigest()[:12]


@dataclass
class Turn:
    phase: str
    session: str
    actor: str
    started_at: float
    first_response_s: float
    status: int
    ok: bool
    reply: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    error: str = ""


@dataclass
class Phase:
    name: str
    note: str
    turns: list = field(default_factory=list)
    metrics_delta: dict = field(default_factory=dict)


def kubectl_ate_bin() -> str:
    """The client binary, defaulting to the one hermesctl staged, not $PATH's.

    These scripts used to shell out to a bare `kubectl-ate`, which is whatever
    build happens to be on PATH. On this box that is a `main` build, and pass 2
    runs release-0.1. The two are not interchangeable: main's `get workers`
    printer reads an ASSIGNED ACTOR column that a rel01 server does not send and
    segfaults on the nil, so a script that only wanted to list the pool dies with
    a Go stack trace and no hint that the client is the wrong version. Other
    commands are worse, because they answer instead of crashing.

    `hermesctl` already builds a client from $SUBSTRATE_SRC into
    gateway/kubectl-ate and stamps which commit it came from, so prefer that.
    KUBECTL_ATE overrides for anyone pointing at a different install.
    """
    override = os.environ.get("KUBECTL_ATE")
    if override:
        return override
    staged = Path(__file__).resolve().parent.parent / "gateway" / "kubectl-ate"
    return str(staged) if os.access(staged, os.X_OK) else "kubectl-ate"


class Cluster:
    """The control-plane half: suspend, and read whether an actor is running."""

    def __init__(self, context: str, atespace: str, kubectl_ate: str = ""):
        self.context, self.atespace = context, atespace
        self.bin = kubectl_ate or kubectl_ate_bin()

    def _run(self, *args: str, timeout: int = 120) -> subprocess.CompletedProcess:
        return subprocess.run(
            [self.bin, "--context", self.context, *args],
            capture_output=True, text=True, timeout=timeout,
        )

    def actors(self) -> dict:
        """name -> state string, or None when the actor is not running.

        Accepts every document shape this control plane has produced: nested
        `status.state`, the legacy flat `status` string, and a suspended actor
        whose `status` is absent or empty. Anything not recognisably RUNNING is
        reported as not running rather than raising - but an unrecognised
        *non-empty* shape raises loudly. That silent third case is what cost a
        day on 25 Aug, and it should never be silent again.
        """
        p = self._run("get", "actor", "-a", self.atespace, "-o", "json")
        if p.returncode != 0:
            raise RuntimeError(f"kubectl-ate get actor failed: {p.stderr.strip()}")
        out = {}
        for a in (json.loads(p.stdout or "{}").get("actors") or []):
            name = a.get("metadata", {}).get("name") or a.get("name")
            st = a.get("status")
            if st in (None, "", {}):
                state = None
            elif isinstance(st, dict):
                state = st.get("state")
            elif isinstance(st, str):
                state = st if st.startswith("ACTOR_STATE_") else st.replace(
                    "STATUS_", "ACTOR_STATE_", 1)
            else:
                raise RuntimeError(f"unrecognised actor status shape: {st!r}")
            out[name] = state
        return out

    def suspend(self, name: str) -> bool:
        return self._run("suspend", "actor", name, "-a", self.atespace).returncode == 0

    def wait_suspended(self, name: str, timeout: float = 120.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.actors().get(name) != "ACTOR_STATE_RUNNING":
                return True
            time.sleep(2.0)
        return False


def scrape(url: str) -> dict:
    """Histogram _sum/_count keyed by (metric, label set), summed over atelets.

    Grouping by label set rather than by metric name is not a refinement, it is
    the difference between a true number and a false one. In this family a
    single restore emits seven observations - six stages and a `total` - and
    failed attempts carry `ate_failure_reason`/`error_type` but otherwise share
    the family. Adding a metric up blind therefore reports roughly 7× the
    restores that happened, at a mean that is a blend of stage durations and
    aborted attempts.
    """
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            body = r.read().decode("utf-8", "replace")
    except Exception as e:                       # collector down is not fatal
        return {"_error": str(e)}
    acc: dict = {}
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.match(r"([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([0-9.eE+-]+)$",
                     line)
        if not m:
            continue
        name, labels_raw, value = m.group(1), m.group(2) or "", m.group(3)
        for h in HISTOGRAMS:
            for suffix in ("_sum", "_count"):
                if name != h + suffix:
                    continue
                labels = parse_labels(labels_raw)
                key = (h, series_key(labels), suffix)
                try:
                    acc[key] = acc.get(key, 0.0) + float(value)
                except ValueError:
                    pass
    return acc


def _failed(label_key: str) -> bool:
    return any(f"{lbl}=" in label_key for lbl in FAILURE_LABELS)


def delta(before: dict, after: dict) -> dict:
    """What the control plane did during this phase, per metric.

    Successes and failures are reported separately and never averaged together.
    For the staged families the headline is the `total` stage; the per-stage
    breakdown is kept alongside for diagnosis, clearly not summable with it.

    A phase that performed no restores says so, rather than printing 0.000s -
    which reads like a suspiciously fast restore.
    """
    if "_error" in before or "_error" in after:
        return {"_error": before.get("_error") or after.get("_error")}
    out: dict = {}
    for h in HISTOGRAMS:
        keys = {k[1] for k in after if k[0] == h}
        if not keys:
            out[h] = {"note": "series absent from the collector"}
            continue
        totals, stages, failures = {}, {}, {}
        for lk in sorted(keys):
            c = (after.get((h, lk, "_count"), 0.0)
                 - before.get((h, lk, "_count"), 0.0))
            s = (after.get((h, lk, "_sum"), 0.0)
                 - before.get((h, lk, "_sum"), 0.0))
            if c <= 0:
                continue
            entry = {"count": int(c), "total_s": round(s, 3),
                     "mean_s": round(s / c, 3)}
            if _failed(lk):
                failures[lk] = entry
            elif f"ate_snapshot_phase={TOTAL_PHASE}" in lk or \
                    "ate_snapshot_phase=" not in lk:
                totals[lk] = entry
            else:
                stages[lk] = entry
        if not totals and not stages and not failures:
            out[h] = {"count": 0, "note": "no observations in this phase"}
            continue
        rec: dict = {"observed": totals}
        if stages:
            rec["stages"] = stages          # diagnostic; not additive with above
        if failures:
            rec["FAILED"] = failures        # excluded from every reported mean
        out[h] = rec
    return out


def one_turn(base: str, phase: str, session_key: str, prompt: str,
             timeout: float) -> Turn:
    actor = actor_name_for(session_key)
    body = json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request(
        f"{base}/v1/chat/completions", data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + os.environ["GATEWAY_INGRESS_KEY"],
                 "X-Hermes-Session-Key": session_key})
    started, t0 = time.time(), time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw, status = r.read(), r.status
    except urllib.error.HTTPError as e:          # a 4xx/5xx is a result, not a crash
        raw, status = e.read(), e.code
    except Exception as e:
        return Turn(phase, session_key, actor, started,
                    round(time.monotonic() - t0, 3), 0, False, error=repr(e))
    elapsed = time.monotonic() - t0
    reply, pt, ct = "", 0, 0
    try:
        d = json.loads(raw)
        reply = (d["choices"][0]["message"]["content"] or "")[:120]
        pt = d.get("usage", {}).get("prompt_tokens", 0)
        ct = d.get("usage", {}).get("completion_tokens", 0)
    except Exception:
        reply = raw[:120].decode("utf-8", "replace")
    return Turn(phase, session_key, actor, started, round(elapsed, 3), status,
                200 <= status < 300, reply, pt, ct)


def summarise(vals: list) -> dict:
    """p50/p90/p99 with the sample size, because a p99 of four samples is a max.

    Nearest-rank on the sorted sample, no interpolation: every number printed is
    a latency that was actually observed.
    """
    if not vals:
        return {"n": 0}
    s = sorted(vals)

    def pct(p):
        k = max(0, min(len(s) - 1, int(round(p / 100.0 * len(s) + 0.5)) - 1))
        return round(s[k], 3)

    return {"n": len(s), "min": round(s[0], 3), "p50": pct(50), "p90": pct(90),
            "p99": pct(99), "max": round(s[-1], 3),
            "mean": round(statistics.fmean(s), 3)}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gateway", default="http://127.0.0.1:18080")
    ap.add_argument("--metrics", default="http://127.0.0.1:18889/metrics")
    ap.add_argument("--context",
                    default="REPLACE_WITH_KUBECTL_CONTEXT",
                    help="kubectl context, pinned explicitly - several clusters "
                         "exist and kubectl silently flips between them")
    ap.add_argument("--atespace", default="hermes-pass2")
    ap.add_argument("--kubectl-ate", default=kubectl_ate_bin())
    ap.add_argument("--tag", default="t15", help="session-key prefix for this run")
    ap.add_argument("--cold", type=int, default=2)
    ap.add_argument("--warm", type=int, default=10)
    ap.add_argument("--resume", type=int, default=6)
    ap.add_argument("--burst", type=int, default=4)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--prompt", default="Reply with exactly: OK")
    ap.add_argument("--out", default="t15-results")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    cl = Cluster(a.context, a.atespace, a.kubectl_ate)

    # Fail before spending money on model calls, not after.
    try:
        cl.actors()
    except Exception as e:
        print(f"control plane unreachable: {e}", file=sys.stderr)
        return 1
    if "_error" in scrape(a.metrics):
        print(f"WARNING: no metrics at {a.metrics} - gateway-side numbers will "
              f"still be valid, control-plane context will be missing",
              file=sys.stderr)

    phases: list = []

    def run_phase(name: str, note: str, fn) -> None:
        print(f"\n=== {name}: {note}", flush=True)
        before = scrape(a.metrics)
        turns = fn()
        ph = Phase(name, note, turns, delta(before, scrape(a.metrics)))
        for t in turns:
            print(f"  {'ok ' if t.ok else 'ERR'} {t.first_response_s:7.2f}s  "
                  f"{t.actor}  {t.error or t.reply[:40]!r}", flush=True)
        phases.append(ph)

    # --- cold: an actor that does not exist yet -----------------------------
    # Distinct session keys per run (give --tag a timestamp) so this is genuinely
    # a create, not a resume of something a previous run left behind.
    def cold():
        return [one_turn(a.gateway, "cold", f"{a.tag}-cold-{i}", a.prompt,
                         a.timeout) for i in range(a.cold)]

    # --- warm: same actor, already awake, back to back ----------------------
    def warm():
        key = f"{a.tag}-warm"
        res = [one_turn(a.gateway, "warm-first", key, a.prompt, a.timeout)]
        res += [one_turn(a.gateway, "warm", key, a.prompt, a.timeout)
                for _ in range(a.warm)]
        return res

    # --- resume: suspend through the control plane, then ask ----------------
    def resume():
        key = f"{a.tag}-resume"
        name = actor_name_for(key)
        one_turn(a.gateway, "resume-seed", key, a.prompt, a.timeout)
        res = []
        for i in range(a.resume):
            if not cl.suspend(name):
                print(f"  .. suspend failed for {name}, skipping cycle {i}")
                continue
            if not cl.wait_suspended(name):
                print(f"  .. {name} did not suspend in time, skipping cycle {i}")
                continue
            res.append(one_turn(a.gateway, "resume", key, a.prompt, a.timeout))
        return res

    # --- burst: k suspended actors, all asked at once -----------------------
    def burst():
        keys = [f"{a.tag}-burst-{i}" for i in range(a.burst)]
        with ThreadPoolExecutor(max_workers=max(1, a.burst)) as ex:
            list(ex.map(lambda k: one_turn(a.gateway, "burst-seed", k, a.prompt,
                                           a.timeout), keys))
        for k in keys:
            cl.suspend(actor_name_for(k))
        for k in keys:
            cl.wait_suspended(actor_name_for(k))
        with ThreadPoolExecutor(max_workers=max(1, a.burst)) as ex:
            return list(ex.map(lambda k: one_turn(a.gateway, "burst", k, a.prompt,
                                                  a.timeout), keys))

    if a.cold:
        run_phase("cold", "actor created from nothing", cold)
    if a.warm:
        run_phase("warm", "same actor, already awake", warm)
    if a.resume:
        run_phase("resume", "woken from a control-plane suspend", resume)
    if a.burst:
        run_phase("burst", f"{a.burst} suspended actors asked at once", burst)

    # ---- output ------------------------------------------------------------
    raw = out / f"{a.tag}-raw.jsonl"
    with raw.open("w") as fh:
        for ph in phases:
            for t in ph.turns:
                fh.write(json.dumps(asdict(t)) + "\n")

    report = {"tag": a.tag, "atespace": a.atespace, "context": a.context,
              "gateway": a.gateway, "phases": [], "control_plane": []}
    for ph in phases:
        by: dict = {}
        for t in ph.turns:
            by.setdefault(t.phase, []).append(t)
        for sub, turns in by.items():
            report["phases"].append({
                "phase": sub, "note": ph.note,
                "first_response_s": summarise(
                    [t.first_response_s for t in turns if t.ok]),
                "failures": [asdict(t) for t in turns if not t.ok],
            })
        # One scrape window per phase, recorded once. Copying it onto each
        # sub-phase would read as though each had independently observed it.
        report["control_plane"].append({
            "measured_over": ph.name, "sub_phases": sorted(by),
            "caveat": "cluster-wide over the phase window, not attributable to "
                      "an individual turn; a different clock from "
                      "first_response_s and never to be added to it",
            "metrics": ph.metrics_delta,
        })
    (out / f"{a.tag}-summary.json").write_text(json.dumps(report, indent=2))

    print("\n" + "=" * 72)
    print("T15 - message-to-first-response, gateway-side wall clock (seconds)")
    print("=" * 72)
    print(f"{'phase':<14}{'n':>4}{'min':>9}{'p50':>9}{'p90':>9}{'p99':>9}{'max':>9}")
    for p in report["phases"]:
        s = p["first_response_s"]
        if not s.get("n"):
            print(f"{p['phase']:<14}{0:>4}   (no successful turns)")
            continue
        print(f"{p['phase']:<14}{s['n']:>4}{s['min']:>9.2f}{s['p50']:>9.2f}"
              f"{s['p90']:>9.2f}{s['p99']:>9.2f}{s['max']:>9.2f}")
    # Where the control plane spent the wall clock. The stages partition the
    # `total` series (see TOTAL_PHASE), so these percentages are real shares.
    for g in report["control_plane"]:
        for metric, rec in g["metrics"].items():
            if "note" in rec or not rec.get("stages"):
                continue
            for tot_lk, tot in rec.get("observed", {}).items():
                kind = next((p.split("=")[1] for p in tot_lk.split(",")
                             if p.startswith("ate_snapshot_kind=")), "-")
                mine = {lk: e for lk, e in rec["stages"].items()
                        if f"ate_snapshot_kind={kind}" in lk}
                if not mine:
                    continue
                short = metric.replace("_duration_seconds", "").replace("ate_", "")
                print(f"\n{short} / {kind} - {g['measured_over']} phase, "
                      f"n={tot['count']}, total {tot['mean_s']:.3f}s")
                for lk, e in sorted(mine.items(), key=lambda kv: -kv[1]["mean_s"]):
                    stage = next((p.split("=")[1] for p in lk.split(",")
                                  if p.startswith("ate_snapshot_phase=")), lk)
                    pct = 100.0 * e["mean_s"] / tot["mean_s"] if tot["mean_s"] else 0
                    print(f"    {stage:<18}{e['mean_s']:7.3f}s{pct:7.1f}%")
                acct = sum(e["mean_s"] for e in mine.values())
                print(f"    {'(accounted)':<18}{acct:7.3f}s"
                      f"{100.0 * acct / tot['mean_s'] if tot['mean_s'] else 0:7.1f}%")

    fails = sum(len(p["failures"]) for p in report["phases"])
    print(f"\nfailures: {fails}")
    print(f"raw:      {raw}")
    print(f"summary:  {out / (a.tag + '-summary.json')}")
    print("\nMessage-to-acknowledgement is not separately observable on this API:"
          "\n/v1/chat/completions is synchronous, so the only gateway-side event is"
          "\nthe completed response. The wake component is in `control_plane` in the"
          "\nsummary, as a histogram delta - a different clock, reported separately.")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
