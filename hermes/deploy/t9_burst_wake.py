#!/usr/bin/env python3
"""T9 - burst wake. All ten agents woken simultaneously from zero.

The criterion in ACCEPTANCE-PLAN-2026-08-24.md is "all ten respond correctly;
distribution captured separately from single-wake numbers", and it requires the
node count, because a burst distribution is meaningless without knowing how many
workers it landed on.

Three things this driver is careful about, each of which has already cost a
result once in this pass.

**Simultaneity is enforced, not hoped for.** A ThreadPoolExecutor started ten
ways does not send ten requests at the same instant; the pool spins threads up
in sequence and the last can leave hundreds of milliseconds after the first. A
threading.Barrier holds all ten immediately before the send, so the spread at
the wire is the thing being measured rather than the thread pool's warm-up.

**A provider error is not a platform failure.** A ten-way burst against a
per-minute quota is the single most likely place in this pass to draw
RESOURCE_EXHAUSTED, and Hermes returns a failed model call as HTTP 200 with the
provider's text in the body. `t1_soak.py` explicitly leaves this case to T9. The
classifier is imported from t2_overnight rather than reimplemented, so all three
runs agree on what a provider error is.

**Only an agent that was SUSPENDED at send evidences a burst wake.** State is
read immediately before the barrier and recorded per agent. An agent found
RUNNING is reported, not counted, exactly as the soak does.

It does not restart the gateway (T10 does that, last) and it does not recover
anything (actor_watchdog.py owns recovery).

    ./t9_burst_wake.py run --run 260827
    ./t9_burst_wake.py report --run 260827
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from t2_overnight import (
    AGENTS, ATESPACE, CONTEXT, FLEET_PROVENANCE, OUTDIR,
    actor_docs, actor_name, ate, check_registry, now, provider_error,
    state_of, turn, wait_states,
)
from t1_soak import preflight

# The question is what a simultaneous wake costs, so the request timeout has to
# sit above the gateway's 300s route ceiling. Below it, a censored turn returns
# as a client-side abort and is indistinguishable from a platform hang; above
# it, the 504 arrives as data and is recorded as censoring.
DEFAULT_TIMEOUT = 360.0
ROUTE_CEILING_S = 300

BURST_PROMPT = ("Quick one - reply with a short sentence confirming you are "
                "awake, and include today's shipment reference if you have it.")


def path_for(run: str, what: str, ext: str = "json") -> Path:
    return OUTDIR / f"t9-{what}-{run}.{ext}"


def cluster_shape() -> dict:
    """Node and worker counts. Recorded, never inferred later - the burst
    distribution cannot be read against a different cluster shape, and the
    shape changes every time a worker pool is replaced."""
    def kubectl(*args: str) -> str:
        p = subprocess.run(["kubectl", "--context", CONTEXT, *args],
                           capture_output=True, text=True, timeout=60)
        return (p.stdout or "").strip() if p.returncode == 0 else ""

    nodes = [n for n in kubectl("get", "nodes",
                                "-o", "jsonpath={.items[*].metadata.name}").split() if n]
    # `atelet` is the per-node worker daemon (a DaemonSet), so this is normally
    # equal to the node count. Recorded separately anyway: a burst that lands on
    # fewer atelets than nodes is exactly the cold-node case, and the two
    # numbers diverging is the finding.
    workers = [w for w in kubectl("-n", "ate-system", "get", "pods",
                                  "-l", "app=atelet",
                                  "-o", "jsonpath={.items[*].metadata.name}").split() if w]
    # The WorkerPool replica count, which is the number that actually gates a
    # burst: a worker holds one actor while it is awake, so a pool smaller than
    # the fleet turns the last wakes into a queue behind a released worker. The
    # atelet count above does not show this - it is per node, and stays at 3
    # whether the pool is 9 or 12. Recorded because a burst distribution read
    # without it cannot be told apart from a pool-ceiling measurement.
    pool = kubectl("get", "workerpools", "-A", "-o", "jsonpath="
                   "{range .items[*]}{.metadata.namespace}/{.metadata.name}="
                   "{.status.readyReplicas}/{.spec.replicas} {end}").split()
    return {"nodes": len(nodes), "node_names": nodes,
            "workers": len(workers), "worker_names": workers,
            "worker_pools": pool}


def quiesce(actors: dict[str, str], budget: float) -> dict[str, str]:
    """Get all ten asleep by control-plane suspend.

    Not by idle timer: the long tier is 3600s and the medium 900s, so waiting
    them out would take the run past an hour for no gain. The suspend mode is
    recorded - a burst wake from a forced suspend and one from an expired idle
    timer are the same restore, but saying so is the reader's call, not ours.
    """
    docs = actor_docs()
    for agent_id, actor in sorted(actors.items()):
        st = state_of(docs, actor)
        if st == "ACTOR_STATE_SUSPENDED":
            continue
        p = ate("suspend", "actor", actor, "-a", ATESPACE)
        if p.returncode != 0:
            print(f"  {agent_id:12s} suspend failed rc={p.returncode} "
                  f"{p.stderr.strip()[:160]}")
    return wait_states(sorted(actors.values()), "ACTOR_STATE_SUSPENDED", budget)


def cmd_run(a: argparse.Namespace) -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    out = path_for(a.run, "burst")
    if out.exists() and not a.force:
        raise SystemExit(f"{out} exists - rerunning would overwrite the burst "
                         f"it records. Use --force if you mean it.")

    print(f"T9 burst wake, run {a.run} - preflight")
    preflight(a)

    shape = cluster_shape()
    print(f"  cluster: {shape['nodes']} nodes, {shape['workers']} atelet pods, "
          f"pools {' '.join(shape['worker_pools']) or 'none found'}")
    if shape["workers"] == 0:
        print("  WARNING: no atelet pods found by label - the burst still "
              "runs, but the distribution has no denominator")
    ready = sum(int(p.split("=")[1].split("/")[0])
                for p in shape["worker_pools"]
                if p.split("=")[1].split("/")[0].isdigit())
    if ready and ready < len(AGENTS):
        print(f"  WARNING: {ready} ready workers for {len(AGENTS)} agents - the "
              f"last wakes queue behind a released worker, so the tail of this "
              f"distribution is the pool ceiling and not a restore. Scale the "
              f"pool to at least {len(AGENTS)} first.")
        print("  and quiesce the fleet before scaling it back down afterwards. "
              "Removing a worker pod destroys any actor awake on it: the actor "
              "goes to ACTOR_STATE_CRASHED, which is terminal, and its clean "
              "snapshot cannot be used to bring it back. A SUSPENDED actor "
              "survives the same removal untouched. O-41.")

    actors = {aid: actor_name(aid) for aid in AGENTS}

    print(f"\nquiescing all {len(actors)} (control-plane suspend, "
          f"budget {a.suspend_wait}s)")
    states = quiesce(actors, a.suspend_wait)
    for agent_id, actor in sorted(actors.items()):
        print(f"  {agent_id:12s} {states.get(actor, 'ABSENT')}")

    asleep = sum(1 for actor in actors.values()
                 if states.get(actor) == "ACTOR_STATE_SUSPENDED")
    if asleep < len(actors) and not a.allow_partial:
        raise SystemExit(
            f"\nonly {asleep}/{len(actors)} suspended - a partial burst measures "
            f"something other than what T9 asks for. Re-run, or --allow-partial "
            f"to record it as a partial burst.")

    # State immediately before the send. wait_states' last observation can be up
    # to a poll old, and "was it asleep when the message arrived" is the whole
    # basis on which a wake counts.
    docs = actor_docs()
    before = {aid: state_of(docs, actor) for aid, actor in actors.items()}

    barrier = threading.Barrier(len(AGENTS))
    t_zero = time.monotonic()

    def one(item):
        agent_id, spec = item
        session = spec[0]
        barrier.wait()
        left = time.monotonic()
        r = turn(a.gateway, session, BURST_PROMPT, a.timeout)
        return agent_id, {
            "agent_id": agent_id,
            "session": session,
            "actor": actors[agent_id],
            "idle_tier": spec[1],
            "state_before": before[agent_id],
            "wake": "cold" if before[agent_id] == "ACTOR_STATE_SUSPENDED" else "warm",
            "left_at_offset_s": round(left - t_zero, 4),
            "status": r["status"],
            "ok": r["ok"],
            "first_response_s": r["elapsed_s"],
            "provider_error": provider_error(r.get("reply", "")),
            "censored": r["status"] == 504 and r["elapsed_s"] >= ROUTE_CEILING_S,
            "reply_head": (r.get("reply") or "")[:200],
            "error": r.get("error"),
        }

    print(f"\nfiring {len(AGENTS)} simultaneously via {a.gateway} "
          f"(timeout {a.timeout:.0f}s, route ceiling {ROUTE_CEILING_S}s)")
    with ThreadPoolExecutor(max_workers=len(AGENTS)) as ex:
        records = dict(ex.map(one, AGENTS.items()))

    spread = max(r["left_at_offset_s"] for r in records.values()) - \
        min(r["left_at_offset_s"] for r in records.values())

    for agent_id, r in sorted(records.items()):
        if r["provider_error"]:
            mark = "PROV"
        elif r["censored"]:
            mark = "CENS"
        elif r["ok"]:
            mark = "ok  "
        else:
            mark = "FAIL"
        print(f"  {mark} {agent_id:12s} {r['status']:3d}  "
              f"{r['first_response_s']:7.2f}s  {r['wake']:4s}  "
              f"{(r['reply_head'] or r.get('error') or '')[:70]}")

    doc = {
        "test": "T9", "run": a.run, "at": now(),
        "gateway": a.gateway, "atespace": ATESPACE, "context": CONTEXT,
        "provenance": FLEET_PROVENANCE,
        "prompt": BURST_PROMPT,
        "suspend_mode": "forced (control-plane suspend, not idle timer)",
        "route_ceiling_s": ROUTE_CEILING_S,
        "timeout_s": a.timeout,
        "send_spread_s": round(spread, 4),
        "cluster": shape,
        "records": records,
        "note": ("Burst numbers are not comparable with T15's single-wake "
                 "distribution and must never be pooled with them. A turn at "
                 f"or above {ROUTE_CEILING_S}s returning 504 is right-censored "
                 "by the gateway route timeout, not a completed measurement."),
    }
    out.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"\n  send spread across the ten: {spread * 1000:.0f} ms")
    print(f"wrote {out}\nnow:  ./t9_burst_wake.py report --run {a.run}")
    return 0


def cmd_report(a: argparse.Namespace) -> int:
    doc = json.loads(path_for(a.run, "burst").read_text())
    recs = doc["records"]
    rows = sorted(recs.values(), key=lambda r: r["agent_id"])

    cold = [r for r in rows if r["wake"] == "cold"]
    prov = [r for r in rows if r["provider_error"]]
    cens = [r for r in rows if r["censored"]]
    # Graded: a cold wake that returned a real answer. Provider errors and
    # censored turns are excluded from the distribution and reported by count,
    # the same rule the soak applies.
    graded = [r for r in cold if r["ok"] and not r["provider_error"]]
    lat = sorted(r["first_response_s"] for r in graded)

    def pct(p: float) -> float:
        if not lat:
            return float("nan")
        return lat[min(len(lat) - 1, int(round((p / 100) * (len(lat) - 1))))]

    L = [f"# T9 - burst wake, run {doc['run']}", "",
         f"Fired {doc['at']}. Ten stand-in agents - **{doc['provenance']}**. "
         f"All ten released by a barrier; **send spread "
         f"{doc['send_spread_s'] * 1000:.0f} ms** across the ten.", "",
         f"Cluster at the time of the burst: **{doc['cluster']['nodes']} nodes, "
         f"{doc['cluster']['workers']} atelet pods**. Suspend mode: "
         f"{doc['suspend_mode']}.", "",
         "| Agent | Tier | State before | Status | First response s | Note |",
         "|---|---|---|---|---|---|"]
    for r in rows:
        note = ("provider error" if r["provider_error"] else
                "censored at route ceiling" if r["censored"] else
                "not asleep at send - not counted" if r["wake"] != "cold" else
                "" if r["ok"] else "failed")
        L.append(f"| {r['agent_id']} | {r['idle_tier']} | "
                 f"{r['state_before'].replace('ACTOR_STATE_', '')} | {r['status']} | "
                 f"{r['first_response_s']:.2f} | {note} |")

    L += ["",
          f"**T9** - {len(graded)}/{len(rows)} graded burst wakes "
          f"({len(cold)} were asleep at send). Provider errors: {len(prov)}. "
          f"Censored at the {doc['route_ceiling_s']}s route ceiling: {len(cens)}. "
          f"Outright failures: "
          f"{sum(1 for r in rows if not r['ok'] and not r['censored'])}."]
    if lat:
        L.append(f"Burst first-response over {len(lat)} graded wakes - "
                 f"min {lat[0]:.2f}s, p50 {statistics.median(lat):.2f}s, "
                 f"p90 {pct(90):.2f}s, max {lat[-1]:.2f}s.")
    L += ["",
          "**Not comparable with T15.** Those are single-wake numbers taken one "
          "at a time; these are ten restores contending for the same workers. "
          "The two distributions are reported side by side and never pooled.",
          ""]
    if cens:
        L.append(f"**{len(cens)} turn(s) right-censored.** The gateway route "
                 f"timeout is {doc['route_ceiling_s']}s, so these are a lower "
                 f"bound on the wake, not a measured wake, and the max above is "
                 f"a max over completed turns only.")
        L.append("")

    md = "\n".join(L)
    print(md)
    rp = path_for(a.run, "report", "md")
    rp.write_text(md + "\n")
    print(f"wrote {rp}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="quiesce all ten, then wake them together")
    r.add_argument("--run", required=True)
    r.add_argument("--gateway", default="http://127.0.0.1:18080")
    r.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    r.add_argument("--suspend-wait", type=float, default=600.0)
    r.add_argument("--allow-partial", action="store_true",
                   help="record a burst even if not all ten were suspended")
    r.add_argument("--no-watchdog", action="store_true")
    r.add_argument("--force", action="store_true")
    r.set_defaults(func=cmd_run)

    p = sub.add_parser("report", help="read the burst back")
    p.add_argument("--run", required=True)
    p.set_defaults(func=cmd_report)

    a = ap.parse_args()
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
