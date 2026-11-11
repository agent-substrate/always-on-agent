#!/usr/bin/env python3
"""Measure the fixed cost of a restore/checkpoint on OUR hardware.

Companion to t15_baseline.py. Same cluster, same build, same nodes, same bucket,
same instrument - the only thing changed is the workload, which here is busybox
asleep instead of a live Hermes agent.

The point is a like-for-like comparison with upstream's benchmark figures, which
come from a near-empty actor on different hardware and a different build.
Subtracting one from the other to get "fixed cost" crosses several variables at
once. This run collapses that to one: the workload.

Reuses t15_baseline's scrape/delta so the numbers are produced by exactly the
same code that produced the Hermes ones - including the label-aware grouping and
the quarantine of failed operations, both of which are load-bearing.

Usage:
  python3 ctrl_floor_run.py --cycles 10
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import t15_baseline as t15  # noqa: E402

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
# A bare name. release-0.1 create actor has only --template-ref, which
# resolves in the actor's own atespace and rejects a slashed reference.
TEMPLATE = os.environ.get("CTRL_FLOOR_TEMPLATE", "ctrl-floor")
METRICS = "http://127.0.0.1:18889/metrics"


def ate(*args: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run([t15.kubectl_ate_bin(), "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def state(cl: "t15.Cluster", name: str):
    return cl.actors().get(name)


def wait_for(cl, name, want_running: bool, timeout: float = 180.0):
    """Wait until the actor is (or is not) RUNNING. Returns seconds waited."""
    t0 = time.monotonic()
    deadline = t0 + timeout
    while time.monotonic() < deadline:
        running = state(cl, name) == "ACTOR_STATE_RUNNING"
        if running == want_running:
            return time.monotonic() - t0
        time.sleep(1.0)
    raise TimeoutError(
        f"{name} did not reach running={want_running} within {timeout}s")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycles", type=int, default=10)
    ap.add_argument("--actor", default="ctrl-floor-1")
    ap.add_argument("--metrics", default=METRICS)
    ap.add_argument("--settle", type=float, default=90.0,
                    help="seconds to wait after the last cycle before the "
                         "closing scrape, so the tail is not truncated")
    ap.add_argument("--keep", action="store_true",
                    help="leave the actor in place afterwards")
    a = ap.parse_args()

    cl = t15.Cluster(CONTEXT, ATESPACE)

    # A restore by anything else during the window lands in the same histograms,
    # because the harness drops ate_template_* labels deliberately (grouping by
    # template would fragment every series). Isolation is procedural, not
    # label-based: check the fleet is quiet before starting, and again at the end.
    running_before = [n for n, s in cl.actors().items()
                      if s == "ACTOR_STATE_RUNNING" and n != a.actor]
    if running_before:
        print(f"REFUSING: {len(running_before)} actor(s) already RUNNING - their "
              f"activity would land in the same histograms: {running_before[:5]}")
        return 2
    print("fleet quiet, no actors RUNNING")

    existing = cl.actors()
    if a.actor not in existing:
        # `--template-ref <bare name>`, not `-t <namespace>/<name>`. The main
        # client still takes the latter and the two are not aliases: rel01's
        # client has no `-t` at all and exits on the unknown shorthand, which is
        # at least loud. This script runs the staged rel01 client, so it takes
        # the rel01 spelling.
        p = ate("create", "actor", a.actor, "-a", ATESPACE, "--template", TEMPLATE)
        if p.returncode != 0:
            print(f"create failed: {p.stderr.strip()}")
            return 1
        print(f"created {a.actor} from {TEMPLATE}")
    else:
        print(f"{a.actor} already exists (state={existing[a.actor]})")

    # The first restore is from `golden`; we want `latest`, which is what the
    # Hermes 1.29 s figure measures. So bring it up, snapshot it once, and only
    # then start counting.
    if state(cl, a.actor) != "ACTOR_STATE_RUNNING":
        ate("resume", "actor", a.actor, "-a", ATESPACE)
        wait_for(cl, a.actor, True)
    print("actor up (golden restore, not counted)")
    cl.suspend(a.actor)
    wait_for(cl, a.actor, False)
    print("primed: one checkpoint written, subsequent restores are `latest`")

    before = t15.scrape(a.metrics)
    if "_error" in before:
        print(f"metrics unreachable: {before['_error']}")
        return 1

    wall = {"resume": [], "suspend": []}
    for i in range(a.cycles):
        t0 = time.monotonic()
        p = ate("resume", "actor", a.actor, "-a", ATESPACE)
        if p.returncode != 0:
            print(f"  cycle {i}: resume FAILED: {p.stderr.strip()}")
            continue
        wait_for(cl, a.actor, True)
        wall["resume"].append(time.monotonic() - t0)

        t1 = time.monotonic()
        cl.suspend(a.actor)
        wait_for(cl, a.actor, False)
        wall["suspend"].append(time.monotonic() - t1)
        print(f"  cycle {i}: resume {wall['resume'][-1]:.2f}s  "
              f"suspend {wall['suspend'][-1]:.2f}s")

    # The atelets push to the collector on an interval, and the collector's
    # /metrics reflects its last receipt. Scraping the instant the last cycle
    # ends therefore truncates the tail - the first run of this script lost 5 of
    # 10 restores that way. Wait out at least one full interval before reading.
    print(f"settling {a.settle:.0f}s so the last cycles reach the collector")
    time.sleep(a.settle)

    after = t15.scrape(a.metrics)
    d = t15.delta(before, after)

    stragglers = [n for n, s in cl.actors().items()
                  if s == "ACTOR_STATE_RUNNING" and n != a.actor]
    if stragglers:
        print(f"WARNING: {stragglers} ran during the window; deltas are polluted")

    report = {
        "run": "ctrl-floor",
        # Read off the run rather than restated, so a report cannot claim a
        # cluster the numbers did not come from.
        "cluster": CONTEXT.rsplit("_", 1)[-1],
        "machine_type": os.environ.get("WORKER_MACHINE_TYPE", "c2d-standard-4"),
        "build": os.environ.get("SUBSTRATE_BUILD", "c48b3a3c"),
        "workload": "busybox sleep, 256Mi, gvisor, Full/Full",
        "cycles": a.cycles,
        "wall_clock_control_plane": {
            k: {"n": len(v), "min": round(min(v), 3), "mean": round(sum(v) / len(v), 3),
                "max": round(max(v), 3)} if v else {"n": 0}
            for k, v in wall.items()
        },
        "control_plane": d,
    }
    out = str(Path(RUN_DIR) / "ctrl-floor-summary.json")
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report["wall_clock_control_plane"], indent=2))
    print(f"\nwrote {out}")

    if not a.keep:
        ate("suspend", "actor", a.actor, "-a", ATESPACE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
