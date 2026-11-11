#!/usr/bin/env python3
"""Pre-warm every worker in the pool, and prove which ones were warmed.

§5 stage 3 of the acceptance plan. A worker that has never run a sandbox
extracts the gVisor asset on its first restore. Through the control plane that
just makes the first restore slow; through a *request-driven* resume the
caller's deadline expires mid-extraction, every retry restarts the extraction
from scratch, and the actor wedges in RESUMING permanently - the cold-node trap
(O-1). So this is a step in the run order, not a precaution: no resume latency
measured before it is worth quoting, and T9's first cycle is distorted without
it.

Warming is done by occupying every worker at once. The scheduler assigns one
actor per worker, so N concurrent control-plane resumes against N free workers
covers the pool in a single round. The script then *reads back* which worker
each actor landed on and fails loudly if any worker went untouched - the whole
point is a coverage claim, and "we resumed ten actors" is not one.

**There are two cold caches, not one, and a busybox warmer only fills the
first.** Learned the hard way on 27 Aug, after a GKE auto-upgrade replaced every
worker node:

  1. the *gVisor asset*, which is per node and workload-independent - a
     `ctrl-floor` warmer fills it, and that is what this script originally did;
  2. the *actor image layer cache* (`imageCache.EnsureImage`), which is per
     image. Warming with busybox leaves the real agent image entirely unpacked,
     so the first request-driven resume of a real agent still wedges - same
     symptom as (1), different cause, and invisible to a coverage claim made
     with the wrong template.

The agent image is large enough that unpacking it exceeds the request-driven
resume deadline, and the retry restarts the layer. So the pool has to be warmed
**once per template that will be resumed by request**, and a run order that
warms only `ctrl-floor` is not warm.

`ctrl-floor` (busybox asleep, 256Mi) is still the right first pass: it is the
benchmark control, it warms the node-level asset, and it warms fast.

Usage:
  python3 prewarm_workers.py                            # gVisor asset only
  python3 prewarm_workers.py -t hermes -p imgwarm-        # image cache too
  python3 prewarm_workers.py --keep                     # leave warmers in place

Both passes are required before T9, T1 or any request-driven turn. Run the
light one first - a worker that cannot boot busybox will not tell you anything
useful about an agent image.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import t15_baseline as t15  # noqa: E402

# Overridable, and defaulting to pass 2 now that it is the live cluster.
# Hardcoding the context meant a run against the wrong cluster looked like a run
# against no workers; leaving the pass 1 default in place after the move meant
# the same thing whenever an export was forgotten, and only some of the deploy
# scripts would follow.
CONTEXT = os.environ.get(
    "KUBECTL_CONTEXT", "REPLACE_WITH_KUBECTL_CONTEXT")
ATESPACE = os.environ.get("ATESPACE", "hermes-pass2")
NAMESPACE = os.environ.get("NAMESPACE", "hermes")
POOL = os.environ.get("WORKER_POOL", "hermes")
# A bare name. Template references used to be "atespace/name"; --template-ref
# resolves in the actor's own atespace and rejects a slashed name outright.
TEMPLATE = os.environ.get("PREWARM_TEMPLATE", "ctrl-floor")
PREFIX = "prewarm-"


def ate(*args: str, timeout: int = 240) -> subprocess.CompletedProcess:
    return subprocess.run([t15.kubectl_ate_bin(), "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def kubectl(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl", "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def workers() -> list[str]:
    """Worker pod names in the pool, from the control plane's own view."""
    p = ate("get", "workers", "-n", NAMESPACE)
    if p.returncode != 0:
        raise SystemExit(f"listing workers failed: {p.stderr.strip()}")
    names = []
    for line in p.stdout.splitlines()[1:]:
        f = line.split()
        if len(f) >= 4 and f[1] == POOL:
            names.append(f[3])
    return names


def actor_docs() -> dict:
    p = ate("get", "actors", "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        raise SystemExit(f"listing actors failed: {p.stderr.strip()}")
    return {a["metadata"]["name"]: a for a in json.loads(p.stdout).get("actors", [])}


def placement(doc: dict) -> str:
    """Worker pod this actor is on, or '' - status shape per #1025 (25 Aug).

    `worker_pod` is the denormalized copy WorkerAssignment keeps precisely so a
    reader does not have to fetch the Worker; `worker` is the authoritative ref.
    Prefer the pod name because that is what `get workers` prints, so a coverage
    claim can be checked against the pool listing by eye.
    """
    wa = (doc.get("status") or {}).get("workerAssignment") or {}
    return wa.get("workerPod") or (wa.get("worker") or {}).get("name", "")


def wait_running(names: list[str], timeout: float = 300.0) -> dict:
    """Wait for all names to be RUNNING; return name -> worker placement."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        docs = actor_docs()
        states = {n: (docs.get(n, {}).get("status") or {}).get("state") for n in names}
        if all(s == "ACTOR_STATE_RUNNING" for s in states.values()):
            return {n: placement(docs[n]) for n in names}
        if any(s == "ACTOR_STATE_CRASHED" for s in states.values()):
            bad = [n for n, s in states.items() if s == "ACTOR_STATE_CRASHED"]
            raise SystemExit(f"warmer(s) crashed: {bad}")
        time.sleep(2.0)
    raise SystemExit(f"timed out waiting for warmers: {states}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true",
                    help="leave the warmer actors suspended rather than deleting")
    ap.add_argument("--rounds", type=int, default=2,
                    help="rounds of occupy-everything; a second round catches a "
                         "worker the scheduler happened to skip in the first")
    ap.add_argument("-t", "--template", default=TEMPLATE,
                    help="template to warm with. The default warms the node's "
                         "gVisor asset; pass the real agent template to warm "
                         "its image layer cache as well")
    ap.add_argument("-p", "--prefix", default=PREFIX,
                    help="warmer actor name prefix; use a distinct one per "
                         "template so two passes do not collide")
    a = ap.parse_args()
    template, prefix = a.template, a.prefix

    pool = workers()
    if not pool:
        print(f"no workers in pool {NAMESPACE}/{POOL}")
        return 1
    print(f"pool {NAMESPACE}/{POOL}: {len(pool)} workers · warming with {template}")

    names = [f"{prefix}{i}" for i in range(len(pool))]
    existing = actor_docs()
    covered: set[str] = set()

    for r in range(a.rounds):
        for n in names:
            if n not in existing:
                p = ate("create", "actor", n, "-a", ATESPACE, "--template", template)
                if p.returncode != 0:
                    print(f"create {n} failed: {p.stderr.strip()}")
                    return 1
        existing = actor_docs()

        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=len(names)) as ex:
            results = list(ex.map(
                lambda n: (n, ate("resume", "actor", n, "-a", ATESPACE)), names))
        for n, p in results:
            if p.returncode != 0:
                print(f"  resume {n} failed: {p.stderr.strip()}")

        place = wait_running(names)
        print(f"round {r}: {len(names)} up in {time.monotonic() - t0:.1f}s")
        for n in names:
            w = place[n] or "?"
            print(f"    {n:14s} -> {w}")
            if w != "?":
                covered.add(w)

        for n in names:
            ate("suspend", "actor", n, "-a", ATESPACE)
        # Suspended actors release their worker; give the pool a moment to free
        # up before the next round, or the second round schedules onto nothing.
        time.sleep(15.0)

        if covered >= set(pool):
            break

    missed = sorted(set(pool) - covered)
    print(f"\nwarmed {len(covered)}/{len(pool)} workers")
    if missed:
        print("NOT WARMED - a request-driven resume onto these will wedge:")
        for w in missed:
            print(f"    {w}")

    if not a.keep:
        for n in names:
            ate("suspend", "actor", n, "-a", ATESPACE)
            ate("delete", "actor", n, "-a", ATESPACE)
        print("warmer actors deleted")

    out = {"pool": pool, "template": template, "warmed": sorted(covered), "not_warmed": missed}
    slug = template.replace("/", "-")
    # The coverage record IS the deliverable. "We resumed nine actors" is not a
    # coverage claim, the per-worker readback is. Writing it last against a
    # hardcoded directory meant a run that warmed the whole pool correctly still
    # ended in FileNotFoundError and left no record, which is the one failure that
    # cannot be recovered by re-reading the cluster. So the directory is made, and
    # the location is overridable per run.
    out_dir = Path(os.environ.get(
        "PREWARM_OUT_DIR",
        Path(__file__).resolve().parent.parent / "runs" / "pass2"))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"prewarm-coverage-{slug}.json"
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {path}")
    return 1 if missed else 0


if __name__ == "__main__":
    sys.exit(main())
