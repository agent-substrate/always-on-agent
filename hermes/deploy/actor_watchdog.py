#!/usr/bin/env python3
"""Watch the fleet for stranded actors, clear them, and record every one.

§5 stage 6 gate, and the thing that lets T1 run attended overnight. Risk 7 is
open at 4c1b37d: a restore that fails on a live worker leaves the actor in
RESUMING holding its worker, and the documented recovery is
`kubectl-ate admin debug-flush-redis` by hand. Left alone that turns one wedge
at hour six into a lost night.

**This never runs debug-flush-redis.** That command flushes the entire store -
every actor in the atespace, not the wedged one - so as an automated recovery
during a ten-agent soak it is worse than the fault. Recovery here is a ladder,
least destructive first, and each rung's actual result is recorded rather than
assumed. If the ladder runs out, the watchdog says so and names the manual
command; it does not escalate to the nuclear option on its own.

The ladder is four rungs because three were not enough. Proven against a live
wedge on 26 Aug: an actor whose image pull fails stops in RESUMING, and
a forced delete then parks it in DELETING permanently - ateom's
TerminateWorkload always cleans up a pause container that in this case was never
created, `runsc state` exits 128, and cleanup calls that fatal instead of
already-gone. suspend, suspend-retry and delete all failed; restarting the
worker pod released it in one step. So an actor that fails to pull its image is,
today, undeletable and holds its worker for as long as the worker lives - which
on a ten-worker pool is a tenth of the fleet per occurrence.

**Clearing is the lesser half. Recording is the point.** How often a 1,000-cycle
soak wedges, at which rung it came back, and what the atelet said at the time,
is one of the more useful things this pass can report - and the plan is explicit
that a wedge is a finding for the report, not a failed test. So every trip
writes a T1 event with the evidence captured *before* anything is touched.

It also writes a heartbeat every poll. T1's criterion is "zero stranded actors",
and until 26 Aug nothing was actually observing that continuously - the stale
client had been answering STATUS_UNSPECIFIED for every actor for two weeks
(O-13). A criterion with no instrument is not a criterion.

Usage:
  python3 actor_watchdog.py                        # watch, clear, record
  python3 actor_watchdog.py --dry-run              # record only, touch nothing
  python3 actor_watchdog.py --wedge-after 120
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import t15_baseline as t15  # noqa: E402

# Pass 2, and overridable on the same names prewarm_workers.py uses. These were
# hardcoded to the pass 1 acceptance cluster, and pointing a watchdog at the
# wrong control plane does not error: it reads an atespace that exists and is
# empty, reports a serenely healthy fleet all night, and clears nothing.
CONTEXT = os.environ.get(
    "KUBECTL_CONTEXT", "REPLACE_WITH_KUBECTL_CONTEXT")
ATESPACE = os.environ.get("ATESPACE", "hermes-pass2")
NAMESPACE = os.environ.get("NAMESPACE", "hermes")
# Store watchdog events alongside the other local run artifacts.
EVENTS = os.environ.get("WATCHDOG_EVENTS", str(
    Path(__file__).resolve().parent.parent / "runs" / "pass2"
    / "t1-watchdog-events.jsonl"))

# States an actor passes *through*. Sitting in one of these past the threshold
# is the wedge. SUSPENDING is included deliberately: a checkpoint that dies
# mid-flight strands the actor and holds its worker exactly as a failed restore
# does, and the soak suspends as often as it resumes.
TRANSITIONAL = {
    "ACTOR_STATE_RESUMING",
    "ACTOR_STATE_SUSPENDING",
    "ACTOR_STATE_PAUSING",
    "ACTOR_STATE_DELETING",
}
SETTLED = {"ACTOR_STATE_RUNNING", "ACTOR_STATE_SUSPENDED", "ACTOR_STATE_PAUSED"}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def ate(*args: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run([t15.kubectl_ate_bin(), "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def kubectl(*args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl", "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def emit(rec: dict) -> None:
    rec = {"time": now(), **rec}
    Path(EVENTS).parent.mkdir(parents=True, exist_ok=True)
    with open(EVENTS, "a") as f:
        f.write(json.dumps(rec) + "\n")
    if rec.get("event") != "heartbeat":
        print(json.dumps(rec), flush=True)


def actor_docs() -> dict:
    p = ate("get", "actors", "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        raise RuntimeError(f"get actors failed: {p.stderr.strip()}")
    return {a["metadata"]["name"]: a
            for a in (json.loads(p.stdout or "{}").get("actors") or [])}


def state_of(doc: dict) -> str:
    return (doc.get("status") or {}).get("state") or "ACTOR_STATE_UNSPECIFIED"


def template_of(doc: dict) -> str:
    """The actor's template, as a bare name --template-ref will accept.

    release-0.1 nests this under `actorTemplate` as {atespace, name}. The flat
    `actorTemplateName` this used to read does not exist there, so the guard on
    it was always false and --recreate was a silent no-op that still reported
    "recovered at rung delete" - a freed worker and a permanently missing agent,
    recorded as a success. The old spelling is still tried for the pass 1
    cluster.
    """
    at = doc.get("actorTemplate") or {}
    return at.get("name") or doc.get("actorTemplateName") or ""


def worker_of(doc: dict) -> str:
    """Whatever the assignment names the worker. Usually a pod, sometimes not."""
    wa = (doc.get("status") or {}).get("workerAssignment") or {}
    return wa.get("workerPod") or (wa.get("worker") or {}).get("name", "")


def worker_pod(worker: str) -> str:
    """Resolve whatever worker_of returned to the pod backing it.

    On release-0.1 a running actor carries `workerAssignment.workerPod`, so the
    first branch of worker_of already yields a pod and this is a lookup that
    confirms it. The second branch is the reason this exists:
    `workerAssignment.worker.name` is a UUID, and everything downstream wants a
    pod. Rung 4 deletes it and capture() reads the atelet log out of it, and
    neither fails loudly on a UUID - `kubectl logs <uuid>` puts NotFound where
    the evidence should be, and rung 4 spends its turn on a pod that was never
    there, so the ladder exhausts one rung early on the one fault it exists for.

    Costs one `get workers` per call, which is nothing next to a wedge.

    Returns "" when the mapping cannot be made, so callers keep their existing
    "no worker" path rather than acting on a name that is not a pod.
    """
    if not worker:
        return ""
    p = ate("get", "workers", "-o", "json")
    if p.returncode != 0:
        return ""
    try:
        workers = json.loads(p.stdout or "{}").get("workers") or []
    except json.JSONDecodeError:
        return ""
    for w in workers:
        names = {(w.get("metadata") or {}).get("name"), w.get("workerPodUid"),
                 w.get("workerPod")}
        if worker in names:
            return w.get("workerPod") or ""
    return ""


def capture(name: str, doc: dict, last_worker: str = "") -> dict:
    """Everything worth knowing, read before the actor is touched.

    Order matters: the ladder's first rung can change the actor document and
    can cause the atelet to log more, so a capture taken afterwards describes
    the recovery rather than the fault.

    `last_worker` is the worker this actor was on the last time it named one,
    and it is what makes a crash investigable. A crash clears
    `status.workerAssignment` as part of the same update that sets CRASHED, so
    by the time this runs the document no longer says where the actor was. The
    first crashes of pass 2 were captured with an empty worker, an empty pod
    and an empty log for exactly that reason. Falling back to the remembered
    worker keeps the atelet log reachable.
    """
    worker = worker_of(doc) or last_worker
    pod = worker_pod(worker)
    ev = {"actor_document": doc, "worker": worker, "worker_pod": pod,
          "worker_from_memory": bool(worker and not worker_of(doc))}
    if pod:
        p = kubectl("logs", "-n", NAMESPACE, pod, "--tail=80", "--since=15m")
        ev["atelet_log"] = (p.stdout or p.stderr)[-8000:]
    elif worker:
        # Recorded rather than skipped. A wedge whose worker cannot be named is
        # itself the finding, and it is the case where rung 4 will not fire.
        ev["atelet_log"] = f"<no pod resolved for worker {worker}>"
    p = kubectl("logs", "-n", "ate-system", "deploy/ate-api-server",
                "--tail=400", "--since=15m")
    ev["api_server_log"] = "\n".join(
        l for l in (p.stdout or "").splitlines() if name in l)[-8000:]
    return ev


def settled(name: str, timeout: float) -> str | None:
    """Poll until the actor reaches a settled state or vanishes. Returns it."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(5.0)
        try:
            docs = actor_docs()
        except RuntimeError:
            continue
        if name not in docs:
            return "GONE"
        st = state_of(docs[name])
        if st in SETTLED or st == "ACTOR_STATE_CRASHED":
            return st
    return None


def clear(name: str, doc: dict, dry_run: bool, recreate: bool,
          rung_timeout: float) -> dict:
    """Least-destructive-first recovery. Every rung's real result recorded."""
    template = template_of(doc)
    rungs = []

    if dry_run:
        return {"outcome": "dry-run", "rungs": [], "template": template}

    # 1. Ask nicely. A suspend on a stranded actor sometimes completes the
    #    half-finished transition and gives the worker back with state intact.
    p = ate("suspend", "actor", name, "-a", ATESPACE)
    st = settled(name, rung_timeout) if p.returncode == 0 else None
    rungs.append({"rung": "suspend", "rc": p.returncode,
                  "stderr": p.stderr.strip()[:400], "settled_to": st})
    if st in (*SETTLED, "GONE"):
        return {"outcome": "recovered", "at_rung": "suspend", "rungs": rungs,
                "state_preserved": True, "template": template}

    # 2. Once more. The lock has a 30s TTL and renews while held, so a rung
    #    that lost a race with an in-flight workflow can win the next one.
    p = ate("suspend", "actor", name, "-a", ATESPACE)
    st = settled(name, rung_timeout) if p.returncode == 0 else None
    rungs.append({"rung": "suspend-retry", "rc": p.returncode,
                  "stderr": p.stderr.strip()[:400], "settled_to": st})
    if st in (*SETTLED, "GONE"):
        return {"outcome": "recovered", "at_rung": "suspend-retry",
                "rungs": rungs, "state_preserved": True, "template": template}

    # 3. Delete. This frees the worker, which is the part the soak cannot do
    #    without - but it is where the agent's conversation stops being safe to
    #    assume. Flagged, not hidden: a T1 cycle recovered at this rung is a
    #    recovered *worker*, and T2's continuity claim must not count it.
    p = ate("delete", "actor", name, "-a", ATESPACE)
    st = settled(name, rung_timeout) if p.returncode == 0 else None
    rungs.append({"rung": "delete", "rc": p.returncode,
                  "stderr": p.stderr.strip()[:400], "settled_to": st})
    if st == "GONE":
        out = {"outcome": "recovered", "at_rung": "delete", "rungs": rungs,
               "state_preserved": False, "template": template}
        if recreate and template:
            # --template-ref, and a bare name. release-0.1's create actor has
            # no -t and no --template at all, so the old call did not create a
            # wrong actor, it exited on an unknown shorthand flag - and the
            # only trace was recreate_stderr in a JSONL nobody reads until the
            # morning.
            r = ate("create", "actor", name, "-a", ATESPACE,
                    "--template", template)
            out["recreated"] = r.returncode == 0
            out["recreate_stderr"] = r.stderr.strip()[:400]
        elif recreate:
            out["recreated"] = False
            out["recreate_stderr"] = "no template on the actor document"
        return out

    # 4. Restart the worker the actor is pinned to, then delete again.
    #
    #    Established on 26 Aug against a real wedge: an actor whose image pull
    #    fails is left in RESUMING, and a forced delete then leaves it in
    #    DELETING forever, because ateom's TerminateWorkload unconditionally
    #    cleans up a pause container that was never created - `runsc state`
    #    exits 128 and the cleanup treats that as fatal rather than as
    #    already-gone. Neither suspend nor delete can ever win that argument;
    #    the only thing that does is taking the worker away. Restarting the pod
    #    moved the actor DELETING -> CRASHED with its worker released, and the
    #    next delete succeeded immediately.
    #
    #    This is a blast radius, not a command: anything else on that worker
    #    goes down with it. So the co-tenants are enumerated and recorded as
    #    collateral *before* the restart, and the pool - not this script - is
    #    what brings the worker back.
    worker = worker_of(doc)
    pod = worker_pod(worker)
    if pod:
        cotenants = []
        try:
            cotenants = [n for n, d in actor_docs().items()
                         if n != name and worker_of(d) == worker]
        except RuntimeError:
            cotenants = ["<unreadable>"]
        p = kubectl("delete", "pod", pod, "-n", NAMESPACE,
                    "--wait=true", "--timeout=120s", timeout=180)
        rungs.append({"rung": "restart-worker", "worker": worker, "pod": pod,
                      "rc": p.returncode, "stderr": p.stderr.strip()[:400],
                      "collateral_actors": cotenants,
                      "note": "every actor listed in collateral_actors lost its "
                              "sandbox too; a T1 cycle count must subtract them"})
        st = settled(name, rung_timeout)
        p = ate("delete", "actor", name, "-a", ATESPACE)
        st = settled(name, rung_timeout) if p.returncode == 0 else st
        rungs.append({"rung": "delete-after-worker-restart", "rc": p.returncode,
                      "stderr": p.stderr.strip()[:400], "settled_to": st})
        if st == "GONE":
            return {"outcome": "recovered", "at_rung": "restart-worker",
                    "rungs": rungs, "state_preserved": False,
                    "collateral_actors": cotenants, "template": template}

    # Out of rungs. Say so loudly and name the manual step rather than reaching
    # for it: debug-flush-redis would take the other nine agents with it.
    return {
        "outcome": "LADDER_EXHAUSTED", "rungs": rungs, "template": template,
        "manual": f"{t15.kubectl_ate_bin()} --context {CONTEXT} "
                  f"admin debug-flush-redis "
                  f"- DESTROYS ALL ACTORS IN THE STORE, not just {name}. "
                  f"Stop the soak first and record the cycle count.",
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=20.0)
    ap.add_argument("--wedge-after", type=float, default=180.0,
                    help="seconds in a transitional state before it counts as "
                         "stranded; well above a normal restore (~1.3s) and "
                         "above the gateway's own wake budget")
    ap.add_argument("--rung-timeout", type=float, default=90.0)
    ap.add_argument("--dry-run", action="store_true",
                    help="detect and record, change nothing")
    ap.add_argument("--recreate", action="store_true",
                    help="after a delete, recreate the actor from its template")
    ap.add_argument("--once", action="store_true", help="one poll, then exit")
    a = ap.parse_args()

    since: dict = {}          # actor -> (state, monotonic first seen in it)
    crashed_seen: set = set()
    tripped: dict = {}        # actor -> monotonic of last trip, for backoff
    last_worker: dict = {}    # actor -> the worker it last named, for capture()
    emit({"event": "watchdog_start", "atespace": ATESPACE,
          "wedge_after_s": a.wedge_after, "dry_run": a.dry_run,
          "interval_s": a.interval})

    while True:
        t = time.monotonic()
        try:
            docs = actor_docs()
        except Exception as e:
            # A control plane we cannot read is itself a T1 event. Do not treat
            # an unreadable fleet as an empty one.
            emit({"event": "poll_failed", "error": repr(e)[:400]})
            time.sleep(a.interval)
            if a.once:
                return 1
            continue

        hist: dict = {}
        for name, doc in docs.items():
            st = state_of(doc)
            hist[st] = hist.get(st, 0) + 1
            if w := worker_of(doc):
                last_worker[name] = w
            prev = since.get(name)
            if prev is None or prev[0] != st:
                since[name] = (st, t)
                continue

            held = t - prev[1]
            if st == "ACTOR_STATE_CRASHED" and name not in crashed_seen:
                crashed_seen.add(name)
                emit({"event": "actor_crashed", "actor": name,
                      "note": "not cleared - a crashed actor is evidence; its "
                              "document carries no reason field, so the atelet "
                              "log below is the only account of why. CRASHED is "
                              "terminal on release-0.1: resume, suspend and "
                              "pause are all refused from it, so nothing here "
                              "can put the actor back and the run needs a "
                              "delete and recreate",
                      "evidence": capture(name, doc,
                                          last_worker.get(name, ""))})
                continue

            if st in TRANSITIONAL and held >= a.wedge_after:
                last = tripped.get(name, 0.0)
                if t - last < a.wedge_after:
                    continue          # already handling; do not stack recoveries
                tripped[name] = t
                ev = capture(name, doc)
                emit({"event": "actor_stranded", "actor": name, "state": st,
                      "held_s": round(held, 1), "worker": ev["worker"],
                      "evidence": ev})
                res = clear(name, doc, a.dry_run, a.recreate, a.rung_timeout)
                emit({"event": "recovery", "actor": name, "state": st,
                      "held_s": round(held, 1), **res})
                since.pop(name, None)

        # crashed_total is separate from stranded_total on purpose. stranded
        # counts actors this watchdog tripped on and tried to recover; a crashed
        # actor is never tripped on, because there is no recovery to attempt. A
        # reader watching only heartbeats saw stranded_total 0 through two dead
        # actors, so the count that cannot be recovered is the one to surface.
        emit({"event": "heartbeat", "actors": len(docs), "states": hist,
              "stranded_total": len(tripped),
              "crashed_total": hist.get("ACTOR_STATE_CRASHED", 0),
              "crashed": sorted(n for n, d in docs.items()
                                if state_of(d) == "ACTOR_STATE_CRASHED")})
        if a.once:
            return 0
        time.sleep(a.interval)


if __name__ == "__main__":
    sys.exit(main())
