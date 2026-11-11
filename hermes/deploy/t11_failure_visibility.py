#!/usr/bin/env python3
"""T11 - a deliberately failed wake, and what the user is actually told.

The requirement is "distinct user-visible message per failure class". So the
only thing this harness collects is the bytes a user would see, per class, and
the only judgement it makes is whether those bytes differ from each other and
say something true about the cause.

**A message that is distinct but wrong fails this test.** That is not
hypothetical: on 26 Aug the egress gateway served an expired certificate for
five hours, every actor's TLS handshake to it was reset, and the message the
user got named the model provider and suggested they shorten their tool calls.
Distinct, fluent, and pointing at the wrong component and at the user's own
behaviour. That transcript is included below as an observed class rather than an
injected one, because it is the most instructive result this test has.

Classes injected here, all reversible and all at the template or pool level so
no real agent's snapshot is touched:

  no_worker         WorkerPool scaled to zero, then a suspended agent is woken.
  image_missing     Actor from a template whose image does not exist. The image
                    must be a *pinned digest* that is absent from the registry:
                    admission refuses an unpinned tag outright (see
                    unpinned_image), so a tag never reaches the pull path.
  snapshot_store    Actor from a template pointing at a bucket that does not
                    exist. Note this cannot be provoked by creating and resuming
                    - a fresh actor boots from its image and never touches the
                    snapshot store, and the first attempt at this test recorded a
                    happy ACTOR_STATE_RUNNING against a bucket that does not
                    exist. The store is first used on *checkpoint*, so the actor
                    has to be suspended before the class exists at all.
  unpinned_image    Admission-time rejection, kept because it is the one message
                    in this set that arrives before anything is created and it
                    explains itself completely.

Not injected, and why: corrupting a real snapshot object in GCS is the sharpest
version of the restore-failure class, and it needs bucket credentials this host
does not currently hold (`gcloud auth login`). Left as a gap in the write-up
rather than silently dropped.

Usage:
  python3 t11_failure_visibility.py
  python3 t11_failure_visibility.py --only no_worker
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

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
NAMESPACE = "hermes"
POOL = "hermes"
GATEWAY = "http://127.0.0.1:18080"
# Was hardcoded to 4c1b37d, a build this cluster has never run. A stale literal
# here is worse than no field: the report states a commit, nothing contradicts
# it, and the result is attributed to the wrong code.
BUILD = os.environ.get("SUBSTRATE_BUILD", "c48b3a3c")

# Observed in the field on 26 Aug rather than injected - see the module note.
# Kept verbatim, including the advice to the user, because the wording is the
# finding.
FIELD_OBSERVED = {
    "class": "provider_unreachable",
    "injected": False,
    "observed_at": "2026-08-26T07:00Z",
    "cause": "atenet-egress served an expired servicedns leaf for ~5h after "
             "kubelet rotated the file on disk at 01:49:49Z; every actor's mTLS "
             "handshake to the egress gateway was reset",
    "user_visible": "API call failed after 3 retries: Gemini streaming request "
                    "failed: [Errno 104] Connection reset by peer\n\nThe "
                    "provider's stream connection keeps dropping - this often "
                    "happens when generating very large tool call responses "
                    "(e.g. write_file with long content). Try asking me to use "
                    "execute_code with Python's open() for large files, or to "
                    "write in smaller sections.",
    "http_status": 200,
    "assessment": "distinct, but names the wrong component and blames the "
                  "user's prompt for a platform certificate fault; the turn "
                  "also returned HTTP 200 with finish_reason=error, so a client "
                  "checking status alone records a success",
}

# The three fault templates below are built rather than pasted, because the
# ActorTemplate CRD was deleted on 2026-09-02 and every field they used moved
# with it: no apiVersion, no kind, no spec wrapper, `namespace` became
# `atespace`, `sandboxClass: gvisor` became a sandboxConfig block naming a
# cluster SandboxConfig, `snapshotsConfig.location` became `storageLocation`,
# the scope enums gained their SNAPSHOT_CONTENT_SCOPE_ prefix, and
# resources.limits went from a map to a list of {name, quantity}.
#
# That matters more here than anywhere else in the suite: T11 is about whether a
# broken template produces a legible message. A template broken in the way this
# file used to break them is rejected as malformed, which would have this test
# reporting an excellent error message for the wrong fault entirely.


# On Substrate v1 a per-template snapshotConfig.storageLocation is a preview
# field: install with --preview=PerTemplateSnapshotLocation, or the API rejects
# these templates as Forbidden before the fault under test is ever reached.
def fault_template(name: str, image: str, storage: str) -> str:
    return f"""metadata:
  atespace: {ATESPACE}
  name: {name}
workerSelector:
  matchLabels:
    workload: {POOL}
containers:
  - name: nope
    image: {image}
    command: ["/bin/sleep", "1000000"]
resources:
  limits:
    cpuMillicores: 250
    memoryBytes: 268435456 # 256Mi
snapshotConfig:
  storageLocation: {storage}
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
"""


# The run's own bucket. Pointing this at pass 1's meant T11 wrote its
# deliberately-broken snapshot store next to a different run's evidence.
GOOD_STORE = os.environ.get(
    "T11_GOOD_STORE",
    f"gs://{os.environ.get('BUCKET_NAME', 'REPLACE_WITH_SNAPSHOT_BUCKET')}"
    "/hermes-pass2-t11")
BUSYBOX = ("busybox@sha256:1487d0af5f52b4ba31c7e465126ee2123fe3f2305d638e7827"
           "681e7cf6c83d5e")

# A digest, not a tag. `...:t11` is rejected at admission before the actor
# exists, which is a different (and better) failure than the one this class is
# meant to provoke. The digest below is well-formed and absent from the
# registry, so it survives admission and fails on pull.
BAD_IMAGE_TEMPLATE = fault_template(
    "t11-image-missing",
    "registry.invalid/ate-images/definitely-not-a-real-image@sha256:"
    + "1" * 64,
    GOOD_STORE,
)

BAD_BUCKET_TEMPLATE = fault_template(
    "t11-snapshot-store", BUSYBOX,
    "gs://REPLACE_WITH_NONEXISTENT_BUCKET/snapshots",
)

# The original bad-image attempt, kept deliberately: a tag never gets as far as
# a pull, so this is its own class rather than a mistake to be corrected away.
UNPINNED_IMAGE_TEMPLATE = fault_template(
    "t11-unpinned-image",
    "registry.invalid/ate-images/definitely-not-a-real-image:t11",
    GOOD_STORE,
)


def ate(*args: str, timeout: int = 240) -> subprocess.CompletedProcess:
    return subprocess.run([t15.kubectl_ate_bin(), "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def ate_stdin(*args: str, stdin: str,
              timeout: int = 240) -> subprocess.CompletedProcess:
    return subprocess.run([t15.kubectl_ate_bin(), "--context", CONTEXT, *args],
                          input=stdin, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(*args: str, stdin: str | None = None,
            timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl", "--context", CONTEXT, *args],
                          input=stdin, capture_output=True, text=True,
                          timeout=timeout)


def pool_replicas(n: int) -> subprocess.CompletedProcess:
    return kubectl("patch", "workerpool", POOL, "-n", NAMESPACE,
                   "--type=merge", "-p", json.dumps({"spec": {"replicas": n}}))


def pool_size() -> int:
    """What the pool is set to now, so the test puts back what it found.

    Restoring a hardcoded number instead leaves the cluster a different size
    than the test started with, and the pass 2 pool is 9, not 10. The next thing
    to read the pool then measures a fleet it was not sized for and nothing says
    the test did it.
    """
    r = kubectl("get", "workerpool", POOL, "-n", NAMESPACE,
                "-o", "jsonpath={.spec.replicas}")
    try:
        return int((r.stdout or "").strip())
    except ValueError:
        raise RuntimeError(
            f"cannot read WorkerPool {POOL} replicas: {r.stderr.strip()[:200]}")


def actor_state(name: str) -> str:
    p = ate("get", "actor", name, "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        return f"ERR: {p.stderr.strip()[:200]}"
    acts = json.loads(p.stdout or "{}").get("actors") or []
    if not acts:
        return "GONE"
    return (acts[0].get("status") or {}).get("state", "?")


# States that taking the pool to zero cannot make worse. SUSPENDED is safe
# because the actor is entirely in the bucket. CRASHED and DELETING are safe
# because the actor is already gone and no verb brings it back, so there is
# nothing left for O-41 to destroy.
#
# Listing them matters: the snapshot_store class leaves a CRASHED actor behind
# every single run, by design, and it will never suspend. Treating it as
# something to protect makes quiesce time out and no_worker skip itself, which
# loses the one T11 class that actually exercises the platform.
SAFE_THROUGH_POOL_LOSS = {
    "ACTOR_STATE_SUSPENDED", "ACTOR_STATE_CRASHED", "ACTOR_STATE_DELETING",
    "ACTOR_STATE_ABSENT", "",
}


def awake_actors(exclude: str = "") -> list[str]:
    p = ate("get", "actors", "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        raise RuntimeError(f"listing actors failed: {p.stderr.strip()[:200]}")
    return sorted(a["metadata"]["name"]
                  for a in (json.loads(p.stdout or "{}").get("actors") or [])
                  if a["metadata"]["name"] != exclude
                  and (a.get("status") or {}).get("state", "")
                  not in SAFE_THROUGH_POOL_LOSS)


def quiesce(exclude: str) -> list[str]:
    """Suspend every other actor and return the ones that were awake.

    Taking the pool to zero is not a survivable event for an actor that is awake
    on it. The control plane moves it to ACTOR_STATE_CRASHED, which is terminal:
    there is no verb that brings it back, and its clean snapshot in the bucket
    cannot be started from. A SUSPENDED actor comes through untouched. O-41.

    Run against the pass 2 fleet without this, the always-on agent is RUNNING by
    definition and this test destroys it every time.
    """
    awake = awake_actors(exclude)
    if not awake:
        return []
    print(f"  quiescing {len(awake)} awake actor(s) before taking the pool to "
          f"zero (O-41): {', '.join(awake)}")
    for n in awake:
        ate("suspend", "actor", n, "-a", ATESPACE)
    for _ in range(60):
        if not awake_actors(exclude):
            return awake
        time.sleep(2)
    raise RuntimeError(
        f"refusing to scale the pool to zero: {awake_actors(exclude)} would "
        f"NOT suspend and would be destroyed permanently (O-41)")


def class_no_worker(gw: str, timeout: float) -> dict:
    """Wake a suspended agent with nowhere to put it."""
    key = "t11-no-worker"
    name = t15.actor_name_for(key)
    seed = t15.one_turn(gw, "t11-seed", key, "Reply with exactly: OK", timeout)
    if not seed.ok:
        return {"class": "no_worker", "injected": True,
                "setup_failed": seed.reply[:200]}
    ate("suspend", "actor", name, "-a", ATESPACE)
    for _ in range(30):
        if actor_state(name) == "ACTOR_STATE_SUSPENDED":
            break
        time.sleep(2)

    try:
        restore_to = pool_size()
        was_awake = quiesce(exclude=name)
    except RuntimeError as e:
        return {"class": "no_worker", "injected": True, "setup_failed": str(e)}

    p = pool_replicas(0)
    if p.returncode != 0:
        return {"class": "no_worker", "injected": True,
                "setup_failed": p.stderr.strip()[:300]}
    # Wait for the workers to actually go, or the wake finds one still up.
    for _ in range(60):
        out = kubectl("get", "pods", "-n", NAMESPACE,
                      "-l", "ate.dev/workerpool=" + POOL, "--no-headers")
        if not (out.stdout or "").strip():
            break
        time.sleep(3)
    try:
        turn = t15.one_turn(gw, "t11-no-worker", key,
                            "Reply with exactly: OK", timeout)
        st = actor_state(name)
    finally:
        pool_replicas(restore_to)
        for _ in range(80):
            r = kubectl("get", "workerpool", POOL, "-n", NAMESPACE, "-o",
                        "jsonpath={.status.readyReplicas}")
            if (r.stdout or "").strip() == str(restore_to):
                break
            time.sleep(5)
        # Put back what quiesce took down. The always-on agent in particular is
        # awake as its whole tier definition, and leaving it asleep makes the
        # next thing that reads the fleet report the platform not honouring it.
        for n in was_awake:
            ate("resume", "actor", n, "-a", ATESPACE)
    return {"class": "no_worker", "injected": True, "actor": name,
            "audience": "user (gateway response)",
            "http_status": turn.status, "elapsed_s": turn.first_response_s,
            "user_visible": turn.reply, "transport_error": turn.error,
            "actor_state_after": st,
            "quiesced_first": was_awake}


def force_delete(actor: str) -> dict:
    """Delete an actor, including one that cannot be deleted the normal way.

    image_missing strands the actor in RESUMING, and a forced delete
    then parks it in DELETING permanently - ateom cleans up a pause
    container that was never created and treats `runsc state`'s exit 128 as
    fatal. Without this, every run of this test costs the pool one worker until
    someone notices. Restarting the assigned worker releases it.
    """
    p = ate("delete", "actor", actor, "-a", ATESPACE)
    if p.returncode == 0:
        return {"forced": False}
    st = actor_state(actor)
    if st == "GONE":
        return {"forced": False}
    pod = ""
    g = ate("get", "actor", actor, "-a", ATESPACE, "-o", "json")
    if g.returncode == 0:
        acts = json.loads(g.stdout or "{}").get("actors") or []
        if acts:
            wa = (acts[0].get("status") or {}).get("workerAssignment") or {}
            pod = wa.get("workerPod") or (wa.get("worker") or {}).get("name", "")
    if not pod:
        return {"forced": True, "ok": False, "why": f"no worker to restart; {st}"}
    kubectl("delete", "pod", pod, "-n", NAMESPACE, "--wait=true",
            "--timeout=120s", timeout=180)
    time.sleep(10)
    p2 = ate("delete", "actor", actor, "-a", ATESPACE)
    return {"forced": True, "ok": p2.returncode == 0, "worker_restarted": pod,
            "first_error": p.stderr.strip()[:300]}


def class_from_template(name: str, cls: str, manifest: str, actor: str,
                        timeout: float, cycle: bool = False) -> dict:
    """Create an actor from a deliberately broken template and try to run it.

    `cycle` suspends and re-resumes rather than stopping at the first resume.
    Some classes do not exist until the actor has been checkpointed at least
    once - see snapshot_store.

    The apply's return code is checked. The first version of this did not, and
    an admission rejection was reported as a resume failure of an actor that had
    never been created, which reads as a much worse platform than the real one.
    """
    # Templates are not Kubernetes objects any more, so `kubectl apply` would
    # report "no matches for kind" - an admission failure for every class,
    # which this function faithfully records as the platform refusing the
    # template. Creating through the Substrate API instead.
    ap = ate_stdin("create", "actor-template", "-f", "-", stdin=manifest)
    # T11 asks for a distinct *user-visible* message. These classes are provoked
    # through kubectl-ate, so what they yield is an operator message on stderr.
    # That is not the same audience and the report must not blur them: an
    # excellent gRPC error tells the person running the platform what broke and
    # tells the caller nothing at all.
    out: dict = {"class": cls, "injected": True, "actor": actor,
                 "template": f"{ATESPACE}/{name}", "audience": "operator (CLI)"}
    if ap.returncode != 0:
        # Not a harness failure - the platform refused the template, and that
        # refusal *is* the user-visible message for this class.
        out.update({"stage": "admission",
                    "operator_visible": (ap.stderr.strip()
                                         or ap.stdout.strip())[:800],
                    "actor_state_after": "NEVER CREATED"})
        return out
    time.sleep(2)
    try:
        # Bare name: --template-ref resolves in the actor's own atespace.
        c = ate("create", "actor", actor, "-a", ATESPACE, "--template", name)
        out["create_rc"] = c.returncode
        out["create_stderr"] = c.stderr.strip()[:600]
        t0 = time.monotonic()
        r = ate("resume", "actor", actor, "-a", ATESPACE)
        out["resume_rc"] = r.returncode
        out["stage"] = "resume"
        out["operator_visible"] = (r.stderr.strip() or r.stdout.strip())[:800]
        out["resume_elapsed_s"] = round(time.monotonic() - t0, 2)
        time.sleep(20)
        out["actor_state_after"] = actor_state(actor)

        if cycle and out["actor_state_after"] == "ACTOR_STATE_RUNNING":
            # The store is untouched until something is written to it.
            t0 = time.monotonic()
            s = ate("suspend", "actor", actor, "-a", ATESPACE)
            out["suspend_rc"] = s.returncode
            out["suspend_elapsed_s"] = round(time.monotonic() - t0, 2)
            out["suspend_operator_visible"] = (s.stderr.strip()
                                               or s.stdout.strip())[:800]
            time.sleep(15)
            out["actor_state_after_suspend"] = actor_state(actor)
            t0 = time.monotonic()
            r2 = ate("resume", "actor", actor, "-a", ATESPACE)
            out["resume2_rc"] = r2.returncode
            out["resume2_elapsed_s"] = round(time.monotonic() - t0, 2)
            out["resume2_operator_visible"] = (r2.stderr.strip()
                                               or r2.stdout.strip())[:800]
            time.sleep(15)
            out["actor_state_after_resume2"] = actor_state(actor)
            # Whichever step actually failed is the one whose message counts.
            for stage, rc, key in (("suspend", out.get("suspend_rc"),
                                    "suspend_operator_visible"),
                                   ("resume-after-suspend", out.get("resume2_rc"),
                                    "resume2_operator_visible")):
                if rc not in (0, None):
                    out["stage"] = stage
                    out["operator_visible"] = out[key]
                    break
            else:
                out["stage"] = "none - no step failed"
    finally:
        out["cleanup"] = force_delete(actor)
        # Delete the template only after the actor is gone. The other way round
        # makes the actor unsuspendable - LoadActorForSuspend needs the template
        # and fails hard without it, which is how the 26 Aug wedge became
        # unrecoverable by anything short of a worker restart.
        # No --ignore-not-found on this verb; a missing template is not an
        # error worth failing cleanup over, so the return code is dropped.
        ate("delete", "actor-template", name, "-a", ATESPACE)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default=GATEWAY)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--only", default="",
                    help="run one class: no_worker|image_missing|snapshot_store")
    ap.add_argument("--out",
                    default=RUN_DIR)
    a = ap.parse_args()

    want = {c for c in a.only.split(",") if c} or {
        "no_worker", "image_missing", "snapshot_store", "unpinned_image"}
    results = []

    if "unpinned_image" in want:
        print("=== unpinned_image", flush=True)
        results.append(class_from_template(
            "t11-unpinned-image", "unpinned_image", UNPINNED_IMAGE_TEMPLATE,
            "t11-unpinned-1", a.timeout))
        print(json.dumps(results[-1], indent=2), flush=True)

    if "image_missing" in want:
        print("=== image_missing", flush=True)
        results.append(class_from_template(
            "t11-image-missing", "image_missing", BAD_IMAGE_TEMPLATE,
            "t11-img-1", a.timeout))
        print(json.dumps(results[-1], indent=2), flush=True)

    if "snapshot_store" in want:
        print("=== snapshot_store (needs a checkpoint before the class exists)",
              flush=True)
        results.append(class_from_template(
            "t11-snapshot-store", "snapshot_store", BAD_BUCKET_TEMPLATE,
            "t11-snap-1", a.timeout, cycle=True))
        print(json.dumps(results[-1], indent=2), flush=True)

    # Last, because it takes the whole pool down and back up.
    if "no_worker" in want:
        print("=== no_worker (scales the pool to zero and back)", flush=True)
        results.append(class_no_worker(a.gateway, a.timeout))
        print(json.dumps(results[-1], indent=2), flush=True)

    results.append(FIELD_OBSERVED)

    # The actual T11 question: are they different from one another?
    msgs = {}
    for r in results:
        m = (r.get("user_visible") or r.get("operator_visible") or "").strip()
        msgs.setdefault(m, []).append(r["class"])
    collisions = {m[:120]: cs for m, cs in msgs.items() if len(cs) > 1}

    by_audience: dict = {}
    for r in results:
        by_audience.setdefault(r.get("audience", "user (gateway response)"),
                               []).append(r["class"])

    report = {
        "test": "T11", "build": BUILD, "atespace": ATESPACE,
        "cluster": CONTEXT.rsplit("_", 1)[-1],
        "classes": len(results),
        "distinct_messages": len(msgs),
        "collisions": collisions,
        "by_audience": by_audience,
        "audience_caveat":
            "T11 asks for a distinct *user-visible* message. Only no_worker and "
            "the field-observed provider_unreachable case were seen through the "
            "gateway; the rest are operator messages from kubectl-ate. The "
            "distinct-message count should be read as a platform-diagnostics "
            "result, not as evidence that a caller is ever told which "
            "of these five happened.",
        "not_injected": [
            "snapshot object corrupted in GCS - needs bucket credentials "
            "(gcloud auth login); the sharpest version of the restore-failure "
            "class and it is not covered here",
        ],
        "results": results,
    }
    out = Path(a.out) / "t11-failure-visibility.json"
    out.write_text(json.dumps(report, indent=2))
    print("\n" + json.dumps({k: v for k, v in report.items() if k != "results"},
                            indent=2))
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
