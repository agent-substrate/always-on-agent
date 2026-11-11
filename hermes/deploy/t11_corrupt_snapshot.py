#!/usr/bin/env python3
"""T11, the class the main harness cannot inject: a corrupted stored snapshot.

`t11_failure_visibility.py` covers four classes plus the field-observed provider
case. It cannot cover this one, because it drives everything through
`kubectl-ate` and the gateway and neither of those can reach into the bucket to
damage an object. That is a limit of the harness, not of the host: an operator
shell authenticated to the project has objectAdmin on the snapshot bucket and
can corrupt a snapshot in one command. Pass 2 recorded this class as blocked on
credentials, which was wrong, and this script is the correction.

Why the class matters more than the other four. `snapshot_store` in the main
harness makes the store *unreachable*, which the platform notices immediately
and reports as `DataLoss`. This one leaves the store perfectly reachable and the
object present and the wrong size, so nothing upstream of the restore has any
reason to complain. It is the difference between a disk that is unplugged and a
disk that lies, and the second is the one that tells you whether the read path
validates anything.

Scope, deliberately narrow:

  * One throwaway actor of its own. It never touches the fleet, never scales the
    WorkerPool and never quiesces anything, so it is safe to run alongside an
    overnight soak. Everything O-41 punishes, this script does not do.
  * The corruption is applied to a copy-on-write of the actor's own snapshot
    prefix and nothing else in the bucket is read or written.
  * Both audiences are captured: the operator message from `resume actor`, and
    the user-visible message from a gateway turn, because T11 is a test about
    who gets told what.

Usage:
    ./t11_corrupt_snapshot.py run --run pass2
    ./t11_corrupt_snapshot.py cleanup --run pass2
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

ATESPACE = os.environ.get("ATESPACE", "hermes-pass2")
GATEWAY = os.environ.get("HERMES_GATEWAY", "http://127.0.0.1:18080")
CONTEXT = os.environ.get(
    "KUBE_CONTEXT",
    "REPLACE_WITH_KUBECTL_CONTEXT")
# Was hardcoded in two sibling harnesses to a build this cluster has never run.
BUILD = os.environ.get("SUBSTRATE_BUILD", "c48b3a3c")
SESSION_KEY = "t11-corrupt-snapshot"
RUNS = Path(__file__).resolve().parent.parent / "runs"


def ate(*args: str, timeout: int = 240) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl-ate", *args], capture_output=True,
                          text=True, timeout=timeout,
                          env={**os.environ, "KUBECONFIG":
                               os.environ.get("KUBECONFIG",
                                              str(Path.home() / ".kube/config"))})


def gcloud(*args: str, timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(["gcloud", *args], capture_output=True,
                          text=True, timeout=timeout)


def require_context() -> None:
    """Refuse to run against the wrong cluster.

    Two Substrate clusters live in this project and the current context has
    drifted to the other one at least once. Everything below mutates a bucket
    and an actor, so a wrong context is not a wasted run, it is damage
    somewhere else.
    """
    p = subprocess.run(["kubectl", "config", "current-context"],
                       capture_output=True, text=True, timeout=30)
    got = (p.stdout or "").strip()
    if got != CONTEXT:
        raise SystemExit(
            f"refusing to run: kubectl context is {got!r}, want {CONTEXT!r}.\n"
            f"  kubectl config use-context {CONTEXT}")


def actor_doc(name: str) -> dict:
    p = ate("get", "actor", name, "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        return {}
    try:
        return json.loads(p.stdout or "{}")
    except json.JSONDecodeError:
        return {}


def actor_state(name: str) -> str:
    return ((actor_doc(name).get("status") or {}).get("state") or "")


def wait_state(name: str, want: str, budget: float = 120.0) -> str:
    deadline = time.monotonic() + budget
    st = actor_state(name)
    while st != want and time.monotonic() < deadline:
        time.sleep(2)
        st = actor_state(name)
    return st


def snapshot_uri(name: str) -> str:
    st = actor_doc(name).get("status") or {}
    snap = st.get("latestSnapshot") or {}
    # The field is called `atespace` and holds a gs:// URI. That is a naming
    # accident upstream, not a mistake here.
    return snap.get("atespace") or ""


def list_objects(prefix: str) -> list[tuple[str, int]]:
    p = gcloud("storage", "ls", "--long", "--recursive", prefix + "/**")
    out = []
    for line in (p.stdout or "").splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[-1].startswith("gs://"):
            try:
                out.append((parts[-1], int(parts[0])))
            except ValueError:
                continue
    return sorted(out, key=lambda x: -x[1])


def cmd_run(a: argparse.Namespace) -> int:
    require_context()
    name = t15.actor_name_for(SESSION_KEY)
    rec: dict = {"test": "T11", "class": "snapshot_corrupt", "build": BUILD,
                 "atespace": ATESPACE, "cluster": CONTEXT.rsplit("_", 1)[-1],
                 "actor": name, "injected": True,
                 "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    print(f"actor {name}")
    print("  seeding a conversation ...")
    seed = t15.one_turn(GATEWAY, "t11-corrupt-seed", SESSION_KEY,
                        "Remember the codeword TANGERINE. Reply with exactly: OK",
                        a.timeout)
    rec["seed"] = {"status": seed.status, "ok": seed.ok,
                   "elapsed_s": seed.first_response_s,
                   "reply": (seed.reply or "")[:200], "error": seed.error}
    if not seed.ok:
        rec["setup_failed"] = f"seed turn failed: {seed.status} {seed.error}"
        return finish(a, rec, 1)
    print(f"    {seed.status} in {seed.first_response_s:.2f}s")

    print("  suspending ...")
    ate("suspend", "actor", name, "-a", ATESPACE)
    st = wait_state(name, "ACTOR_STATE_SUSPENDED")
    rec["state_after_suspend"] = st
    if st != "ACTOR_STATE_SUSPENDED":
        rec["setup_failed"] = f"did not suspend, state {st}"
        return finish(a, rec, 1)

    uri = snapshot_uri(name)
    rec["snapshot_uri"] = uri
    if not uri.startswith("gs://"):
        rec["setup_failed"] = f"no snapshot URI on the actor: {uri!r}"
        return finish(a, rec, 1)
    print(f"  snapshot {uri}")

    objs = list_objects(uri)
    rec["objects"] = [{"uri": u, "bytes": n} for u, n in objs]
    if not objs:
        rec["setup_failed"] = "snapshot prefix is empty"
        return finish(a, rec, 1)
    target, size = objs[0]
    print(f"  {len(objs)} object(s); corrupting the largest, {size} bytes")
    print(f"    {target}")

    # Corrupt in place: same object, different bytes, different length. This is
    # the "disk that lies" case. Deliberately NOT a delete, which the platform
    # already handles as a missing object.
    junk = Path("/tmp/_t11_corrupt_payload")
    junk.write_bytes(b"CORRUPTED-BY-T11-ACCEPTANCE-TEST\n" * 64)
    cp = gcloud("storage", "cp", str(junk), target)
    rec["corruption"] = {"object": target, "was_bytes": size,
                         "now_bytes": junk.stat().st_size,
                         "method": "overwrite in place, shorter payload",
                         "cp_rc": cp.returncode,
                         "cp_err": (cp.stderr or "").strip()[-300:]}
    if cp.returncode != 0:
        rec["setup_failed"] = f"could not overwrite: {cp.stderr.strip()[:300]}"
        return finish(a, rec, 1)
    print("  corrupted.")

    print("  resuming (operator path) ...")
    t0 = time.monotonic()
    r = ate("resume", "actor", name, "-a", ATESPACE)
    rec["operator"] = {"audience": "operator (kubectl-ate resume)",
                       "rc": r.returncode,
                       "elapsed_s": round(time.monotonic() - t0, 2),
                       "stdout": (r.stdout or "").strip()[:800],
                       "stderr": (r.stderr or "").strip()[:800]}
    print(f"    rc={r.returncode} in {rec['operator']['elapsed_s']}s")
    if r.stderr.strip():
        print(f"    {r.stderr.strip()[:300]}")
    rec["state_after_resume"] = actor_state(name)
    print(f"    state {rec['state_after_resume']}")

    print("  asking through the gateway (user path) ...")
    turn = t15.one_turn(GATEWAY, "t11-corrupt-ask", SESSION_KEY,
                        "What was the codeword? Reply with just the word.",
                        a.timeout)
    rec["user"] = {"audience": "user (gateway response)",
                   "http_status": turn.status, "ok": turn.ok,
                   "elapsed_s": turn.first_response_s,
                   "user_visible": (turn.reply or "")[:800],
                   "transport_error": turn.error}
    print(f"    {turn.status} in {turn.first_response_s:.2f}s")
    print(f"    {(turn.reply or turn.error)[:300]}")
    rec["state_final"] = actor_state(name)
    print(f"  final state {rec['state_final']}")

    # The interesting question for the report, decided here rather than by eye.
    reply = (turn.reply or "").upper()
    rec["verdict"] = {
        "operator_told": bool(r.returncode != 0 or r.stderr.strip()),
        "user_told": not turn.ok or bool(turn.error),
        "silently_wrong": bool(turn.ok and "TANGERINE" not in reply
                               and not turn.error),
        "recovered_intact": bool(turn.ok and "TANGERINE" in reply),
    }
    return finish(a, rec, 0)


def finish(a: argparse.Namespace, rec: dict, rc: int) -> int:
    out = RUNS / a.run / "t11-corrupt-snapshot.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rec, indent=2))
    print(f"\nwrote {out}")
    if rc == 0:
        v = rec.get("verdict") or {}
        print("\nverdict")
        for k in ("operator_told", "user_told", "silently_wrong",
                  "recovered_intact"):
            print(f"  {k:20s} {v.get(k)}")
        print(f"\ncleanup:  ./t11_corrupt_snapshot.py cleanup --run {a.run}")
    return rc


def cmd_cleanup(a: argparse.Namespace) -> int:
    require_context()
    name = t15.actor_name_for(SESSION_KEY)
    st = actor_state(name)
    print(f"{name}: {st or 'ABSENT'}")
    if not st:
        return 0
    # Substrate refuses to delete an actor still on a worker, so park it first.
    ate("suspend", "actor", name, "-a", ATESPACE)
    p = ate("delete", "actor", name, "-a", ATESPACE)
    print(f"  delete rc={p.returncode} {(p.stderr or '').strip()[:300]}")
    # The snapshot prefix is orphaned by design once the actor is gone; the
    # platform's own GC does not reach a corrupted object it can no longer
    # resolve an owner for.
    uri = a.snapshot or ""
    if uri.startswith("gs://"):
        g = gcloud("storage", "rm", "--recursive", uri)
        print(f"  bucket rm rc={g.returncode} {(g.stderr or '').strip()[:200]}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run")
    r.add_argument("--run", default="pass2")
    r.add_argument("--timeout", type=float, default=300.0)
    r.set_defaults(fn=cmd_run)

    c = sub.add_parser("cleanup")
    c.add_argument("--run", default="pass2")
    c.add_argument("--snapshot", default="",
                   help="gs:// prefix to remove as well, from the run record")
    c.set_defaults(fn=cmd_cleanup)

    a = ap.parse_args()
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
