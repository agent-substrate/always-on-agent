#!/usr/bin/env python3
"""T10 - queued delivery across a gateway restart.

The criterion in ACCEPTANCE-PLAN-2026-08-24.md is "multiple messages to a
sleeping agent; gateway restarted mid-flight; observed loss, ordering and
duplication documented", and §6 says plainly that this is **expected to document
a gap rather than pass**: there is no durable per-actor queue in the gateway,
delivery rests on the upstream channel's own retry, and there is no quiesce
window. So this driver is built to describe behaviour accurately, not to reach a
verdict.

**It runs last** because it restarts the gateway, which invalidates any soak or
latency run in flight.

The measurement has two independent instruments, and they are reported
separately because they disagree in the interesting cases.

1. **What the caller saw** - the HTTP outcome of each send. This is what an
   upstream channel would see and therefore what decides whether it retries.
2. **What the agent remembers** - a recall turn after the gateway is back,
   asking the agent to enumerate the tags it received. This is ground truth for
   delivery, and it is the only instrument that can show the dangerous case: a
   message whose *response* was lost but whose *content* was delivered. To the
   user that is a message that failed; to the agent it is a message that
   happened.

The recall instrument is an LLM reading its own conversation, so it is graded by
substring on distinctive tags and the raw reply is always recorded. Duplication
in particular is a weak instrument here - the agent's own account of receiving
something twice is evidence, not proof - and the report says so rather than
quietly counting it.

    ./t10_queued_delivery.py run --run 260827
    ./t10_queued_delivery.py report --run 260827
"""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path

from t2_overnight import (
    AGENTS, ATESPACE, CONTEXT, FLEET_PROVENANCE, OUTDIR,
    actor_docs, actor_name, ate, check_registry, now, provider_error,
    state_of, turn, wait_states,
)

GATEWAY_NS = "hermes"
GATEWAY_DEPLOY = "hermes-gateway"

# Distinct from the soak canary and from T2's cold facts. WORD-4digits so the
# substring grading matches every other test in this pass.
#
# The digits are salted with the run id, and that is load-bearing rather than
# tidy. The first attempt at this test reused one fixed tag set, so the second
# run sent the *same* six tags to the *same* session the first run had already
# planted - and the recall could not tell "delivered just now" from "still in
# the conversation from twenty minutes ago". Every re-run against a session that
# has seen this test before needs tags that session has never seen.
TAG_WORDS = ["QUAYSIDE", "FLINTLOCK", "MERIDIAN",
             "SALTMARSH", "KESTREL", "BRAMBLE"]


def tags_for(run: str) -> list[str]:
    out = []
    for w in TAG_WORDS:
        h = sha256(f"{run}:{w}".encode()).hexdigest()
        out.append(f"{w}-{int(h[:8], 16) % 9000 + 1000}")
    return out

SEND_TEMPLATE = (
    "Message {n} of {total}. Tag: {tag}. Please hold on to this tag - I am "
    "going to ask you to list every tag I sent, in order, in a moment.")

RECALL_PROMPT = (
    "Now list every tag I sent you in the messages just before this one. "
    "Answer in exactly this form and nothing else:\n"
    "COUNT: <how many tagged messages you received>\n"
    "TAGS: <the tags, in the order you received them, comma separated>\n"
    "If you received none, say COUNT: 0 and TAGS: none. Do not invent tags.")


def path_for(run: str, what: str, ext: str = "json") -> Path:
    return OUTDIR / f"t10-{what}-{run}.{ext}"


def kubectl(*args: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl", "--context", CONTEXT, "-n", GATEWAY_NS, *args],
                          capture_output=True, text=True, timeout=timeout)


def gateway_pods() -> list[dict]:
    p = kubectl("get", "pods", "-l", f"app={GATEWAY_DEPLOY}", "-o", "json")
    if p.returncode != 0:
        return []
    return [{"name": i["metadata"]["name"],
             "uid": i["metadata"]["uid"],
             "started": i["metadata"].get("creationTimestamp"),
             "phase": i.get("status", {}).get("phase")}
            for i in json.loads(p.stdout).get("items", [])]


def gateway_live(base: str, timeout: float = 5.0) -> bool:
    try:
        with urllib.request.urlopen(f"{base}/fleet", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def wait_gateway_back(base: str, budget: float) -> dict:
    """Wait for the rollout AND for the port-forward to be usable again.

    Two separate things, and only the second one matters to a caller. The
    port-forward dies with the pod; pf_keepalive.sh re-establishes it, so this
    polls rather than re-creating it - two processes forwarding the same local
    port would produce a confusing partial failure.
    """
    t0 = time.monotonic()
    ro = kubectl("rollout", "status", f"deploy/{GATEWAY_DEPLOY}",
                 f"--timeout={int(budget)}s")
    rollout_s = round(time.monotonic() - t0, 2)
    deadline = t0 + budget
    while time.monotonic() < deadline:
        if gateway_live(base):
            return {"rollout_rc": ro.returncode, "rollout_s": rollout_s,
                    "reachable_s": round(time.monotonic() - t0, 2),
                    "reachable": True}
        time.sleep(2.0)
    return {"rollout_rc": ro.returncode, "rollout_s": rollout_s,
            "reachable_s": None, "reachable": False}


def cmd_run(a: argparse.Namespace) -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    out = path_for(a.run, "delivery")
    if out.exists() and not a.force:
        raise SystemExit(f"{out} exists - a second run would overwrite it. "
                         f"--force if you mean it.")
    if a.agent not in AGENTS:
        raise SystemExit(f"unknown agent {a.agent}; one of {sorted(AGENTS)}")

    tags = tags_for(a.run)[:a.messages]
    session = AGENTS[a.agent][0]
    actor = actor_name(a.agent)

    print(f"T10 queued delivery, run {a.run}")
    check_registry(a.gateway)
    before_pods = gateway_pods()
    print(f"  gateway pods before: {[p['name'] for p in before_pods]}")

    print(f"\nquiescing {a.agent} ({actor})")
    docs = actor_docs()
    if state_of(docs, actor) != "ACTOR_STATE_SUSPENDED":
        p = ate("suspend", "actor", actor, "-a", ATESPACE)
        if p.returncode != 0:
            print(f"  suspend rc={p.returncode} {p.stderr.strip()[:200]}")
    states = wait_states([actor], "ACTOR_STATE_SUSPENDED", a.suspend_wait)
    state_before = states.get(actor, "ABSENT")
    print(f"  {a.agent}: {state_before}")
    if state_before != "ACTOR_STATE_SUSPENDED" and not a.allow_awake:
        raise SystemExit("target is not suspended - T10 asks for messages to a "
                         "*sleeping* agent. --allow-awake to record it anyway.")

    # Staggered concurrent sends. Concurrent so they are genuinely in flight
    # together when the gateway goes; staggered so there is a defined intended
    # order to compare the agent's recall against. Ten simultaneous sends would
    # make the ordering question meaningless before the restart even happened.
    t_zero = time.monotonic()
    wall_zero = now()
    sends: dict[str, dict] = {}
    restart_marks: dict = {}

    def send(i_tag):
        i, tag = i_tag
        delay = i * a.stagger
        while time.monotonic() - t_zero < delay:
            time.sleep(0.01)
        left = time.monotonic()
        r = turn(a.gateway, session,
                 SEND_TEMPLATE.format(n=i + 1, total=len(tags), tag=tag),
                 a.timeout)
        return tag, {
            "n": i + 1, "tag": tag,
            "sent_at_offset_s": round(left - t_zero, 3),
            "returned_at_offset_s": round(time.monotonic() - t_zero, 3),
            "elapsed_s": r["elapsed_s"], "status": r["status"], "ok": r["ok"],
            "provider_error": provider_error(r.get("reply", "")),
            "error": r.get("error"),
            "reply_head": (r.get("reply") or "")[:200],
        }

    def restarter():
        while time.monotonic() - t_zero < a.restart_after:
            time.sleep(0.02)
        restart_marks["mode"] = a.restart_mode
        restart_marks["issued_at_offset_s"] = round(time.monotonic() - t_zero, 3)
        restart_marks["issued_at"] = now()
        if a.restart_mode == "none":
            # Control. Same concurrent staggered sends, no disruption at all.
            # Without this arm, "ordering was not preserved" is unattributable:
            # six concurrent turns on one session can interleave on their own,
            # and that would look identical to a restart scrambling them.
            restart_marks["rc"] = 0
            restart_marks["stderr"] = ""
            print("\n  >>> control arm: no gateway disruption")
            return
        if a.restart_mode == "rollout":
            # A Deployment rolling restart is graceful by construction: the
            # replacement becomes Ready before the old pod is terminated, so
            # requests already on the old pod finish there. This is the upgrade
            # path, and it is the *optimistic* case.
            p = kubectl("rollout", "restart", f"deploy/{GATEWAY_DEPLOY}")
        else:
            # SIGKILL, no grace period - what an OOM kill, a node failure or a
            # preemption does. In-flight requests die with the process, which is
            # the case plan §6 is actually about.
            names = [q["name"] for q in before_pods] or [GATEWAY_DEPLOY]
            p = kubectl("delete", "pod", *names, "--force", "--grace-period=0")
        restart_marks["rc"] = p.returncode
        restart_marks["stderr"] = p.stderr.strip()[:300]
        print(f"\n  >>> gateway {a.restart_mode} issued at "
              f"+{restart_marks['issued_at_offset_s']:.2f}s rc={p.returncode}")

    print(f"\nsending {len(tags)} messages, {a.stagger}s apart, "
          f"restarting the gateway at +{a.restart_after}s")
    rt = threading.Thread(target=restarter, daemon=True)
    rt.start()
    with ThreadPoolExecutor(max_workers=len(tags)) as ex:
        sends = dict(ex.map(send, list(enumerate(tags))))
    rt.join(timeout=120)

    for tag in tags:
        s = sends[tag]
        mark = "ok  " if s["ok"] else "FAIL"
        print(f"  {mark} msg{s['n']} {tag:16s} sent +{s['sent_at_offset_s']:5.2f}s  "
              f"status {s['status']:3d}  {s['elapsed_s']:7.2f}s  "
              f"{(s['error'] or s['reply_head'] or '')[:60]}")

    print("\nwaiting for the gateway and the port-forward to come back")
    back = wait_gateway_back(a.gateway, a.back_budget)
    after_pods = gateway_pods()
    print(f"  rollout rc={back['rollout_rc']} in {back['rollout_s']}s; "
          f"reachable={back['reachable']} at {back['reachable_s']}s")
    print(f"  gateway pods after: {[p['name'] for p in after_pods]}")
    replaced = {p["uid"] for p in before_pods} != {p["uid"] for p in after_pods}
    print(f"  pod actually replaced: {replaced}")
    if not back["reachable"]:
        raise SystemExit("gateway did not come back within the budget - the "
                         "recall turn cannot run, and without it this is not a "
                         "T10 result. Check pf_keepalive.sh and re-run.")

    # Settle before recall: asking the instant the port-forward returns measures
    # the gateway's cold start, not delivery.
    time.sleep(a.settle)
    print(f"\nrecall turn (after {a.settle}s settle)")
    recall = turn(a.gateway, session, RECALL_PROMPT, a.timeout)
    print(f"  status {recall['status']}  {recall['elapsed_s']:.2f}s")
    print("  ---")
    for line in (recall.get("reply") or "").splitlines()[:12]:
        print(f"  {line}")
    print("  ---")

    docs = actor_docs()
    doc = {
        "test": "T10", "run": a.run, "at": wall_zero,
        "gateway": a.gateway, "atespace": ATESPACE, "context": CONTEXT,
        "provenance": FLEET_PROVENANCE,
        "agent": a.agent, "session": session, "actor": actor,
        "state_before": state_before,
        "state_after": state_of(docs, actor),
        "tags_sent": tags,
        "stagger_s": a.stagger,
        "restart": restart_marks,
        "gateway_pods_before": before_pods,
        "gateway_pods_after": after_pods,
        "pod_replaced": replaced,
        "gateway_back": back,
        "sends": sends,
        "recall": {"status": recall["status"], "ok": recall["ok"],
                   "elapsed_s": recall["elapsed_s"],
                   "provider_error": provider_error(recall.get("reply", "")),
                   "reply": recall.get("reply", ""), "error": recall.get("error")},
        "note": ("Expected to document a gap rather than pass - there is no "
                 "durable per-actor queue in the gateway (plan §6). Recall is "
                 "an LLM reading its own conversation and is graded by "
                 "substring; the raw reply is recorded for judgement."),
    }
    out.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"\nwrote {out}\nnow:  ./t10_queued_delivery.py report --run {a.run}")
    return 0


def cmd_report(a: argparse.Namespace) -> int:
    doc = json.loads(path_for(a.run, "delivery").read_text())
    tags = doc["tags_sent"]
    sends = doc["sends"]
    reply = doc["recall"]["reply"] or ""
    up = reply.upper()

    rows = []
    for tag in tags:
        s = sends[tag]
        occurrences = up.count(tag.upper())
        rows.append({
            "tag": tag, "n": s["n"], "status": s["status"], "ok": s["ok"],
            "sent_at": s["sent_at_offset_s"], "elapsed_s": s["elapsed_s"],
            "remembered": occurrences > 0, "occurrences": occurrences,
            "position": up.find(tag.upper()),
        })

    acked = [r for r in rows if r["ok"]]
    remembered = [r for r in rows if r["remembered"]]
    # The two instruments disagreeing is the whole point of running both.
    silent_delivery = [r for r in rows if r["remembered"] and not r["ok"]]
    acked_but_lost = [r for r in rows if r["ok"] and not r["remembered"]]
    dup = [r for r in rows if r["occurrences"] > 1]

    # Ordering: intended order is send order; observed is first-mention order in
    # the recall, restricted to tags actually remembered.
    observed = sorted((r for r in remembered), key=lambda r: r["position"])
    order_ok = [r["n"] for r in observed] == sorted(r["n"] for r in observed)

    rs = doc["restart"]
    back = doc["gateway_back"]
    L = [f"# T10 - queued delivery across a gateway restart, run {doc['run']}", "",
         f"{doc['at']}. Agent `{doc['agent']}` - **{doc['provenance']}**. "
         f"{len(tags)} messages {doc['stagger_s']}s apart to a **{doc['state_before'].replace('ACTOR_STATE_', '')}** "
         f"actor, gateway disruption mode **{rs.get('mode', 'rollout')}** issued at "
         f"**+{rs.get('issued_at_offset_s')}s** while they were in flight.",
         "",
         ("Control arm - the same concurrent staggered sends with **no gateway "
          "disruption at all**. It exists to attribute the ordering result: six "
          "concurrent turns on one session can interleave on their own, and that "
          "would be indistinguishable from a restart scrambling them."
          if rs.get("mode") == "none" else
          "A `rollout restart` is graceful by construction - the replacement pod "
          "becomes Ready before the old one is terminated, so requests already on "
          "the old pod finish there. This is the upgrade path and the optimistic "
          "case."
          if rs.get("mode", "rollout") == "rollout" else
          "A forced delete with no grace period is SIGKILL - what an OOM kill, a "
          "node failure or a preemption does. In-flight requests die with the "
          "process, which is the case plan §6 is about."), "",
         (f"Gateway pod replaced: **{doc['pod_replaced']}** - expected False here, "
          f"and a True would invalidate the control."
          if rs.get("mode") == "none" else
          f"Gateway pod replaced: **{doc['pod_replaced']}** "
          f"({[p['name'] for p in doc['gateway_pods_before']]} → "
          f"{[p['name'] for p in doc['gateway_pods_after']]}). "
          f"Rollout completed in {back['rollout_s']}s; the endpoint was reachable "
          f"again {back['reachable_s']}s after the restart was issued."), "",
         "| # | Tag | Sent at | Caller saw | Agent remembers | Note |",
         "|---|---|---|---|---|---|"]
    for r in sorted(rows, key=lambda r: r["n"]):
        seen = f"{r['status']}" if r["status"] else "no response"
        note = ("**delivered, response lost**" if r in silent_delivery else
                "**acknowledged then lost**" if r in acked_but_lost else
                f"seen {r['occurrences']}× in recall" if r["occurrences"] > 1 else
                "" if r["remembered"] else "not delivered")
        L.append(f"| {r['n']} | {r['tag']} | +{r['sent_at']:.2f}s | {seen} | "
                 f"{'yes' if r['remembered'] else 'no'} | {note} |")

    L += ["",
          f"**Loss** - {len(tags)} sent, {len(acked)} acknowledged to the caller, "
          f"{len(remembered)} present in the agent's own recall.",
          "",
          f"**The two instruments disagree on {len(silent_delivery) + len(acked_but_lost)} "
          f"message(s).** {len(silent_delivery)} were delivered to the agent with no "
          f"response to the caller - to a user those failed, to the agent they "
          f"happened, and an upstream channel that retries on error would send "
          f"them a second time. {len(acked_but_lost)} were acknowledged to the "
          f"caller but do not appear in the conversation.",
          "",
          f"**Ordering** - the agent's recall order is "
          f"{'consistent with' if order_ok else '**not** consistent with'} send "
          f"order, over the {len(observed)} tag(s) it recalled.",
          "",
          f"**Duplication** - {len(dup)} tag(s) appear more than once in the "
          f"recall. This is the weakest instrument here: it is the agent's own "
          f"account, graded by substring, and a model can repeat a tag for "
          f"reasons that have nothing to do with delivery. Treat it as a prompt "
          f"to read the raw reply, below, rather than as a count.",
          "",
          "**This documents a gap, and is not recorded as a pass.** Plan §6: "
          "there is no durable per-actor queue in the gateway, delivery rests on "
          "the upstream channel's own retry, and there is no quiesce window. The "
          "desired handling is an open question in the requirements document, so "
          "an accurate description of current behaviour is the deliverable.",
          "", "## Raw recall reply", "", "```", reply.strip() or "(empty)", "```", ""]

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

    r = sub.add_parser("run", help="messages to a sleeping agent across a restart")
    r.add_argument("--run", required=True)
    r.add_argument("--agent", default=next(iter(AGENTS)))
    r.add_argument("--gateway", default="http://127.0.0.1:18080")
    r.add_argument("--messages", type=int, default=len(TAG_WORDS))
    r.add_argument("--stagger", type=float, default=0.5)
    r.add_argument("--restart-after", type=float, default=3.0,
                   help="seconds after the first send to issue the restart")
    r.add_argument("--restart-mode", choices=("rollout", "kill", "none"), default="rollout",
                   help="rollout: graceful Deployment restart, the upgrade path. "
                        "kill: SIGKILL with no grace period, what an OOM kill or "
                        "a node failure does - this is the case §6 is about")
    r.add_argument("--timeout", type=float, default=360.0)
    r.add_argument("--suspend-wait", type=float, default=300.0)
    r.add_argument("--back-budget", type=float, default=420.0)
    r.add_argument("--settle", type=float, default=20.0)
    r.add_argument("--allow-awake", action="store_true")
    r.add_argument("--force", action="store_true")
    r.set_defaults(func=cmd_run)

    p = sub.add_parser("report", help="read the delivery record back")
    p.add_argument("--run", required=True)
    p.set_defaults(func=cmd_report)

    a = ap.parse_args()
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
