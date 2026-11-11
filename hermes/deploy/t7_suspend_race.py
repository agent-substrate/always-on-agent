#!/usr/bin/env python3
"""T7 - a message deliberately timed to land while a suspend is in progress.

The requirement is "observed behaviour documented", not a pass mark, so this
harness is built to make the observation *checkable* rather than to make it come
out well.

Two things have to be true for a T7 result to mean anything, and both are easy
to skip:

**The message really did land mid-suspend.** A test that fires a turn "during"
a suspend and happens to miss the window every time reports normal wake latency
and calls it a race. So a sampler polls the control plane throughout and the run
records the actor's observed state at the moment the request went out. A trial
whose sampler never saw SUSPENDING or SUSPENDED before the request is reported
as `window: missed` and excluded from the conclusion rather than counted.

**Nothing was lost.** A race that returns 200 quickly but drops the turn off the
end of the conversation is worse than one that returns an error. Each trial
plants a distinct fact before the race, asks for it back after, and the check is
on the answer, not on the status code. §3.1 makes memory loss a standing
condition on every test, so it is checked here rather than at the end.

Offsets are swept across the suspend rather than fixed, because the checkpoint
takes about a second on this workload and where in that second the message lands
is the whole question.

Usage:
  python3 t7_suspend_race.py
  python3 t7_suspend_race.py --offsets 0,0.25,0.5,0.75,1.0,1.5 --trials 2
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
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
GATEWAY = "http://127.0.0.1:18080"
# Was hardcoded to 4c1b37d, a build this cluster has never run. A stale literal
# here is worse than no field: the report states a commit, nothing contradicts
# it, and the number is attributed to the wrong code.
BUILD = os.environ.get("SUBSTRATE_BUILD", "c48b3a3c")


def ate(*args: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run([t15.kubectl_ate_bin(), "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


class Sampler(threading.Thread):
    """Poll one actor's state as fast as the API will answer, and keep it all.

    Sampling through kubectl-ate rather than a raw client keeps this on the same
    read path as the rest of the harness - including the status shape - so a
    control-plane change cannot make the sampler and the test disagree.
    """

    def __init__(self, cl: "t15.Cluster", name: str):
        super().__init__(daemon=True)
        self.cl, self.name = cl, name
        self.samples: list = []
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                st = self.cl.actors().get(self.name)
            except Exception as e:
                st = f"ERR:{e!r}"[:80]
            self.samples.append((time.monotonic(), st))

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=10)

    def state_at(self, t: float) -> str:
        """State observed at or immediately before t."""
        seen = [s for ts, s in self.samples if ts <= t]
        return seen[-1] if seen else "?"

    def seen(self) -> list:
        out = []
        for _, s in self.samples:
            if not out or out[-1] != s:
                out.append(s)
        return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default=GATEWAY)
    ap.add_argument("--tag", default="t7")
    ap.add_argument("--offsets", default="0,0.25,0.5,0.75,1.0,1.5",
                    help="seconds after the suspend call to fire the message")
    ap.add_argument("--trials", type=int, default=1, help="repeats per offset")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--out",
                    default=RUN_DIR)
    a = ap.parse_args()

    offsets = [float(x) for x in a.offsets.split(",")]
    cl = t15.Cluster(CONTEXT, ATESPACE)
    results = []

    for trial in range(a.trials):
        for off in offsets:
            key = f"{a.tag}-{trial}-{str(off).replace('.', 'p')}"
            name = t15.actor_name_for(key)
            fact = f"MAGENTA-{trial}-{int(off * 1000)}"

            # Plant a fact and get the actor RUNNING.
            seed = t15.one_turn(
                a.gateway, "t7-seed", key,
                f"Remember this codeword exactly: {fact}. Reply with just: stored",
                a.timeout)
            if not seed.ok:
                results.append({"offset_s": off, "trial": trial, "actor": name,
                                "outcome": "seed failed", "seed": seed.reply})
                continue

            samp = Sampler(cl, name)
            samp.start()
            # Wait for one real sample before racing anything. A kubectl-ate
            # round trip is ~1.6s because it port-forwards per invocation, so
            # without this every offset under two seconds reports "?" and the
            # run scores itself as having missed a window it in fact hit.
            for _ in range(40):
                if samp.samples:
                    break
                time.sleep(0.1)

            suspend_done = threading.Event()
            t0 = time.monotonic()

            def _suspend():
                ate("suspend", "actor", name, "-a", ATESPACE)
                suspend_done.set()

            th = threading.Thread(target=_suspend, daemon=True)
            th.start()

            time.sleep(max(0.0, off))
            fired_at = time.monotonic()
            # Whether the message raced the suspend is decided by the two
            # clocks this process owns - when the suspend call went out and
            # whether it had returned - not by a sampler that cannot poll
            # faster than the window is wide. The sampler corroborates; it does
            # not adjudicate.
            in_flight = not suspend_done.is_set()
            state_when_fired = samp.state_at(fired_at)
            race = t15.one_turn(a.gateway, "t7-race", key,
                                "Reply with exactly: PONG", a.timeout)
            th.join(timeout=180)
            suspend_returned_at = time.monotonic()
            time.sleep(2.0)
            samp.stop()

            # Did anything get lost? Ask for the planted fact back.
            recall = t15.one_turn(
                a.gateway, "t7-recall", key,
                "What was the codeword I asked you to remember? Reply with just "
                "the codeword.", a.timeout)

            trajectory = samp.seen()

            results.append({
                "trial": trial, "offset_s": off, "actor": name,
                "window": "hit" if in_flight else "missed",
                "window_basis": "suspend call had not returned when the "
                                "message was sent",
                "state_when_message_sent": state_when_fired,
                "state_trajectory": trajectory,
                "suspend_to_send_s": round(fired_at - t0, 3),
                "suspend_call_duration_s": round(suspend_returned_at - t0, 3),
                "race_status": race.status, "race_ok": race.ok,
                "race_first_response_s": race.first_response_s,
                "race_reply": race.reply[:120], "race_error": race.error,
                "planted": fact,
                "recall_reply": recall.reply[:120],
                "memory_intact": fact in (recall.reply or ""),
            })
            r = results[-1]
            print(f"off={off:<5} window={r['window']:<7} "
                  f"state={r['state_when_message_sent']!s:<24} "
                  f"race={r['race_status']} {r['race_first_response_s']:6.2f}s "
                  f"reply={r['race_reply'][:20]!r:<24} "
                  f"memory={'ok' if r['memory_intact'] else 'LOST'}", flush=True)

            ate("suspend", "actor", name, "-a", ATESPACE)

    hits = [r for r in results if r.get("window") == "hit"]
    report = {
        "test": "T7", "build": BUILD, "atespace": ATESPACE,
        "cluster": CONTEXT.rsplit("_", 1)[-1],
        "trials": len(results),
        "landed_in_window": len(hits),
        "missed_window": len(results) - len(hits),
        "of_those_that_landed": {
            "delivered_on_same_request": sum(
                1 for r in hits if r["race_ok"] and "PONG" in (r["race_reply"] or "")),
            "errored": sum(1 for r in hits if not r["race_ok"]),
            "by_status": {str(s): sum(1 for r in hits if r["race_status"] == s)
                          for s in sorted({r["race_status"] for r in hits})},
            "memory_intact": sum(1 for r in hits if r["memory_intact"]),
            "memory_lost": sum(1 for r in hits if not r["memory_intact"]),
        },
        "caveat": "trials marked window=missed did not race anything and are "
                  "excluded from the counts above",
        "trials_detail": results,
    }
    out = Path(a.out) / f"{a.tag}-suspend-race.json"
    out.write_text(json.dumps(report, indent=2))
    print("\n" + json.dumps({k: v for k, v in report.items()
                             if k != "trials_detail"}, indent=2))
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
