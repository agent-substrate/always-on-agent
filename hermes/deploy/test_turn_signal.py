#!/usr/bin/env python3
# Copyright 2026 Nous Research
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the turn-signal plugin against a REAL /proc, with real processes.

The offline suite covers the parsing and the tree walk with fabricated data,
which is where the subtle bugs are - but it stubs out the one thing that can
only be wrong on Linux: whether walking /proc actually finds a process the agent
backgrounded. This runs the plugin unmodified, spawns real children, and checks
what it concludes.

    hermesctl test-turn-signal        # runs it inside the gateway pod

Exits non-zero on the first wrong answer, so it works in CI as well as by hand.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import time

FAILURES: "list[str]" = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'}  {label}" + (f"   ({detail})" if detail else ""))
    if not ok:
        FAILURES.append(label)


def load_plugin(path: str):
    spec = importlib.util.spec_from_file_location("turn_signal", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/turn_signal.py"
    if not os.path.exists(path):
        print(f"plugin not found at {path}", file=sys.stderr)
        return 2
    ts = load_plugin(path)

    print(f"turn-signal on {sys.platform}, pid {os.getpid()}")
    if not os.path.isdir("/proc"):
        print("  /proc is absent - this must run on Linux", file=sys.stderr)
        return 2

    sent: "list[int]" = []
    working: "list[int]" = []
    ts._notify = lambda: sent.append(1)
    ts._notify_working = lambda: working.append(1)

    # --- 1. /proc is actually readable and finds our own children ----------
    mine = ts._descendants(os.getpid())
    check("/proc is readable (not the 'cannot tell' path)", mine is not None)
    if mine is None:
        return 1

    child = subprocess.Popen(["sleep", "30"])
    time.sleep(0.3)
    found = ts._descendants(os.getpid())
    check("a backgrounded process is found as our descendant",
          found is not None and child.pid in found, f"pid {child.pid}")

    # --- 2. a turn that spawns nothing signals at once ---------------------
    child.kill(); child.wait()
    time.sleep(0.3)
    sent.clear()
    ts._on_turn_start()
    ts._on_turn_end()
    check("turn that spawned nothing reports done immediately", len(sent) == 1)
    check("turn that spawned nothing does not claim to be working", len(working) == 0)

    # --- 3. a turn that leaves work running does NOT signal ---------------
    # Shorten the poll BEFORE the watcher starts: its first wait is whatever
    # POLL_SECONDS was when the thread entered it, so setting this afterwards
    # leaves the thread asleep for the full 30s and the next two checks pass or
    # fail for reasons that have nothing to do with the logic.
    ts.POLL_SECONDS = 0.2
    sent.clear()
    ts._on_turn_start()
    bg = subprocess.Popen(["sleep", "30"])       # spawned DURING the turn
    time.sleep(0.3)
    ts._on_turn_end()
    check("turn that left a process running does NOT report done",
          len(sent) == 0, f"spawned pid {bg.pid}")
    # The half a live run caught: withholding "done" achieves nothing unless the
    # gateway is told to hold the actor, because the only other thing that tells
    # it is the turn request timing out - which a fast turn never does.
    check("turn that left a process running DOES assert still-working",
          len(working) == 1)

    # --- 4. ...and signals once that work drains --------------------------
    bg.kill(); bg.wait()
    deadline = time.time() + 10
    while time.time() < deadline and not sent:
        time.sleep(0.2)
    check("reports done once the spawned work finishes", len(sent) == 1,
          "watcher thread fired")

    # --- 5. a newer turn supersedes a pending watch ------------------------
    sent.clear()
    ts._on_turn_start()
    bg2 = subprocess.Popen(["sleep", "30"])
    time.sleep(0.3)
    ts._on_turn_end()                       # starts a watcher for this turn
    ts._on_turn_start()                     # a NEW turn begins: epoch moves
    bg2.kill(); bg2.wait()
    # Long enough for several poll cycles: with POLL_SECONDS still at 0.2 the
    # watcher HAS woken and seen the work drain, so silence here is the epoch
    # guard doing its job rather than the thread simply not having run yet.
    time.sleep(2.0)
    check("a newer turn cancels the old watch instead of signalling",
          len(sent) == 0)

    # --- 6. concurrent turns, with real processes -------------------------
    # The offline suite covers per-turn keying with fabricated pids. This is the
    # same logic against a real process tree, which is where an id mix-up would
    # actually show up: two turns in flight, one leaves work behind, the other
    # finishes clean, and the clean one must not release the other's hold.
    ts._active.clear(); ts._watching.clear()
    sent.clear(); working.clear()

    ts._on_turn_start(turn_id="slow")
    slow = subprocess.Popen(["sleep", "30"])       # spawned by "slow"
    time.sleep(0.3)
    ts._on_turn_start(turn_id="quick")
    ts._on_turn_end(turn_id="slow")                # leaves the process running
    check("the turn that spawned work asserts still-working", len(working) == 1)
    check("it does not report done", len(sent) == 0)

    ts._on_turn_end(turn_id="quick")               # spawned nothing itself
    check("a clean turn does not release another turn's work",
          len(sent) == 0, f"spawned pid {slow.pid} still alive")

    slow.kill(); slow.wait()
    deadline = time.time() + 10
    while time.time() < deadline and not sent:
        time.sleep(0.2)
    check("done is reported once the last work actually drains", len(sent) == 1)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        return 1
    print("all turn-signal checks passed against a real /proc")
    return 0


if __name__ == "__main__":
    sys.exit(main())
