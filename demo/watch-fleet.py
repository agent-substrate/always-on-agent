#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A small always-on view of the control plane, for the corner of a screen.

Runs `kubectl ate get workers` and `kubectl ate get actors` on a loop and
renders the result narrow enough to sit under something else. The raw tables are
five and eight columns wide and wrap into noise at that size, so this prints the
two facts the fleet panel is claiming -- which workers are busy, and what is
awake -- and nothing else.

It is a rendering of those commands rather than their output, so the commands it
ran are printed as the section headers. Beat 6 of the demo is somebody not
believing a dashboard, and that only works if what replaces it is checkable.

  demo/watch-fleet.py --atespace openclaw-demo

Each kubectl-ate call sets up a port-forward and costs about 1.3s, so the two
run in parallel and a frame lands in roughly that. With the default interval the
pane refreshes about every three seconds, which is close enough to the
dashboard's own 2s poll that the two do not visibly disagree on camera.
"""

import argparse
import json
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

RESET = "\033[0m"
DIM = "\033[2m"
BOLD = "\033[1m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"


class Style:
    """No-op styling when --no-color is set, so the same f-strings work."""

    def __init__(self, enabled):
        self.on = enabled

    def __call__(self, code, text):
        return f"{code}{text}{RESET}" if self.on else str(text)


def run(cmd, timeout=20):
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return p.stdout, p.returncode
    except subprocess.TimeoutExpired:
        return "", 124


def short(pod):
    """Last dash-segment of a pod name. `openclaw-6bfd48d59-5k8gq` -> `5k8gq`.

    The generate-name prefix is identical on every worker, so it is the only
    part that carries no information and the only part worth dropping when the
    pane is 60 columns wide.
    """
    return pod.rsplit("-", 1)[-1] if pod and pod != "-" else pod


def get_workers(kubectl_ate):
    out, rc = run(f"{kubectl_ate} get workers 2>/dev/null")
    if rc != 0 or not out.strip():
        return None
    workers = []
    for line in out.splitlines():
        parts = line.split()
        # NAMESPACE POOL CLASS POD STATUS
        if len(parts) < 5 or parts[0] == "NAMESPACE":
            continue
        workers.append({"pod": parts[-2], "status": parts[-1]})
    return workers


def get_actors(kubectl_ate, atespace):
    out, rc = run(f"{kubectl_ate} get actors -a {atespace} -o json 2>/dev/null")
    if rc != 0 or not out.strip().startswith("{"):
        return None
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return None
    actors = []
    for a in data.get("actors") or []:
        md = a.get("metadata") or {}
        st = a.get("status") or {}
        # workerAssignment is absent while suspended, which is the resting
        # state rather than a missing field.
        wa = st.get("workerAssignment") or {}
        actors.append({
            "name": md.get("name", "?"),
            "state": str(st.get("state", "")).replace("ACTOR_STATE_", "") or "UNKNOWN",
            "pod": wa.get("workerPod", ""),
        })
    return actors


def render(workers, actors, atespace, s, width):
    lines = []
    stamp = datetime.now().strftime("%H:%M:%S")

    hdr = "kubectl ate get workers"
    lines.append(s(CYAN, hdr) + " " * max(1, width - len(hdr) - len(stamp)) + s(DIM, stamp))

    if workers is None:
        lines.append("  " + s(RED, "control plane unreachable"))
    else:
        # Which actor is on which worker, from the actor side. `get workers`
        # reports FREE or BUSY but not the occupant, and the occupant is the
        # half that makes the pane worth looking at.
        by_pod = {a["pod"]: a["name"] for a in (actors or []) if a.get("pod")}
        for w in workers:
            occupant = by_pod.get(w["pod"], "")
            busy = w["status"].upper() != "FREE" or occupant
            if busy:
                label = s(GREEN, "BUSY")
                who = "  " + s(BOLD, occupant) if occupant else ""
            else:
                label = s(DIM, "FREE")
                who = ""
            lines.append(f"  {short(w['pod']):<8} {label}{who}")

    lines.append("")
    hdr2 = f"kubectl ate get actors -a {atespace}"
    lines.append(s(CYAN, hdr2))

    if actors is None:
        lines.append("  " + s(RED, "control plane unreachable"))
        return lines

    counts = {}
    for a in actors:
        counts[a["state"]] = counts.get(a["state"], 0) + 1
    total = len(actors)
    order = ["RUNNING", "RESUMING", "SUSPENDING", "SUSPENDED"]
    parts = [f"{s(BOLD, total)} actors"]
    for k in order + [k for k in counts if k not in order]:
        if counts.get(k):
            colour = GREEN if k in ("RUNNING", "RESUMING") else DIM
            parts.append(s(colour, f"{counts[k]} {k}"))
    lines.append("  " + "   ".join(parts))

    awake = [a for a in actors if a["state"] in ("RUNNING", "RESUMING", "SUSPENDING")]
    if not awake:
        lines.append("  " + s(DIM, "all checkpointed, nothing on a worker"))
        return lines

    # Only the awake actors the workers block does not already account for.
    # Listing the rest again says the same thing twice in a pane chosen for
    # being small, and during a burst the duplicate list is the part that
    # overflows. What is left is the interesting case: an actor that is coming
    # up but has not been placed yet.
    placed = {w["pod"] for w in (workers or [])}
    unplaced = [a for a in awake if not a["pod"] or a["pod"] not in placed]
    for a in unplaced[:4]:
        lines.append(f"  {s(YELLOW, a['state'].lower()):<12} {a['name']} "
                     + s(DIM, "(no worker yet)"))
    if len(unplaced) > 4:
        lines.append("  " + s(DIM, f"... and {len(unplaced) - 4} more waiting"))
    return lines


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--atespace", default="openclaw-demo")
    p.add_argument("--kubectl-ate", default="kubectl-ate",
                   help="path to the pinned CLI; a main-built one segfaults on `get workers`")
    p.add_argument("--interval", type=float, default=1.5)
    p.add_argument("--no-color", action="store_true")
    p.add_argument("--once", action="store_true", help="render one frame and exit")
    p.add_argument("--title", default="control plane",
                   help="terminal window title; the default replaces a cwd that "
                        "would otherwise sit in the title bar for the whole recording")
    args = p.parse_args()

    s = Style(not args.no_color and sys.stdout.isatty())
    width = min(shutil.get_terminal_size((64, 20)).columns, 72)

    # Both calls at once. Each spends most of its ~1.3s setting up a
    # port-forward and waiting, so running them in sequence doubles the time the
    # pane is stale for no reason. Overlapped, a frame costs about as much as
    # one call.
    pool = ThreadPoolExecutor(max_workers=2)

    if not args.once and sys.stdout.isatty():
        # Wipe the screen and scrollback before the first frame, and rename the
        # window. The first frame takes about as long as one CLI call, and until
        # it lands the shell prompt and the command that started this are still
        # on screen; the prompt carries a username, a hostname and a working
        # directory, and the title bar carries the directory for the whole
        # recording. Painting over them a second and a half later is too late if
        # that second and a half is the cold open.
        sys.stdout.write("\033[2J\033[3J\033[H")
        sys.stdout.write(f"\033]0;{args.title}\007")
        sys.stdout.flush()

    try:
        while True:
            fw = pool.submit(get_workers, args.kubectl_ate)
            fa = pool.submit(get_actors, args.kubectl_ate, args.atespace)
            workers, actors = fw.result(), fa.result()
            lines = render(workers, actors, args.atespace, s, width)
            if args.once:
                print("\n".join(lines))
                return 0
            # Home, draw, then clear to end of screen. Clearing first gives a
            # visible flash every cycle, which is exactly the sort of thing that
            # pulls the eye away from the panel the demo is about.
            sys.stdout.write("\033[H" + "\n".join(lines) + "\033[J")
            sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        sys.stdout.write("\n")
        return 0


if __name__ == "__main__":
    sys.exit(main())
