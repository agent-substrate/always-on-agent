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
"""Oversubscription, measured rather than asserted.

Two subcommands, and they are meant to be used in that order.

  measure   Reconstruct how long actors actually held a worker, from the
            control plane's own RPC log. Reports achieved density and peak
            concurrency over the observed window.

  model     Take a per-wake occupancy in seconds (from `measure`, or from a
            workload you are describing) and project what a cron-shaped
            workload would need. Answers "N agents on a cron every P, how many
            workers" without hand-waving.

Why the split. The average density of a sparse workload is the inverse of its
duty cycle and nothing more, so it can be made arbitrarily large by making the
cron rarer. It is not, on its own, a claim about the system. What the system is
responsible for is what happens at the peak, and the peak depends entirely on
whether the schedule is aligned. `model` exists to make that visible, because
every cron in the world fires at :00 unless somebody stops it.

Usage:

  # everything ate-api-server still has in its ring buffer
  kubectl -n ate-system logs -l app=ate-api-server --tail=-1 \\
    | demo/measure/density.py measure --atespace openclaw-demo

  # project a fleet from the occupancy that came back
  demo/measure/density.py model --occupancy 10.5 --actors 1000 --period 15m
"""

import argparse
import json
import random
import re
import statistics
import sys
from datetime import datetime, timezone

RESUME = "/ateapi.Control/ResumeActor"
SUSPEND = "/ateapi.Control/SuspendActor"


def parse_time(s):
    """RFC3339 with variable-width fractional seconds, which fromisoformat
    refuses on older Pythons and mishandles on some newer ones."""
    m = re.match(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z?$", s.strip())
    if not m:
        return None
    base = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    frac = float("0." + m.group(2)) if m.group(2) else 0.0
    return base.timestamp() + frac


def parse_duration(s):
    """'15m', '2h', '90s', or a bare number of seconds."""
    m = re.match(r"^([\d.]+)\s*([smhd]?)$", str(s).strip().lower())
    if not m:
        raise argparse.ArgumentTypeError(f"cannot read {s!r} as a duration")
    n = float(m.group(1))
    return n * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]


def human(sec):
    if sec >= 86400:
        return f"{sec / 86400:.1f}d"
    if sec >= 3600:
        return f"{sec / 3600:.1f}h"
    if sec >= 60:
        return f"{sec / 60:.1f}m"
    return f"{sec:.1f}s"


def read_events(stream, atespace):
    """Pull (timestamp, actor, kind) out of the ate-api-server JSON log.

    Lines that are not JSON are skipped rather than fatal: `kubectl logs
    --prefix` and any interleaved plain-text startup banner both end up in the
    same pipe, and dropping one line is better than refusing the run.
    """
    events = []
    skipped = 0
    for line in stream:
        line = line.strip()
        # --prefix puts `[pod/name/container]` in front of the JSON.
        brace = line.find("{")
        if brace == -1:
            continue
        try:
            rec = json.loads(line[brace:])
        except json.JSONDecodeError:
            skipped += 1
            continue
        method = rec.get("method")
        if method not in (RESUME, SUSPEND):
            continue
        # A failed RPC did not move the actor, so counting it would invent
        # occupancy that never happened.
        if rec.get("err"):
            continue
        actor = (rec.get("req") or {}).get("actor") or {}
        if atespace and actor.get("atespace") != atespace:
            continue
        ts = parse_time(rec.get("time", ""))
        if ts is None:
            continue
        name = actor.get("name")
        if not name:
            continue
        events.append((ts, name, "resume" if method == RESUME else "suspend"))
    events.sort(key=lambda e: e[0])
    return events, skipped


def build_intervals(events):
    """Pair each resume with the next suspend of the same actor.

    Two cases are deliberately not errors. An actor still running when the log
    ends has an open interval, which is closed at the window edge and flagged,
    because dropping it would understate busy time. A suspend with no matching
    resume belongs to a wake that happened before the window started, and is
    ignored rather than back-dated to the window edge, which would invent
    occupancy out of a log truncation.
    """
    open_at = {}
    intervals = []
    orphan_suspends = 0
    for ts, name, kind in events:
        if kind == "resume":
            # A second resume with no suspend between means the first interval
            # was closed by something the control plane did not log. Keep the
            # later one; the earlier is unmeasurable.
            open_at[name] = ts
        else:
            start = open_at.pop(name, None)
            if start is None:
                orphan_suspends += 1
                continue
            intervals.append((start, ts, name))
    return intervals, open_at, orphan_suspends


def concurrency(intervals):
    """Sweep line. Returns (peak, list of (t, level)) for the busy-worker count."""
    points = []
    for start, end, _ in intervals:
        points.append((start, 1))
        points.append((end, -1))
    # Close before open at the same instant: an actor releasing a worker at
    # exactly the moment another takes one does not need two workers.
    points.sort(key=lambda p: (p[0], p[1]))
    level = 0
    peak = 0
    trace = []
    for t, delta in points:
        level += delta
        peak = max(peak, level)
        trace.append((t, level))
    return peak, trace


def cmd_measure(args):
    events, skipped = read_events(sys.stdin, args.atespace)
    if not events:
        print("No ResumeActor/SuspendActor records found.", file=sys.stderr)
        print(
            "Check the atespace filter, and that the log window still reaches "
            "back to when something ran.",
            file=sys.stderr,
        )
        return 1

    t_start, t_end = events[0][0], events[-1][0]
    window = t_end - t_start
    intervals, still_open, orphan_suspends = build_intervals(events)

    # Close anything still running at the window edge, so its time counts.
    for name, start in still_open.items():
        intervals.append((start, t_end, name))

    if not intervals:
        print("Found events but no complete resume/suspend pair.", file=sys.stderr)
        return 1

    durations = sorted(e - s for s, e, _ in intervals)
    busy_worker_sec = sum(durations)
    actors = sorted({n for _, _, n in intervals})
    peak, _ = concurrency(intervals)
    mean_conc = busy_worker_sec / window if window > 0 else 0.0

    def pct(p):
        if not durations:
            return 0.0
        k = min(len(durations) - 1, int(round((p / 100.0) * (len(durations) - 1))))
        return durations[k]

    print(f"Window                {human(window)}  "
          f"({datetime.fromtimestamp(t_start, timezone.utc):%Y-%m-%d %H:%M:%S}Z "
          f"to {datetime.fromtimestamp(t_end, timezone.utc):%H:%M:%S}Z)")
    print(f"Atespace              {args.atespace or 'all'}")
    print(f"Distinct actors       {len(actors)}")
    print(f"Wakes measured        {len(intervals)}"
          + (f"  ({len(still_open)} still running at window end)" if still_open else ""))
    if orphan_suspends:
        print(f"Unpaired suspends     {orphan_suspends} (woken before the window; ignored)")
    if skipped:
        print(f"Unparseable lines     {skipped}")
    print()
    print("Occupancy per wake    how long one wake holds one worker")
    print(f"  mean                {statistics.mean(durations):.1f}s")
    print(f"  P50                 {pct(50):.1f}s")
    print(f"  P95                 {pct(95):.1f}s")
    print(f"  max                 {max(durations):.1f}s")
    print()
    print("Demand")
    print(f"  busy worker-time    {human(busy_worker_sec)} over a {human(window)} window")
    print(f"  mean concurrency    {mean_conc:.2f} workers")
    print(f"  peak concurrency    {peak} workers")
    if mean_conc > 0:
        print(f"  achieved density    {len(actors) / mean_conc:.1f}:1  "
              f"({len(actors)} actors on {mean_conc:.2f} workers of average demand)")
    duty = busy_worker_sec / (len(actors) * window) if actors and window else 0
    print(f"  duty cycle          {100 * duty:.2f}% of wall time per actor")
    print()
    # The line that stops the headline number being quoted on its own.
    if mean_conc > 0:
        print(f"Read it as: the {len(actors) / mean_conc:.1f}:1 is 1/duty-cycle and grows as the")
        print(f"agents get quieter. Sizing the pool is the peak, which was {peak}.")
    if args.occupancy_out:
        print()
        print(f"Feed to model: --occupancy {statistics.mean(durations):.1f}")
    return 0


def cmd_model(args):
    occ = args.occupancy
    n = args.actors
    period = args.period
    if occ >= period:
        print("Occupancy is at least the cron period, so every actor is always "
              "awake and there is nothing to oversubscribe.", file=sys.stderr)
        return 1

    mean_conc = n * occ / period
    ratio = n / mean_conc  # == period / occ

    print(f"Fleet                 {n} actors")
    print(f"Cron period           {human(period)} per actor")
    print(f"Occupancy per firing  {occ:.1f}s")
    print()
    print(f"Mean worker demand    {mean_conc:.2f} workers")
    print(f"Average density       {ratio:.1f}:1")
    print()
    print("Peak is the number that sizes the pool, and it depends entirely on")
    print("whether the schedules collide.")
    print()
    print(f"  aligned (all at :00)   {n} workers, or everything after the first")
    print(f"                         few queues behind a {occ:.1f}s wake")
    print(f"                         achieved density at the peak: 1.0:1")

    # Jitter: each actor picks a uniform offset in [0, period). Concurrency is
    # the number of wakes covering an instant. Simulated rather than closed-form
    # because the useful figure is a high quantile of the peak, not the mean,
    # and the binomial tail is what people get wrong by eye.
    # Concurrency only ever changes at a wake, so the peak is reached at one of
    # the offsets: for each, count the wakes still holding a worker, which is
    # the offsets falling in the circular window (o - occ, o]. Binary search
    # rather than a sweep because the window wraps the period boundary, and an
    # actor that wakes just before the boundary is still busy after it.
    from bisect import bisect_right

    rng = random.Random(args.seed)
    peaks = []
    for _ in range(args.trials):
        offsets = sorted(rng.random() * period for _ in range(n))
        peak = 0
        for o in offsets:
            lo = o - occ
            if lo >= 0:
                c = bisect_right(offsets, o) - bisect_right(offsets, lo)
            else:
                c = bisect_right(offsets, o) + (n - bisect_right(offsets, period + lo))
            if c > peak:
                peak = c
        peaks.append(peak)
    peaks.sort()

    def q(p):
        return peaks[min(len(peaks) - 1, int(round((p / 100.0) * (len(peaks) - 1))))]

    print()
    print(f"  jittered over the period, {args.trials} trials")
    print(f"    median peak          {q(50)} workers")
    print(f"    P99 peak             {q(99)} workers")
    print(f"    worst seen           {peaks[-1]} workers")
    print(f"    achieved density at P99 peak: {n / max(1, q(99)):.1f}:1")
    print()
    print(f"So the pool is {q(99)} workers jittered against {n} aligned, and the")
    print(f"density you can actually bank is {n / max(1, q(99)):.1f}:1, not {ratio:.1f}:1.")
    print("Spreading the schedule is a scheduling decision rather than anything")
    print("the platform can do for you, and it is what decides whether the")
    print("average is reachable at all.")
    return 0


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("measure", help="reconstruct occupancy from ate-api-server logs (stdin)")
    m.add_argument("--atespace", default="openclaw-demo",
                   help="only count actors in this atespace; empty string for all")
    m.add_argument("--occupancy-out", action="store_true",
                   help="print the --occupancy flag to hand to `model`")
    m.set_defaults(func=cmd_measure)

    d = sub.add_parser("model", help="project a cron-shaped workload")
    d.add_argument("--occupancy", type=float, required=True,
                   help="seconds one wake holds a worker (from `measure`)")
    d.add_argument("--actors", type=int, required=True, help="fleet size")
    d.add_argument("--period", type=parse_duration, required=True,
                   help="cron period per actor, e.g. 15m")
    d.add_argument("--trials", type=int, default=2000, help="jitter simulation trials")
    d.add_argument("--seed", type=int, default=0, help="RNG seed, for a repeatable table")
    d.set_defaults(func=cmd_model)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
