#!/usr/bin/env python3
"""How many workers the pass 2 fleet needs, from the duty cycles in fleet.json.

A worker holds exactly one non-suspended actor, so `WorkerPool.replicas` is the
ceiling on how many agents can be awake at the same moment. Set it too low and
the (N+1)th conversation waits out `WORKER_WAIT_SECONDS` and then fails; set it
high and the soak proves nothing about density, which is half of what pass 2 is
for.

**The number cannot be read off the message counts, and that is the whole point
of this script.** An agent is awake for its turn *plus its idle timeout*, and on
the long tier that timeout is an hour against a median gap of under two. An agent
receiving four messages a workday can be awake for more than half of its working
window. Sizing on "four messages a day is nearly nothing" underprovisions the
pool by a factor of several, and the failure it produces is not a queue: it is
the gateway retrying onto a fresh actor, so the user sees the agent forget them.

## What is modelled

  - arrivals inside each agent's own active hours, in its own timezone, at the
    stated median gap, on its stated active days
  - the idle timeout for its tier, which is what actually holds the worker
  - cron jobs, which fire whether or not anyone is talking to the agent
  - always-on agents, which never release a worker at all

## What is not, and which way each one is wrong

  - Turn duration is a flat `--turn-seconds`. Real turns vary with the model
    call; against idle timeouts of 15 and 60 minutes it moves the answer very
    little, which is checkable by running with 0 and with 300.
  - Restore and suspend transitions are treated as instant. A worker is really
    held a little longer than modelled, so the answer is a floor.
  - Retries and the cold-node trap are not modelled. Both consume workers.
  - The golden actor and any prewarm actors are not counted. They are transient,
    but warming needs free workers, so it must not be run against a full pool.

Every one of those omissions pushes the same way: the demand here is a lower
bound. That is why the recommendation adds headroom rather than taking the peak.

**The example duty cycles are estimates.** Inputs marked `measured: false` are
assumptions, not observations. The sensitivity table shows how pool sizing changes
with those assumptions; it does not establish production workload measurements.

Usage:
    ./pool_sizing.py demo/fleet/fleet.example.json
    ./pool_sizing.py demo/fleet/fleet.example.json --trials 2000 --turn-seconds 300
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Seconds a worker stays held after the last turn, per tier. These mirror the
# gateway's own tiers (see IDLE_TIER_DEFAULT in k8s/hermes-gateway.yaml); change
# them there and here together or the pool is sized for a policy that is not
# running.
TIERS = {"always-on": None, "short": 60, "medium": 15 * 60, "long": 60 * 60}

DAYS = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


def parse_days(spec: str) -> set[int]:
    """'Mon-Fri' or 'Mon,Wed,Fri' to weekday numbers."""
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = (DAYS[x.strip()] for x in part.split("-"))
            out |= set(range(a, b + 1))
        elif part:
            out.add(DAYS[part])
    return out


def parse_hhmm(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def cron_minutes(expr: str, tz: ZoneInfo, day0: datetime, days: int) -> list[float]:
    """Fire times, in UTC seconds from day0, for the subset of cron we use.

    Only `M H * * <dow>` is supported, which is every job in the fleet file. A
    field this cannot read raises rather than being skipped: a cron job silently
    dropped from the model is a wake the pool was never sized for.
    """
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron {expr!r}: want 5 fields")
    minute, hour, dom, mon, dow = parts
    if dom != "*" or mon != "*":
        raise ValueError(f"cron {expr!r}: day-of-month and month must be '*'")
    if not minute.isdigit() or not hour.isdigit():
        raise ValueError(f"cron {expr!r}: minute and hour must be literal")
    if dow == "*":
        want = set(range(7))
    else:
        want = set()
        for part in dow.split(","):
            if "-" in part:
                a, b = (int(x) for x in part.split("-"))
                want |= set(range(a, b + 1))
            else:
                want.add(int(part))
        # cron counts Sunday as 0; Python counts Monday as 0.
        want = {(d - 1) % 7 for d in want}
    out = []
    for d in range(days):
        local = (day0.astimezone(tz) + timedelta(days=d)).replace(
            hour=int(hour), minute=int(minute), second=0, microsecond=0)
        if local.weekday() in want:
            out.append((local.astimezone(day0.tzinfo) - day0).total_seconds())
    return out


def agent_intervals(agent: dict, defaults: dict, day0: datetime, days: int,
                    turn_s: float, rng: random.Random,
                    uniform: bool) -> list[tuple[float, float]]:
    """Merged [start, end) windows where this agent holds a worker."""
    tier = agent.get("idle_tier") or defaults.get("idle_tier", "medium")
    if tier not in TIERS:
        raise ValueError(f"{agent['id']}: unknown idle tier {tier!r}")
    idle = TIERS[tier]
    horizon = days * 86400.0
    if idle is None:
        return [(0.0, horizon)]          # always-on never gives the worker back

    tz = ZoneInfo(agent.get("timezone") or defaults.get("timezone", "UTC"))
    duty = agent.get("duty_cycle") or {}

    def need(field: str) -> str:
        if field not in duty:
            raise ValueError(f"{agent['id']}: duty_cycle is missing {field}")
        return duty[field]

    start_m, end_m = (parse_hhmm(x) for x in need("active_hours").split("-"))
    active = parse_days(duty.get("active_days", "Mon-Fri"))
    per_day = float(duty.get("messages_per_workday", 0))
    # Only the gaps reading uses this. Requiring it in both is how a fleet file
    # that states counts and no gaps gets refused for a field it never needed.
    gap_min = 0.0 if uniform else float(need("median_gap_minutes"))

    starts: list[float] = []
    for d in range(days):
        local = (day0.astimezone(tz) + timedelta(days=d)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        if local.weekday() not in active:
            continue
        window_start = (local + timedelta(minutes=start_m)).astimezone(
            day0.tzinfo) - day0
        t0 = window_start.total_seconds()
        span = (end_m - start_m) * 60.0
        if uniform:
            # The count is fixed and the placement is random. This is the reading
            # that trusts messages_per_workday.
            starts += [t0 + rng.uniform(0, span) for _ in range(int(per_day))]
        else:
            # Gaps are exponential with the stated median, and the count falls
            # out. This is the reading that trusts median_gap_minutes. The two
            # fields do not perfectly agree in the fleet file, so both are run.
            t = t0
            while True:
                t += rng.expovariate(1.0) * gap_min * 60.0 / 0.6931471805599453
                if t - t0 > span:
                    break
                starts.append(t)

    for job in agent.get("jobs") or []:
        starts += cron_minutes(job["cron"], tz, day0, days)

    if not starts:
        return []
    starts.sort()
    hold = turn_s + idle
    merged: list[list[float]] = []
    for s in starts:
        if merged and s <= merged[-1][1]:
            merged[-1][1] = s + hold      # a turn inside the idle window extends it
        else:
            merged.append([s, s + hold])
    return [(a, min(b, horizon)) for a, b in merged if a < horizon]


def peak(intervals: list[tuple[float, float]]) -> int:
    """Maximum simultaneous holders, by sweeping the endpoints.

    Ends are processed before starts at an equal timestamp: a worker released at
    exactly the moment another actor wakes is available to it, and counting the
    tie the other way inflates every peak by the number of exact collisions.
    """
    events = [(t, +1) for t, _ in intervals] + [(t, -1) for _, t in intervals]
    events.sort(key=lambda e: (e[0], e[1]))
    cur = best = 0
    for _, delta in events:
        cur += delta
        best = max(best, cur)
    return best


def occupancy(intervals: list[tuple[float, float]], horizon: float) -> float:
    """Mean workers held, which is what the pool costs rather than what it needs."""
    return sum(b - a for a, b in intervals) / horizon if horizon else 0.0


def simulate(fleet: dict, trials: int, days: int, turn_s: float,
             uniform: bool, seed: int) -> dict:
    rng = random.Random(seed)
    day0 = datetime(2026, 9, 7, tzinfo=ZoneInfo("UTC"))   # a Monday
    defaults = fleet.get("defaults") or {}
    peaks, means = [], []
    per_agent_hold: dict[str, list[float]] = {}
    for _ in range(trials):
        allint: list[tuple[float, float]] = []
        for a in fleet["agents"]:
            iv = agent_intervals(a, defaults, day0, days, turn_s, rng, uniform)
            allint += iv
            per_agent_hold.setdefault(a["id"], []).append(
                occupancy(iv, days * 86400.0))
        peaks.append(peak(allint))
        means.append(occupancy(allint, days * 86400.0))
    return {"peaks": peaks, "means": means, "per_agent": per_agent_hold}


def pct(v: list[float], p: float) -> float:
    s = sorted(v)
    return s[min(len(s) - 1, int(round(p / 100.0 * (len(s) - 1))))]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("fleet")
    ap.add_argument("--trials", type=int, default=1000)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--turn-seconds", type=float, default=45.0)
    ap.add_argument("--seed", type=int, default=20260905)
    ap.add_argument("--headroom", type=int, default=2,
                    help="workers above the simulated worst case. The model is a "
                         "floor (see the docstring), and a pool at exactly its "
                         "peak has nowhere to warm and nowhere to retry")
    a = ap.parse_args()

    with open(a.fleet) as f:
        fleet = json.load(f)
    n = len(fleet["agents"])
    defaults = fleet.get("defaults") or {}

    print(f"{a.fleet}: {n} agents, {a.days}-day weeks, {a.trials} trials, "
          f"turn {a.turn_seconds:.0f}s\n")

    print("PER AGENT  (share of the week holding a worker)")
    print(f"{'agent':22s} {'tier':10s} {'idle':>7s} {'msg/day':>8s} "
          f"{'gap min':>8s} {'held':>7s}")
    base = simulate(fleet, a.trials, a.days, a.turn_seconds, False, a.seed)
    for ag in fleet["agents"]:
        tier = ag.get("idle_tier") or defaults.get("idle_tier", "medium")
        idle = TIERS[tier]
        duty = ag.get("duty_cycle") or {}
        held = statistics.mean(base["per_agent"][ag["id"]])
        print(f"{ag['id']:22s} {tier:10s} "
              f"{('always' if idle is None else f'{idle // 60}m'):>7s} "
              f"{duty.get('messages_per_workday', '-'):>8} "
              f"{duty.get('median_gap_minutes', '-'):>8} {held * 100:6.1f}%")

    print("\nPEAK CONCURRENT AWAKE")
    rows = []
    for label, uniform in (("gaps (median_gap_minutes)", False),
                           ("counts (messages_per_workday)", True)):
        r = base if not uniform else simulate(
            fleet, a.trials, a.days, a.turn_seconds, True, a.seed)
        rows.append((label, r))
        p = r["peaks"]
        print(f"  {label:32s} p50 {pct(p, 50):3.0f}   p95 {pct(p, 95):3.0f}   "
              f"max {max(p):3.0f}   mean held {statistics.mean(r['means']):.1f}")

    worst = max(max(r["peaks"]) for _, r in rows)
    rec = worst + a.headroom
    print(f"\n  worst case across both readings: {worst} of {n} agents awake at once")
    print(f"  recommended WorkerPool.replicas: {rec}   ({worst} + {a.headroom} headroom)")
    held = statistics.mean(base["means"])
    print(f"  mean workers held across the week: {held:.1f} of {n} agents")
    print(f"  so the pool has to be sized for {worst} and is used for {held:.1f}. "
          f"Peak is {worst / held:.0f}x the mean,")
    print("  which is the density result: suspension pays on cost, not on the "
          "ceiling.")
    if rec >= n:
        print(f"  {rec} is the whole fleet or more. At these idle timeouts "
              "suspension buys")
        print("  nothing at all on peak sizing. Say that plainly rather than "
              "reporting a ratio.")

    print("\nSENSITIVITY  (worst-case peak as the turn length moves)")
    for t in (0.0, 45.0, 300.0):
        r = simulate(fleet, max(200, a.trials // 5), a.days, t, False, a.seed)
        print(f"  turn {t:5.0f}s -> peak {max(r['peaks']):3.0f}")
    print("  If these barely move, the idle timeouts set the answer and the turn")
    print("  length does not, which is the expected shape.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
