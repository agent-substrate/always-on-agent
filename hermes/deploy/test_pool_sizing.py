"""Exercise pool_sizing before its answer is used to size a node pool.

Run it from anywhere:  python3 deploy/test_pool_sizing.py

The output of pool_sizing is a single integer that gets typed into
k8s/hermes-workerpool.yaml, and a wrong one is not visible in the cluster: too
low and the fleet quietly queues behind a worker that never frees, too high and
the density result is reported against a pool nobody needed. So the parts that
turn a fleet file into worker-holding intervals are pinned here, one property at
a time, rather than checked by eye against the summary table.
"""
import importlib.util
import random
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("ps", _HERE / "pool_sizing.py")
ps = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ps)

fails = []


def check(label, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + label + (f"   {extra}" if extra and not cond else ""))
    if not cond:
        fails.append(label)


UTC = ZoneInfo("UTC")
LA = ZoneInfo("America/Los_Angeles")
# A Monday, so weekday arithmetic below reads straight off the day offset.
DAY0 = datetime(2026, 9, 7, 0, 0, tzinfo=UTC)

# ---- day and time parsing ----
check("Mon-Fri is five weekdays", ps.parse_days("Mon-Fri") == {0, 1, 2, 3, 4})
check("a comma list is taken literally", ps.parse_days("Mon,Wed,Fri") == {0, 2, 4})
check("hh:mm is minutes past midnight", ps.parse_hhmm("08:45") == 525)

# ---- cron ----
fires = ps.cron_minutes("45 8 * * 1-5", UTC, DAY0, 7)
check("a weekday cron fires five times in a week", len(fires) == 5, len(fires))
check("the first fire is 08:45 on day 0", fires[0] == 8 * 3600 + 45 * 60, fires[0])
check("fires are one day apart", all(
    abs((b - a) - 86400) < 1 for a, b in zip(fires, fires[1:])))

check("'*' fires every day", len(ps.cron_minutes("0 9 * * *", UTC, DAY0, 7)) == 7)

# cron counts Sunday as 0, Python counts Monday as 0. Getting this off by one
# puts a Monday brief on a Sunday, where it wakes an agent nobody is reading.
sunday = ps.cron_minutes("0 9 * * 0", UTC, DAY0, 7)
check("cron dow 0 is Sunday, and Sunday is day 6", len(sunday) == 1 and
      abs(sunday[0] - (6 * 86400 + 9 * 3600)) < 1, sunday)
check("cron dow 1 is Monday, and Monday is day 0",
      abs(ps.cron_minutes("0 9 * * 1", UTC, DAY0, 7)[0] - 9 * 3600) < 1)

# An agent's cron is written in its own timezone, so the same expression is a
# different UTC instant per agent. Ignoring that stacks ten wakes on one moment
# and invents a peak the fleet never has.
la = ps.cron_minutes("0 9 * * 1", LA, DAY0, 7)
check("a Los Angeles cron fires 7h after the same UTC cron (PDT)",
      abs(la[0] - (9 * 3600 + 7 * 3600)) < 1, la[0] / 3600)

for bad in ("0 9 * *", "0 9 1 * *", "*/15 9 * * 1"):
    try:
        ps.cron_minutes(bad, UTC, DAY0, 7)
        check(f"cron {bad!r} is refused", False)
    except ValueError:
        check(f"cron {bad!r} is refused", True)

# ---- intervals ----
rng = random.Random(1)
always = ps.agent_intervals({"id": "a", "idle_tier": "always-on"}, {},
                            DAY0, 7, 45.0, rng, False)
check("an always-on agent holds a worker for the whole week",
      always == [(0.0, 7 * 86400.0)], always)

try:
    ps.agent_intervals({"id": "a", "idle_tier": "hourly"}, {}, DAY0, 7, 45.0, rng, False)
    check("an unknown idle tier is refused", False)
except ValueError:
    check("an unknown idle tier is refused", True)

# A short-tier agent with one message a day cannot hold a worker for more than
# a turn plus 60s, ten times over. If merging is wrong this comes out at days.
short = ps.agent_intervals(
    {"id": "a", "idle_tier": "short", "timezone": "UTC",
     "duty_cycle": {"messages_per_workday": 1, "active_hours": "09:00-18:00",
                    "active_days": "Mon-Fri"}},
    {}, DAY0, 7, 45.0, rng, True)
held = sum(e - s for s, e in short)
check("a short-tier agent at one message a day holds a worker for minutes, not days",
      0 < held <= 5 * (45.0 + 60.0) + 1, held)
check("intervals are sorted and disjoint after merging",
      all(a[1] <= b[0] for a, b in zip(short, short[1:])), short)

# The counts reading never looks at median_gap_minutes, so a fleet file that
# states one and not the other has to run rather than being refused for a field
# the chosen reading does not use.
counts_only = {"id": "a", "idle_tier": "short", "timezone": "UTC",
               "duty_cycle": {"messages_per_workday": 2,
                              "active_hours": "09:00-18:00",
                              "active_days": "Mon-Fri"}}
try:
    ps.agent_intervals(counts_only, {}, DAY0, 7, 45.0, rng, True)
    check("counts alone are enough for the counts reading", True)
except Exception as e:
    check("counts alone are enough for the counts reading", False, repr(e))

try:
    ps.agent_intervals(counts_only, {}, DAY0, 7, 45.0, rng, False)
    check("but the gaps reading says which field is missing, and whose", False)
except ValueError as e:
    check("but the gaps reading says which field is missing, and whose",
          "median_gap_minutes" in str(e) and "a:" in str(e), str(e))

# ---- peak ----
check("no intervals is a peak of zero", ps.peak([]) == 0)
check("disjoint intervals never stack", ps.peak([(0, 10), (20, 30), (40, 50)]) == 1)
check("three overlapping intervals peak at three",
      ps.peak([(0, 10), (5, 15), (9, 20)]) == 3)
# A worker released at exactly the moment another actor wakes is available to
# it. Counting that as two is how a pool gets sized one too large per handoff.
check("a worker freed at t is reusable at t", ps.peak([(0, 10), (10, 20)]) == 1)

# ---- occupancy ----
check("occupancy is worker-seconds over the horizon",
      abs(ps.occupancy([(0, 50), (0, 50)], 100.0) - 1.0) < 1e-9,
      ps.occupancy([(0, 50), (0, 50)], 100.0))

print()
if fails:
    print(f"{len(fails)} FAILED:")
    for f in fails:
        print("  -", f)
    raise SystemExit(1)
print("ALL PASS")
