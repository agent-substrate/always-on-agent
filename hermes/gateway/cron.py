# Copyright 2026 Google LLC
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

"""Minimal 5-field cron parser - enough for scheduled agent work, nothing more.

Why hand-rolled rather than `croniter`: the gateway's whole dependency list is
aiohttp, and it ships into a lean actor-adjacent image. Sixty lines of parsing is a
better trade than a dependency for one feature, and it keeps the semantics visible
to the reader - including the one that surprises people (see `matches`).

Field order is the standard one:

    ┌───────────── minute        0-59
    │ ┌─────────── hour          0-23
    │ │ ┌───────── day of month  1-31
    │ │ │ ┌─────── month         1-12
    │ │ │ │ ┌───── day of week   0-6, Sunday = 0 (7 also accepted)
    * * * * *

Each field accepts `*`, `N`, `a,b,c`, `a-b`, `*/n` and `a-b/n`.

Times are timezone-aware. A job may name an IANA zone (`Europe/London`); if the
container has no tzdata the zone falls back to UTC with a warning rather than
failing the job, because a scheduled brief arriving at the wrong hour is a much
smaller problem than one that never arrives at all.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

log = logging.getLogger("gateway.cron")

# (name, lo, hi) per field, in cron order.
_FIELDS = (
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day", 1, 31),
    ("month", 1, 12),
    ("weekday", 0, 6),
)

# Give up looking for the next fire time beyond this. A spec like "0 0 30 2 *"
# (February 30th) never matches, and we would otherwise search forever.
_HORIZON_DAYS = 366


class CronError(ValueError):
    pass


def _parse_field(raw: str, name: str, lo: int, hi: int) -> frozenset:
    """Expand one cron field into the set of values it matches."""
    out: set = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            raise CronError(f"{name}: empty term in {raw!r}")

        step = 1
        if "/" in part:
            part, _, step_s = part.partition("/")
            try:
                step = int(step_s)
            except ValueError:
                raise CronError(f"{name}: bad step {step_s!r}") from None
            if step < 1:
                raise CronError(f"{name}: step must be >= 1, got {step}")

        if part == "*":
            start, end = lo, hi
        elif "-" in part.lstrip("-"):
            start_s, _, end_s = part.partition("-")
            try:
                start, end = int(start_s), int(end_s)
            except ValueError:
                raise CronError(f"{name}: bad range {part!r}") from None
        else:
            try:
                start = end = int(part)
            except ValueError:
                raise CronError(f"{name}: bad value {part!r}") from None

        # Sunday is 0, but 7 is the widely-accepted alias and people do write it.
        if name == "weekday":
            start = 0 if start == 7 else start
            end = 0 if end == 7 else end
            if end < start:  # e.g. "5-0" (Fri..Sun) - wrap rather than reject
                out.update(range(start, 7))
                out.update(range(0, end + 1))
                continue

        if start < lo or end > hi or end < start:
            raise CronError(f"{name}: {part!r} out of range {lo}-{hi}")
        out.update(range(start, end + 1, step))

    return frozenset(out)


def _resolve_tz(name: str):
    """IANA zone name -> tzinfo, degrading to UTC rather than failing the job."""
    if not name or name.upper() == "UTC":
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception as exc:
        log.warning("cron: unknown timezone %r (%s) - using UTC", name, exc)
        return timezone.utc


@dataclass(frozen=True)
class CronSpec:
    minute: frozenset
    hour: frozenset
    day: frozenset
    month: frozenset
    weekday: frozenset
    tz: object
    raw: str

    # Whether the field was literally "*", which decides the day-of-month /
    # day-of-week rule below. Kept separate because "*" expands to a full set that
    # is otherwise indistinguishable from "0-6".
    day_any: bool
    weekday_any: bool

    @classmethod
    def parse(cls, expr: str, tz: str = "UTC") -> "CronSpec":
        parts = expr.split()
        if len(parts) != 5:
            raise CronError(f"expected 5 fields, got {len(parts)}: {expr!r}")
        values = {
            name: _parse_field(raw, name, lo, hi)
            for raw, (name, lo, hi) in zip(parts, _FIELDS)
        }
        return cls(
            **values,
            tz=_resolve_tz(tz),
            raw=expr,
            day_any=parts[2].strip() == "*",
            weekday_any=parts[4].strip() == "*",
        )

    def matches(self, dt: datetime) -> bool:
        """Does this local datetime fall on a fire time?

        The surprise: when BOTH day-of-month and day-of-week are restricted, cron
        fires when *either* matches, not both. `0 0 1 * 1` is "the 1st, and every
        Monday" - not "Mondays that fall on the 1st". Vixie cron behaves this way and
        so does every scheduler people have muscle memory for, so we match it.
        """
        if dt.minute not in self.minute or dt.hour not in self.hour:
            return False
        if dt.month not in self.month:
            return False

        dom_ok = dt.day in self.day
        # Python: Monday = 0. Cron: Sunday = 0.
        dow_ok = ((dt.weekday() + 1) % 7) in self.weekday

        if self.day_any and self.weekday_any:
            return True
        if self.day_any:
            return dow_ok
        if self.weekday_any:
            return dom_ok
        return dom_ok or dow_ok

    def next_after(self, after: datetime) -> datetime:
        """First fire time strictly after `after`. Returned in UTC.

        Walks minute by minute, but skips a whole day at a time when the date cannot
        match - so the worst case is ~366 date checks plus 1440 minute checks rather
        than half a million minute checks.
        """
        local = after.astimezone(self.tz).replace(second=0, microsecond=0) + timedelta(minutes=1)
        limit = local + timedelta(days=_HORIZON_DAYS)

        while local < limit:
            if not self._date_could_match(local):
                # Jump to 00:00 tomorrow. Recomputing from the date (rather than
                # adding 24h) keeps this correct across DST transitions.
                local = (local + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if self.matches(local):
                return local.astimezone(timezone.utc)
            local += timedelta(minutes=1)

        raise CronError(f"{self.raw!r} has no fire time within {_HORIZON_DAYS} days")

    def _date_could_match(self, dt: datetime) -> bool:
        """Day-level prefilter for next_after - same either/or rule as `matches`."""
        if dt.month not in self.month:
            return False
        dom_ok = dt.day in self.day
        dow_ok = ((dt.weekday() + 1) % 7) in self.weekday
        if self.day_any and self.weekday_any:
            return True
        if self.day_any:
            return dow_ok
        if self.weekday_any:
            return dom_ok
        return dom_ok or dow_ok

    def between(self, start: datetime, end: datetime, cap: int = 64) -> list:
        """Every fire time in (start, end], oldest first, at most `cap` of them.

        The scheduler uses this to see what it slept through: if the gateway was down
        over a fire time, that occurrence is still in this list when it comes back.
        """
        out: list = []
        cursor = start
        while len(out) < cap:
            try:
                nxt = self.next_after(cursor)
            except CronError:
                break
            if nxt > end:
                break
            out.append(nxt)
            cursor = nxt
        return out
