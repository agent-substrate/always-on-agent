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

"""Scheduled work for agents that are asleep (T3).

A suspended actor is a checkpoint in a bucket. It has no clock, no timer, and no way
to wake itself - so *something else* has to hold the schedule. The gateway is that
something: it is the only always-on component, and it already owns the path that
wakes an actor (an inbound request through atenet triggers resume-on-demand). A
cron fire is therefore just a turn that no human typed.

Three behaviours are worth reading before the code:

**Coalescing.** If the gateway was down across six fire times, the agent gets *one*
run, not six. Six identical morning briefs at 09:00 is a bug that looks like a
feature in the test and like spam in production. The run is told it is late and how
many it stands for, so the agent can say so.

**A catch-up window.** Occurrences older than the window are dropped, not run. After
a weekend outage nobody wants Friday's 8am brief on Monday; it is stale, and acting
on stale instructions is worse than skipping them. Dropped occurrences are logged.

**Best-effort persistence.** The cursor is written to disk so a pod restart doesn't
lose or replay the schedule. On a *fresh* cursor the scheduler starts from now
rather than from epoch - the alternative is a new deployment firing a year of
backlog on its first tick. Without a mounted volume the file is lost on restart,
which degrades to "a restart can miss a fire" rather than anything worse; the fix is
a PVC, noted in the deployment manifest.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from agents import CronJob

log = logging.getLogger("gateway.scheduler")

DEFAULT_STATE_PATH = "/var/lib/hermes-gateway/cron-state.json"
DEFAULT_TICK_SECONDS = 30.0
DEFAULT_CATCHUP_SECONDS = 3600.0  # one hour; older occurrences are dropped as stale

_LATE_PREFIX = (
    "[Scheduled run, delivered late - the scheduler was unavailable at the intended "
    "time of {when}. {count} occurrence(s) were missed and coalesced into this one. "
    "Mention the delay if it changes your answer.]\n\n"
)


class CronScheduler:
    """Fires agent turns on a schedule, waking suspended actors to do it.

    `run_turn(job, prompt, session_key) -> str` performs the turn (the same path an
    inbound message takes, so it inherits resume-on-demand, worker-starvation
    backoff and idle bracketing). `deliver(address, text)` puts the result in front
    of a human.
    """

    def __init__(
        self,
        jobs: "list[CronJob]",
        run_turn,
        deliver,
        *,
        state_path: str = DEFAULT_STATE_PATH,
        tick_seconds: float = DEFAULT_TICK_SECONDS,
        catchup_seconds: float = DEFAULT_CATCHUP_SECONDS,
    ):
        self.jobs = list(jobs)
        self._run_turn = run_turn
        self._deliver = deliver
        self.state_path = state_path
        self.tick_seconds = tick_seconds
        self.catchup = timedelta(seconds=catchup_seconds)
        self._cursor: "dict[str, datetime]" = {}
        self._running: "set[str]" = set()
        self._task: "asyncio.Task | None" = None
        self._dirty = False

    # ---- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if not self.jobs:
            log.info("no cron jobs configured - scheduler idle")
            return
        self._load()
        now = _utcnow()
        for job in self.jobs:
            self._cursor.setdefault(job.key, now)
            with contextlib.suppress(Exception):
                log.info("cron %s [%s tz=%s] next %s",
                         job.key, job.spec.raw, _tzname(job.spec.tz),
                         job.spec.next_after(now).isoformat())
        self._save()
        self._task = asyncio.create_task(self._loop())
        log.info("cron scheduler active: %d job(s), tick %ss, catch-up window %ss",
                 len(self.jobs), self.tick_seconds, int(self.catchup.total_seconds()))

    def stop(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None
        self._save()

    # ---- the clock -----------------------------------------------------------

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.tick_seconds)
            try:
                self._tick()
            except Exception:
                # The scheduler must not die on one bad job; a dead scheduler is
                # silent, and silence is the failure mode nobody notices.
                log.exception("cron tick failed")

    def _tick(self) -> None:
        now = _utcnow()
        horizon = now - self.catchup
        for job in self.jobs:
            since = self._cursor.get(job.key)
            if since is None:
                self._cursor[job.key] = now
                continue

            # Clamp to the catch-up window *before* enumerating, not after. A long
            # outage of a frequent job produces more occurrences than `between`
            # will return, and the ones it returns are the oldest - so filtering
            # afterwards would throw away the whole (capped) list as stale and miss
            # the genuinely-due fire from five minutes ago.
            if since < horizon:
                log.warning("cron %s: cursor is %s, older than the %ds catch-up "
                            "window - anything before %s is stale and skipped",
                            job.key, since.isoformat(timespec="minutes"),
                            int(self.catchup.total_seconds()),
                            horizon.isoformat(timespec="minutes"))
                since = horizon

            due = job.spec.between(since, now)
            if not due:
                continue

            self._cursor[job.key] = now
            self._dirty = True

            if job.key in self._running:
                # The previous run of this job is still going. Skipping is the
                # conservative choice: two concurrent runs of the same job would
                # both wake the same actor and race on its session.
                log.warning("cron %s still running - skipping this fire", job.key)
                continue

            self._running.add(job.key)
            asyncio.create_task(self._fire(job, due))

        if self._dirty:
            self._save()

    # ---- one run -------------------------------------------------------------

    async def _fire(self, job: CronJob, occurrences: list) -> None:
        started = time.monotonic()
        intended = occurrences[-1]
        late = _utcnow() - intended
        try:
            prompt = job.prompt
            # >2 ticks late means we genuinely missed it, rather than the normal
            # sub-tick lag between the fire time and us noticing it.
            if late.total_seconds() > self.tick_seconds * 2 or len(occurrences) > 1:
                prompt = _LATE_PREFIX.format(
                    when=intended.isoformat(timespec="minutes"),
                    count=len(occurrences),
                ) + prompt

            log.info("cron %s firing (intended %s, %d occurrence(s))",
                     job.key, intended.isoformat(timespec="minutes"), len(occurrences))
            reply = await self._run_turn(job, prompt, _session_for(job))
            if reply:
                await self._deliver(job.deliver_to, reply)
            log.info("cron %s done in %.1fs", job.key, time.monotonic() - started)
        except Exception as e:
            log.exception("cron %s failed", job.key)
            # Tell the human. A scheduled brief that silently never arrives is
            # indistinguishable from one the agent decided not to send.
            with contextlib.suppress(Exception):
                await self._deliver(
                    job.deliver_to,
                    f"Scheduled task '{job.name}' failed to run: {e}",
                )
        finally:
            self._running.discard(job.key)

    # ---- cursor persistence --------------------------------------------------

    def _load(self) -> None:
        try:
            with open(self.state_path, encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError:
            log.info("no cron state at %s - starting from now", self.state_path)
            return
        except Exception as e:
            log.warning("cron state at %s unreadable (%s) - starting from now",
                        self.state_path, e)
            return
        for key, iso in (raw.get("cursor") or {}).items():
            try:
                self._cursor[key] = datetime.fromisoformat(iso)
            except ValueError:
                log.warning("cron state: bad timestamp for %s: %r", key, iso)
        log.info("cron state loaded: %d cursor(s) from %s",
                 len(self._cursor), self.state_path)

    def _save(self) -> None:
        payload = {"cursor": {k: v.isoformat() for k, v in self._cursor.items()}}
        tmp = f"{self.state_path}.tmp"
        try:
            os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, self.state_path)  # atomic; never a half-written cursor
            self._dirty = False
        except Exception as e:
            # Non-fatal by design: losing the cursor costs at most one missed
            # catch-up, and is not worth taking the gateway down for.
            log.warning("could not persist cron state to %s: %s", self.state_path, e)


def _session_for(job: CronJob) -> str:
    """Which conversation a scheduled result belongs to.

    The delivery address, not the job. The point of a scheduled brief is that it
    lands in the thread you already talk to your agent in, and that you can reply
    "expand on the second one" - which only works if the run shares that thread's
    session.
    """
    return job.deliver_to


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _tzname(tz) -> str:
    return getattr(tz, "key", None) or str(tz)
