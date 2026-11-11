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

"""Keep the user company while a sleeping agent wakes up.

The requirement (T4) is that the user gets feedback *every* time, including on the
slow path. The obvious implementation - send a typing indicator when the turn starts
- quietly fails that: Telegram's `sendChatAction` expires after about five seconds,
and a cold restore takes longer than that. So the indicator appears, dies, and the
user spends the rest of the wake looking at a silent chat. It looks worse than no
indicator at all, because something visibly gave up.

Two mechanisms, because they answer different questions:

* **the ticker** - re-asserts the channel's ephemeral indicator every few seconds
  for as long as the turn runs. Answers "is it still working?"
* **the slow-wake notice** - one real message, sent only if the turn crosses a
  threshold. Answers "why is this taking so long?" Sent once, never repeated: a
  progress bar that keeps talking is worse than silence.

Both are channel-agnostic - this module knows nothing about Telegram. Callers pass
in the two callables. Neither is allowed to break a turn: a channel API that fails
here costs a typing dot, and swallowing that is obviously right, so failures are
logged and the ticker gives up quietly after a few consecutive errors.

    async with PresenceTicker(tick=send_typing, notify=send_text, label=actor):
        reply = await do_the_turn()
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time

log = logging.getLogger("gateway.presence")

# Telegram's typing action lasts ~5s. Re-assert inside that window with enough
# margin to absorb a slow round-trip, but not so often that we rate-limit ourselves.
DEFAULT_INTERVAL = 4.0

# Long enough that a warm turn never trips it (a warm reply beats this comfortably),
# short enough that a cold restore does.
DEFAULT_SLOW_AFTER = 8.0

DEFAULT_SLOW_MESSAGE = (
    "Waking up - I've been suspended to save resources, so this first reply "
    "takes a few seconds longer. Still working on it."
)

# After this many consecutive failures the channel is clearly not accepting
# presence updates; stop rather than log once per interval for the whole turn.
_MAX_CONSECUTIVE_FAILURES = 3


class PresenceTicker:
    """Repeats a channel's presence signal for the lifetime of a turn."""

    def __init__(
        self,
        tick,
        *,
        interval: float = DEFAULT_INTERVAL,
        notify=None,
        slow_after: "float | None" = DEFAULT_SLOW_AFTER,
        slow_message: str = DEFAULT_SLOW_MESSAGE,
        label: str = "",
    ):
        self._tick = tick
        self._interval = interval
        self._notify = notify
        self._slow_after = slow_after
        self._slow_message = slow_message
        self._label = label
        self._task: "asyncio.Task | None" = None
        self._started = 0.0
        self.slow = False  # did this turn cross the slow threshold?

    async def __aenter__(self) -> "PresenceTicker":
        self._started = time.monotonic()
        # Fire the first tick inline so the indicator is up before we await the
        # turn - scheduling it costs a round-trip of dead air at exactly the moment
        # the user is looking.
        await self._safe_tick()
        self._task = asyncio.create_task(self._loop())
        return self

    async def __aexit__(self, *exc) -> bool:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        return False  # never swallow the turn's exception

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._started

    async def _loop(self) -> None:
        failures = 0
        notified = False
        while True:
            await asyncio.sleep(self._interval)

            if (
                not notified
                and self._notify is not None
                and self._slow_after is not None
                and self.elapsed >= self._slow_after
            ):
                notified = True
                self.slow = True
                try:
                    await self._notify(self._slow_message)
                    log.info("slow wake for %s - told the user (%.1fs in)",
                             self._label or "actor", self.elapsed)
                except Exception as e:
                    log.warning("slow-wake notice failed for %s: %s",
                                self._label or "actor", e)

            if await self._safe_tick():
                failures = 0
            else:
                failures += 1
                if failures >= _MAX_CONSECUTIVE_FAILURES:
                    log.warning("presence ticker for %s giving up after %d failures",
                                self._label or "actor", failures)
                    return

    async def _safe_tick(self) -> bool:
        try:
            await self._tick()
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("presence tick failed for %s: %s", self._label or "actor", e)
            return False
