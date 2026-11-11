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

"""Gateway-side idle-suspend (Python port of idle-suspender.ts).

A Substrate actor runs inside a gVisor sandbox with no ateapi credentials, so it
cannot suspend itself. The gateway - which already holds the credentialed
kubectl-ate path used to create actors - tracks per-actor activity and calls
SuspendActor once a conversation's actor has been idle past the timeout, freeing
its worker. Resume is automatic: the next turn hits atenet, which restores the
actor from its checkpoint.

Turn-awareness is the key correctness property: an actor is NEVER suspended while
a turn is in-flight (inflight > 0), only after it has been quiet for the timeout.

The timeout is **per actor**, not global (§7.3.4). Suspending trades idle cost
against the wake latency of the next turn, and the right point on that dial is a
property of the agent: a support agent someone messages all day should stay warm,
one that runs a nightly job should not. `timeout_for` resolves the actor's policy at
suspend-decision time - so a config change takes effect without a restart - and
returning None means never suspend (the always-on tier).
"""
from __future__ import annotations

import asyncio
import os
import logging
import time

from ateapi_client import KubectlAteClient

log = logging.getLogger("gateway.idle")

# How long an actor may claim to be working before we stop believing it. Long
# enough for a genuinely long task, short enough that a lost completion signal
# costs one worker for an hour rather than until someone notices.
MAX_WORKING_SECONDS = float(os.environ.get("MAX_WORKING_SECONDS", "3600"))


class IdleSuspender:
    def __init__(
        self,
        atespace: str,
        client: KubectlAteClient,
        idle_timeout_seconds: float,
        poll_seconds: float = 5.0,
        timeout_for=None,
    ):
        self.atespace = atespace
        self.client = client
        self.idle_timeout = idle_timeout_seconds
        self.poll = poll_seconds
        # actor -> float | None, where **None means always-on**, not "unknown".
        # A resolver must return its own default for actors it doesn't recognise
        # (AgentRegistry.idle_timeout_for_actor does); returning None for those
        # would pin every unrecognised actor to a worker forever.
        self._timeout_for = timeout_for or (lambda _actor: idle_timeout_seconds)
        self._last_activity: dict[str, float] = {}
        self._inflight: dict[str, int] = {}
        self._suspending: set[str] = set()
        self._task: asyncio.Task | None = None
        # Actors that are still working after we stopped waiting for them.
        # actor -> when we started waiting, so the ceiling below can apply.
        self._working: dict[str, float] = {}

    def touch(self, actor: str) -> None:
        self._last_activity[actor] = time.monotonic()

    def begin(self, actor: str) -> None:
        self._inflight[actor] = self._inflight.get(actor, 0) + 1
        self._last_activity[actor] = time.monotonic()

    def end(self, actor: str) -> None:
        self._inflight[actor] = max(0, self._inflight.get(actor, 0) - 1)
        self._last_activity[actor] = time.monotonic()

    def still_working(self, actor: str) -> None:
        """We gave up waiting, but the agent has not said it is done.

        An open HTTP request is the only evidence this class has that an agent is
        busy, and that request times out long before a genuinely long task
        finishes. Treating the timeout as "idle" suspends an actor mid-work - the
        one thing suspension must never do. So the actor stays protected until it
        signals completion, or until the ceiling below.
        """
        if actor not in self._working:
            # Logged because it is a state change that changes suspension
            # behaviour, and an unlogged state change is one you cannot debug.
            log.info("actor %s: stopped waiting, but it has not reported "
                     "finishing - holding it awake", actor)
        self._working.setdefault(actor, time.monotonic())
        self._last_activity[actor] = time.monotonic()

    @property
    def working(self) -> "dict[str, float]":
        """Actors we are holding awake, and when we started. Read by /fleet."""
        now = time.monotonic()
        return {a: now - t for a, t in self._working.items()}

    def signal_done(self, actor: str) -> bool:
        """The agent says it has finished. Returns whether we were waiting."""
        was = self._working.pop(actor, None) is not None
        self._last_activity[actor] = time.monotonic()
        if was:
            log.info("actor %s reported its turn finished - suspendable again",
                     actor)
        return was

    def start(self) -> None:
        log.info(
            "idle-suspend active (default timeout %ss, atespace %s)",
            self.idle_timeout, self.atespace,
        )
        self._task = asyncio.create_task(self._loop())

    def stop(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.poll)
            now = time.monotonic()
            for name, ts in list(self._last_activity.items()):
                if name in self._suspending:
                    continue
                if self._inflight.get(name, 0) > 0:      # never suspend mid-turn
                    continue
                started = self._working.get(name)
                if started is not None:
                    # Working past our patience. Hold it awake, but not forever:
                    # a lost signal (agent crash, plugin missing, gateway
                    # restart) must not pin a worker permanently.
                    if now - started < MAX_WORKING_SECONDS:
                        continue
                    log.warning("actor %s has been working %.0f min with no "
                                "completion signal - releasing it for suspend",
                                name, (now - started) / 60)
                    self._working.pop(name, None)
                timeout = self._resolve(name)
                if timeout is None:                      # always-on tier
                    continue
                if now - ts < timeout:
                    continue
                self._suspending.add(name)
                asyncio.create_task(self._suspend(name))

    def _resolve(self, actor: str) -> "float | None":
        """This actor's idle timeout, or None for the always-on tier."""
        try:
            return self._timeout_for(actor)
        except Exception as e:
            log.warning("idle policy lookup for %s failed (%s) - using default", actor, e)
            return self.idle_timeout

    async def _suspend(self, name: str) -> None:
        try:
            log.info("actor %s/%s idle - suspending", self.atespace, name)
            await self.client.suspend_actor(self.atespace, name)
            self._last_activity.pop(name, None)
            log.info("suspended %s/%s", self.atespace, name)
        except Exception as e:
            # An actor that no longer exists is never coming back, so stop
            # tracking it. Retrying forever produced a warning every idle period
            # for hours after an actor was deleted out from under us - noise that
            # buries real failures and never resolves.
            if "NotFound" in str(e) or "not found" in str(e).lower():
                self._last_activity.pop(name, None)
                log.info("actor %s/%s is gone - dropping it from idle tracking",
                         self.atespace, name)
                return
            # Anything else is worth retrying, but refresh the timestamp first so
            # we back off to one attempt per idle period rather than a hot loop.
            self._last_activity[name] = time.monotonic()
            log.warning("suspend %s failed: %s", name, e)
        finally:
            self._suspending.discard(name)
