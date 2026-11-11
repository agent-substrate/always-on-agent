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

"""What each agent has actually been doing.

The question this exists to answer is "is this agent working, and
it cost", which today can only be answered by grepping a pod log that will be
replaced. Everything here is per agent and cumulative.

Deliberately NOT a per-message ledger. Counters plus a few timestamps answer the
operational questions ("is anything failing", "who is expensive") at a fraction
of the cost, and the durable per-message record belongs in the control plane
alongside real storage rather than being half-built here.

Persisted to the gateway's state volume because "what did this agent cost" is
meaningless if it resets on every deploy, and deploys are routine. Written on a
every turn, atomically. At pilot rates that is a handful of small writes a
minute, and anything cleverer has to be right about shutdown ordering, which the
first attempt was not.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time

log = logging.getLogger("gateway.fleet")

# Token prices, per million tokens, for turning usage into money. Zero means
# "report tokens, not dollars", which is the honest default: a wrong cost figure
# is worse than none, and the right number depends on the customer's contract.
PRICE_IN = float(os.environ.get("MODEL_PRICE_PER_MTOK_IN", "0"))
PRICE_OUT = float(os.environ.get("MODEL_PRICE_PER_MTOK_OUT", "0"))


class AgentStat:
    """Cumulative counters for one agent."""

    __slots__ = ("agent_id", "turns", "failures", "tokens_in", "tokens_out",
                 "last_turn", "last_ok", "last_error", "streak")

    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.turns = 0
        self.failures = 0
        self.tokens_in = 0
        self.tokens_out = 0
        self.last_turn: "float | None" = None
        self.last_ok: "float | None" = None
        self.last_error: str = ""
        self.streak = 0                      # consecutive failures

    @property
    def tokens(self) -> int:
        return self.tokens_in + self.tokens_out

    @property
    def cost(self) -> "float | None":
        if not (PRICE_IN or PRICE_OUT):
            return None
        return (self.tokens_in / 1e6) * PRICE_IN + (self.tokens_out / 1e6) * PRICE_OUT

    def as_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "turns": self.turns,
            "failures": self.failures,
            "tokens_in": self.tokens_in,
            "tokens_out": self.tokens_out,
            "tokens": self.tokens,
            "cost": self.cost,
            "last_turn": self.last_turn,
            "last_ok": self.last_ok,
            "last_error": self.last_error,
            "consecutive_failures": self.streak,
        }

    def load(self, d: dict) -> None:
        for k in ("turns", "failures", "tokens_in", "tokens_out", "streak"):
            setattr(self, k, int(d.get(k, 0) or 0))
        for k in ("last_turn", "last_ok"):
            v = d.get(k)
            setattr(self, k, float(v) if v else None)
        self.last_error = str(d.get("last_error", ""))


class FleetStats:
    """Per-agent counters, persisted so they survive a deploy."""

    def __init__(self, path: "str | None" = None):
        self.path = path
        self._by_agent: "dict[str, AgentStat]" = {}
        self._load()

    def _stat(self, agent_id: str) -> AgentStat:
        s = self._by_agent.get(agent_id)
        if s is None:
            s = self._by_agent[agent_id] = AgentStat(agent_id)
        return s

    def record(self, agent_id: str, ok: bool, usage: "dict | None" = None,
               error: str = "") -> None:
        """One completed turn. `usage` is the model's own token report, if any."""
        s = self._stat(agent_id)
        s.turns += 1
        s.last_turn = time.time()
        if ok:
            s.last_ok = s.last_turn
            s.streak = 0
        else:
            s.failures += 1
            s.streak += 1
            # Trimmed: this is shown in a table, and a wall of provider JSON in a
            # status view is how people stop reading status views.
            s.last_error = (error or "")[:160]
        if usage:
            s.tokens_in += int(usage.get("prompt_tokens") or 0)
            s.tokens_out += int(usage.get("completion_tokens") or 0)
        # Written on every turn, not debounced. The debounce was premature: it
        # only flushed on a LATER write, so a single turn followed by a quiet
        # period never persisted - which is exactly a pilot's traffic pattern -
        # and the background flusher meant to cover that never ran, because
        # aiohttp has already iterated cleanup_ctx by the time on_startup adds to
        # it. At a few turns a minute an atomic write of a small JSON file costs
        # nothing, and counters that survive a deploy were the entire point.
        self.save()

    def snapshot(self) -> "list[dict]":
        return [s.as_dict() for s in self._by_agent.values()]

    def totals(self) -> dict:
        t = {"agents": len(self._by_agent), "turns": 0, "failures": 0,
             "tokens": 0, "cost": None, "failing_now": 0}
        cost = 0.0
        for s in self._by_agent.values():
            t["turns"] += s.turns
            t["failures"] += s.failures
            t["tokens"] += s.tokens
            if s.streak:
                t["failing_now"] += 1
            if s.cost is not None:
                cost += s.cost
        if PRICE_IN or PRICE_OUT:
            t["cost"] = round(cost, 4)
        return t

    # ---- persistence -----------------------------------------------------

    def save(self) -> None:
        if not self.path:
            return
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            # Atomic: a half-written stats file that fails to parse on the next
            # boot would silently reset every counter, which is exactly the thing
            # persistence was added to prevent.
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.path))
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"agents": {k: v.as_dict() for k, v in self._by_agent.items()}},
                          fh)
            os.replace(tmp, self.path)
        except Exception as exc:                 # never fail a turn over stats
            log.warning("could not persist fleet stats: %s", exc)

    def _load(self) -> None:
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as fh:
                doc = json.load(fh)
            for agent_id, d in (doc.get("agents") or {}).items():
                self._stat(agent_id).load(d)
            log.info("fleet stats restored for %d agents", len(self._by_agent))
        except Exception as exc:
            log.warning("could not read fleet stats (starting fresh): %s", exc)
