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

"""Runtime routing, idle policies and scheduled jobs for persistent agents.

Each configured agent has an arbitrary routing ID. Multiple channel bindings can
route to the same actor while their conversation keys remain separate Hermes
session IDs. An unregistered routing key gets its own actor with the default
policy. No external account is required for either routing mode.

Config is a JSON file (typically a mounted ConfigMap) at AGENTS_CONFIG:

    {
      "defaults": { "idle_tier": "short", "timezone": "UTC" },
      "agents": [
        {
          "id": "research-agent",
          "idle_tier": "medium",
          "timezone": "America/Los_Angeles",
          "channels": ["telegram:100200300"],
          "jobs": [
            { "name": "morning-brief",
              "cron": "45 8 * * 1-5",
              "prompt": "Summarise anything I should know before the day starts.",
              "deliver_to": "telegram:100200300" }
          ]
        }
      ]
    }
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field

from actor_router import actor_name_for_agent
from cron import CronError, CronSpec

log = logging.getLogger("gateway.agents")

# §7.3.4 wants the cost/latency trade to be a policy choice rather than a constant.
# None means never suspend: the actor holds its worker and answers with no wake at
# all, which is the expensive end of the same dial.
IDLE_TIERS: "dict[str, float | None]" = {
    "always-on": None,
    "short": 60.0,
    "medium": 900.0,
    "long": 3600.0,
}
DEFAULT_TIER = "short"


class AgentConfigError(ValueError):
    pass


@dataclass(frozen=True)
class CronJob:
    """One scheduled wake for one agent.

    `deliver_to` is a channel address (`telegram:<chat_id>`). It is required: a job
    whose result goes nowhere burns a wake and a model call to write into a log, and
    that is never what someone meant.
    """
    name: str
    spec: CronSpec
    prompt: str
    deliver_to: str
    agent_id: str

    @property
    def key(self) -> str:
        """Stable routing key for the fire-time cursor. Survives restarts."""
        return f"{self.agent_id}/{self.name}"


@dataclass(frozen=True)
class Agent:
    """One configured runtime agent."""
    agent_id: str
    actor: str
    idle_tier: str
    idle_timeout: "float | None"
    timezone: str = "UTC"
    jobs: "tuple[CronJob, ...]" = field(default_factory=tuple)
    registered: bool = True
    @property
    def always_on(self) -> bool:
        return self.idle_timeout is None


def _tier_timeout(tier: str, where: str) -> "tuple[str, float | None]":
    if tier not in IDLE_TIERS:
        raise AgentConfigError(
            f"{where}: unknown idle_tier {tier!r} - expected one of {sorted(IDLE_TIERS)}"
        )
    return tier, IDLE_TIERS[tier]


class AgentRegistry:
    """Channel routing key -> agent, plus every scheduled job in the fleet."""

    def __init__(
        self,
        default_tier: str = DEFAULT_TIER,
        default_tz: str = "UTC",
        default_timeout_override: "float | None" = None,
    ):
        self.default_tier, self.default_timeout = _tier_timeout(default_tier, "defaults")
        if default_timeout_override is not None:
            self.default_timeout = default_timeout_override
        self.default_tz = default_tz
        self._by_channel: "dict[str, Agent]" = {}
        self._by_id: "dict[str, Agent]" = {}
        self._by_actor: "dict[str, Agent]" = {}

    # ---- loading -------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "AgentRegistry":
        path = os.environ.get("AGENTS_CONFIG", "").strip()
        tier = os.environ.get("IDLE_TIER_DEFAULT", DEFAULT_TIER).strip() or DEFAULT_TIER
        tz = os.environ.get("CRON_TIMEZONE", "UTC").strip() or "UTC"

        # Tiers replaced a single IDLE_TIMEOUT_SECONDS knob, and existing manifests
        # still set it. Honour it as the default timeout - a deployment that sets
        # the old variable and silently gets a different one is the kind of upgrade
        # surprise that wastes an afternoon. Naming a tier explicitly wins.
        override = None
        raw_seconds = os.environ.get("IDLE_TIMEOUT_SECONDS", "").strip()
        if raw_seconds and not os.environ.get("IDLE_TIER_DEFAULT", "").strip():
            try:
                override = float(raw_seconds)
                log.info("IDLE_TIMEOUT_SECONDS=%s overrides the default idle timeout",
                         raw_seconds)
            except ValueError:
                log.warning("IDLE_TIMEOUT_SECONDS=%r is not a number - ignoring", raw_seconds)

        if not path:
            log.info("no AGENTS_CONFIG - every conversation becomes its own agent "
                     "(tier %s); set it to assign agents to users", tier)
            return cls(tier, tz, override)
        if not os.path.exists(path):
            # Not fatal: a missing ConfigMap degrades to the unregistered behaviour rather
            # than crash-looping the only always-on component in the system. Nor is it an
            # error - k8s/hermes-agents.example.yaml is deliberately not applied by any
            # script, so an absent file is the expected state of a fresh deploy, and an
            # ERROR here would have every first-time user chasing a non-problem.
            log.info("AGENTS_CONFIG %s does not exist - continuing unregistered; apply "
                     "k8s/hermes-agents.example.yaml to bind agents to users", path)
            return cls(tier, tz, override)
        with open(path, encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh), fallback_tier=tier, fallback_tz=tz,
                                 default_timeout_override=override)

    @classmethod
    def from_dict(
        cls,
        doc: dict,
        fallback_tier: str = DEFAULT_TIER,
        fallback_tz: str = "UTC",
        default_timeout_override: "float | None" = None,
    ) -> "AgentRegistry":
        defaults = doc.get("defaults") or {}
        reg = cls(
            default_tier=defaults.get("idle_tier", fallback_tier),
            default_tz=defaults.get("timezone", fallback_tz),
            default_timeout_override=default_timeout_override,
        )
        for raw in doc.get("agents") or []:
            reg._add(raw)
        log.info("agent registry: %d agents, %d channel bindings, %d scheduled jobs",
                 len(reg._by_id), len(reg._by_channel), len(reg.all_jobs()))
        return reg

    def _add(self, raw: dict) -> None:
        agent_id = str(raw.get("id", "")).strip()
        if not agent_id:
            raise AgentConfigError("agent entry is missing 'id'")
        if agent_id in self._by_id:
            raise AgentConfigError(f"duplicate agent id {agent_id!r}")

        raw_tier = raw.get("idle_tier")
        if raw_tier is None:
            # Inherit the registry default, including any IDLE_TIMEOUT_SECONDS
            # override - an agent that expressed no opinion should not silently get
            # a different timeout from the unregistered ones.
            tier, timeout = self.default_tier, self.default_timeout
        else:
            tier, timeout = _tier_timeout(raw_tier, f"agent {agent_id}")
        tz = raw.get("timezone", self.default_tz)

        jobs = tuple(self._job(agent_id, j, tz) for j in (raw.get("jobs") or []))
        agent = Agent(
            agent_id=agent_id,
            actor=actor_name_for_agent(agent_id),
            idle_tier=tier,
            idle_timeout=timeout,
            timezone=tz,
            jobs=jobs,
        )

        self._by_id[agent_id] = agent
        self._by_actor[agent.actor] = agent
        for chan in raw.get("channels") or []:
            chan = str(chan).strip()
            if not chan:
                continue
            if chan in self._by_channel:
                raise AgentConfigError(
                    f"channel {chan!r} is bound to both "
                    f"{self._by_channel[chan].agent_id!r} and {agent_id!r}"
                )
            self._by_channel[chan] = agent

    @staticmethod
    def _job(agent_id: str, raw: dict, default_tz: str) -> CronJob:
        name = str(raw.get("name", "")).strip()
        expr = str(raw.get("cron", "")).strip()
        prompt = str(raw.get("prompt", "")).strip()
        deliver_to = str(raw.get("deliver_to", "")).strip()
        where = f"agent {agent_id} job {name or '<unnamed>'}"
        if not name:
            raise AgentConfigError(f"{where}: missing 'name'")
        if not prompt:
            raise AgentConfigError(f"{where}: missing 'prompt'")
        if not deliver_to:
            raise AgentConfigError(f"{where}: missing 'deliver_to' - a job whose "
                                   "result goes nowhere is never what was meant")
        try:
            spec = CronSpec.parse(expr, tz=raw.get("timezone", default_tz))
        except CronError as exc:
            raise AgentConfigError(f"{where}: {exc}") from None
        return CronJob(
            name=name, spec=spec, prompt=prompt, deliver_to=deliver_to, agent_id=agent_id
        )

    # ---- lookup --------------------------------------------------------------

    def for_channel(self, channel_key: str) -> Agent:
        """The agent that owns this conversation.

        Matched against declared channels first, then against agent ids. The id
        fallback matters because an agent id in this fleet IS a routing key, so
        addressing an agent by the name it is registered under is the obvious
        thing to do and used to miss: the lookup only knew about `channels`, so
        `demo-agent` fell through to the ad-hoc path and came back as an
        unregistered agent on the DEFAULT tier with none of its cron jobs. That
        is silent. The turn works, the reply looks right, and the only trace is
        one INFO line - while every tier in the fleet has quietly collapsed to
        the default, which is exactly the variable a soak is measuring.

        An unregistered key becomes its own agent, so ad-hoc callers, tests and the
        demo scripts keep working with no config at all. Those are cached so that
        repeat traffic from the same key is stable, and so the idle-suspender can
        look the actor's policy back up by name.
        """
        hit = self._by_channel.get(channel_key) or self._by_id.get(channel_key)
        if hit is not None:
            return hit

        agent = Agent(
            agent_id=channel_key,
            actor=actor_name_for_agent(channel_key),
            idle_tier=self.default_tier,
            idle_timeout=self.default_timeout,
            timezone=self.default_tz,
            registered=False,
        )
        self._by_channel[channel_key] = agent
        self._by_actor[agent.actor] = agent
        log.info("unregistered routing key %s -> ad-hoc agent (actor %s, tier %s)",
                 channel_key, agent.actor, agent.idle_tier)
        return agent

    def by_id(self, agent_id: str) -> "Agent | None":
        return self._by_id.get(agent_id)

    def registered_agents(self) -> "list[Agent]":
        """Only explicitly configured agents, excluding ad-hoc channel bindings."""
        return list(self._by_id.values())

    def by_actor(self, actor: str) -> "Agent | None":
        return self._by_actor.get(actor)

    def idle_timeout_for_actor(self, actor: str) -> "float | None":
        """Per-actor idle policy for the suspender. Unknown actors get the default."""
        agent = self._by_actor.get(actor)
        return agent.idle_timeout if agent else self.default_timeout

    def all_jobs(self) -> "list[CronJob]":
        return [j for a in self._by_id.values() for j in a.jobs]
