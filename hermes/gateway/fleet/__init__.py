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

"""Fleet view: runtime state and usage.

Today the answer to "is this agent working" is to grep a pod log
be replaced on the next deploy. This assembles the three places the answer
actually lives:

    registry   who is supposed to have an agent, and its address
    Substrate  what that agent's actor is doing right now
    counters   what it has done and what it consumed (stats.py)

Mounted as one read-only endpoint. It is deliberately an operator surface, not a
product one: it reports the fleet in aggregate and the failures in detail, and it
does NOT expose message contents or who talked to whom. An admin console that
shows "this person emailed their agent 40 times last night" is surveillance,
the first thing a privacy review will object to.

Wiring is one call from ingress: `register(app, ...)`. Removing this directory
removes the feature.
"""
from __future__ import annotations

import json
import logging
import time

from aiohttp import web

from ateapi_client import actor_state, actor_worker_pod_uid

from .stats import FleetStats

log = logging.getLogger("gateway.fleet")

__all__ = ["register", "FleetStats"]

ROUTE = "/fleet"


def register(app: web.Application, *, registry, ate_client, atespace: str,
             stats: "FleetStats | None" = None,
             extra_fields=None, idle=None) -> None:
    app["fleet"] = stats or FleetStats()
    app["fleet_registry"] = registry
    app["fleet_ate"] = ate_client
    app["fleet_atespace"] = atespace
    app["fleet_extra_fields"] = extra_fields
    app["fleet_idle"] = idle
    app.router.add_get(ROUTE, _handle)

    log.info("fleet view on: %s", ROUTE)


async def _actor_state(ate, atespace: str) -> "dict[str, dict]":
    """actor name -> live state, or empty if Substrate cannot be reached.

    Failure here degrades the view rather than breaking it: knowing what an agent
    has done is still useful when the control plane is unreachable, and an
    operator looking at this during an outage is exactly when it must not 500.
    """
    try:
        rc, out = await ate._run(["get", "actor", "-a", atespace, "-o", "json"])
        if rc != 0:
            return {}
        doc = json.loads(out)
    except Exception as exc:
        log.warning("fleet: could not read actor state: %s", exc)
        return {}
    state = {}
    for a in doc.get("actors", []):
        name = (a.get("metadata") or {}).get("name")
        if name:
            state[name] = {
                # Both readers go through ateapi_client: the control plane has
                # changed the shape of this document once already, and the way
                # that showed up here was every actor reading "unknown" while
                # the fleet was perfectly healthy.
                "status": actor_state(a).replace("ACTOR_STATE_", "").lower(),
                "worker": actor_worker_pod_uid(a)[:8],
                # Also moved: it is metadata.version now, and was top-level.
                "version": (a.get("metadata") or {}).get("version") or a.get("version"),
            }
    return state


async def _handle(request: web.Request) -> web.Response:
    registry = request.app["fleet_registry"]
    stats: FleetStats = request.app["fleet"]
    actors = await _actor_state(request.app["fleet_ate"],
                                request.app["fleet_atespace"])
    extra_fields = request.app.get("fleet_extra_fields")

    idle = request.app.get("fleet_idle")
    working = idle.working if idle is not None else {}
    by_agent = {s["agent_id"]: s for s in stats.snapshot()}
    rows = []
    for agent in registry.registered_agents():
        row = {
            "agent_id": agent.agent_id,
            "actor": agent.actor,
            "idle_tier": agent.idle_tier,
            # "unknown" rather than "suspended": we could not ask, and reporting a
            # guess as a fact is how a status view stops being trusted.
            # "working" outranks the control plane's view: an actor we are
            # deliberately holding awake past a timeout is doing something, and
            # reporting it as merely "running" hides the interesting case.
            "state": ("working" if agent.actor in working
                      else actors.get(agent.actor, {}).get("status", "unknown")),
            "working_seconds": round(working.get(agent.actor, 0)) or None,
            "worker": actors.get(agent.actor, {}).get("worker", ""),
            "version": actors.get(agent.actor, {}).get("version"),
        }
        row.update(by_agent.pop(agent.agent_id, {
            "turns": 0, "failures": 0, "tokens": 0, "cost": None,
            "last_turn": None, "last_ok": None, "last_error": "",
            "consecutive_failures": 0,
        }))
        row["agent_id"] = agent.agent_id
        if extra_fields:
            row.update(extra_fields(agent))
        rows.append(row)

    # Agents with history but no registry entry: someone was removed from the
    # registry while their actor still has durable data. That is
    # an offboarding question, not a bug, and it should be visible rather than
    # silently dropped from the view.
    orphans = list(by_agent.values())

    return web.json_response({
        "generated_at": time.time(),
        "control_plane_reachable": bool(actors),
        "totals": stats.totals(),
        "agents": sorted(rows, key=lambda r: r["agent_id"]),
        "unregistered_with_history": orphans,
    })
