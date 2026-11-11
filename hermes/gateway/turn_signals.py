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

"""Authenticated busy/completion callbacks for every Substrate agent."""
from __future__ import annotations

import logging
from aiohttp import web
from caller_auth import ActorResolver, ASSERTED_ACTOR_HEADER

log = logging.getLogger("gateway.turn_signals")
TURN_ENDED = "/agent/turn-ended"
TURN_WORKING = "/agent/turn-working"

def register_turn_signals(app: web.Application, *, resolver: ActorResolver,
                          on_turn_ended=None, on_turn_working=None) -> None:
    """Mount authenticated lifecycle callbacks for idle suspension."""
    app["turn_resolver"] = resolver
    if on_turn_ended is not None:
        app["on_turn_ended"] = on_turn_ended
        app.router.add_post(TURN_ENDED, _turn_ended)
    if on_turn_working is not None:
        app["on_turn_working"] = on_turn_working
        app.router.add_post(TURN_WORKING, _turn_working)
    log.info("turn signalling on: %s, %s", TURN_WORKING, TURN_ENDED)


def _origin(request: web.Request) -> "tuple[str, str]":
    """Where this request came from: the peer address, and what the egress proxy
    says about it.

    Both are passed to the resolver rather than chosen here, because which one is
    believed depends on what the peer turns out to be, and that is the resolver's
    call to make. See caller_auth.py.
    """
    return request.remote or "", request.headers.get(ASSERTED_ACTOR_HEADER, "")


async def _turn_ended(request: web.Request) -> web.Response:
    """An agent reports that it has finished working.

    No token is required and none would help: the only claim being made is "I am
    done", and the identity of the claimant comes from the network - either the
    address it called from, or the egress proxy's stamp when interception has
    replaced that address with its own. Either way it is not something the agent
    chose. That is what makes this safe to expose - an agent cannot name another
    actor, so it cannot end anyone else's turn or cause anyone else to be
    suspended. The worst it can do is give up its own remaining protection, which
    costs only itself.

    """
    peer, asserted = _origin(request)
    actor = await request.app["turn_resolver"].actor_for(peer, asserted)
    if actor is None:
        log.warning("turn-ended from %s could not be resolved to an actor", peer)
        return web.json_response({"error": "unidentified caller"}, status=403)
    was_waiting = request.app["on_turn_ended"](actor)
    return web.json_response({"ok": True, "actor": actor,
                              "was_awaiting_signal": bool(was_waiting)})


async def _turn_working(request: web.Request) -> web.Response:
    """An agent reports that work it started will outlive the turn.

    Same identity rule as turn-ended, safe for the same reason inverted: an agent
    can only ever protect ITS OWN actor, never pin someone else's. The worst it
    can do is keep its own worker occupied, which the suspender's ceiling bounds.

    Deliberately not a heartbeat. One assertion puts the actor in the working set
    until it reports done or the ceiling expires, so a dropped packet costs a held
    worker rather than frozen work.
    """
    peer, asserted = _origin(request)
    actor = await request.app["turn_resolver"].actor_for(peer, asserted)
    if actor is None:
        log.warning("turn-working from %s could not be resolved to an actor", peer)
        return web.json_response({"error": "unidentified caller"}, status=403)
    request.app["on_turn_working"](actor)
    return web.json_response({"ok": True, "actor": actor})
