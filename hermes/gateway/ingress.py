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

"""Standalone Hermes-on-Substrate gateway (Milestone 2).

This is the tiny always-on control helper the monolithic design calls for - NOT a
channel gateway, and Hermes itself stays completely unmodified. It:

  1. resolves each inbound conversation -> the agent that owns it -> a
     deterministic actor                                (agents, actor_router)
  2. creates that actor on first contact                (actor_provisioner)
  3. forwards the turn to the actor through atenet      (Host-header routing ->
     atenet cold-restores a suspended actor on demand)
  4. keeps the user company while that restore runs     (presence)
  5. brackets the turn for idle tracking, and suspends idle actors to reclaim
     their workers, on a per-agent policy               (idle_suspender)
  6. holds the clock for agents that are asleep, so scheduled work still runs
                                                        (scheduler, cron)

It presents an OpenAI-compatible surface (/v1/chat/completions, /v1/models) plus a
generic /message webhook, so any OpenAI client or webhook source drives the full
provision -> resume -> answer -> suspend lifecycle with zero manual steps.

Env:
  ATESPACE            (default hermes-demo)
  TEMPLATE            (default hermes/hermes)
  ATENET_ENDPOINT     (default http://atenet-router.ate-system.svc.cluster.local:80)
  ACTOR_DOMAIN        (default actors.resources.substrate.ate.dev)
  API_SERVER_KEY      bearer token forwarded to the actor's Hermes API server
  GATEWAY_INGRESS_KEY  distinct gateway-only bearer token for trusted HTTP adapters
  AGENTS_CONFIG       path to the agent registry JSON (see agents.py); optional
  IDLE_TIER_DEFAULT   always-on | short | medium | long   (default short)
  IDLE_TIMEOUT_SECONDS(default 60; only used for agents with no tier)
  CRON_STATE_PATH     where the scheduler's cursor is persisted
  CRON_TICK_SECONDS   (default 30)
  CRON_CATCHUP_SECONDS(default 3600 - older missed fires are dropped as stale)
  WORKER_WAIT_SECONDS (default 120)
  LISTEN_PORT         (default 8080)

Channels are opt-in and additive; see whatsapp_cloud.py / telegram.py for their env.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import hmac
import os
import re
import time
from datetime import datetime, timezone

from aiohttp import ClientSession, ClientTimeout, web

import telegram as tg
import whatsapp_cloud as wa
from actor_provisioner import ActorProvisioner
from actor_router import DEFAULT_ACTOR_DOMAIN, actor_host
from router_tls import RouterTLS
from agents import AgentRegistry
import fleet
from caller_auth import ActorResolver
from turn_signals import register_turn_signals
from ateapi_client import ACTOR_RUNNING, KubectlAteClient
from idle_suspender import IdleSuspender
from turn_dedup import TurnDedup, DEFAULT_TURN_STORE
from presence import PresenceTicker
from scheduler import (
    DEFAULT_CATCHUP_SECONDS,
    DEFAULT_STATE_PATH,
    DEFAULT_TICK_SECONDS,
    CronScheduler,
)
from vertex_proxy import VertexProxy, VertexProxyConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("gateway")

ATESPACE = os.environ.get("ATESPACE", "hermes-demo")
TEMPLATE = os.environ.get("TEMPLATE", "hermes/hermes")
# Substrate v1 serves actor ingress only over HTTPS with a pod identity client
# certificate (router_tls.py). An http:// endpoint is the release-0.1 shape.
ATENET = os.environ.get("ATENET_ENDPOINT", "https://atenet-router.ate-system.svc:443")
ROUTER_TLS = RouterTLS() if ATENET.startswith("https://") else None
ACTOR_DOMAIN = os.environ.get("ACTOR_DOMAIN", DEFAULT_ACTOR_DOMAIN)
API_SERVER_KEY = os.environ.get("API_SERVER_KEY", "")
IDLE_TIMEOUT = float(os.environ.get("IDLE_TIMEOUT_SECONDS", "60"))
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8080"))
MODEL_DEFAULT = os.environ.get("MODEL_DEFAULT", "")
TURN_DEBUG = int(os.environ.get("TURN_DEBUG_CHARS", "0"))
# How long we wait for one turn before giving up on the HTTP request. Note this
# does NOT abandon the agent: on timeout the actor is marked working, and only a
# completion signal (turn-signal plugin) or MAX_WORKING_SECONDS releases it.
# Configurable mainly so the long-task path can actually be exercised - with a
# hardcoded 300s it could only ever be unit-tested.
TURN_TIMEOUT = float(os.environ.get("TURN_TIMEOUT_SECONDS", "300"))
# How long a turn will wait for a worker to free up before giving up (see _post_turn).
WORKER_WAIT = float(os.environ.get("WORKER_WAIT_SECONDS", "120"))
# Retries for a cold first turn (see _wake_timeout). Each costs one upstream
# timeout, so this is a wall-clock budget, not just a count. Sized for the worst
# real case rather than the common one: the first turn after an ActorTemplate is
# rebuilt has to pull image layers onto the worker as well as restore, which
# measured past a 3-attempt budget on a live cluster and failed a turn that was
# healthy and nearly done. A warm turn never reaches this path.
WAKE_TIMEOUT_RETRIES = int(os.environ.get("WAKE_TIMEOUT_RETRIES", "5"))
# Envoy answers 503 for TWO different conditions with the same body: a dead
# pooled socket (fixed by reconnecting, instantly) and "this actor has no
# endpoint yet because it is being restored" (fixed by waiting a few seconds).
# The original budget - 3 tries over ~3s - was sized for the first and is far too
# short for the second, so an ordinary wake from suspend failed and the user
# was told "lost the connection to your agent while it was waking up" while the
# actor came up healthy moments later and sat idle.
#
# Sized for a restore now: ~30s of wall clock. Costs nothing when the socket was
# merely stale, because that succeeds on the first retry.
STALE_UPSTREAM_RETRIES = int(os.environ.get("STALE_UPSTREAM_RETRIES", "3"))
# How long to wait for a suspended actor to finish restoring before giving up.
# Generous: a restore measures ~2s, but the actor may also be queued behind
# another on a full worker pool.
RESUME_WAIT = float(os.environ.get("RESUME_WAIT_SECONDS", "90"))
# Once the control plane says RUNNING, its network endpoint still has to reach
# Envoy, and until it does the turn keeps getting 503. Retrying is SAFE in this
# window - an actor that is already running is not resumed again - so this is an
# ordinary poll, unlike the pre-RUNNING case where retrying restarts the restore.
ENDPOINT_RETRIES = int(os.environ.get("ENDPOINT_RETRIES", "15"))
ENDPOINT_POLL = float(os.environ.get("ENDPOINT_POLL_SECONDS", "2.0"))
# Backoff grows 0.5s, 1.0s … then holds, so the early retries stay fast for the
# stale-socket case while the tail covers a restore.
STALE_UPSTREAM_BACKOFF_MAX = float(os.environ.get("STALE_UPSTREAM_BACKOFF_MAX", "4.0"))
# Retries for a restore that failed on a stale worker entry (see _resume_failed).
RESUME_RETRIES = int(os.environ.get("RESUME_RETRIES", "2"))
CRON_STATE_PATH = os.environ.get("CRON_STATE_PATH", DEFAULT_STATE_PATH)
CRON_TICK = float(os.environ.get("CRON_TICK_SECONDS", str(DEFAULT_TICK_SECONDS)))
CRON_CATCHUP = float(os.environ.get("CRON_CATCHUP_SECONDS", str(DEFAULT_CATCHUP_SECONDS)))

# WhatsApp Cloud channel is optional: routes are only registered when the required
# credentials are present (phone number id + access token).
WA_CFG = wa.WhatsAppConfig.from_env()
# Telegram is optional too, and inverted: it registers no routes at all, because the
# gateway dials out to getUpdates rather than being called. Needs only a bot token.
TG_CFG = tg.TelegramConfig.from_env()


def _session_key(req: web.Request, body: dict) -> str:
    """Canonical conversation key: explicit session header/field wins, else per-caller."""
    return (
        req.headers.get("X-Hermes-Session-Key")
        or req.headers.get("X-Hermes-Session-Id")
        or (body.get("user") if isinstance(body, dict) else None)
        or req.remote
        or "anonymous"
    )


def _worker_starved(status: int, raw: bytes) -> bool:
    """Is this atenet 503 the worker pool being full, rather than a real failure?

    atenet answers `no free workers available` when every worker in the pool is
    already bound to a running actor. It is a capacity signal, not an error: a
    worker frees the moment any idle actor is suspended.
    """
    return status == 503 and b"no free workers" in raw.lower()


def _stale_upstream(status: int, raw: bytes) -> bool:
    """Envoy 503 from a pooled connection to an actor that no longer exists.

    Envoy pools HTTP/1.1 connections by destination address, but a worker pod keeps
    its IP while the actor sandbox behind port 80 is torn down on every suspend - so
    a pooled connection can belong to an actor that is already gone, and the request
    dies in a dead socket. Substrate fixed this upstream in #549 (max_requests_per_
    connection=1 on the forward-proxy cluster); this cluster's control plane predates
    that, so we retry instead. Safe: 'before headers' means the actor never saw it.

    Delete this once the control plane carries #549 - a retry only hides the race.
    """
    return status == 503 and b"before headers" in raw.lower()


# Two different hops report the same cold-start condition with two different
# bodies, and matching only one of them silently disables the retry:
#
#   "upstream request timeout"                  - Envoy, waiting on the actor
#   "actor hermes/agent-… request timed out"    - atenet, waiting on a resume
#
# Observed live: the second appears while the actor is STATUS_RESUMING, i.e.
# exactly the case this retry exists for.
_WAKE_TIMEOUT_BODIES = (b"upstream request timeout", b"request timed out")


def _wake_timeout(status: int, raw: bytes) -> bool:
    """A hop timed out waiting for the actor to come up on the FIRST turn.

    The cold path is restore + Hermes becoming ready + the model call, and that
    can sit just past a route timeout while every individual step is healthy.
    Crucially the actor is warm (or nearly) by the time the 504 arrives, so a
    retry skips the restore and lands on a running agent. Treating this as fatal
    fails a turn that would have succeeded a second later - which is how a
    perfectly good agent looks broken to whoever emailed it.
    """
    return status == 504 and any(b in raw for b in _WAKE_TIMEOUT_BODIES)


def _actor_missing(status: int, raw: bytes) -> bool:
    """The actor we were told exists does not.

    Happens when an actor is deleted out from under the gateway - an operator
    clearing them, or hermesctl rebuilding an immutable ActorTemplate. The
    provisioner caches "this actor exists" forever, so without noticing this the
    gateway keeps routing to a name that is gone and the agent appears dead
    for good.
    """
    return status in (404, 500) and b"not found" in raw and b"actor" in raw


def _resume_failed(status: int, raw: bytes) -> bool:
    """Did the restore itself fail, rather than the actor answering badly?

    Observed on the live cluster as

        500 error resuming actor "agent-…"
        (ateapi: workflow failed at step CallAteletRestore: worker pod not found)

    - the control plane picked a worker that no longer exists, having not yet
    reconciled its registry after a suspend released one. Measured at roughly one
    resume in six on this cluster, and an immediate retry lands on a live worker.

    Retrying is safe *specifically because the restore failed*: the actor never ran,
    so nothing was half-applied, and the checkpoint is untouched. That is a narrower
    claim than "500s are retryable" - a 500 from Hermes itself is not retried here.

    Upstream fix is worker-registry GC / a restore preflight; until then the
    alternative is telling a user their agent didn't wake when trying twice would
    have woken it.
    """
    return status >= 500 and b"error resuming actor" in raw.lower()


async def _post_turn(
    client: ClientSession, url: str, body: str, headers: dict, actor: str,
    ate=None,
) -> tuple[int, bytes, str]:
    """POST one turn to atenet, waiting out worker-pool exhaustion.

    A pool with N workers can only hold N non-suspended actors, so the (N+1)th
    concurrent conversation gets an instant 503. Failing the turn there is the
    wrong answer twice over: the caller sees an error for what is really a queue,
    and - because the actor was already provisioned - a retry lands on a *fresh*
    actor with no memory, which reads to the user as the agent forgetting them.

    So we wait instead. Capacity is reclaimed by the idle-suspender, which runs on
    its own timer (IDLE_TIMEOUT_SECONDS, polled every 5s), so the backoff is
    deliberately coarse - there is nothing to gain from hammering. Everything that
    is not worker starvation returns on the first attempt, untouched.
    """
    deadline = time.monotonic() + WORKER_WAIT
    delay, attempt, stale_retries, resume_retries = 1.0, 0, 0, 0
    wake_retries = 0
    # Two distinct waits for a 503: before the actor is RUNNING (must not
    # re-poke - that restarts the restore) and after (safe to poll while its
    # endpoint propagates to Envoy).
    endpoint_retries = 0
    running_confirmed = False
    while True:
        attempt += 1
        # Long timeout: the first turn includes an atenet cold-restore of the actor.
        tls = ({"ssl": ROUTER_TLS.context()}
               if ROUTER_TLS is not None and url.startswith("https://") else {})
        async with client.post(
            url, data=body, headers=headers, timeout=ClientTimeout(total=TURN_TIMEOUT),
            **tls,
        ) as resp:
            raw = await resp.read()
            ctype = resp.headers.get("Content-Type", "application/json")
            if _stale_upstream(resp.status, raw):
                # Envoy answers 503 both for a dead pooled socket and for an
                # actor with no endpoint because it is being restored.
                #
                # Do NOT simply retry the turn. Every request to a suspended
                # actor makes atenet issue another ResumeActor RPC, so a retry
                # loop keeps restarting the restore it is waiting for: six
                # ResumeActor calls in 26 seconds, and a turn that failed while
                # the actor was healthy and came up moments later. The retry was
                # the cause, not the cure.
                #
                # One immediate retry covers the stale-socket case, which costs
                # nothing. After that, ask the control plane and WAIT.
                if running_confirmed:
                    # The actor IS running; we are only waiting for its endpoint
                    # to reach Envoy. Safe to poll - no resume is triggered.
                    if endpoint_retries < ENDPOINT_RETRIES:
                        endpoint_retries += 1
                        log.info("actor %s running but no route yet - waiting "
                                 "%d/%d", actor, endpoint_retries, ENDPOINT_RETRIES)
                        await asyncio.sleep(ENDPOINT_POLL)
                        continue
                    log.error("actor %s is RUNNING but never got a route (%.0fs)",
                              actor, ENDPOINT_RETRIES * ENDPOINT_POLL)
                    return resp.status, raw, ctype

                if stale_retries < STALE_UPSTREAM_RETRIES:
                    stale_retries += 1
                    if stale_retries == 1:
                        log.warning("actor %s: no upstream (Envoy 503) - one fast "
                                    "retry in case the socket was stale", actor)
                        await asyncio.sleep(0.5)
                        continue
                    log.warning("actor %s: still no upstream - waiting for the "
                                "restore to finish rather than re-poking it", actor)
                    if ate is not None and await ate.wait_running(
                            ATESPACE, actor, timeout=RESUME_WAIT):
                        log.info("actor %s is running - now waiting for its route",
                                 actor)
                        running_confirmed = True
                        continue
                    log.error("actor %s did not reach RUNNING within %.0fs",
                              actor, RESUME_WAIT)
                    return resp.status, raw, ctype
                return resp.status, raw, ctype
            if _resume_failed(resp.status, raw):
                if resume_retries < RESUME_RETRIES:
                    resume_retries += 1
                    log.warning("actor %s: resume failed (%s) - retry %d/%d",
                                actor, raw[:120].decode("utf-8", "replace"),
                                resume_retries, RESUME_RETRIES)
                    # Give the control plane a moment to reconcile its worker
                    # registry; retrying instantly lands on the same dead entry.
                    await asyncio.sleep(2.0 * resume_retries)
                    continue
                log.error("actor %s: resume still failing after %d retries",
                          actor, resume_retries)
                return resp.status, raw, ctype
            if _wake_timeout(resp.status, raw):
                # A 504 says a hop stopped waiting. It does NOT say whether the
                # actor received the turn, and that distinction decides whether a
                # retry is a fix or a second copy of the work.
                #
                #   actor not RUNNING  -> it cannot have taken the turn. This is
                #                         the cold-start race the retry exists for,
                #                         and re-sending is safe.
                #   actor RUNNING      -> it most likely HAS the turn and is
                #                         working on it right now. Re-sending
                #                         queues the same request again: observed
                #                         live as one long turn becoming six, each
                #                         doing the whole job.
                #
                # The same reasoning the 503 path already applies ("the retry was
                # the cause, not the cure") - it was simply never applied here.
                running = None
                if ate is not None:
                    try:
                        running = await ate.actor_status(ATESPACE, actor)
                    except Exception as exc:
                        log.warning("could not read actor status for %s: %s", actor, exc)
                if running == ACTOR_RUNNING:
                    log.warning("actor %s: upstream timed out but the actor is "
                                "RUNNING and has probably taken this turn - not "
                                "retrying, because a retry would run it twice. "
                                "Holding it awake until it reports finishing.",
                                actor)
                    return resp.status, raw, ctype
                if wake_retries < WAKE_TIMEOUT_RETRIES:
                    wake_retries += 1
                    log.warning("actor %s: upstream timed out while the actor was "
                                "%s - it cannot have taken the turn, retrying "
                                "%d/%d", actor, running or "unreadable",
                                wake_retries, WAKE_TIMEOUT_RETRIES)
                    continue
                log.error("actor %s: still timing out after %d retries",
                          actor, wake_retries)
                return resp.status, raw, ctype
            if not _worker_starved(resp.status, raw):
                return resp.status, raw, ctype

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            log.error(
                "actor %s: no worker after %.0fs (%d attempts) - giving up; "
                "the pool is undersized for this load",
                actor, WORKER_WAIT, attempt,
            )
            return resp.status, raw, ctype

        wait = min(delay, remaining)
        log.info(
            "actor %s: pool full, waiting %.0fs for a worker (attempt %d, %.0fs left)",
            actor, wait, attempt, remaining,
        )
        await asyncio.sleep(wait)
        delay = min(delay * 2, 10.0)



async def _forward_turn(
    app: web.Application, session_key: str, payload: dict, agent=None,
    turn_key: str = "",
) -> tuple[int, bytes, str]:
    """Ensure the actor, forward one turn through atenet, bracketed for idle.

    Returns (status, raw_body, content_type). Shared core for the HTTP surface
    (_proxy_turn), the chat channels (which need the raw body to extract the reply
    text and send it back out), and the cron scheduler.

    The actor is the **agent's**, not the conversation's: several conversations with
    the same person resolve to one agent, and the conversation key travels on
    as the Hermes session id so Hermes keeps the threads apart inside it.
    """
    # Channels may supply an already-resolved agent; otherwise use the registry.
    agent = agent or app["agents"].for_channel(session_key)
    name = agent.actor
    host = actor_host(name, ATESPACE, ACTOR_DOMAIN)

    # A turn carries an identity when the channel gave us one that is stable
    # across its own retries. Claim it BEFORE the actor is asked to do anything:
    # a claim written afterwards leaves the hole it exists to close.
    turns: "TurnDedup | None" = app.get("turns")
    claimed = False
    if turns is not None and turn_key:
        prior = turns.begin(turn_key)
        if prior is not None:
            replay = TurnDedup.replay(prior)
            if replay is not None:
                # The work already happened. Its answer was lost with the
                # connection, and this retry is what delivers it.
                log.info("turn %s already applied, replaying stored response",
                         turn_key)
                return replay
            if prior.get("state") == "orphaned":
                # Dispatched by a process that died. Whether the actor applied it
                # is not knowable, and the evidence says it usually did, so it is
                # answered rather than run a second time.
                log.warning("turn %s was in flight when the gateway restarted, "
                            "not re-running it", turn_key)
                return (200, json.dumps({"choices": [{"message": {
                    "role": "assistant",
                    "content": ("That message reached me, but my reply was lost "
                                "when the gateway restarted. I have not run it "
                                "again in case it had already been acted on. "
                                "Please resend if nothing happened."),
                }}]}).encode(), "application/json")
            log.warning("turn %s is already running, refusing the duplicate",
                        turn_key)
            return (409, b"duplicate turn already in flight", "text/plain")
        claimed = True

    await app["provisioner"].ensure(name, TEMPLATE)

    idle: IdleSuspender = app["idle"]
    idle.begin(name)
    dispatched = False
    try:
        headers = {
            # v1 routes only on ate-target-actor. release-0.1 routes on Host, and
            # over plain HTTP only, so Host is set only on that path: over TLS
            # the URL's own host has to match the router's certificate.
            "ate-target-actor": f"{ATESPACE}/{name}",
            "Content-Type": "application/json",
            "X-Hermes-Session-Id": session_key,
        }
        if ROUTER_TLS is None:
            headers["Host"] = host
        if API_SERVER_KEY:
            headers["Authorization"] = f"Bearer {API_SERVER_KEY}"
        url = f"{ATENET}/v1/chat/completions"
        sent = payload
        dispatched = True
        status, raw, ctype = await _post_turn(
            app["http"], url, json.dumps(sent), headers, name, ate=app.get("ate")
        )
        if _wake_timeout(status, raw) or status == 504:
            # We stopped waiting; the agent may well still be working. Protect it
            # until it says otherwise (turn-signal plugin) or the ceiling in
            # IdleSuspender expires.
            idle.still_working(name)
        if _actor_missing(status, raw):
            # Someone deleted it. Recreate and try once - the alternative is
            # answering "not found" to every turn until the gateway restarts.
            log.warning("actor %s is gone - recreating and retrying once", name)
            app["provisioner"].forget(name)
            await app["provisioner"].ensure(name, TEMPLATE)
            status, raw, ctype = await _post_turn(
                app["http"], url, json.dumps(payload),
                headers, name, ate=app.get("ate")
            )
        log.info("turn %s/%s (agent %s, session %s) -> %s (%d bytes)",
                 ATESPACE, name, agent.agent_id, session_key, status, len(raw))
        stats = app.get("fleet")
        if stats is not None:
            # Token usage comes from the model's own report in the response, so
            # cost is measured rather than estimated.
            usage, err = None, ""
            if 200 <= status < 300:
                try:
                    usage = json.loads(raw).get("usage")
                except Exception:
                    usage = None
            else:
                err = raw[:160].decode("utf-8", "replace")
            stats.record(agent.agent_id, 200 <= status < 300, usage, err)
        if TURN_DEBUG:
            # Optional diagnostics for forwarded messages.
            for m in (sent.get("messages") or []):
                log.info("  turn-debug %s: %s", m.get("role"),
                         str(m.get("content"))[:TURN_DEBUG][:4000].replace("\n", " | "))
        if claimed:
            turns.finish(turn_key, status, raw, ctype)
        return status, raw, ctype
    except BaseException:
        if claimed:
            if dispatched:
                # The actor may have applied it, so the retry must not run it
                # again. Store the failure instead: a replayed explanation is
                # honest, an unbounded 409 loop is not.
                turns.finish(turn_key, 502, b"turn dispatched but failed before "
                             b"a response was recorded", "text/plain")
            else:
                # Nothing reached the actor, so this is not a turn that happened
                # and its retry should be allowed to succeed.
                turns.abandon(turn_key)
        raise
    finally:
        idle.end(name)


async def _proxy_turn(app: web.Application, session_key: str, payload: dict) -> web.Response:
    """Forward a turn and wrap the actor's response as an HTTP response."""
    status, raw, ctype = await _forward_turn(app, session_key, payload)
    # Pass Content-Type through the header, not the content_type kwarg: the actor's
    # value includes a charset (e.g. "application/json; charset=utf-8"), which
    # web.Response(content_type=...) rejects.
    return web.Response(status=status, body=raw, headers={"Content-Type": ctype})


def _extract_reply(raw: bytes) -> str:
    """Pull the assistant text out of the actor's OpenAI-compatible response body."""
    try:
        data = json.loads(raw)
        content = data["choices"][0]["message"]["content"]
        return content if isinstance(content, str) else json.dumps(content)
    except Exception:
        return ""


# How many consecutive failures before we stop calling it transient. Two is a
# blip; three in a row means something is broken and telling the user to
# retry just walks them into the same wall.
SYSTEMIC_AFTER = int(os.environ.get("SYSTEMIC_FAILURES", "3"))


class FailureStreak:
    """Consecutive failed turns, so a message can tell a blip from an outage.

    Deliberately in-memory and per-process. This is not monitoring - that comes
    later in the control plane - it is the minimum state needed to stop saying
    "usually transient, please send that again" during an outage that lasted
    twenty-one hours and told every user exactly that, every time.
    """

    def __init__(self) -> None:
        self._n: "dict[str, int]" = {}
        self._since: "dict[str, float]" = {}

    def failed(self, key: str) -> "tuple[int, float]":
        self._n[key] = self._n.get(key, 0) + 1
        self._since.setdefault(key, time.time())
        return self._n[key], self._since[key]

    def ok(self, key: str) -> None:
        self._n.pop(key, None)
        self._since.pop(key, None)

    def state(self, key: str) -> "tuple[int, float | None]":
        return self._n.get(key, 0), self._since.get(key)


def _how_long(since: float) -> str:
    mins = max(1, int((time.time() - since) / 60))
    if mins < 60:
        return f"{mins} minute{'s' if mins != 1 else ''}"
    hours = mins / 60
    if hours < 24:
        return f"{hours:.0f} hour{'s' if round(hours) != 1 else ''}"
    return f"{hours / 24:.0f} day{'s' if round(hours / 24) != 1 else ''}"


def _systemic_message(count: int, since: float) -> str:
    """What to say once it is clearly not a blip.

    Names the duration, because "it has been failing for 3 hours" is what makes
    someone escalate instead of politely retrying. And explicitly tells them NOT
    to resend, which is the opposite of the transient advice.
    """
    return (f"Your agent is not responding, and this is not a temporary "
            f"blip: every attempt has failed for the last {_how_long(since)} "
            f"({count} in a row). Resending won't help. This needs an operator - "
            f"please pass this message on, and mention that turns are failing "
            f"before they reach the agent.")


def _failure_message(status: int, raw: bytes) -> str:
    """What to tell a human when the turn didn't produce an answer (T11).

    "Sorry, something went wrong" is the same sentence for a full worker pool, a
    sandbox that failed to restore, and a bad API key - three situations with three
    different responses ("wait a minute", "tell an operator", "fix the config"). The
    user can't act on the generic version and neither can whoever they forward it to,
    so each condition we can actually recognise gets its own sentence. Anything we
    can't recognise still falls through to the generic one, but the status code goes
    with it so a screenshot is enough to start debugging.
    """
    if _worker_starved(status, raw):
        return ("Every agent worker is busy right now, and one didn't free up in "
                "time. Your message wasn't lost - try again in a minute.")
    if _stale_upstream(status, raw):
        return ("Lost the connection to your agent while it was waking up. This is "
                "usually transient - please send that again.")
    if status in (401, 403):
        return ("I couldn't authenticate to your agent. That's a configuration "
                "problem on my side, not something you can retry - please tell an "
                "operator.")
    if status == 404:
        return ("Your agent doesn't exist yet, or its template is missing. Please "
                "tell an operator.")
    if _resume_failed(status, raw):
        return ("Your agent couldn't be restored from its saved state - the "
                "platform tried more than once. Please try again shortly, and tell "
                "an operator if it keeps happening.")
    if status in (502, 503, 504):
        return ("Your agent didn't wake up in time. It was suspended to save "
                "resources and the restore didn't complete - please try again.")
    if status >= 500:
        return f"Your agent hit an internal error (HTTP {status}). Please try again."
    return f"I couldn't get a response from your agent (HTTP {status})."


def _reply_or_error(status: int, raw: bytes, who: str,
                    streak: "FailureStreak | None" = None) -> str:
    """The assistant's text, or an actionable explanation of why there isn't any."""
    if 200 <= status < 300:
        reply = _extract_reply(raw)
        if reply:
            if streak:
                streak.ok(who)
            return reply
        log.error("turn for %s returned %d but no assistant content: %s",
                  who, status, raw[:400])
        return ("Your agent answered, but the reply was empty. Please try "
                "rephrasing that.")
    log.error("turn for %s failed: HTTP %d %s", who, status, raw[:400])
    if streak:
        count, since = streak.failed(who)
        if count >= SYSTEMIC_AFTER:
            # Loud, and greppable, because nothing is watching this yet.
            log.error("SYSTEMIC FAILURE: %s has failed %d consecutive turns over "
                      "%s - an operator needs to look", who, count,
                      _how_long(since))
            return _systemic_message(count, since)
    return _failure_message(status, raw)


def _authorize_ingress(req: web.Request) -> None:
    # Trusted adapters only: this grants fleet-wide routing, never give it to actors.
    expected = os.environ.get("GATEWAY_INGRESS_KEY", "")
    if not expected or expected == os.environ.get("API_SERVER_KEY", ""):
        raise web.HTTPServiceUnavailable(text="generic ingress is not configured")
    supplied = req.headers.get("Authorization", "")
    if not hmac.compare_digest(supplied.encode(), ("Bearer " + expected).encode()):
        raise web.HTTPUnauthorized(text="unauthorized")


async def chat_completions(req: web.Request) -> web.Response:
    _authorize_ingress(req)
    body = await req.json()
    return await _proxy_turn(req.app, _session_key(req, body), body)


async def webhook_message(req: web.Request) -> web.Response:
    """Generic webhook: {"session":"...","text":"..."} -> one chat turn."""
    _authorize_ingress(req)
    body = await req.json()
    session_key = body.get("session") or _session_key(req, body)
    payload = {
        "model": body.get("model", os.environ.get("MODEL_DEFAULT", "")),
        "messages": [{"role": "user", "content": body.get("text", "")}],
    }
    return await _proxy_turn(req.app, session_key, payload)


async def whatsapp_verify(req: web.Request) -> web.Response:
    """Meta's GET verify handshake - echo hub.challenge when the token matches."""
    challenge = wa.verify_challenge(WA_CFG, dict(req.query))
    if challenge is None:
        log.warning("whatsapp verify failed (bad mode/token)")
        return web.Response(status=403, text="forbidden")
    log.info("whatsapp webhook verified")
    return web.Response(text=challenge)


async def whatsapp_receive(req: web.Request) -> web.Response:
    """Meta's POST webhook.

    We verify the signature, de-dup, and dispatch each message to a background
    task, then return 200 immediately. A chat turn can take 7-17s (cold restore),
    far longer than Meta's webhook timeout - blocking here would make Meta retry
    and double-fire. The reply is delivered out-of-band via the Graph API.
    """
    raw = await req.read()
    if not wa.verify_signature(WA_CFG, raw, req.headers.get("X-Hub-Signature-256")):
        log.warning("whatsapp webhook signature rejected")
        return web.Response(status=403, text="bad signature")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return web.Response(status=400, text="bad json")

    dedup: wa.WamidDedup = req.app["wa_dedup"]
    for msg in wa.parse_messages(payload):
        if dedup.seen_before(msg.wamid):
            log.info("whatsapp dup wamid %s ignored", msg.wamid)
            continue
        asyncio.create_task(_handle_whatsapp(req.app, msg))
    return web.Response(text="EVENT_RECEIVED")


async def _handle_whatsapp(app: web.Application, msg: wa.InboundMessage) -> None:
    """Run one WhatsApp message as a chat turn against the sender's own actor.

    Session key is scoped by (business number, sender) so each end user gets a
    dedicated suspend/resume actor whose memory persists across suspends, and
    multiple business numbers (tenants) never collide.
    """
    session_key = f"whatsapp:{msg.phone_number_id}:{msg.from_wa_id}"
    payload = {
        "model": MODEL_DEFAULT,
        "messages": [{"role": "user", "content": msg.text}],
    }
    # WhatsApp Cloud has no typing action available to us, so there is no ticker to
    # run - but the slow-wake notice still applies, and it is the part that matters.
    presence = PresenceTicker(
        tick=_noop,
        notify=lambda text: wa.send_text(app["http"], WA_CFG, msg.from_wa_id, text),
        label=f"whatsapp:{msg.from_wa_id}",
    )
    try:
        async with presence:
            status, raw, _ctype = await _forward_turn(
                app, session_key, payload, turn_key=f"whatsapp:{msg.wamid}")
        reply = _reply_or_error(status, raw, f"whatsapp:{msg.from_wa_id}",
                                app.get("failures"))
    except Exception as exc:
        log.exception("whatsapp turn failed for %s: %s", msg.from_wa_id, exc)
        reply = "Sorry - something went wrong handling your message."
    await wa.send_text(app["http"], WA_CFG, msg.from_wa_id, reply)


async def _handle_telegram(app: web.Application, msg: tg.InboundMessage) -> None:
    """Run one Telegram message as a chat turn against that chat's own actor.

    The session key is scoped by chat id, so each chat (a DM, or a group) is its own
    thread; which *agent* owns that thread is the registry's call.

    The turn runs inside a PresenceTicker because Telegram's typing indicator expires
    after ~5s while a cold restore takes longer - one `sendChatAction` at the start
    leaves the user watching a silent chat for most of the wake. See presence.py.
    """
    session_key = f"telegram:{msg.chat_id}"
    payload = {"model": MODEL_DEFAULT, "messages": [{"role": "user", "content": msg.text}]}
    presence = PresenceTicker(
        tick=lambda: tg.send_typing(app["http"], TG_CFG, msg.chat_id),
        notify=lambda text: tg.send_text(app["http"], TG_CFG, msg.chat_id, text),
        label=f"telegram:{msg.chat_id}",
    )
    try:
        async with presence:
            status, raw, _ctype = await _forward_turn(
                app, session_key, payload, turn_key=f"telegram:{msg.update_id}")
        reply = _reply_or_error(status, raw, f"telegram:{msg.chat_id}",
                                app.get("failures"))
    except Exception as exc:
        log.exception("telegram turn failed for chat %s: %s", msg.chat_id, exc)
        reply = "Sorry - something went wrong handling your message."
    await tg.send_text(app["http"], TG_CFG, msg.chat_id, reply)


async def _noop() -> None:
    """A presence tick for channels that have no presence signal."""


# ---------------------------------------------------------------------------
# Channel addresses
#
# `telegram:<chat_id>` / `whatsapp:<phone_number_id>:<wa_id>` - the same strings the
# channel handlers use as session keys, which is what lets a scheduled job name where
# its result should land and lets that result share the human's thread.
# ---------------------------------------------------------------------------


async def _deliver_to_address(app: web.Application, address: str, text: str) -> None:
    """Send text to a channel address. Raises if the address can't be delivered to."""
    scheme, _, rest = address.partition(":")
    if scheme == "telegram" and TG_CFG.enabled:
        await tg.send_text(app["http"], TG_CFG, int(rest), text)
        return
    if scheme == "whatsapp" and WA_CFG.enabled:
        _phone_number_id, _, wa_id = rest.partition(":")
        await wa.send_text(app["http"], WA_CFG, wa_id, text)
        return
    raise ValueError(f"cannot deliver to {address!r} - channel unavailable or unknown")


async def _run_cron_turn(app: web.Application, job, prompt: str, session_key: str) -> str:
    """One scheduled turn. Same path as an inbound message, so it inherits
    resume-on-demand, the worker-starvation wait and idle bracketing."""
    payload = {"model": MODEL_DEFAULT, "messages": [{"role": "user", "content": prompt}]}
    status, raw, _ctype = await _forward_turn(app, session_key, payload)
    return _reply_or_error(status, raw, f"cron:{job.key}", app.get("failures"))


async def _telegram_poll_loop(app: web.Application) -> None:
    """Long-poll getUpdates forever, dispatching each message as its own turn.

    Two things worth knowing. First, we skip whatever piled up while the gateway was
    down: replaying a backlog on boot would cold-restore actors for conversations
    nobody is watching, which costs real money and answers questions the user has
    long since stopped waiting on. Second, each turn is dispatched as a task rather
    than awaited - a cold restore is ~10s and blocking the loop on it would stall
    every other chat, and worse, delay the offset ack until Telegram redelivered.
    """
    http: ClientSession = app["http"]
    offset: int | None = None
    try:
        # offset=-1 asks for just the most recent update; its id+1 becomes our cursor,
        # which permanently acks everything older. Short poll - nothing to wait for.
        priming = dataclasses.replace(TG_CFG, poll_timeout=1)
        _drop, offset = await tg.get_updates(http, priming, -1)
        log.info("telegram backlog skipped (cursor at %s)", offset)
    except Exception as exc:
        log.warning("telegram backlog skip failed (%s) - starting from live", exc)

    backoff = 1.0
    while True:
        try:
            msgs, next_offset = await tg.get_updates(http, TG_CFG, offset)
            backoff = 1.0
            if next_offset is not None:
                offset = next_offset
            for m in msgs:
                if not TG_CFG.allows(m.chat_id):
                    log.warning("telegram chat %s not in allowlist - ignored", m.chat_id)
                    continue
                log.info("telegram msg from chat %s (%s)", m.chat_id, m.name or "?")
                asyncio.create_task(_handle_telegram(app, m))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("telegram poll failed: %s - retrying in %.0fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)



async def _worker_pod_ips() -> "dict[str, str]":
    """{pod_ip: pod_uid} for the worker pods, read with the gateway's own
    credentials. The gateway resolves callers through this rather than trusting
    anything in the request."""
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "get", "pods", "-n", os.environ.get("NAMESPACE", "hermes"),
        "-o", "jsonpath={range .items[*]}{.status.podIP} {.metadata.uid}{\"\\n\"}{end}",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    out, _ = await proc.communicate()
    pods = {}
    for line in out.decode().splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0]:
            pods[parts[0]] = parts[1]
    return pods


_proxy_ip_warned = False


async def _egress_proxy_ips() -> "set[str]":
    """Addresses allowed to say who is calling on an actor's behalf.

    Egress interception rewrites every actor's source address to the egress
    gateway's, so the gateway can no longer tell callers apart by address alone;
    the plug-in stamps the verified identity on the request instead, and this is
    the list of peers whose stamp is believed. See caller_auth.py.

    Selected by ServiceAccount rather than by the `app` label, so the address we
    trust here belongs to the same principal the plug-in pins on the other side
    of the connection (spiffe://cluster.local/ns/ate-system/sa/atenet-egress). A
    label is metadata anyone with edit rights can copy onto a Pod; a
    ServiceAccount is the identity itself.
    """
    global _proxy_ip_warned
    ns = os.environ.get("EGRESS_PROXY_NAMESPACE", "ate-system")
    sa = os.environ.get("EGRESS_PROXY_SERVICE_ACCOUNT", "atenet-egress")
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "get", "pods", "-n", ns, "-o",
        "jsonpath={range .items[?(@.spec.serviceAccountName==\"" + sa + "\")]}"
        "{.status.podIP}{\"\\n\"}{end}",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    ips = {line.strip() for line in out.decode().splitlines() if line.strip()}
    if not ips and not _proxy_ip_warned:
        # Once, not per request: with egress off this is the normal state, and a
        # warning per call would bury the one case that matters - RBAC missing on
        # a cluster that DOES intercept, where every turn signal is refused.
        _proxy_ip_warned = True
        log.warning("no %s/%s pods visible%s - if this cluster intercepts egress, "
                    "actors cannot be identified and turn signalling will be "
                    "refused fleet-wide", ns, sa,
                    ": " + err.decode().strip() if err else "")
    return ips


async def health(_req: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})



async def on_startup(app: web.Application) -> None:
    registry = AgentRegistry.from_env()
    client = KubectlAteClient()
    # Shared with the turn path: a 503 means "restore in progress", and the only
    # safe way to wait for it is to ask the control plane rather than re-send the
    # turn (which starts the restore over).
    app["ate"] = client
    await client.create_atespace(ATESPACE)
    app["http"] = ClientSession()
    vertex_cfg = VertexProxyConfig.from_env()
    if vertex_cfg.enabled:
        app["vertex_proxy"] = VertexProxy(vertex_cfg, _egress_proxy_ips)
        log.info("model proxy on: backend=%s model=%s project=%s location=%s",
                 vertex_cfg.backend, vertex_cfg.active_model,
                 vertex_cfg.project, vertex_cfg.location)
    app["agents"] = registry
    app["provisioner"] = ActorProvisioner(ATESPACE, client)
    app["idle"] = IdleSuspender(
        ATESPACE, client, IDLE_TIMEOUT,
        # Per-agent policy, resolved at suspend-decision time (§7.3.4).
        timeout_for=registry.idle_timeout_for_actor,
    )
    app["idle"].start()
    app["wa_dedup"] = wa.WamidDedup()
    # Survives the process, because the process dying is the whole scenario.
    app["turns"] = TurnDedup(os.environ.get("TURN_STORE", DEFAULT_TURN_STORE))
    # Shared across channels so a run of failures reads as one outage rather than
    # as unrelated blips per conversation.
    app["failures"] = FailureStreak()

    # Fleet view: runtime state, activity and usage.
    # Counters live beside the cron cursor on the state volume so "what did this
    # cost" is not reset by every deploy.
    fleet.register(
        app,
        registry=registry,
        ate_client=client,
        atespace=ATESPACE,
        stats=fleet.FleetStats(os.path.join(os.path.dirname(CRON_STATE_PATH),
                                            "fleet-stats.json")),
        idle=app["idle"],
    )
    # Authenticate lifecycle callbacks against running Substrate actors.
    _caller_resolver = ActorResolver(
        atespace=ATESPACE,
        namespace=os.environ.get("NAMESPACE", "hermes"),
        ate_client=client,
        k8s_pods=_worker_pod_ips,
        proxy_pods=_egress_proxy_ips,
    )

    # Keep actors awake until background work has finished.
    register_turn_signals(
        app,
        resolver=_caller_resolver,
        on_turn_ended=app["idle"].signal_done,
        on_turn_working=app["idle"].still_working,
    )

    app["cron"] = CronScheduler(
        registry.all_jobs(),
        run_turn=lambda job, prompt, session: _run_cron_turn(app, job, prompt, session),
        deliver=lambda address, text: _deliver_to_address(app, address, text),
        state_path=CRON_STATE_PATH,
        tick_seconds=CRON_TICK,
        catchup_seconds=CRON_CATCHUP,
    )
    app["cron"].start()

    if TG_CFG.enabled:
        if not TG_CFG.allowed_chat_ids:
            log.warning("TELEGRAM_ALLOWED_CHAT_IDS unset - the bot will answer ANY chat "
                        "that finds it, spending LLM budget; set an allowlist")
        app["telegram"] = asyncio.create_task(_telegram_poll_loop(app))
    log.info("gateway up: atespace=%s template=%s atenet=%s idle-default=%s "
             "whatsapp=%s telegram=%s",
             ATESPACE, TEMPLATE, ATENET, registry.default_tier,
             f"on ({WA_CFG.webhook_path})" if WA_CFG.enabled else "off",
             f"on (long-poll {TG_CFG.poll_timeout}s)" if TG_CFG.enabled else "off")


async def on_cleanup(app: web.Application) -> None:
    app["idle"].stop()
    app["cron"].stop()
    tasks = []
    for name in ("telegram",):
        task = app.get(name)
        if task:
            task.cancel()
            tasks.append(task)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    await app["http"].close()


async def vertex_chat_completions(req: web.Request) -> web.Response:
    proxy = req.app.get("vertex_proxy")
    if proxy is None:
        return web.json_response({"error": "Vertex proxy disabled"}, status=404)
    return await proxy.chat_completions(req)


def build_app() -> web.Application:
    app = web.Application()
    app.add_routes([
        web.post("/v1/chat/completions", chat_completions),
        web.post("/message", webhook_message),
        web.get("/health", health),
        web.post("/model/v1/chat/completions", vertex_chat_completions),
    ])
    if WA_CFG.enabled:
        app.add_routes([
            web.get(WA_CFG.webhook_path, whatsapp_verify),
            web.post(WA_CFG.webhook_path, whatsapp_receive),
        ])
        log.info("whatsapp cloud channel enabled at %s", WA_CFG.webhook_path)
    else:
        log.info("whatsapp cloud channel disabled (set WHATSAPP_CLOUD_PHONE_NUMBER_ID + "
                 "WHATSAPP_CLOUD_ACCESS_TOKEN to enable)")
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=LISTEN_PORT)
