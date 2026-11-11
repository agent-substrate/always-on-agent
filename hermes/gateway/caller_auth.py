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

"""Authenticate lifecycle callback callers.

Accept only a RUNNING actor resolved from its worker pod, or an asserted identity
from a verified egress-proxy pod. Request-supplied identities from other peers
are never trusted.
"""
from __future__ import annotations

import json
import logging
import time

from ateapi_client import ACTOR_RUNNING, actor_state, actor_worker_pod_uid

log = logging.getLogger("gateway.caller_auth")

CACHE_TTL_SECONDS = 5.0

# Set by the egress plug-in from the identity Envoy verified, never by the agent.
# Only believed when the connection comes from the egress gateway itself; see the
# module header.
ASSERTED_ACTOR_HEADER = "X-Hermes-Actor"

# spiffe://<trust domain>/atespace/<atespace>/actor/<name>. Substrate mints it;
# we only take it apart, and only after the atespace has been checked against
# ours, so a neighbouring install cannot name our actors.
_SPIFFE_PREFIX = "spiffe://"


def actor_from_spiffe(spiffe: str, atespace: str) -> "str | None":
    """The actor name in a Substrate actor SPIFFE id, or None if it is not one.

    Strict on purpose. This value decides whose turn ends, so every part of the
    shape is checked rather than reaching for the last path segment.
    """
    spiffe = (spiffe or "").strip()
    if not spiffe.startswith(_SPIFFE_PREFIX):
        return None
    path = spiffe[len(_SPIFFE_PREFIX):]
    _, _, path = path.partition("/")          # drop the trust domain
    parts = path.split("/")
    if len(parts) != 4 or parts[0] != "atespace" or parts[2] != "actor":
        return None
    if parts[1] != atespace:
        log.warning("asserted identity names atespace %r, not ours (%r) - refusing",
                    parts[1], atespace)
        return None
    return parts[3] or None


class ActorResolver:
    """Resolve network callers to running actors."""

    def __init__(self, atespace, namespace, ate_client, k8s_pods, proxy_pods=None):
        self.atespace = atespace
        self.namespace = namespace
        self.ate = ate_client
        self.k8s_pods = k8s_pods
        self.proxy_pods = proxy_pods
        self._actor_cache = {}

    async def _actors(self) -> "list | None":
        rc, out = await self.ate._run(
            ["get", "actor", "-a", self.atespace, "-o", "json"])
        if rc != 0:
            log.warning("actor list failed (rc=%s) - refusing the call", rc)
            return None
        try:
            return json.loads(out).get("actors", [])
        except ValueError:
            log.warning("actor list was not JSON - refusing the call")
            return None

    async def _actor_on_pod(self, pod_uid: str) -> "str | None":
        actors = await self._actors()
        if actors is None:
            return None
        for a in actors:
            # RUNNING only: a suspended actor cannot be the one calling us, and
            # accepting one would widen the race window during a resume.
            if (actor_worker_pod_uid(a) == pod_uid
                    and actor_state(a) == ACTOR_RUNNING):
                return (a.get("metadata") or {}).get("name")
        return None

    async def _is_running(self, actor: str) -> bool:
        """Is this a RUNNING actor of ours?

        The same test the address path applies, asked the other way round. It is
        what stops an asserted identity naming an actor that is suspended,
        deleted, or was never ours - none of which can be the one calling.
        """
        actors = await self._actors()
        if actors is None:
            return False
        for a in actors:
            if ((a.get("metadata") or {}).get("name") == actor
                    and actor_state(a) == ACTOR_RUNNING):
                return True
        return False

    async def _actor_via_proxy(self, peer_ip: str, asserted: str) -> "str | None":
        """Believe the proxy's word for who is calling - and only the proxy's."""
        if self.proxy_pods is None:
            log.warning("a caller at %s asserted an identity but no egress proxy is "
                        "configured - refusing", peer_ip)
            return None
        proxies = await self.proxy_pods()
        if peer_ip not in proxies:
            # Not the proxy and not a worker: somebody else in the cluster is
            # claiming to be an actor. Loud, because nothing legitimate does this.
            log.warning("%s is neither a worker pod nor the egress gateway but "
                        "asserted an actor identity - refusing", peer_ip)
            return None
        actor = actor_from_spiffe(asserted, self.atespace)
        if actor is None:
            log.warning("the egress gateway asserted %r, which is not an actor "
                        "identity in this atespace - refusing", asserted)
            return None
        if not await self._is_running(actor):
            log.warning("asserted actor %s is not RUNNING - refusing", actor)
            return None
        return actor

    async def actor_for(self, peer_ip: str, asserted: str = "") -> "str | None":
        """Resolve the caller using verified network and control-plane data."""
        key = f"{peer_ip}|{asserted}"
        hit = self._actor_cache.get(key)
        if hit and hit[0] > time.monotonic():
            return hit[1]

        actor: "str | None" = None
        try:
            pods = await self.k8s_pods()
            pod_uid = pods.get(peer_ip)
            if pod_uid is not None:
                # Address first, always. An actor that reaches us directly is
                # identified by where it is and never gets to assert anything.
                actor = await self._actor_on_pod(pod_uid)
                if actor is None:
                    log.warning("no RUNNING actor on the pod at %s - refusing", peer_ip)
            elif asserted:
                actor = await self._actor_via_proxy(peer_ip, asserted)
            else:
                log.warning("no worker pod has address %s and it asserted no "
                            "identity - refusing", peer_ip)
        except Exception as exc:                     # never fail open
            log.warning("caller resolution failed for %s: %s", peer_ip, exc)
            actor = None

        self._actor_cache[key] = (time.monotonic() + CACHE_TTL_SECONDS, actor)
        return actor
