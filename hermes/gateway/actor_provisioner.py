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

"""Idempotent actor provisioner (Python port of actor-provisioner.ts).

ensure(name, template) creates the actor from its golden ActorTemplate if it does
not already exist, then caches success so subsequent turns are free. Concurrent
first-messages for the same new conversation are collapsed with a per-name
singleflight, and an AlreadyExists is treated as success - so create is idempotent.

The cache is an optimistic assertion, not a fact: an actor can disappear from
under the gateway (an operator deletes it, an ActorTemplate is rebuilt and its
actors are cleared). Without forget(), that cached "ready" is permanent and every
later turn answers "actor not found" until someone restarts the gateway - the
agent looks dead while the cluster is perfectly healthy. ingress calls
forget() when a turn reports the actor missing, so the next turn recreates it.
"""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile

from ateapi_client import KubectlAteClient

log = logging.getLogger("gateway.provisioner")

# Egress for each agent the gateway creates. Substrate v1 denies an actor's
# outbound traffic until it has a policy, and Hermes needs its model API, the web
# tools and this gateway's own callbacks. The default allows any host over HTTP
# and HTTPS, which matches release-0.1's behaviour; set EGRESS_POLICY_FILE to a
# tighter manifest, or EGRESS_POLICY=off to manage policies yourself.
DEFAULT_EGRESS_POLICY = """rules:
- http: {hostnames: ["*"], ports: {all: {}}}
- https: {hostnames: ["*"]}
"""


def _egress_policy_path() -> str | None:
    if os.environ.get("EGRESS_POLICY", "on").lower() in ("off", "0", "false"):
        return None
    path = os.environ.get("EGRESS_POLICY_FILE", "")
    if path:
        return path
    fd, path = tempfile.mkstemp(prefix="hermes-egress-policy-", suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        f.write(DEFAULT_EGRESS_POLICY)
    return path


class ActorProvisioner:
    def __init__(self, atespace: str, client: KubectlAteClient):
        self.atespace = atespace
        self.client = client
        self._ready: set[str] = set()
        self._inflight: dict[str, asyncio.Task] = {}
        self._egress_policy = _egress_policy_path()

    def forget(self, name: str) -> None:
        """Drop the cached belief that this actor exists.

        Called when the cluster tells us otherwise. The next ensure() recreates
        it, so a deleted actor costs one failed turn rather than every turn.
        """
        self._ready.discard(name)
        log.info("forgetting actor %s - it will be recreated on the next turn", name)

    async def ensure(self, name: str, template: str) -> None:
        if name in self._ready:
            return
        task = self._inflight.get(name)
        if task is None:
            task = asyncio.create_task(self._create(name, template))
            self._inflight[name] = task
        try:
            await task
        finally:
            self._inflight.pop(name, None)

    async def _create(self, name: str, template: str) -> None:
        # create_actor already treats "already exists" as success.
        await self.client.create_actor(self.atespace, name, template)
        # Before the first turn wakes it: a missing policy is cached as deny for
        # a few seconds, so creating it late would fail the turn that follows.
        if self._egress_policy:
            await self.client.ensure_egress_policy(self.atespace, name, self._egress_policy)
        self._ready.add(name)
        log.info("created/ensured actor %s/%s from %s", self.atespace, name, template)
