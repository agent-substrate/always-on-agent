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

"""kubectl-ate-backed control client (Python port of kubectl-ate-client.ts).

A plain Deployment on this cluster has no PodCertificate to present mTLS to ateapi,
so - exactly like OpenClaw's gateway and the dashboard - we shell out to the
`kubectl-ate` binary, which port-forwards to ateapi and handles auth itself.
Requires the kubectl-ate binary in the pod + RBAC for pods/portforward.
"""
from __future__ import annotations

import asyncio
import time
import json
import logging
import os


class AteApiError(RuntimeError):
    pass


log = logging.getLogger("gateway.ateapi")

# An actor's lifecycle state, in the vocabulary the control plane uses TODAY.
#
# It has used two. `get actor -o json` used to give each actor a flat
# "status": "STATUS_RUNNING"; it now gives a nested
# "status": {"state": "ACTOR_STATE_RUNNING", "workerAssignment": {...}}. Nothing
# fails loudly when a reader assumes the old shape: `a.get("status")` still
# returns a truthy value, it is simply a dict that never equals any string, so
# every "is this actor running?" test quietly answers no. What that looked like
# in practice, all at once and none of it pointing here:
#
#   * the turn-timeout guard decided a RUNNING actor could not have taken the
#     turn and re-sent it - the exact "one long turn becomes six" failure the
#     guard was written to prevent, logging the actor's state as
#     {'state': 'ACTOR_STATE_RUNNING', ...} in the line that says it is not;
#   * wait_running() never saw RUNNING and burned its full 90s on every resume;
#   * caller resolution matched no actor, so an agent could not be identified,
#     and turn signalling was refused fleet-wide.
#
# So the shape is read in ONE place, and both spellings are accepted: this
# gateway has to run against the acceptance cluster and against the older PoC
# install, and a reader that works on one and silently lies on the other is
# worse than either.
ACTOR_RUNNING = "ACTOR_STATE_RUNNING"
ACTOR_CRASHED = "ACTOR_STATE_CRASHED"

_LEGACY_PREFIX = "STATUS_"


def actor_state(actor: dict) -> str:
    """The lifecycle state of one actor from a `get actor -o json` entry."""
    status = actor.get("status")
    state = status.get("state", "") if isinstance(status, dict) else (status or "")
    if state.startswith(_LEGACY_PREFIX):
        state = "ACTOR_STATE_" + state[len(_LEGACY_PREFIX):]
    return state


def actor_worker_pod_uid(actor: dict) -> str:
    """Which worker Pod is hosting this actor, "" if it is not scheduled.

    Renamed along with the shape change: top-level `ateomPodUid` became
    `status.workerAssignment.workerPodUid`.
    """
    status = actor.get("status")
    if isinstance(status, dict):
        assignment = status.get("workerAssignment") or {}
        uid = assignment.get("workerPodUid")
        if uid:
            return uid
    return actor.get("ateomPodUid") or ""


class KubectlAteClient:
    def __init__(self, bin_path: str | None = None, endpoint: str | None = None):
        self.bin = bin_path or os.environ.get("KUBECTL_ATE", "kubectl-ate")
        # Without --endpoint, kubectl-ate auto-detects by reading the ate-api-server
        # Deployment in the pod's namespace - wrong for a gateway outside ate-system.
        self.endpoint = endpoint or os.environ.get("ATE_ENDPOINT", "")

    async def _run(self, args: list[str]) -> tuple[int, str]:
        cmd = [self.bin, *args]
        if self.endpoint:
            cmd += ["--endpoint", self.endpoint]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
        return proc.returncode or 0, out.decode("utf-8", "replace")

    # The flag naming the template was renamed --template -> --template-ref when
    # the ActorTemplate CRD was deleted. Unlike the state shape above this cannot
    # be dual-read, because it is an argument rather than a response, so the new
    # spelling is tried first and the old one only after the client itself says
    # it does not know the flag. Same reason as the state shape: this gateway has
    # to run against the acceptance cluster and against the older PoC install.
    _TEMPLATE_FLAGS = ("--template-ref", "--template")

    @staticmethod
    def _template_ref(atespace: str, template: str) -> str:
        """Drop a leading `<atespace>/` from the template reference.

        `--template` took "atespace/name". `--template-ref` resolves in the
        actor's own atespace and wants a bare RFC 1123 label, and release-0.1
        rejects the slashed form outright:

            actor.actor_template.name: Invalid value: "hermes/hermes": a
            lowercase RFC 1123 label must consist of ...

        which reads as a naming-convention complaint about the template rather
        than a shape change in the argument, and it arrives on the first turn a
        user sends, not at deploy time. TEMPLATE still defaults to the slashed
        spelling because the older PoC install needs it, so the normalisation
        happens here where the atespace is already known.

        Only a prefix that MATCHES the atespace is stripped. A reference to some
        other atespace is not expressible through --template-ref at all, and
        quietly rewriting it to a same-named template in this one would resolve
        to the wrong template instead of failing.
        """
        prefix = f"{atespace}/"
        return template[len(prefix):] if template.startswith(prefix) else template

    async def create_actor(self, atespace: str, name: str, template: str) -> None:
        for i, flag in enumerate(self._TEMPLATE_FLAGS):
            ref = self._template_ref(atespace, template) if flag == "--template-ref" \
                else template
            code, out = await self._run(
                ["create", "actor", name, "-a", atespace, flag, ref]
            )
            if code == 0:
                return
            if "already exists" in out.lower():
                return  # idempotent - another turn/replica won the race
            # Only an unknown-flag rejection is worth retrying. Anything else is
            # a real failure and retrying it under a different spelling would
            # just report the wrong cause.
            last = i == len(self._TEMPLATE_FLAGS) - 1
            if last or "unknown flag" not in out.lower():
                raise AteApiError(f"create actor failed: {out.strip()}")
            log.info("client rejected %s, retrying create actor with %s",
                     flag, self._TEMPLATE_FLAGS[i + 1])

    async def ensure_egress_policy(self, atespace: str, name: str, manifest: str) -> None:
        """Create the actor's egress policy from a manifest file, if it has none.

        Substrate v1 egress is deny by default and policies are per actor, so an
        actor without one cannot reach its model or anything else. release-0.1
        has no egress policies at all; there the command does not exist, and that
        is treated as nothing to do rather than a failure.
        """
        code, out = await self._run(
            ["create", "egress-policy", name, "-a", atespace, "-f", manifest]
        )
        low = out.lower()
        if code == 0 or "already exists" in low:
            return
        if "unknown command" in low:
            log.info("no egress policies on this Substrate; skipping for %s", name)
            return
        raise AteApiError(f"create egress-policy failed: {out.strip()}")

    async def suspend_actor(self, atespace: str, name: str) -> None:
        code, out = await self._run(["suspend", "actor", name, "-a", atespace])
        if code != 0:
            raise AteApiError(f"suspend actor failed: {out.strip()}")

    async def create_atespace(self, atespace: str) -> None:
        code, out = await self._run(["create", "atespace", atespace])
        if code != 0 and "already exists" not in out.lower():
            raise AteApiError(f"create atespace failed: {out.strip()}")

    async def actor_status(self, atespace: str, name: str) -> "str | None":
        """ACTOR_STATE_* for one actor, or None if it cannot be read.

        None means "we could not ask", which is not the same as any state and
        must not be treated as one - see the retry decision in ingress.
        """
        rc, out = await self._run(["get", "actor", "-a", atespace, "-o", "json"])
        if rc != 0:
            return None
        try:
            for a in json.loads(out).get("actors", []):
                if (a.get("metadata") or {}).get("name") == name:
                    return actor_state(a) or None
        except ValueError:
            return None
        return None

    async def wait_running(self, atespace: str, name: str, timeout: float = 90.0,
                           poll: float = 2.0) -> bool:
        """Block until an actor is RUNNING. True if it got there in time.

        Exists because retrying the TURN is the wrong way to wait. Every request
        to a suspended actor makes atenet issue another ResumeActor RPC, so a
        retry loop restarts the restore it is waiting for and the actor never
        finishes coming up - observed as six ResumeActor calls in 26 seconds and
        a turn that failed while the actor was perfectly healthy.

        So: ask the control plane, once, and wait for the answer.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = await self.actor_status(atespace, name)
            if status == ACTOR_RUNNING:
                return True
            if status in (ACTOR_CRASHED, None):
                log.warning("actor %s/%s is %s while waiting to run",
                            atespace, name, status)
            await asyncio.sleep(poll)
        return False
