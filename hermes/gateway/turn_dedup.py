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
"""Turn identity, so a retried turn is a no-op instead of a second turn.

The failure this exists for is not lost messages. Under an ungraceful gateway
restart every message was delivered and processed and every *response* was lost,
and the caller saw a connection reset it cannot tell apart from "never arrived".
Loss would have been honest: the caller retries and the work happens once. This
is worse, because every channel retries on error, so the work happens twice
against external services with real side effects.

So the primitive is not a queue. It is an identity the gateway can recognise on
the second delivery, plus the response it produced the first time.

Two properties do the work:

  durable   the record outlives the process, because the process dying is the
            whole scenario. A dedup set in memory cannot help here, which is why
            the per-channel ones already in the gateway do not.

  ordered   the in-flight record is committed BEFORE the turn is dispatched. A
            record written afterwards leaves exactly the hole it was meant to
            close: work applied, no record, retry applies it again.

Replaying a stored response is what turns the channel's retry from the hazard
into the recovery: the answer lost with the connection is delivered on the retry
rather than recomputed.

States
------
`done`      the turn completed and its response is stored. A retry replays it.
`inflight`  a turn is running now, in this process. A retry is a genuine
            concurrent duplicate and is refused.
`orphaned`  an inflight record left by a process that died. We cannot know
            whether the actor applied it, and the evidence says it usually did,
            so it is never re-dispatched. A retry is answered, not re-run.

Orphan detection is by boot id rather than by timeout: a record belonging to a
boot that is not ours cannot be ours to finish, and that is decidable rather
than guessed. A timeout would have to be longer than the slowest legitimate
turn, which is exactly when re-dispatching is most expensive.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid

log = logging.getLogger("gateway.turn_dedup")

DEFAULT_TURN_STORE = "/var/lib/hermes-gateway/turns.json"

# Bounded, like the per-channel sets it replaces. A retry that arrives after this
# many turns is indistinguishable from a new message, and holding every turn
# forever would keep user content on disk with no expiry.
DEFAULT_CAPACITY = 2000

# Responses are replayed, so they are stored. A turn that produced something huge
# is truncated rather than dropped: replaying a truncated answer is worse than
# replaying a whole one, and better than re-running the turn.
MAX_STORED_BODY = 256 * 1024

DONE = "done"
INFLIGHT = "inflight"
ORPHANED = "orphaned"


class TurnDedup:
    """Durable turn records. One process, one file, no locking.

    The gateway runs a single replica against a ReadWriteOnce volume, so a plain
    file is enough and a shared store would be unused complexity. If it is ever
    scaled past one replica this has to move, and that is a deliberate decision
    rather than something to discover: two replicas would each keep their own
    file and neither would recognise the other's turns.
    """

    def __init__(self, path: str = DEFAULT_TURN_STORE,
                 capacity: int = DEFAULT_CAPACITY) -> None:
        self.path = path
        self.capacity = capacity
        self.boot = uuid.uuid4().hex
        self._records: "dict[str, dict]" = {}
        self._order: "list[str]" = []
        self._load()

    # ---- persistence -----------------------------------------------------

    def _load(self) -> None:
        """Read what a previous process left, and disown its unfinished turns."""
        try:
            with open(self.path) as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as e:
            # A corrupt store must not stop the gateway serving. Losing the
            # records costs duplicate work on a retry; refusing to start costs
            # every turn.
            log.warning("turn store at %s unreadable (%s), starting empty",
                        self.path, e)
            return

        recs = data.get("turns") or {}
        order = data.get("order") or list(recs)
        orphans = 0
        for k in order:
            r = recs.get(k)
            if not isinstance(r, dict):
                continue
            if r.get("state") == INFLIGHT:
                # Its process is gone, so nothing will ever finish it. The turn
                # was probably applied: that is what the T10 run observed.
                r["state"] = ORPHANED
                orphans += 1
            self._records[k] = r
            self._order.append(k)
        if orphans:
            log.warning("turn store: %d turn(s) were in flight when the previous "
                        "process ended; they will not be re-dispatched", orphans)

    def _flush(self) -> None:
        """Write atomically. A half-written store is worse than no store."""
        tmp = None
        try:
            d = os.path.dirname(self.path) or "."
            os.makedirs(d, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=d, prefix=".turns-", suffix=".tmp")
            with os.fdopen(fd, "w") as fh:
                json.dump({"turns": self._records, "order": self._order}, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
            tmp = None
        except OSError as e:
            # Same trade as a corrupt read: a gateway that cannot write its store
            # still has to answer. The cost is duplicate work after a crash.
            log.warning("could not persist turn store (%s)", e)
        finally:
            if tmp and os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    def _evict(self) -> None:
        while len(self._order) > self.capacity:
            k = self._order.pop(0)
            self._records.pop(k, None)

    # ---- the two calls that matter ---------------------------------------

    def begin(self, key: str) -> "dict | None":
        """Claim `key`, or return the record that already owns it.

        None means "yours, go ahead", and by the time it returns the claim is on
        disk. Anything else is a duplicate and the caller must not dispatch.
        """
        if not key:
            return None
        existing = self._records.get(key)
        if existing is not None:
            return existing
        self._records[key] = {
            "state": INFLIGHT, "boot": self.boot, "started": time.time(),
        }
        self._order.append(key)
        self._evict()
        self._flush()          # before dispatch, never after
        return None

    def finish(self, key: str, status: int, body: bytes, ctype: str) -> None:
        """Record the response, so a retry can be answered instead of re-run."""
        if not key or key not in self._records:
            return
        truncated = len(body) > MAX_STORED_BODY
        self._records[key] = {
            "state": DONE,
            "boot": self.boot,
            "finished": time.time(),
            "status": int(status),
            "ctype": ctype or "application/json",
            "body": (body[:MAX_STORED_BODY]).decode("utf-8", "replace"),
            "truncated": truncated,
        }
        self._flush()

    def abandon(self, key: str) -> None:
        """Release a claim for a turn that never reached the actor.

        Dispatch failing before anything could be applied is not a turn that
        happened, and leaving the claim would refuse the retry that should be
        allowed to succeed.
        """
        if key and self._records.get(key, {}).get("state") == INFLIGHT:
            self._records.pop(key, None)
            try:
                self._order.remove(key)
            except ValueError:
                pass
            self._flush()

    # ---- reading a duplicate ---------------------------------------------

    @staticmethod
    def replay(record: dict) -> "tuple[int, bytes, str] | None":
        """The stored response for a completed turn, if there is one."""
        if record.get("state") != DONE:
            return None
        return (int(record.get("status", 200)),
                (record.get("body") or "").encode("utf-8"),
                record.get("ctype") or "application/json")

    def stats(self) -> "dict[str, int]":
        out = {DONE: 0, INFLIGHT: 0, ORPHANED: 0}
        for r in self._records.values():
            s = r.get("state")
            if s in out:
                out[s] += 1
        return out
