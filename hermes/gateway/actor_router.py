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

"""Stable routing-key to actor-name mapping.

Hashing a routing key produces a valid RFC-1123 DNS label without embedding the
raw key in the actor name. Registered agents can serve multiple conversation
sessions. New callers use actor_name_for_agent; actor_name_for_conversation is
retained for compatibility with earlier demo scripts and existing actors.
"""
from __future__ import annotations

import hashlib

DEFAULT_ACTOR_DOMAIN = "actors.resources.substrate.ate.dev"


def _hash12(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


def actor_name_for_agent(agent_id: str) -> str:
    """agent-<first 12 hex of sha256(agent_id)> - deterministic, DNS-safe, stable.

    The prefix differs from `conv-` deliberately: an operator running
    `kubectl ate get actors` can tell at a glance which naming era an actor belongs
    to, and the two namespaces cannot collide.
    """
    return f"agent-{_hash12(agent_id)}"


def actor_name_for_conversation(canonical_session_key: str) -> str:
    """conv-<first 12 hex of sha256(session_key)> - the pre-agent-registry keying.

    Retained for `demo/` and for addressing actors created before agent keying.
    """
    return f"conv-{_hash12(canonical_session_key)}"


def actor_host(name: str, atespace: str, domain: str = DEFAULT_ACTOR_DOMAIN) -> str:
    """The Host header atenet routes on (atenet routes by Host to actor port 80)."""
    return f"{name}.{atespace}.{domain}"
