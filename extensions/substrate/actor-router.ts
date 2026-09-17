// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/**
 * Conversation → actor placement helpers.
 *
 * The actor name is derived from the canonical conversation key (OpenClaw's
 * sessionKey, which encodes the (accountId, peer) pair). Hashing rather than
 * embedding accountId|peer is deliberate: the peer's phone number never
 * appears in the actor name, DNS, or logs, and the result is always a valid
 * RFC-1123 DNS label regardless of peer format. The mapping is deterministic
 * and stable, so the same conversation always resolves to the same actor and
 * its state persists across suspends in that actor's snapshot.
 */
import { createHash } from "node:crypto";

export const DEFAULT_ACTOR_DOMAIN = "actors.resources.substrate.ate.dev";

/** conv-<first 12 hex of sha256(sessionKey)>: deterministic, DNS-safe, stable. */
export function actorNameForConversation(canonicalSessionKey: string): string {
  const h = createHash("sha256").update(canonicalSessionKey).digest("hex").slice(0, 12);
  return `conv-${h}`;
}

/** atenet Host-routed URL for an actor. atenet routes by Host to actor port 80. */
export function actorUrlFor(name: string, atespace: string, domain: string = DEFAULT_ACTOR_DOMAIN): string {
  return `http://${name}.${atespace}.${domain}`;
}