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
import http from "node:http";
import https from "node:https";
import { Readable } from "node:stream";

export const DEFAULT_ACTOR_DOMAIN = "actors.resources.substrate.ate.dev";
export const DEFAULT_ROUTER_URL = "http://atenet-router.ate-system.svc.cluster.local:80";

/** The atenet router, overridable with ROUTER_URL. */
export function routerUrl(): string {
  return process.env.ROUTER_URL || DEFAULT_ROUTER_URL;
}

/** conv-<first 12 hex of sha256(sessionKey)>: deterministic, DNS-safe, stable. */
export function actorNameForConversation(canonicalSessionKey: string): string {
  const h = createHash("sha256").update(canonicalSessionKey).digest("hex").slice(0, 12);
  return `conv-${h}`;
}

/** The hostname release-0.1 atenet routes an actor by. */
export function actorHostFor(name: string, atespace: string, domain: string = DEFAULT_ACTOR_DOMAIN): string {
  return `${name}.${atespace}.${domain}`;
}

export type ActorRef = { name: string; atespace: string; domain?: string };

/**
 * Sends a request to an actor through the atenet router, addressed both ways
 * atenet has understood. release-0.1 routes by Host
 * (`<actor>.<atespace>.<domain>`, which its CoreDNS stub resolves to the router)
 * and ignores the header. main dropped that DNS and routes only by
 * `ate-target-actor: <atespace>/<actor>`, ignoring Host. Sending both to the
 * router service works on either.
 *
 * node:http rather than fetch, because fetch silently replaces a caller's Host
 * with the URL's, and release-0.1 would then route nothing. Connection failures
 * reject with a TypeError, as fetch's do, so callers keep treating them as
 * retryable.
 */
export function fetchActor(
  ref: ActorRef,
  path: string,
  init: { method?: string; headers?: Record<string, string>; body?: string; signal?: AbortSignal } = {},
): Promise<Response> {
  const url = new URL(path, routerUrl());
  const client = url.protocol === "https:" ? https : http;
  return new Promise((resolve, reject) => {
    const req = client.request(
      url,
      {
        method: init.method ?? "GET",
        headers: {
          ...init.headers,
          Host: actorHostFor(ref.name, ref.atespace, ref.domain),
          "ate-target-actor": `${ref.atespace}/${ref.name}`,
        },
        signal: init.signal,
      },
      (res) => {
        const headers = new Headers();
        for (const [k, v] of Object.entries(res.headers)) {
          if (v !== undefined) headers.set(k, Array.isArray(v) ? v.join(", ") : v);
        }
        const status = res.statusCode ?? 502;
        const nullBody = [101, 204, 205, 304].includes(status);
        if (nullBody) res.resume();
        resolve(
          new Response(nullBody ? null : (Readable.toWeb(res) as ReadableStream<Uint8Array>), {
            status,
            statusText: res.statusMessage,
            headers,
          }),
        );
      },
    );
    req.on("error", (err) => {
      if (init.signal?.aborted) reject(err);
      else reject(new TypeError(`actor request failed: ${err.message}`, { cause: err }));
    });
    req.end(init.body);
  });
}