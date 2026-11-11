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
import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import os from "node:os";
import { Readable } from "node:stream";

export const DEFAULT_ACTOR_DOMAIN = "actors.resources.substrate.ate.dev";
// Substrate v1 serves actor ingress only over HTTPS, to clients presenting a
// pod identity certificate on the install's --ingress-allowed-spiffe-ids.
export const DEFAULT_ROUTER_URL = "https://atenet-router.ate-system.svc:443";

// Projected into the gateway pod (manifests/gateway.yaml, router-client volume):
// this pod's podidentity key and chain, and the CA that signs the router's
// serving certificate. The kubelet rotates the certificate in place.
const DEFAULT_CLIENT_BUNDLE = "/run/router-client/credential-bundle.pem";
const DEFAULT_TRUST_BUNDLE = "/run/router-client/router-trust-bundle.pem";

let tlsCache: { key: string; agent: https.Agent } | undefined;

/** An https.Agent with the current client certificate, rebuilt when the files change. */
function routerAgent(): https.Agent {
  const bundle = process.env.ROUTER_CLIENT_BUNDLE || DEFAULT_CLIENT_BUNDLE;
  const trust = process.env.ROUTER_TRUST_BUNDLE || DEFAULT_TRUST_BUNDLE;
  const key = `${fs.statSync(bundle).mtimeMs}:${fs.statSync(trust).mtimeMs}`;
  if (!tlsCache || tlsCache.key !== key) {
    // The bundle holds the private key and the chain in one PEM, which Node
    // accepts as both key and cert.
    const pem = fs.readFileSync(bundle);
    tlsCache?.agent.destroy();
    tlsCache = {
      key,
      agent: new https.Agent({ keepAlive: true, key: pem, cert: pem, ca: fs.readFileSync(trust) }),
    };
  }
  return tlsCache.agent;
}

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

/** This pod's IP: POD_IP from the downward API, else the first non-internal IPv4. */
function clientIp(): string | undefined {
  if (process.env.POD_IP) return process.env.POD_IP;
  for (const addrs of Object.values(os.networkInterfaces())) {
    for (const a of addrs ?? []) if (a.family === "IPv4" && !a.internal) return a.address;
  }
  return undefined;
}

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
          // v1 routes only on ate-target-actor. Host-based routing is the
          // release-0.1 path and only works over plain HTTP; over TLS the
          // URL's own host has to match the router's certificate.
          ...(url.protocol === "https:" ? {} : { Host: actorHostFor(ref.name, ref.atespace, ref.domain) }),
          "ate-target-actor": `${ref.atespace}/${ref.name}`,
          // v1's ingress adds X-Forwarded-Proto but no X-Forwarded-For, and
          // OpenClaw refuses a proxied request it cannot attribute to a client
          // ("proxy_attribution_required"). The gateway is that client, so say
          // so. Nothing else on the path sets or rewrites the header.
          ...(clientIp() ? { "X-Forwarded-For": clientIp() as string } : {}),
        },
        ...(url.protocol === "https:" ? { agent: routerAgent() } : {}),
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