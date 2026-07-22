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
 * Actor-side idle monitor. Tracks in-flight agent work via plugin hooks and,
 * once idle for `idleTimeoutSeconds`, calls ateapi.SuspendActor to free the
 * worker pod. No-op unless running inside a Substrate actor (detected via the
 * bind-mounted /run/ate/actor-id file).
 */
import { readFileSync } from "node:fs";
import { createAteApiClient, ateApiConfigFromEnv } from "./ateapi-client.js";

const ACTOR_ID_PATH = "/run/ate/actor-id"; // bind-mounted by ateom; contains the actor NAME
const DEFAULT_ATEAPI = "api.ate-system.svc.cluster.local:443";
const POLL_MS = 5_000;

// Current OSS Substrate addresses actors by (atespace, name); an actor name is
// only unique within its atespace. The ateom currently injects only the actor
// name (ACTOR_ID_PATH), so the atespace must be supplied to the actor out of band
// — via OPENCLAW_ACTOR_ATESPACE, or a future ateom-written /run/ate/actor-atespace.
const ACTOR_ATESPACE_PATH = "/run/ate/actor-atespace";

type Logger = { info: (m: string) => void; warn?: (m: string) => void; error?: (m: string) => void };

export function createIdleMonitor(opts: { idleTimeoutSeconds: number; ateapiAddress?: string }) {
  let inflight = 0;
  let lastActivity = Date.now();
  let timer: ReturnType<typeof setInterval> | null = null;
  let suspending = false;
  let log: Logger | null = null;

  const actorId = readActorId();
  const atespace = readAtespace();
  const idleMs = opts.idleTimeoutSeconds * 1_000;
  const ateapi = opts.ateapiAddress || DEFAULT_ATEAPI;

  const touch = () => {
    lastActivity = Date.now();
  };

  return {
    onRunStart() {
      inflight++;
      touch();
    },
    onRunEnd() {
      inflight = Math.max(0, inflight - 1);
      touch();
    },
    touch,
    start(logger: Logger) {
      log = logger;
      if (!actorId) {
        logger.info("substrate: not running on a Substrate actor (/run/ate/actor-id absent) — idle monitor disabled");
        return;
      }
      logger.info(`substrate: idle monitor active for actor "${actorId}" (timeout ${opts.idleTimeoutSeconds}s)`);
      timer = setInterval(() => {
        if (suspending || inflight > 0) {
          if (inflight > 0) touch();
          return;
        }
        if (Date.now() - lastActivity >= idleMs) {
          suspending = true;
          suspendSelf(atespace, actorId, ateapi, log)
            .catch((e) => {
              log?.error?.(`substrate: SuspendActor failed: ${e?.message ?? e}`);
              suspending = false;
            });
        }
      }, POLL_MS);
      timer.unref?.();
    },
    stop() {
      if (timer) clearInterval(timer);
      timer = null;
    },
  };
}

function readActorId(): string | null {
  try {
    return readFileSync(ACTOR_ID_PATH, "utf-8").trim() || null;
  } catch {
    return null;
  }
}

function readAtespace(): string | null {
  if (process.env.OPENCLAW_ACTOR_ATESPACE) return process.env.OPENCLAW_ACTOR_ATESPACE.trim();
  try {
    return readFileSync(ACTOR_ATESPACE_PATH, "utf-8").trim() || null;
  } catch {
    return null;
  }
}

async function suspendSelf(
  atespace: string | null,
  actorName: string,
  ateapiAddress: string,
  log: Logger | null,
): Promise<void> {
  log?.info(`substrate: idle — suspending actor "${atespace ?? "?"}/${actorName}"`);
  // Shared client dials /ateapi.Control/SuspendActor with podcert mTLS.
  // (Current OSS ObjectRef requires the atespace — a name is unique only within it.)
  const client = createAteApiClient(ateApiConfigFromEnv(ateapiAddress));
  await client.suspendActor({ atespace: atespace ?? "", name: actorName });
}