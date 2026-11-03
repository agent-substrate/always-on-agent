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
 * Gateway-side idle-suspend.
 *
 * A Substrate actor runs inside a gVisor sandbox with no ateapi credentials
 * (only /run/ate/actor-id is bind-mounted), so it cannot suspend itself. The
 * gateway, which already holds the credentialed kubectl-ate path used to create
 * actors, instead tracks per-actor activity and calls SuspendActor once a
 * conversation's actor has been idle past the timeout, freeing its worker.
 * Resume is automatic: the next turn hits atenet, which restores the actor
 * from its checkpoint.
 */
import type { AteApiClient } from "./ateapi-client.js";

type Logger = { info: (m: string) => void; warn?: (m: string) => void; error?: (m: string) => void };

export type IdleSuspender = {
  /** Record activity for an actor (called on ensureSession). */
  touch(actorName: string): void;
  /** Mark a turn in-flight; the actor is never suspended while inflight > 0. */
  begin(actorName: string): void;
  /** Mark a turn finished; resets the idle clock. */
  end(actorName: string): void;
  start(logger: Logger): void;
  stop(): void;
};

export function createIdleSuspender(opts: {
  atespace: string;
  client: AteApiClient;
  idleTimeoutSeconds: number;
  pollMs?: number;
}): IdleSuspender {
  const idleMs = opts.idleTimeoutSeconds * 1_000;
  const pollMs = opts.pollMs ?? 5_000;
  const lastActivity = new Map<string, number>();
  const inflight = new Map<string, number>();
  const suspending = new Set<string>();
  let timer: ReturnType<typeof setInterval> | null = null;
  let log: Logger | null = null;

  return {
    touch(actorName: string) {
      lastActivity.set(actorName, Date.now());
    },
    begin(actorName: string) {
      inflight.set(actorName, (inflight.get(actorName) ?? 0) + 1);
      lastActivity.set(actorName, Date.now());
    },
    end(actorName: string) {
      inflight.set(actorName, Math.max(0, (inflight.get(actorName) ?? 0) - 1));
      lastActivity.set(actorName, Date.now());
    },
    start(logger: Logger) {
      log = logger;
      logger.info(
        `substrate: gateway idle-suspend active (timeout ${opts.idleTimeoutSeconds}s, atespace ${opts.atespace})`,
      );
      timer = setInterval(() => {
        const now = Date.now();
        for (const [name, ts] of lastActivity) {
          if (suspending.has(name)) continue;
          // Never suspend while a turn is being processed.
          if ((inflight.get(name) ?? 0) > 0) continue;
          if (now - ts < idleMs) continue;
          suspending.add(name);
          log?.info(
            `substrate: actor ${opts.atespace}/${name} idle ${opts.idleTimeoutSeconds}s, suspending`,
          );
          opts.client
            .suspendActor({ atespace: opts.atespace, name })
            .then(() => {
              lastActivity.delete(name);
              log?.info(`substrate: suspended ${opts.atespace}/${name}`);
            })
            .catch((e) => {
              // Keep tracking and retry next tick; refresh ts to avoid a hot loop.
              lastActivity.set(name, Date.now());
              log?.warn?.(`substrate: suspend ${name} failed: ${(e as Error)?.message ?? e}`);
            })
            .finally(() => suspending.delete(name));
        }
      }, pollMs);
      timer.unref?.();
    },
    stop() {
      if (timer) clearInterval(timer);
      timer = null;
    },
  };
}