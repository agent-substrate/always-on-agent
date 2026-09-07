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
 * Idempotent actor provisioner (gateway side).
 *
 * ensure(name, template) creates the actor from its golden ActorTemplate if it
 * does not already exist, then caches success so subsequent turns are free.
 * Concurrent first-messages for the same new conversation are collapsed with a
 * singleflight map, and an AlreadyExists error (another turn/replica won the
 * race) is treated as success — so CreateActor is effectively idempotent.
 */
import {
  AteApiClient,
  AteApiClientConfig,
  createAteApiClient,
  GRPC_ALREADY_EXISTS,
} from "./ateapi-client.js";

type Logger = { info: (m: string) => void; warn?: (m: string) => void; error?: (m: string) => void };

export type Provisioner = {
  /** Ensure actor <atespace>/<name> exists, created from "<ns>/<template>". */
  ensure(name: string, template: string): Promise<void>;
};

export function createActorProvisioner(cfg: {
  atespace: string;
  ateapi: AteApiClientConfig | AteApiClient;
  logger?: Logger;
}): Provisioner {
  const client: AteApiClient =
    "createActor" in cfg.ateapi ? cfg.ateapi : createAteApiClient(cfg.ateapi);
  const ready = new Set<string>(); // actors known to exist
  const inflight = new Map<string, Promise<void>>(); // singleflight per name

  return {
    async ensure(name: string, template: string): Promise<void> {
      if (ready.has(name)) return;
      let p = inflight.get(name);
      if (!p) {
        p = (async () => {
          const [ns, tn] = splitTemplate(template);
          try {
            await client.createActor({ atespace: cfg.atespace, name }, ns, tn);
            cfg.logger?.info(`substrate: created actor ${cfg.atespace}/${name} from ${template}`);
          } catch (err) {
            if ((err as { code?: number })?.code !== GRPC_ALREADY_EXISTS) throw err;
          }
          ready.add(name);
        })().finally(() => inflight.delete(name));
        inflight.set(name, p);
      }
      await p;
    },
  };
}

// ActorTemplate is no longer a Kubernetes CRD, so a template has no namespace to
// qualify it with: it is resolved by bare name inside the actor's own atespace.
// `<namespace>/<name>` is still accepted for configs written against the old
// shape, and the namespace is discarded.
function splitTemplate(template: string): [namespace: string, name: string] {
  const i = template.indexOf("/");
  if (i < 0) return ["", template];
  if (i === 0 || i === template.length - 1) {
    throw new Error(`substrate: template must be "<name>", got "${template}"`);
  }
  return [template.slice(0, i), template.slice(i + 1)];
}