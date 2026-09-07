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
 * kubectl-ate–backed implementation of the AteApiClient interface.
 *
 * Used when the gateway cannot present a podcert mTLS client cert to ateapi
 * (a plain Deployment on a pre-1.36 cluster has no PodCertificate). kubectl-ate
 * port-forwards to ateapi and handles auth-mode detection itself — the same
 * mechanism the dashboard already uses successfully in this cluster. Requires
 * the kubectl-ate binary in the pod + RBAC for pods/portforward.
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { ActorRef, AteApiClient } from "./ateapi-client.js";

const pexec = promisify(execFile);

export function createKubectlAteClient(cfg: { binPath?: string; endpoint?: string } = {}): AteApiClient {
  const bin = cfg.binPath || process.env.KUBECTL_ATE || "kubectl-ate";
  // Direct gRPC target for ateapi. Without it kubectl-ate tries to auto-detect by
  // reading the ate-api-server Deployment in the pod's namespace (wrong namespace
  // for a gateway outside ate-system), so we always pass --endpoint.
  const endpoint = cfg.endpoint || process.env.ATE_ENDPOINT || "";
  const endpointArgs = endpoint ? ["--endpoint", endpoint] : [];

  const run = async (args: string[]): Promise<void> => {
    await pexec(bin, [...args, ...endpointArgs], { timeout: 60_000 });
  };

  return {
    async createActor(ref: ActorRef, _templateNamespace: string, templateName: string) {
      try {
        await run([
          "create", "actor", ref.name,
          "-a", ref.atespace,
          // `--template <ns>/<name>` is gone. ActorTemplate stopped being a
          // Kubernetes CRD, so there is no namespace to qualify it with:
          // --template-ref resolves the name inside the actor's own atespace.
          "--template-ref", templateName,
        ]);
      } catch (err) {
        const msg = String((err as { stderr?: string })?.stderr ?? (err as Error)?.message ?? err);
        if (/already exists/i.test(msg)) return; // idempotent
        throw err;
      }
    },
    async suspendActor(ref: ActorRef) {
      await run(["suspend", "actor", ref.name, "-a", ref.atespace]);
    },
    async deleteActor(ref: ActorRef) {
      await run(["delete", "actor", ref.name, "-a", ref.atespace]);
    },
  };
}