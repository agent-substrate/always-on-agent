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
 * port-forwards to ateapi and handles auth-mode detection itself, the same
 * mechanism the dashboard already uses successfully in this cluster. Requires
 * the kubectl-ate binary in the pod + RBAC for pods/portforward.
 */
import { execFile } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import type { ActorRef, AteApiClient } from "./ateapi-client.js";

const pexec = promisify(execFile);

const DEFAULT_EGRESS_POLICY = `rules:
- http: {hostnames: ["*"], ports: {all: {}}}
- https: {hostnames: ["*"]}
`;

let defaultPolicyPath: string | undefined;

/** EGRESS_POLICY_FILE, or the allow-all default written once to a temp file. */
function egressPolicyFile(): string {
  if (process.env.EGRESS_POLICY_FILE) return process.env.EGRESS_POLICY_FILE;
  if (!defaultPolicyPath) {
    defaultPolicyPath = path.join(os.tmpdir(), "openclaw-egress-policy.yaml");
    fs.writeFileSync(defaultPolicyPath, DEFAULT_EGRESS_POLICY);
  }
  return defaultPolicyPath;
}

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
      const errText = (err: unknown) =>
        String((err as { stderr?: string })?.stderr ?? (err as Error)?.message ?? err);
      // v1 spells the flag --template; release-0.1 spelled it --template-ref.
      // Both resolve a bare name inside the actor's atespace.
      try {
        await run(["create", "actor", ref.name, "-a", ref.atespace, "--template", templateName]);
      } catch (err) {
        const msg = errText(err);
        if (/unknown flag: --template/i.test(msg)) {
          try {
            await run(["create", "actor", ref.name, "-a", ref.atespace, "--template-ref", templateName]);
          } catch (err2) {
            if (!/already exists/i.test(errText(err2))) throw err2;
          }
        } else if (!/already exists/i.test(msg)) {
          throw err;
        }
      }
      // Substrate v1 denies an actor's outbound traffic until it has an egress
      // policy, and OpenClaw needs its model API, channels and tools. The
      // default allows any host over HTTP and HTTPS, which matches release-0.1;
      // point EGRESS_POLICY_FILE at a tighter manifest, or set EGRESS_POLICY=off
      // to manage policies yourself. Created before the first turn, because a
      // missing policy is cached as deny for a few seconds.
      if ((process.env.EGRESS_POLICY || "on").toLowerCase() === "off") return;
      try {
        await run(["create", "egress-policy", ref.name, "-a", ref.atespace, "-f", egressPolicyFile()]);
      } catch (err) {
        const msg = errText(err);
        if (/already exists/i.test(msg)) return;
        if (/unknown command/i.test(msg)) return; // release-0.1 has no egress policies
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