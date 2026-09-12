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
 * Agent Substrate plugin entry.
 *
 * Drop-in: place this folder under `extensions/` and enable it via
 * `plugins.entries.substrate.enabled = true` in openclaw.json. It requires
 * NO edits to any existing OpenClaw file: config is declared in
 * openclaw.plugin.json, and startup wiring runs through the plugin service +
 * hook API.
 *
 * Gateway role: registers a "substrate" ACP runtime backend that maps each
 * conversation to its own Substrate actor (create-if-absent from a golden
 * template, then forward turns over HTTP via atenet), wires the generic ACP
 * reply-dispatch hook so channels bound to backend "substrate" are delegated,
 * and suspends each actor once its conversation goes idle.
 *
 * There is deliberately no actor-side role. Substrate projects an actor's
 * identity into the sandbox but no client credential, and ateapi requires mTLS,
 * so an actor cannot call the control plane to suspend itself. See
 * docs/ARCHITECTURE.md.
 */
import {
  registerAcpRuntimeBackend,
  unregisterAcpRuntimeBackend,
  tryDispatchAcpReplyHook,
} from "openclaw/plugin-sdk/acp-runtime-backend";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk/core";
import { createSubstrateAcpRuntime } from "./acp-runtime.js";
import { createActorProvisioner } from "./actor-provisioner.js";
import { ateApiConfigFromEnv } from "./ateapi-client.js";
import { createKubectlAteClient } from "./kubectl-ate-client.js";
import { createIdleSuspender } from "./idle-suspender.js";

const BACKEND_ID = "substrate";
const DEFAULT_ATEAPI = "api.ate-system.svc.cluster.local:443";

type SubstrateConfig = {
  role?: "gateway";
  // Gateway role: per-conversation actor placement.
  atespace?: string;
  template?: string; // bare name of the golden ActorTemplate, resolved in atespace
  templateForAgent?: Record<string, string>;
  actorDomain?: string;
  actorToken?: string;
  // How the gateway provisions actors: "ateapi" (in-band gRPC + podcert mTLS)
  // or "kubectl-ate" (shells out to kubectl-ate; for pods without a podcert).
  provisioner?: "ateapi" | "kubectl-ate";
  kubectlAtePath?: string;
  // Idle window before the gateway suspends a conversation's actor.
  idleTimeoutSeconds?: number;
  // How often the idle sweep runs. The real delay before a suspend is the
  // timeout plus up to one poll interval, so a 5s poll quietly doubles a 5s
  // timeout. Worth turning down when the timeout is short.
  idlePollMs?: number;
  // ateapi control-plane address.
  ateapiAddress?: string;
};

const plugin = {
  id: "substrate",
  name: "Agent Substrate",
  description:
    "Split OpenClaw across an always-on gateway and a suspendable Substrate actor.",
  register(api: OpenClawPluginApi) {
    const cfg = (api.pluginConfig ?? {}) as SubstrateConfig;

    // --- Gateway role: one actor per conversation, created on demand ---
    if (cfg.role === "gateway" && cfg.atespace && cfg.template) {
      const ateapiAddress = cfg.ateapiAddress ?? DEFAULT_ATEAPI;
      const client =
        cfg.provisioner === "kubectl-ate"
          ? createKubectlAteClient({ binPath: cfg.kubectlAtePath, endpoint: ateapiAddress })
          : ateApiConfigFromEnv(ateapiAddress);
      const provisioner = createActorProvisioner({
        atespace: cfg.atespace,
        ateapi: client,
      });
      // The sandboxed actor has no ateapi credentials, so the gateway drives
      // idle-suspend from here using the same credentialed kubectl-ate client.
      const suspender = createIdleSuspender({
        atespace: cfg.atespace,
        client,
        idleTimeoutSeconds: cfg.idleTimeoutSeconds ?? 120,
        pollMs: cfg.idlePollMs,
      });
      const runtime = createSubstrateAcpRuntime({
        atespace: cfg.atespace,
        template: cfg.template,
        templateForAgent: cfg.templateForAgent,
        actorDomain: cfg.actorDomain,
        actorToken: cfg.actorToken,
        provisioner,
        onActivity: (name) => suspender.touch(name),
        onTurnStart: (name) => suspender.begin(name),
        onTurnEnd: (name) => suspender.end(name),
      });
      api.registerService({
        id: "substrate-gateway-backend",
        start(ctx) {
          registerAcpRuntimeBackend({ id: BACKEND_ID, runtime });
          suspender.start(ctx.logger);
          ctx.logger.info(
            `substrate: per-conversation ACP backend on atespace "${cfg.atespace}" (template ${cfg.template})`,
          );
        },
        stop() {
          unregisterAcpRuntimeBackend(BACKEND_ID);
          suspender.stop();
        },
      });
      // Route turns whose binding resolves to an ACP backend (e.g. "substrate").
      api.on("reply_dispatch", (event, ctx) => tryDispatchAcpReplyHook(event, ctx));
    }
  },
};

export default plugin;