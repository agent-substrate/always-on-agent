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
 * Shared mTLS gRPC client for the Substrate ateapi control plane
 * (`ateapi.Control`). Used by both the gateway-side provisioner (CreateActor)
 * and the actor-side idle monitor (SuspendActor).
 *
 * Current OSS ateapi runs auth-mode=mtls: when client cert/key are provided the
 * call presents a podcert client cert; otherwise it falls back to server-only
 * TLS (works only against a non-mTLS ateapi). Server identity is not verified
 * because atenet/ateapi certs are issued from an in-cluster trust bundle whose
 * SANs don't match the Service DNS name we dial.
 */
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const GRPC_ALREADY_EXISTS = 6;
export const GRPC_NOT_FOUND = 5;

export type AteApiClientConfig = {
  address: string;
  caFile?: string;
  certFile?: string;
  keyFile?: string;
};

export type ActorRef = { atespace: string; name: string };

type ControlClient = {
  CreateActor: (req: unknown, cb: (err: unknown, resp: unknown) => void) => void;
  GetActor: (req: unknown, cb: (err: unknown, resp: unknown) => void) => void;
  SuspendActor: (req: unknown, cb: (err: unknown, resp: unknown) => void) => void;
  DeleteActor: (req: unknown, cb: (err: unknown, resp: unknown) => void) => void;
};

export type AteApiClient = {
  createActor(ref: ActorRef, templateNamespace: string, templateName: string): Promise<void>;
  suspendActor(ref: ActorRef): Promise<void>;
  deleteActor(ref: ActorRef): Promise<void>;
};

/** Reads mTLS creds from the standard podcert env vars (shared by both roles). */
export function ateApiConfigFromEnv(address: string): AteApiClientConfig {
  return {
    address,
    caFile: process.env.ATE_API_CA_FILE || undefined, // e.g. /run/servicedns.podcert.ate.dev/ca.crt
    certFile: process.env.ATE_API_CLIENT_CERT || undefined, // e.g. .../credential-bundle.pem
    keyFile: process.env.ATE_API_CLIENT_KEY || undefined, // same bundle for cert+key with podcert
  };
}

export function createAteApiClient(cfg: AteApiClientConfig): AteApiClient {
  let clientP: Promise<ControlClient> | null = null;
  const client = () => (clientP ??= build(cfg));

  const unary = (
    pick: (c: ControlClient) => ControlClient[keyof ControlClient],
    req: unknown,
  ): Promise<unknown> =>
    client().then(
      (c) =>
        new Promise((res, rej) =>
          (pick(c) as ControlClient["CreateActor"]).call(c, req, (err: unknown, resp: unknown) =>
            err ? rej(err) : res(resp),
          ),
        ),
    );

  return {
    async createActor(ref, templateNamespace, templateName) {
      await unary((c) => c.CreateActor, {
        actor: {
          metadata: { atespace: ref.atespace, name: ref.name },
          actor_template_namespace: templateNamespace,
          actor_template_name: templateName,
        },
      });
    },
    async suspendActor(ref) {
      await unary((c) => c.SuspendActor, { actor: { atespace: ref.atespace, name: ref.name } });
    },
    async deleteActor(ref) {
      await unary((c) => c.DeleteActor, { actor: { atespace: ref.atespace, name: ref.name } });
    },
  };
}

async function build(cfg: AteApiClientConfig): Promise<ControlClient> {
  const grpc = await import("@grpc/grpc-js");
  const protoLoader = await import("@grpc/proto-loader");
  const protoPath = resolve(dirname(fileURLToPath(import.meta.url)), "ateapi.proto");
  const def = await protoLoader.load(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const pkg = grpc.loadPackageDefinition(def) as any;
  const Control = pkg.ateapi.Control; // matches server /ateapi.Control/<Method>
  return new Control(cfg.address, makeCreds(grpc, cfg)) as ControlClient;
}

function makeCreds(grpc: any, cfg: AteApiClientConfig) {
  if (cfg.certFile && cfg.keyFile) {
    const ca = cfg.caFile ? readFileSync(cfg.caFile) : null;
    return grpc.credentials.createSsl(ca, readFileSync(cfg.keyFile), readFileSync(cfg.certFile), {
      checkServerIdentity: () => undefined,
    });
  }
  return grpc.credentials.createSsl(null, null, null, { checkServerIdentity: () => undefined });
}