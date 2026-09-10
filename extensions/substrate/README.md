# @openclaw/substrate: Agent Substrate plugin

Run OpenClaw split across an **always-on gateway** (presence: channels + routing)
and a **suspendable Substrate actor** (cognition: LLM, tools, memory). The gateway
suspends the actor to a gVisor snapshot once the conversation goes idle, and the
next message auto-resumes it, so many OpenClaw instances share a small pool of
worker pods.

This is a **true drop-in plugin**: copy this folder into OpenClaw's `extensions/`
and enable it in config. It requires **zero edits to any existing OpenClaw file**:
config is declared here in `openclaw.plugin.json`, and startup wiring runs through
the plugin service + hook API.

## What it does

The plugin runs on the gateway only. It registers a `substrate` ACP runtime
backend that forwards each agent turn over HTTP to the actor's atenet URL, wires
the standard ACP reply-dispatch hook so any channel bound to backend `substrate`
is delegated to the actor, creates the per-conversation actor on first use, and
calls `ateapi.SuspendActor` once the conversation is idle with no turn in flight.
Channels run completely unchanged.

Suspend is driven from the gateway rather than by the actor itself because a
sandboxed actor cannot call the control plane. Substrate projects an actor's
identity into the sandbox (`actorMetadata`, `trustBundle`) but no client
credential, and ateapi requires mTLS. See `docs/ARCHITECTURE.md`.

## Install

1. Copy this folder to `extensions/substrate/` in your OpenClaw checkout (or bundle
   it into your image with `--build-arg OPENCLAW_EXTENSIONS=substrate`).
2. Enable and configure it in `openclaw.json` (no code changes):

**Gateway instance**
```json
{
  "plugins": { "entries": { "substrate": { "enabled": true,
    "config": {
      "role": "gateway",
      "atespace": "openclaw",
      "template": "oc-agent",
      "actorDomain": "actors.resources.substrate.ate.dev",
      "actorToken": "${OPENCLAW_ACTOR_TOKEN}"
    }
  } } },
  "acp": { "enabled": true, "backend": "substrate" },
  "bindings": [
    { "type": "acp", "agentId": "default",
      "match": { "channel": "whatsapp", "accountId": "*", "peer": { "kind": "direct", "id": "*" } },
      "acp": { "backend": "substrate" } }
  ],
  "channels": { "whatsapp": {} }
}
```

**Actor instance**: nothing to configure. The actor is a plain, unmodified OpenClaw
serving `/v1/chat/completions`; it does not load this plugin.

## Config (validated by `openclaw.plugin.json` → `configSchema`)

| Key | Default | Meaning |
|-----|---------|---------|
| `role` | *(required)* | `gateway`, the only value |
| `atespace` | *(none)* | atespace the per-conversation actors are created in |
| `template` | *(none)* | golden ActorTemplate new actors derive from, by bare name |
| `templateForAgent` | *(none)* | optional per-persona template override, keyed by agentId |
| `actorDomain` | `actors.resources.substrate.ate.dev` | DNS domain actors are addressed under |
| `actorToken` | *(none)* | bearer token for the actor's HTTP API |
| `provisioner` | `ateapi` | `ateapi` (in-band gRPC, needs a podcert) or `kubectl-ate` (shell out) |
| `kubectlAtePath` | `kubectl-ate` | path to the binary when `provisioner: "kubectl-ate"` |
| `idleTimeoutSeconds` | 120 | idle time before the gateway suspends the actor |
| `ateapiAddress` | `api.ate-system.svc.cluster.local:443` | Substrate control plane gRPC |

## Files

| File | Purpose |
|------|---------|
| `openclaw.plugin.json` | Manifest + config schema (replaces core config edits) |
| `index.ts` | Plugin entry: registers the backend and idle suspender via the plugin API |
| `acp-runtime.ts` | HTTP `AcpRuntime`: turn → `/v1/chat/completions`, SSE → events |
| `actor-router.ts` | Conversation → actor name/URL, hashed so no phone number reaches DNS |
| `actor-provisioner.ts` | Create-if-absent of a conversation's actor from the golden template |
| `idle-suspender.ts` | Gateway-side idle tracking + `SuspendActor` |
| `ateapi-client.ts` | gRPC client for ateapi (podcert mTLS) |
| `kubectl-ate-client.ts` | Same surface, shelling out to `kubectl-ate` |
| `ateapi.proto` | Minimal proto for the actor RPCs |

Depends only on `@grpc/grpc-js` + `@grpc/proto-loader` (declared here) and the
public `openclaw/plugin-sdk/*` API. Nothing in OpenClaw's core is modified.
