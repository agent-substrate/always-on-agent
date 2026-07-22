# @openclaw/substrate — Agent Substrate plugin

Run OpenClaw split across an **always-on gateway** (presence: channels + routing)
and a **suspendable Substrate actor** (cognition: LLM, tools, memory). The actor
suspends to a gVisor snapshot when idle and auto-resumes on demand, so many
OpenClaw instances share a small pool of worker pods.

This is a **true drop-in plugin**: copy this folder into OpenClaw's `extensions/`
and enable it in config. It requires **zero edits to any existing OpenClaw file** —
config is declared here in `openclaw.plugin.json`, and startup wiring runs through
the plugin service + hook API.

## What it does

- **Gateway role** — registers a `substrate` ACP runtime backend that forwards
  each agent turn over HTTP to the actor's atenet URL, and wires the standard ACP
  reply-dispatch hook so any channel bound to backend `substrate` is delegated to
  the actor. Channels run completely unchanged.
- **Actor role** — watches agent activity through plugin hooks
  (`before_agent_run`, `agent_end`, `message_received`) and calls
  `ateapi.SuspendActor` once idle, freeing the worker pod. State survives in the
  actor's DurableDir.

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
      "actorUrl": "http://<actor>.<atespace>.actors.resources.substrate.ate.dev",
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

**Actor instance** (runs headless, no channels)
```json
{
  "plugins": { "entries": { "substrate": { "enabled": true,
    "config": {
      "role": "actor",
      "idleTimeoutSeconds": 120,
      "ateapiAddress": "api.ate-system.svc.cluster.local:443"
    }
  } } },
  "channels": {}
}
```

## Config (validated by `openclaw.plugin.json` → `configSchema`)

| Key | Role | Default | Meaning |
|-----|------|---------|---------|
| `role` | both | *(required)* | `gateway` or `actor` |
| `actorUrl` | gateway | — | atenet URL of the agent actor |
| `actorToken` | gateway | — | bearer token for the actor's HTTP API |
| `idleTimeoutSeconds` | actor | 120 | idle time before self-suspend |
| `ateapiAddress` | actor | `api.ate-system.svc.cluster.local:443` | Substrate control plane gRPC |

## Files

| File | Purpose |
|------|---------|
| `openclaw.plugin.json` | Manifest + config schema (replaces core config edits) |
| `index.ts` | Plugin entry: registers backend / idle monitor via the plugin API |
| `acp-runtime.ts` | HTTP `AcpRuntime`: turn → `/v1/chat/completions`, SSE → events |
| `idle-monitor.ts` | Actor idle detection + `SuspendActor` gRPC |
| `ateapi.proto` | Minimal proto for the SuspendActor RPC |

Depends only on `@grpc/grpc-js` + `@grpc/proto-loader` (declared here), and the
public `openclaw/plugin-sdk/*` API — nothing in OpenClaw's core is modified.
