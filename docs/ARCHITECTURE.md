# OpenClaw on Agent Substrate: Architecture Design

## Core Requirement

Run many OpenClaw personal-AI-assistant instances on GKE at minimal compute cost. OpenClaw agents are idle more than 95% of the time (waiting for user messages), yet channels such as WhatsApp require persistent connections that cannot be suspended.

**Hard constraint:** Do not modify any community-contributed code (channel plugins, agents, skills). These are maintained by the OpenClaw community; forking and maintaining a Substrate-specific variant of every channel is not sustainable.

## Core Approach: Split the Instance in Two

OpenClaw is split into two roles that run from the **same codebase**, selected by a single config field (`substrate.role`):

- **Gateway (always-on, ~128–256 MB):** holds the channel connections and routes messages. It is cheap because it spends its life waiting on I/O.
- **Agent Actor (Substrate-managed, suspendable):** runs the expensive agentic loop: LLM calls, tool use, memory, skills. It is suspended to a gVisor snapshot when idle (driven by the gateway) and auto-resumes on demand.

The gateway delegates agent work to the actor over HTTP using OpenClaw's **existing ACP (Agent Client Protocol) runtime-backend interface**. We added exactly one new backend, `"substrate"`, so no channel or agent code changes.

## Multi-Tenancy: One Gateway, Many Conversations

The gateway is **not** per-user. A **single always-on gateway process serves every user and every conversation**. It holds all the channel connections (one or many WhatsApp accounts, plus any other channels) and is the only always-on component in the system.

Each conversation is keyed by `(accountId, peer)`, which the gateway hashes into a stable session key and maps to **its own dedicated actor** (`conv-<sha256[:12]>`):

```
many users / conversations ──► one shared, always-on gateway ──► one suspendable actor each
```

- **Isolation:** every conversation gets its own actor with its own DurableDir (SQLite, memory, SOUL), so state never crosses conversations.
- **Independent lifecycle:** each actor resumes and suspends on its own idle clock. A busy conversation never keeps another's actor warm, and an idle one costs nothing.
- **Cost amortization:** the always-on footprint is *one* thin gateway shared across all N users, not N gateways. Per-user always-on cost trends toward zero as tenants are added, which is what lets the split match, and beat, the monolithic approach even in its best case.

## Why Not the Obvious Alternative

The alternative is to make the whole instance suspendable and teach every channel to be suspend-aware. It is tempting because it suspends the channels too, not just the agent. But whether that actually saves compute depends on the channel type, and for the most popular channels it does not. It also requires per-channel code changes (reconnect logic, webhook-URL rewriting) that are unsustainable given channels are community-contributed and constantly changing.

**Our approach keeps channels completely untouched** (only the agent suspends) and works uniformly for every channel type. The result: N logical instances share M ≪ N worker pods, with zero channel code changes.

A full, honest comparison (including the one case where the alternative genuinely wins) is in the *Considerations and Tradeoffs* section below, with a diagram for each approach.

## Architecture Diagram

![OpenClaw on Agent Substrate: full architecture](architecture-diagram.png)

Boxes are color-coded by ownership: **OpenClaw** (upstream), **Substrate**
(upstream), and **new, built by us**.

## Component Ownership: OpenClaw vs Substrate vs New

Everything in the system falls into three buckets. The integration adds a small amount of new code and leaves both OpenClaw and Substrate otherwise untouched. The only Substrate source changes are a few upstreamable fixes, listed separately.

**OpenClaw (upstream, unchanged)**

| Component | Role |
|---|---|
| Gateway process (`gateway` command) | Hosts channels + routing; the *same* binary also runs the actor |
| Channel plugins (WhatsApp/Baileys, Telegram, Slack, …) | Hold the channel connections; deliver messages |
| ACP (Agent Client Protocol) + runtime-backend interface + `reply_dispatch` hook | The extension point we plug into, no edits |
| Agent runtime / agentic loop (LLM, tools, memory, skills) | Runs inside the actor |
| `/v1/chat/completions` endpoint | How the gateway drives the actor's loop |
| Plugin SDK / hook API, cron, Control UI | Used as-is |

**Substrate (upstream)**

| Component | Role |
|---|---|
| `ateapi` (+ valkey state) | Control-plane API: Create / Resume / Suspend / Delete actor |
| `atecontroller` | Reconciles ActorTemplate/WorkerPool; captures the golden snapshot |
| `atenet` (Envoy + ext_proc) | Routes to actors; resume-on-demand on the first request |
| `atelet` (per-node DaemonSet) | Manages worker pods; snapshot ↔ GCS |
| `ateom-gvisor` (worker PID1) | Runs `runsc` create / start / checkpoint / restore |
| CRDs: WorkerPool, ActorTemplate, SandboxConfig (`ate.dev`) | Declarative actor / worker / sandbox config |
| `kubectl-ate` CLI, gVisor / `runsc` | Tooling + sandbox runtime |

**New, built by us**

| Component | What it is |
|---|---|
| `extensions/substrate/` OpenClaw plugin (~800 LOC TS) | The whole gateway-side integration (file breakdown below) |
| Actor image | OpenClaw base + `/v1/chat/completions` enabled + fixed agent identity (SOUL/IDENTITY) |
| Gateway image | OpenClaw slim + compiled plugin + `kubectl-ate` |
| K8s manifests | WorkerPool, ActorTemplate, gateway Deployment, ingress |
| Demo | deploy script, live dashboard, cron config |
| Substrate core fixes (4 changes, ~166-line patch) | Upstreamable; see `substrate-patches/`. Not part of the plugin |

## Message Flow: Suspend / Resume Cycle

1. User sends a WhatsApp message.
2. The gateway's WhatsApp plugin receives it over its always-on persistent connection.
3. The ACP bindings router sends the turn to `SubstrateAcpRuntime`, which issues an HTTP POST to the actor's atenet URL.
4. atenet's ext_proc inspects the Host header; if the actor is SUSPENDED it calls `ateapi.ResumeActor()` (a gVisor restore from GCS in under a second) then forwards the request.
5. The actor's `/v1/chat/completions` endpoint runs the full agentic loop (Gemini), streaming the reply back as Server-Sent Events.
6. The gateway relays the reply to the WhatsApp plugin, which sends it to the user.
7. After the idle window with no in-flight turn for that actor, the **gateway** calls `ateapi.SuspendActor()` (via the same control-plane path it uses to create/resume); the worker pod is freed. Suspend is gateway-driven because a sandboxed actor holds no control-plane credentials: Substrate projects its identity into the sandbox but no podcert, cert or JWT, so it has nothing to authenticate with. The gateway already has both the credentials and the activity signal (it dispatches every turn and sees every reply).
8. The next message repeats from step 3. Conversation state is preserved because SQLite and memory live in the actor's DurableDir.

## What We Built

All new code lives in the gateway-side plugin `extensions/substrate/`; the actor is stock OpenClaw.

| File (`extensions/substrate/`) | Purpose |
|---|---|
| `index.ts` | Plugin entry (gateway role): registers the ACP backend + the idle-suspend service |
| `acp-runtime.ts` | ACP `"substrate"` backend: per-conversation actor URL, POST `/v1/chat/completions`, parse SSE → AcpRuntimeEvent |
| `actor-provisioner.ts` | Create-if-absent actor from the golden template (idempotent + singleflight) |
| `actor-router.ts` | Deterministic `conv-<hash>` placement from the session key (the multi-tenancy map) |
| `idle-suspender.ts` | **Gateway-driven** idle-suspend: per-actor, turn-aware activity tracking → `SuspendActor` when idle |
| `ateapi-client.ts` / `kubectl-ate-client.ts` | Control-plane clients (in-band mTLS gRPC, or `kubectl-ate` shell-out on clusters without pod certs) |
| `ateapi.proto` | Minimal proto: Create / Get / Suspend / Delete actor |

Zero changes to channel plugins, the agent loop, skills, or the memory system. ~800 LOC of plugin TypeScript (~2,540 total including the actor/gateway images, manifests, and demo). The only Substrate *source* changes are the 4 upstreamable fixes (see the ownership table above + `substrate-patches/`).

### Packaged as a drop-in plugin (zero core edits)

The `~30 LOC` of config-type / Zod / `server.impl.ts` plumbing above is only
needed for a *direct* build integration. We also package the whole integration as
a first-class OpenClaw **plugin** in `extensions/substrate/`, which removes those
edits entirely. It is a true drop-in with **no changes to any existing OpenClaw
file**:

| Core edit (direct integration) | How the plugin removes it |
|---|---|
| `src/config/types.openclaw.ts` (`substrate` type) | Declared in `openclaw.plugin.json` → `configSchema`; config lives under `plugins.entries.substrate.config` |
| `src/config/zod-schema.ts` (validation) | Same, the manifest schema validates it |
| `src/gateway/server.impl.ts` (startup wiring) | Plugin `register(api)` + `api.registerService({start,stop})` with `activation.onStartup` |
| `package.json` (gRPC deps) | The plugin's own `package.json` |

The plugin uses only the public plugin surface, modeled on the bundled `acpx`
extension. It runs entirely in the **gateway role** (`substrate.role: "gateway"`);
the actor stays a plain, unmodified OpenClaw serving `/v1/chat/completions`:
- `registerAcpRuntimeBackend({ id: "substrate", runtime })` +
  `api.on("reply_dispatch", tryDispatchAcpReplyHook)` to route turns to the actor.
- Per-conversation placement: create-if-absent (`CreateActor`) and, after the idle
  window with no in-flight turn, `SuspendActor`. Both are control-plane calls made
  from the gateway, which holds the credentials.

**Why idle-suspend lives in the gateway and not in the actor.** A sandboxed actor
cannot call the control plane on current Substrate, so it cannot suspend itself.
Substrate can project identity *facts* into the sandbox (`SystemInfoDataSource`
offers `actorMetadata` and `trustBundle`, which is how we get `/run/ate/actor-id`,
`/run/ate/atespace`, `/run/ate/actor-uid` and `/run/ate/trust-bundle.pem`), but it
projects no client credential: no podcert, no JWT, no key material. The trust
bundle lets the actor *verify* ateapi; nothing lets it *authenticate to* ateapi,
and ateapi requires mTLS. So the plugin has no actor-side role at all, and the
actor image ships stock OpenClaw with the plugin deliberately left out. Until
Substrate can issue a sandboxed workload a credential scoped to acting on
itself, gateway-driven suspend is the only design that works. That credential is
the one thing we would ask for upstream to make the actor self-sufficient.

Install = compile `extensions/substrate/` to JS (`dist/`), declare
`openclaw.extensions` in its `package.json`, and register it with
`openclaw plugins install`; then set `plugins.entries.substrate` in
`openclaw.json`. See `extensions/substrate/README.md`.

## Considerations and Tradeoffs: Why Split, Not Monolithic Suspend

A natural objection to the split design is: *"if we instead made every channel suspend-aware and suspended the whole instance, wouldn't we save more, since the channels suspend too, not just the agent?"* The answer depends entirely on how each channel receives messages, so it is worth working through carefully.

### First, a clarification

In the split design, **the agent loop is not always-on. It is precisely the part that suspends.** The always-on half is the *gateway*, which contains only the channel connections and message routing; it does not run the agent loop, LLM calls, tools, or memory. So both designs suspend the expensive agent loop. The real question is only: **what must stay running to notice that a message arrived?**

### The deciding factor: how a channel delivers messages

Messaging platforms fall into two families:

**Persistent outbound connection.** The client opens and holds a long-lived socket to the platform; messages arrive only while that socket is live. If nothing is running to hold the socket, there is no signal that a message arrived, so nothing can wake a suspended instance.

**Webhook-delivered.** The platform holds the connection on its side and delivers each message as an inbound HTTP POST to a URL you register. Here, an inbound request can itself trigger resume-on-demand, so the instance can be fully suspended between messages.

### Channel classification (OpenClaw's built-in channels)

| Persistent outbound connection | Webhook-delivered (inbound HTTP POST) | Dual-mode (configurable) |
|---|---|---|
| WhatsApp (Baileys WebSocket) | Microsoft Teams (Bot Framework) | Telegram (long-poll **or** setWebhook) |
| Discord (Gateway WebSocket) | Google Chat | Slack (Socket Mode **or** Events API/HTTP) |
| Slack (Socket Mode, default) | LINE | Mattermost (WebSocket **or** slash/webhook) |
| Signal | Feishu / Lark | |
| iMessage (local daemon) | Zalo | |
| Matrix (/sync long-poll) | WeChat | |
| IRC (persistent TCP) | SMS (Twilio) | |
| Nostr (relay WebSockets) | Synology Chat | |
| Twitch | Nextcloud Talk (bot API) | |
| QQ (qqbot) | | |
| Tlon (Urbit) | | |

**WhatsApp, Discord, Slack on its default, Signal, iMessage and Matrix** are all persistent-connection, and they are the most popular assistant channels.

### How each approach behaves

**Approach A: split (current).**

![Approach A, split: always-on gateway + suspendable agent actor](approach-split.png)

Channels of *both* families live in the always-on gateway; the agent actor suspends. This is uniform (the gateway holds persistent sockets and also receives inbound webhooks) and requires zero channel code changes. Always-on cost is just the lightweight gateway (and it can be made multi-tenant, amortizing per-user cost toward zero). Each wake reloads only the agent, not the channel stack.

**Approach B: monolithic + suspend-aware channels (rejected).**

![Approach B, monolithic: whole instance suspends, channels must be suspend-aware](approach-monolithic.png)

The whole instance is one suspendable actor, and the outcome splits by channel type:

- *Webhook channels:* the platform holds the connection and POSTs on each message; that POST can drive resume-on-demand, so the instance can suspend fully and reach **true zero idle cost**, a genuine edge over the split. The catch is that every wake cold-loads the *entire* stack (channels + agent), which is heavier than waking just the agent.
- *Persistent-connection channels:* a suspended instance has a dead socket, so nothing ever learns a message arrived and nothing triggers resume. This forces one of three fallbacks, none better than the split: (1) keep the whole instance up, which saves nothing; (2) add an always-on bridge to hold the sockets, which simply re-creates the gateway and then cold-wakes the full stack per message; or (3) poll on a CronJob, which wakes the full stack every cycle and adds latency. On top of that, it requires per-channel code changes (reconnect handling, webhook-URL rewriting), which is the sustainability problem we set out to avoid.

### Verdict

- For a **webhook-only** deployment with long idle periods, the monolithic approach can genuinely reach zero idle cost and beat the split, an honest edge worth acknowledging.
- For the **realistic multi-channel mix**, dominated by persistent-connection channels, the monolith's always-on cost can only be *relocated* (into a bridge), not eliminated, and it pays a heavier cold-wake on every message.
- The split holds only the cheap channel layer always-on, wakes just the agent, works uniformly across all channel types, and needs zero channel changes. A **multi-tenant gateway** further shrinks the split's per-user always-on cost, closing even the webhook-only edge case.

That is why we chose the split.

## Configuration

**Gateway** (`substrate.role: "gateway"`): sets ACP bindings to route channels to the `substrate` backend, and owns the actor lifecycle: `atespace` + `template` (which golden to create actors from), `idleTimeoutSeconds` (when to suspend), and a control-plane credential path (`ateapiAddress` for in-band mTLS, or `provisioner: "kubectl-ate"` on clusters without pod certificates).

**Actor**: plain, unmodified OpenClaw with no channels and no substrate config; it just serves `/v1/chat/completions`. It needs no control-plane credentials because the gateway drives create/resume/suspend on its behalf.

## Deployment (GKE)

Targets **current OSS Substrate** (`agent-substrate/substrate`, CRD group `ate.dev`).

- Control plane: `ateapi`, `atecontroller`, `atenet`, `atelet` (DaemonSet), `ateom-gvisor`
  deployed via `hack/install-ate.sh --deploy-ate-system` (ko-built), with the
  control-plane changes listed in the top-level README.
- Custom actor image (OpenClaw + `extensions/substrate`), `@sha256`-pinned in the
  ActorTemplate; gateway Deployment behind a LoadBalancer for the channels.
- `WorkerPool` (`ate.dev/v1alpha1`, gVisor `SandboxClass`) + `ActorTemplate`
  (golden snapshot to GCS). runsc comes from the cluster gVisor `SandboxConfig`.
- Actors use the **atespace** model: `kubectl ate create atespace <a>` then
  `kubectl ate create actor <n> -a <a> --template-ref openclaw-agent`, and are
  addressed at `<actor>.<atespace>.actors.resources.substrate.ate.dev` (atenet routes
  restore-on-demand to worker port 80).
- Real-time dashboard: lists live actors (via `kubectl-ate` per atespace), the
  golden-snapshot status, the worker-pod map (which actor is restored where), and
  live gateway/WhatsApp status.

**gVisor runsc caveat (important):** public gvisor.dev runsc releases crash a heavy
multi-process Node.js actor (OpenClaw): the sentry dies ~30–60s after boot, so the
golden captures a dead agent. The GKE-Sandbox runsc build keeps it alive and
checkpoints it cleanly; pin it in the gVisor `SandboxConfig`. This is an upstream
gVisor gap, independent of Substrate.

### Verified end-to-end
`kubectl ate create actor` → HTTP request via atenet → **restore-on-demand of a
~60 MiB live golden** (~18s cold / ~9s warm) → agent serves → `kubectl ate suspend`
→ resume again. Full suspend/resume lifecycle confirmed on the current OSS stack.

## Cost Model

Without Substrate, each OpenClaw instance is an always-running pod even while idle. With this split, the always-on footprint per user is just the lightweight gateway; the heavy agent consumes a worker pod only while actively processing plus a short idle window, then suspends. Many actors share a small worker pool, roughly a 5–10× compute reduction for typical personal-assistant workloads.
