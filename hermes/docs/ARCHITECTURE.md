# Hermes on Substrate: runtime architecture

This integration runs upstream [Hermes Agent](https://github.com/NousResearch/hermes-agent)
in a suspendable Substrate actor. An always-on gateway handles inbound channels,
scheduled jobs, routing and lifecycle management. The actor image installs a
pinned Hermes source revision; this repository does not vendor or patch its core.

## Request path

```text
HTTP / Telegram / WhatsApp
            |
      Hermes gateway
            |
      Substrate router --> restore actor when needed
            |
      Hermes API server --> tools and model calls
            |
           /data
```

The gateway provisions an actor when necessary and sends requests through
Substrate. It remains running while actors sleep, so channel connections and
scheduling do not depend on a suspended process.

## Routing and sessions

The optional registry maps channel keys to arbitrary agent routing IDs. Several
conversations may route to the same actor while retaining separate Hermes session
keys. Without a registry binding, a conversation key becomes its own routing ID.
The gateway hashes that ID into `agent-<12 hex characters>`.

`conv-*` is an older conversation-based naming scheme retained by compatibility
helpers and some direct lifecycle demos. It is not an account type. Renaming an
existing actor is not a state migration: keep its existing name or explicitly
plan how to transfer its data.

## Persistent state and snapshots

The actor uses `/data` as both HOME and its working directory, with
`HERMES_HOME=/data/.hermes`. Its SQLite store, memory, configuration, sessions,
self-authored skills and working files live there. The template declares `/data`
as a durable directory.

The supplied template snapshots at `VOLUMES` fidelity: suspending keeps `/data`
and nothing else, and a resume cold-boots the container from the image with
`/data` restored. That matches Hermes' own design, where everything needed
between turns is on disk. Each boot reruns the entrypoint, which clears the
previous boot's gateway pid, lock and state markers and refreshes the
image-shipped briefing and plugins. New actors start from the template's golden
snapshot. A process resumed in place on its own worker keeps its memory.

Templates are immutable and actor images are digest-pinned. An image upgrade
requires a new template and an explicit data-transfer or reset procedure. Do not
assume that arbitrary memory snapshots work with a different image or sandbox.

## Busy protection and delivery

The gateway tracks active turns and idle deadlines. The actor's `turn-signal`
plugin reports ongoing background processes and turn completion through
authenticated callbacks. This keeps an actor awake when work continues after an
HTTP response; completion makes it eligible for idle suspension again.

The gateway persists turn IDs and recorded responses to avoid redispatching an
already-applied turn. A crash after work is applied but before its response is
stored can still lose the reply. This is not a guarantee of exactly-once effects
or recovery of every response. Clients that require ordering must serialize
their requests appropriately.

## Authentication and model access

Generic HTTP ingress requires `GATEWAY_INGRESS_KEY`, a trusted-adapter credential
distinct from the actor's `API_SERVER_KEY`. Do not distribute the gateway key to
actors or browser clients. Channel adapters use their own authentication.

Substrate caller authentication protects lifecycle callbacks. The egress plugin
can enforce destination policy and forward a verified actor stamp to in-cluster
services. These are runtime security mechanisms, not external user accounts.

With the optional Vertex bridge, the gateway uses GKE Workload Identity to call
the model. The actor uses its internal API key to reach that bridge. Direct
provider access is a separate mode; inline provider keys can enter templates
and snapshots. See [model access](../README.md#model-access-and-egress) and
[egress configuration](../egress-plugin/README.md).

## Deployment boundaries

- Substrate owns workers, sandbox placement, routing and checkpoint/restore.
- This integration owns the actor image, gateway, busy plugin and deployment tools.
- One gateway deployment uses one configured ActorTemplate. Fleet shape files
  are planning/build inputs, not automatic per-agent template selection.
- Channel routing does not replace end-user authorization. Restrict ingress and
  configure channel allowlists before exposing a deployment.

## Validation and remaining limits

An isolated test on an existing GKE/Substrate installation verified real
model/tool turns, file and conversation-memory persistence after confirmed
suspension, background-work protection beyond the idle timeout, automatic
suspension after completion, and rejection of unauthorized generic ingress.

That establishes the tested runtime path, not production readiness, a fresh
platform installation, arbitrary image-upgrade compatibility, or long-duration
reliability. Keep CPU compatibility, snapshot storage permissions, readiness
checks and explicit route timeouts in the deployment plan. See the
[setup and validation guide](../README.md).
