# Hermes Agent on Agent Substrate

## About Hermes Agent

[Hermes Agent](https://github.com/NousResearch/hermes-agent) is the open source agent harness from Nous Research. It is MIT licensed, so every part of it can be read, modified and run anywhere: the terminal, a desktop app, Slack, Telegram, WhatsApp, email, or a cluster like this one. It works with any model from any provider, including models you host yourself.

Hermes is designed to improve with use. When it finishes a task it writes down what worked as a reusable skill, then refines that skill the next time it meets a similar problem. Skills, memories and session history all live as plain files, so an agent's accumulated know-how is portable, inspectable and owned by whoever runs it. For a team, that means the knowledge one agent earns can be shared with the rest.

On May 10, 2026, Hermes ranked #1 by daily token usage among the public apps and agents tracked by OpenRouter ([contemporaneous report](https://startupfortune.com/hermes-agent-leads-openrouter-as-agent-usage-becomes-a-market-signal/)).

Documentation: https://hermes-agent.nousresearch.com/docs/

## Why Hermes fits Agent Substrate

Hermes keeps everything it needs between turns on disk: configuration, sessions, memory, skills and scheduled jobs all sit under one directory. Nothing important lives only in process memory. That is exactly the shape a suspendable actor wants. Substrate can checkpoint `/data`, let the machine go, and cold-boot Hermes back into the same conversation with its files and memory intact, which is what this integration does and what we validated end to end: a turn, a suspension, and a resume with files and memory in place.

A personal agent is idle most of the day but has to look online all of it. On Substrate, each Hermes agent costs compute only while it is working. The always-on gateway holds the channel connections and wakes the agent when a message or a scheduled job arrives. Hermes' own scheduler and its busy signal (the turn-signal plugin in this repo) tell the gateway when background work is still running, so an agent is never suspended mid-task.

The result is a fleet pattern: many agents, each with its own memory and skills, sharing a worker pool instead of each reserving a machine. Scale to zero applies to the agents; the gateway, worker pool and stored state keep running, and a cold resume adds a short delay.

## Core runtime

This integration runs Hermes as a suspendable Substrate actor, with an always-on
gateway for HTTP, Telegram and WhatsApp messaging, scheduling and lifecycle
management. Hermes uses its normal tools, persistent memory and local files.

This is an integration proof of concept, not a production-readiness or GA claim.

## Architecture

```
HTTP / Telegram / WhatsApp
            |
      Hermes gateway      registry, scheduling, durable turn deduplication
            |             idle suspension and authenticated busy signals
      Substrate router    resume on demand
            |
      Hermes actor        tools, conversations, memory, files under /data
```

Substrate owns worker placement, sandboxing and checkpoint/restore. The Hermes
gateway maps messages to actors, keeps scheduled work outside sleeping actors,
and holds actors awake while their work continues.

A registered agent ID determines its actor name; multiple conversations can
share that actor. Without a registry, each conversation gets its own agent.

The actor's `/data` directory holds durable application state. When an actor
suspends, Substrate keeps `/data` only; a resume cold-boots Hermes from the image
and its files, so the entrypoint runs again every time. Actor templates are
immutable, so changing the image or startup configuration requires a new
template and an explicit state migration or reset decision.

## Repository layout

| Path | Purpose |
| --- | --- |
| [actor/](actor/) | Core Hermes image, generic briefing, startup and busy-protection plugin |
| [gateway/](gateway/) | Routing, actor lifecycle, scheduling, deduplication, model proxy and fleet state |
| [k8s/](k8s/) | Core deployment manifests and registry example |
| [deploy/](deploy/) | Core deployment CLI and operational tests |
| [demo/README.md](demo/README.md) | Reproduce the runtime on an existing Substrate cluster |
| [demo/fleet/README.md](demo/fleet/README.md) | Fleet shapes, schedules, idle tiers and workload assumptions |
| [egress-plugin/README.md](egress-plugin/README.md) | Per-actor network policy and verified actor stamps |
| [egress-plugin-example/README.md](egress-plugin-example/README.md) | Separate egress extension example |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Current runtime, persistence, routing and security boundaries |
| [docs/WHATSAPP-SETUP.md](docs/WHATSAPP-SETUP.md) | WhatsApp channel setup |
| [tests/](tests/) | Core regression and image-separation tests |

## Setup

This integration targets Agent Substrate v1. Point `SUBSTRATE_SRC` in
`deploy/env.sh` at a checkout of the matching release tag; the install,
actor template and ingress all changed from earlier releases.

Install deployment dependencies:

```sh
cd hermes
pip install -r deploy/requirements.txt
cp deploy/env.sh.example deploy/env.sh
# Edit deploy/env.sh for your project, registry, cluster and model.
deploy/hermesctl preflight
deploy/hermesctl build
deploy/hermesctl deploy
deploy/hermesctl smoke
```

Build and deploy assume the configured cluster already has a compatible
Substrate installation. The CLI also provides `cluster-up` and
`substrate-install`; review the configuration before using those commands.

For an existing Substrate cluster, [the demo guide](demo/README.md) gives the
prerequisites and ordered lifecycle checks. Its scripts build core images only.

Images used by templates are digest-pinned. Core actor builds are content-tagged
to avoid replacing templates unnecessarily. The deployment reuses an existing
API-server key: changing it would invalidate already-warmed actors.

Do not apply placeholder manifests directly. The deployment tools render their
project, image, model and storage settings.

### Model access and egress

Configure `MODEL_PROVIDER`, `MODEL_DEFAULT` and `MODEL_BASE_URL` together.
The optional Vertex bridge uses the gateway's GKE Workload Identity; actors
call the internal model endpoint using their existing API-server key.

Network policy is separate. See [egress policy](egress-plugin/README.md) before
enabling `HERMES_EGRESS` and `HERMES_EGRESS_PLUGIN`; destination matching and
credential injection depend on the Substrate egress transport in use.

Do not assume that enabling egress policy injects a model credential.
`ACTOR_MODEL_KEY_INLINE=1` places the configured key in the actor template
in plaintext. The internal model bridge avoids storing a provider credential
there. Restrict access to manifests, secrets and snapshots accordingly.

### Installation settings that matter

Use a CPU-homogeneous worker pool: a gVisor checkpoint must be compatible with
the restoring host. A worker is occupied only while an actor is awake.

The deployment supplies explicit atenet request/parking/drain timeouts. Keep
these overrides when adapting manifests; short platform defaults can interrupt
a valid agent turn. Increasing a timeout alone does not guarantee exactly-once
delivery or recovery of an abandoned response.

## Operating the runtime

| Command | Purpose |
| --- | --- |
| `preflight` | Local tooling, project access and credentials |
| `build` | Build core actor and gateway images |
| `deploy` | Render and apply runtime resources |
| `status`, `doctor` | Deployment state and diagnostic checks |
| `smoke` | Send a real HTTP turn through the gateway |
| `test-turn-signal` | Exercise the busy plugin with real Linux processes |
| `fleet` | Agent state, usage and failure counters |
| `egress-status` | Inspect current egress wiring |
| `egress-on`, `egress-off` | Apply or remove configured egress policy |
| `reset-agent` | Destructive reset of actors in the configured atespace |

Inspect `deploy/env.sh` before any mutating command. The sample registry is
[k8s/hermes-agents.example.yaml](k8s/hermes-agents.example.yaml).

Busy protection covers work that outlives the HTTP response. The actor plugin
checks child processes and sends authenticated working/done signals. These
signals authenticate the running Substrate caller.

The gateway persists turn identities and recorded responses. Retrying an
already-applied turn must not redispatch its work. A process crash before a
response is durably recorded can still leave a lost response; successful
delivery is not a promise of response recovery.

The fleet view retains historical agents and reports unknown state when the
control plane is unavailable. Suspend/resume/delete operations go through
`kubectl-ate`, not Kubernetes actor CRDs. Deleting actors or replacing their
templates is not a routine substitute for an upgrade/state-migration plan.

Generic HTTP ingress (`/message` and `/v1/chat/completions`) requires
`Authorization: Bearer <GATEWAY_INGRESS_KEY>`. This is a fleet-wide trusted-adapter
credential, not an end-user token. Never give it to actors or browser clients.
It must differ from `API_SERVER_KEY`; missing or identical keys disable these routes.
`hermesctl deploy` creates and preserves it in `hermes-secrets`. For manual
deployments, add a separately generated random key to that Secret. Existing
generic clients, demos and HTTP acceptance runners must supply the new key
(the bundled runners read `GATEWAY_INGRESS_KEY` from their environment).
Telegram and WhatsApp retain their own authentication paths.

Before exposing HTTP or messaging ingress, put appropriate authentication and
network restrictions in front of it. Configure sender/chat allowlists for
enabled channels. This example is not a hardened public administration service.

## Tests and validation scope

```sh
python3 tests/selftest.py
python3 -m unittest discover -s tests -p 'test_*.py'
python3 actor/test_entrypoint.py
```

Regression tests cover routing, lifecycle callbacks, scheduling, retry handling,
gateway authentication and standalone startup. Test-cluster checks should cover
real model turns, saved-file survival and recall after confirmed suspension,
and background work continuing past the idle timeout.

These checks do not establish full product acceptance, a clean-room installation
from scratch or long-duration reliability.

The cluster test scripts under `deploy/` require an explicit `KUBECTL_CONTEXT`
and the target resource settings for the test installation. Snapshot fault tests
also require `BUCKET_NAME` or `T11_GOOD_STORE`. Placeholder values are not usable
deployment targets. Review each fault test before running it: some deliberately
delete test resources or damage test snapshots.

## License

Apache License 2.0, see [`LICENSE`](LICENSE).

## Trademarks

Hermes Agent is an open source project from Nous Research, MIT licensed. This integration
was built jointly by Google and Nous Research. "Hermes" and related names and logos belong to
Nous Research and are used here to identify the software being integrated.
