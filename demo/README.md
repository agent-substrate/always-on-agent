# WhatsApp Demo: OpenClaw on Agent Substrate

OpenClaw on WhatsApp, backed by Agent Substrate. The agent runs as a suspendable
per-conversation actor: it **self-suspends when idle** and **auto-resumes** when a
message arrives, so you pay for compute only while it's actually thinking. The
always-on gateway holds the WhatsApp connection the whole time.

```
WhatsApp user
    │  (Baileys WebSocket)
    ▼
┌─────────────────────────┐    HTTP POST /v1/chat/completions    ┌──────────────────────────┐
│  Gateway (always-on)    │ ───────────────────────────────────► │  Agent actor (Substrate)  │
│  ~128 MB, ~0 CPU idle   │            via atenet                 │  suspends when idle       │
│  WhatsApp channel       │ ◄─────────────────────────────────── │  resumes on request       │
│  ACP routing            │            SSE stream                 │  LLM calls (Gemini)       │
│  per-conv actor mapping │                                       │  SOUL.md / memory         │
└─────────────────────────┘                                       └──────────────────────────┘
```

## Prerequisites

- A **GKE cluster** with `kubectl` configured, and **Agent Substrate installed** on it
  (see Step 1).
- **gcloud** authenticated to a GCP project (`gcloud auth login`), with **Cloud Build**,
  **Container Registry**, and **Cloud Storage** enabled.
- A **GCS bucket** for golden snapshots.
- A **Gemini API key**.
- A phone with **WhatsApp**.
- **gVisor / runsc note:** the actor is a heavy multi-process Node.js workload.
  Public `gvisor.dev` releases crash its sentry ~30–60 s in; the golden checkpoint
  needs a runsc build that survives it (the GKE-Sandbox build works). Point your
  cluster's gVisor `SandboxConfig` at such a build. This is the least portable part
  of the demo — see [`../substrate-patches/README.md`](../substrate-patches/README.md).

## Step 1 — Install Agent Substrate

```bash
git clone https://github.com/agent-substrate/substrate.git
cd substrate
cp hack/ate-dev-env.sh.example .ate-dev-env.sh   # then edit for your project/cluster
hack/install-ate.sh --deploy-ate-system
make build-atectl && export PATH="$PWD/bin:$PATH" # provides the kubectl-ate CLI
```

Verify:

```bash
kubectl -n ate-system get pods    # ate-api-server, atelet, atecontroller, atenet-*, valkey
kubectl ate version
```

## Step 2 — Set your config

```bash
export PROJECT_ID="your-gcp-project"    # for gcr.io/<project>/openclaw-*
export GCS_BUCKET="your-snapshot-bucket" # no gs:// prefix; must already exist
export GEMINI_API_KEY="..."             # LLM provider key
# optional:
# export SUBSTRATE_REPO=/path/to/substrate   # let deploy-demo.sh install Substrate for you
# export WHATSAPP_PEER="+1..."               # enable scheduled status pings (E.164)
```

## Step 3 — Deploy

```bash
cd always-on-agent/demo
./deploy-demo.sh
```

`deploy-demo.sh` is idempotent and, on first run, **builds both images with Cloud
Build** (`gcr.io/$PROJECT_ID/openclaw-{gateway,actor}:demo`), pins them by digest
(snapshots require `@sha256`-pinned images), then applies the WorkerPool,
ActorTemplate, gateway, and one demo actor. Force a rebuild with `BUILD_IMAGES=true`.

> Building the images yourself needs OpenClaw's own build inputs referenced by
> `../gateway.Dockerfile` and `../actor.Dockerfile`. If you already have images,
> push them as `gcr.io/$PROJECT_ID/openclaw-gateway:demo` and
> `...-actor:demo` and re-run — the script resolves their digests automatically.

## Step 4 — Link WhatsApp

```bash
kubectl -n openclaw port-forward svc/openclaw-gateway 18789:18789
```

Open <http://localhost:18789>, log in with the gateway token the script printed, and
scan the QR shown by the WhatsApp plugin: **WhatsApp > Settings > Linked Devices >
Link a Device**. Credentials persist on the gateway PVC, so you only scan once.

## Step 5 — Try it and watch suspend/resume

Message the linked WhatsApp account; the agent replies. Then watch the lifecycle:

```bash
watch kubectl ate get actors -A
kubectl ate logs actor oc-agent --atespace openclaw-demo -f
```

The actor goes **RUNNING → SUSPENDED** after ~10 s idle, and **SUSPENDED →
RESUMING → RUNNING** on the next message — with conversation state preserved
(it lives in the actor's DurableDir). The idle window is
`plugins.entries.substrate.config.idleTimeoutSeconds` in
[`openclaw-demo-config.yaml`](openclaw-demo-config.yaml) (10 s here for a snappy
demo; raise it for real use).

## Demo recording

_A recording of the end-to-end flow (WhatsApp on the left, actor lifecycle on the
right) will be linked here._ <!-- TODO: link a GitHub Release asset or external URL -->

## Cleanup

```bash
kubectl ate delete actor oc-agent --atespace openclaw-demo
kubectl delete namespace openclaw
```

## How it maps to the manifests

| File | Role |
|------|------|
| [`../openclaw-workerpool.yaml`](../openclaw-workerpool.yaml) | gVisor worker pods the actors run on |
| [`../openclaw-actortemplate.yaml`](../openclaw-actortemplate.yaml) | golden agent template (image pinned by digest, snapshot to your bucket) |
| [`../openclaw-gateway.yaml`](../openclaw-gateway.yaml) | always-on gateway Deployment + LoadBalancer + RBAC |
| [`openclaw-demo-config.yaml`](openclaw-demo-config.yaml) | gateway `openclaw.json` (substrate plugin as **gateway**, WhatsApp binding) |
| [`openclaw-actor-config.yaml`](openclaw-actor-config.yaml) | actor `openclaw.json` (substrate plugin as **actor**) + SOUL.md |
| [`openclaw-demo-secrets.yaml`](openclaw-demo-secrets.yaml) | Secret template (the script creates these directly) |
