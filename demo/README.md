# WhatsApp Demo: OpenClaw on Agent Substrate

OpenClaw on WhatsApp, backed by Agent Substrate. The agent runs as a suspendable
per-conversation actor: it can be **suspended when idle** and **auto-resumes** on
the next message, so you pay for compute only while it's actually thinking. The
always-on gateway holds the WhatsApp connection the whole time.

> **Who suspends the actor.** The gateway does, not the actor itself. A Substrate
> actor runs in a gVisor sandbox with no ateapi credentials, so it cannot call
> `SuspendActor` on its own behalf, with no client certificate to present.
> The gateway already holds the credentialed path it uses to *create* actors, so
> it tracks per-conversation activity and suspends each actor once it has been
> idle past `idleTimeoutSeconds`. Resume is automatic: the next turn hits atenet,
> which restores the actor from its checkpoint. See
> [`../extensions/substrate/idle-suspender.ts`](../extensions/substrate/idle-suspender.ts).
> You can also drive it by hand with
> `kubectl ate suspend actor <name> -a <atespace>`.

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

- A **GKE cluster** created with the PodCertificate beta APIs on, with `kubectl`
  configured against it, and **Agent Substrate installed** on it (all of Step 1).
- **gcloud** authenticated to a GCP project (`gcloud auth login`), with **Cloud Build**,
  **Container Registry**, and **Cloud Storage** enabled.
- On your PATH: `kubectl`, `gcloud`, `go` (to build the Substrate CLI), and
  [`ko`](https://ko.build) (the deploy script builds ateom with it).
- A **GCS bucket** for golden snapshots, with **both** `atelet` and `ate-api-server`
  granted `roles/storage.objectAdmin` and `roles/storage.bucketViewer` on it. The
  from-source install in Step 1 provisions no GCP resources at all, so this one is
  yours to do. `go run ./tools/setup-gcp bootstrap` from a Substrate checkout does
  it, and is idempotent against a bucket that already exists, or by hand:

  ```bash
  WI="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT_ID.svc.id.goog/subject/ns/ate-system/sa"
  for sa in atelet ate-api-server; do
    for role in roles/storage.objectAdmin roles/storage.bucketViewer; do
      gcloud storage buckets add-iam-policy-binding "gs://$GCS_BUCKET" \
        --member="$WI/$sa" --role="$role"
    done
  done
  ```

  `atelet` writes the checkpoints; `ate-api-server` copies them for tags and deletes
  the ones nothing refers to. Miss the `ate-api-server` half and the first suspend of
  each actor still works, because there is no replaced snapshot to collect yet. The
  *second* fails with `storage.objects.list` denied and wedges the actor. See the
  note under Step 5. The full role table is in
  [`tools/setup-gcp/README.md`](https://github.com/agent-substrate/substrate/blob/main/tools/setup-gcp/README.md).
- A **Gemini API key**.
- A phone with **WhatsApp**.
- **gVisor / runsc note:** the actor is a heavy multi-process Node.js workload, but
  it checkpoints and restores fine on the stock public `gvisor.dev` releases that
  the install's default gVisor `SandboxConfig` ships, with no runsc override needed
  (verified on the 20260622, 20260803 and 20260824 builds).

## Step 1: Install Agent Substrate

Install from a Substrate checkout. This is the path the demo was developed and
validated against.

**First, create the cluster with the PodCertificate beta APIs on.** Substrate
needs `certificates.k8s.io/v1beta1/podcertificaterequests` and
`clustertrustbundles`, and GKE does not enable them by default. They have to be
passed at create time:

```bash
gcloud container clusters create "$CLUSTER" --zone "$ZONE" \
  --enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles
```

Without them the install dies at `waiting for ClusterTrustBundle
podidentity.podcert.ate.dev:identity:primary-bundle: context deadline exceeded`,
which does not say what is missing. Check with
`kubectl api-resources | grep clustertrustbundles`.

On a cluster you already have, `gcloud container clusters update` with the same
flag is only half of it. The update flips the API server, but nodes created
before it keep a kubelet that cannot project the bundle, and every ate-system pod
then hangs in `ContainerCreating` on `ClusterTrustBundle projection is not
supported in static kubelet mode`. Recreate the node pools afterwards, then
re-run the install to restore the `ate.dev/substrate-version` node label the
recreated pools drop.

**Then install:**

```bash
git clone https://github.com/agent-substrate/substrate.git
cd substrate
cp hack/ate-dev-env.sh.example .ate-dev-env.sh   # then edit for your project/cluster
hack/install-ate.sh --deploy-ate-system
```

It builds the control plane with ko and installs it, and it provisions no GCP
resources, so the snapshot bucket and its IAM bindings are yours to create first
(see Prerequisites). It also cannot upgrade a cluster installed from an older
build in place; see
[`docs/upgrade.md`](https://github.com/agent-substrate/substrate/blob/main/docs/upgrade.md).

Keep this checkout. Step 3 builds the worker pods' `ateom` from it, so that
component matches the control plane you just installed.

<details>
<summary>The packaged installer (not usable yet)</summary>

[`ai-on-gke/substrate-gke`](https://github.com/ai-on-gke/substrate-gke) wraps all
of the above in an interactive wizard (`make run`, or `make doctor` for preflight
checks only) and provisions the GCP resources for you, including the bucket and
its IAM. It is the nicer path and will become the recommended one.

It does not work yet. Its default track installs pre-built release images, and at
v0.1.0 those are not readable outside the project that publishes them, so the
pull fails with 403 and the installer reports it as an unrelated rollout timeout
(`waiting for deployment/podcertificate-controller ...: client rate limiter Wait
returned an error`), which never mentions an image pull. Use the from-source path
above until that is fixed.
</details>

You also need the `kubectl-ate` CLI on your PATH, built from the same Substrate
commit the control plane runs. From that same checkout:

```bash
make build-atectl && export PATH="$PWD/bin:$PATH"
```

Verify:

```bash
kubectl -n ate-system get pods    # ate-api-server, atelet-<version>, atecontroller, atenet-*, postgres, valkey
kubectl ate --version
```

> Pinned to Substrate **`c48b3a3c`**, the head of the `release-0.1` branch that
> v0.1.0 is cut from. Pinned by SHA and not by branch name, since release-0.1 was
> cut clean off main and still moves. Earlier commits will not work: ActorTemplate
> stopped being a Kubernetes CRD, and `WorkerPool.spec.ateomImage` was renamed to
> `workerImage`.

## Step 2: Set your config

```bash
export PROJECT_ID="your-gcp-project"     # for gcr.io/<project>/openclaw-*
export GCS_BUCKET="your-snapshot-bucket"  # no gs:// prefix; must already exist
export GEMINI_API_KEY="..."              # LLM provider key
export SUBSTRATE_REPO=/path/to/substrate # the checkout from Step 1
# optional:
# export ATEOM_IMAGE=...                     # prebuilt ateom-gvisor from that same commit,
#                                            # instead of building it from SUBSTRATE_REPO
# export WHATSAPP_PEER="+1..."               # enable scheduled status pings (E.164)
```

`SUBSTRATE_REPO` is required unless you set `ATEOM_IMAGE`. The worker pods' ateom
has to be built from the same commit the control plane was installed from,
because it speaks internal protos to atelet and ateapi, and a skewed build fails
golden resume with an opaque error instead of a version message. The script
builds it for you with `ko`, so you need `ko` on your PATH too.

## Step 3: Deploy

From your clone of *this* repo (not the Substrate checkout):

```bash
cd always-on-agent/demo
./deploy-demo.sh
```

`deploy-demo.sh` is idempotent and, on first run, **builds both images with Cloud
Build** (`gcr.io/$PROJECT_ID/openclaw-{gateway,actor}:demo`), pins them by digest
(snapshots require `@sha256`-pinned images), then applies the WorkerPool,
ActorTemplate, gateway, and one demo actor. Force a rebuild with `BUILD_IMAGES=true`.

> Both images build from the **public** `ghcr.io/openclaw/openclaw` release plus
> the plugin source vendored in this repo at `../extensions/substrate/`, so no
> private base image and no OpenClaw source checkout is needed. If you already
> have images, push them as `gcr.io/$PROJECT_ID/openclaw-gateway:demo` and
> `...-actor:demo` and re-run; the script resolves their digests automatically.

> **Three versions have to move together.** The base image is pinned by digest to
> OpenClaw `2026.8.2` in both Dockerfiles (`:slim` floats and rolled to `2026.9.1`
> on 3 Sep). WhatsApp is no longer bundled in that image, so the gateway's init
> container installs `clawhub:@openclaw/whatsapp` at a matching pinned version;
> the floating plugin refuses to install against an older runtime. And
> `kubectl-ate` is built from the pinned Substrate commit, not from `main`, so the
> gateway's CLI cannot drift away from the control plane. Bump all three in the
> same change.

## Step 4: Link WhatsApp

```bash
kubectl -n openclaw port-forward svc/openclaw-gateway 18789:18789
```

Open <http://localhost:18789>, log in with the gateway token the script printed, and
scan the QR shown by the WhatsApp plugin: **WhatsApp > Settings > Linked Devices >
Link a Device**. Credentials persist on the gateway PVC, so you only scan once.

## Step 5: Try it and watch suspend/resume

Message the linked WhatsApp account; the agent replies. Then watch the lifecycle:

```bash
watch kubectl ate get actors -A
kubectl ate logs actor oc-agent --atespace openclaw-demo -f
```

Suspend the actor and message again:

```bash
kubectl ate suspend actor oc-agent -a openclaw-demo
```

It goes **RUNNING → SUSPENDED**, then **SUSPENDED → RESUMING → RUNNING** on the
next message, with the conversation preserved across the checkpoint. The idle
window the gateway uses is `plugins.entries.substrate.config.idleTimeoutSeconds`
in [`openclaw-demo-config.yaml`](openclaw-demo-config.yaml).

Three things to know before you time it:

- The **first** resume on a worker node that has never run a sandbox is slow
  enough that the router gives up (HTTP 504) before it finishes. Pre-warm the node
  with one control-plane resume; after that, request-driven resume is reliable.
- ActorTemplates are immutable now, so changing the image, bucket or key means
  delete-and-recreate, which `deploy-demo.sh` does for you.
- A suspend that fails **after** the checkpoint is written does not roll back, and
  the actor wedges in `SUSPENDING` with no way out: suspend returns
  `runsc checkpoint: exit status 128`, resume and delete both reject the state. The
  only recovery is to delete the worker pod, which moves the actor to `CRASHED`,
  and then delete the actor. The checkpoint that was already uploaded is orphaned
  in the bucket. Getting the bucket IAM right up front avoids the whole path.

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
| [`../manifests/workerpool.yaml`](../manifests/workerpool.yaml) | gVisor worker pods the actors run on |
| [`../manifests/actortemplate.yaml`](../manifests/actortemplate.yaml) | golden agent template (image pinned by digest, snapshot to your bucket). **Not a Kubernetes object**: a protojson `ateapipb.ActorTemplate` created with `kubectl ate create actor-template -f`, not `kubectl apply` |
| [`../manifests/gateway.yaml`](../manifests/gateway.yaml) | always-on gateway Deployment + LoadBalancer + RBAC |
| [`openclaw-demo-config.yaml`](openclaw-demo-config.yaml) | gateway `openclaw.json` (substrate plugin as **gateway**, WhatsApp binding) |
| [`../build/actor/openclaw.json`](../build/actor/openclaw.json) | actor `openclaw.json`, which enables the OpenAI-compatible HTTP endpoint and trusts atenet's link-local proxy address. Baked into the actor image; ActorTemplate volumes cannot mount a ConfigMap. The substrate plugin is **not** installed in the actor image (see the note at the top) |
| [`openclaw-demo-secrets.yaml`](openclaw-demo-secrets.yaml) | Secret template (the script creates these directly) |
