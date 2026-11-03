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

`deploy-demo.sh` is idempotent and, on first run, **builds the images with Cloud
Build** (`gcr.io/$PROJECT_ID/openclaw-{gateway,actor,dashboard}:demo`), pins them
by digest (snapshots require `@sha256`-pinned images), then applies the
WorkerPool, ActorTemplate, gateway, dashboard, and one demo actor. Force a
rebuild with `BUILD_IMAGES=true`, and skip the dashboard with
`DEPLOY_DASHBOARD=false`.

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

Check the channel came up enabled first. If it says `not configured`, the config in
Step 2 did not land and the QR below will have nothing to attach to:

```bash
kubectl -n openclaw exec deploy/openclaw-gateway -- \
  node /app/openclaw.mjs channels list
# WhatsApp default: installed, enabled, not linked
```

Then print a QR in your own terminal and scan it from the phone:

```bash
kubectl -n openclaw exec -it deploy/openclaw-gateway -- \
  node /app/openclaw.mjs channels login --channel whatsapp
```

**WhatsApp > Settings > Linked Devices > Link a Device.** The QR rotates every
20 seconds or so and a new one reprints, so a missed scan costs nothing. When it
links, `channels list` reads `linked`. Credentials persist on the gateway PVC at
`.openclaw/credentials/whatsapp/default`, so you only scan once.

> **Restart the gateway once after the scan.** WhatsApp answers a fresh pairing
> with `code 515`, which asks the client to reconnect. On this build the log stops
> at `waiting for creds to save…` and the socket never comes back, while
> `channels list` already reads `linked` off the saved credentials. So the channel
> looks paired and is not receiving anything. `kubectl -n openclaw rollout restart
> deploy/openclaw-gateway` picks the credentials back up, and the line you want in
> the log is `Listening for WhatsApp inbound messages`. Treat that line, not
> `linked`, as the signal the channel is live.

The Control UI does the same thing if you would rather click: port-forward
`svc/openclaw-gateway 18789:18789`, open <http://localhost:18789> and log in with
the gateway token the script printed. It has to be localhost, not the
LoadBalancer IP, because `gateway.controlUi.allowedOrigins` only lists
`http://localhost:18789`.

> **This account answers anyone who messages it.** The demo config sets
> `channels.whatsapp.dmPolicy: "open"` with `allowFrom: ["*"]`, which is what makes
> the recording a single scan and a single message rather than a pairing dance.
> Link a spare number, not a personal one. To lock it down instead, drop
> `dmPolicy` back to its `"pairing"` default, or put your own number in
> `allowFrom`. Note that `"open"` without `allowFrom: ["*"]` silently drops every
> DM: the gateway logs one config warning at startup and then looks perfectly
> healthy while nothing arrives.

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

### The dashboard

`deploy-demo.sh` also brings up a read-only dashboard behind a LoadBalancer,
which is easier to watch than `kubectl ate get actors` and is what the recording
uses. It shows the request path, the actor lifecycle as a timeline, which worker
pod each actor is currently landed on, and the ratio of actors to worker pods.
"Burst" fires a task at every actor at once, so you can watch them multiplex
onto the pool.

```bash
kubectl -n openclaw get svc openclaw-dashboard   # wait for EXTERNAL-IP, then open :8090
```

> **Suspend by hand after a burst.** Actors woken by Burst stay RUNNING until you
> say otherwise, and they will still be RUNNING the next morning. Nothing is
> wedged: the idle clock lives in the gateway and only follows conversations the
> gateway drove, and Burst goes straight at the actor's `/healthz`. So the
> pre-flight before a recording is a loop over the fleet, and it is worth reading
> the output rather than discarding it:
>
> ```bash
> for i in $(seq 1 15); do kubectl ate suspend actor oc-agent-$i -a openclaw-demo; done
> kubectl ate get workers   # every worker FREE before you start
> ```

It only reads the cluster, and it reads nothing about WhatsApp: link state, the
message thread and the pairing button all used to live here, all of it a second
copy of what WhatsApp Web already shows on the same screen, and it was the only
reason the dashboard held `pods/exec`. The demo runs fine without it
(`DEPLOY_DASHBOARD=false`). It builds from [`dashboard/`](dashboard/) and builds
its own `kubectl-ate` from the pinned Substrate commit, so the CLI it uses
matches the control plane. It used to copy that binary out of the gateway image
instead, which sounds equivalent and isn't:
the gateway image carried a build old enough to predate `--authentication-config`,
so it sent no bearer token and the dashboard showed an empty actor list while
every pod stayed green.

Add `?layout=demo` to the URL for the recording layout. Same panels and the
same data; the only thing hidden is the Economic Savings card. What changes is
density: the fleet becomes a grid of chips so all sixteen actors are on screen,
the pod map drops the IPs so all five workers are, and the atespace suffix comes
off every actor name. The target is a 1280x1080 viewport, which is the left two
thirds of a 1080p capture with the browser in fullscreen. It measures 1027px
tall, so nothing scrolls and nothing sits below the fold.

Two panels sit side by side at the bottom and look like the same panel twice.
They are not. The timeline is per-actor and says what the control plane did,
newest first. The event stream is per-operation and says what was asked of it,
including what it refused: burst more actors than the pool has workers and the
excess come back `no worker free (HTTP 503)`, which is the only place the
one-actor-per-ateom rule is visible.

Layout changes are checked rather than eyeballed. [`dashboard/preview/`](dashboard/preview/)
serves the page's HTML straight out of `dashboard.js` next to a captured
`/api/state`, and screenshots it at a given viewport with the scroll height
printed. That turns a four-minute build-and-rollout into about a second, and it
lets you render a mid-burst fleet without having one. It is a layout harness
only: no server-side code runs in it.

```bash
curl -s http://<dashboard-ip>:8090/api/state > /tmp/state.json
dashboard/preview/serve.py /tmp/state.json &
google-chrome --headless=new --disable-gpu --remote-debugging-port=9222 \
  --user-data-dir=/tmp/chrome-preview &
dashboard/preview/measure.py 'http://127.0.0.1:8099/?layout=demo' 1280 1080 /tmp/shot.png
```

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

[![OpenClaw on Agent Substrate: always-on agents, suspended when idle](../docs/always-on-agents-demo-poster.jpg)](https://www.youtube.com/watch?v=D5a9tyPkaPY)

The end-to-end flow, two minutes with narration. The dashboard is on the left, the
WhatsApp conversation top right, and a terminal polling the control plane below it,
so every claim the dashboard makes is checkable against `kubectl ate` in the same
frame.

| Time | What happens |
|---|---|
| 0:19 | a message arrives for an agent that is not running, and it restores and answers |
| 0:35 | it checkpoints itself and releases the worker, with nothing asking it to |
| 0:47 | a follow-up comes back with the conversation intact |
| 1:00 | ten actors cycle through five workers; the ones that do not fit are refused and retried |
| 1:30 | the control-plane pane and the dashboard agree throughout |

Watch on [YouTube](https://www.youtube.com/watch?v=D5a9tyPkaPY), or download
[`docs/always-on-agents-demo.mp4`](../docs/always-on-agents-demo.mp4) (3.7 MB, 1440p).
Captions are in [`docs/always-on-agents-demo.srt`](../docs/always-on-agents-demo.srt).

## Cleanup

```bash
kubectl ate delete actor oc-agent --atespace openclaw-demo
kubectl delete namespace openclaw
kubectl delete clusterrole,clusterrolebinding openclaw-dashboard
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
| [`dashboard/`](dashboard/) | Read-only lifecycle dashboard: image, RBAC, Deployment + LoadBalancer |
