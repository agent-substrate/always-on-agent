# Running and validating Hermes on Substrate

Run commands from the integration directory, `hermes/`. Use a test installation
for lifecycle experiments; some helpers deliberately delete actors or snapshots.
The [main README](../README.md) describes the runtime and security boundaries.

## Prerequisites

- A GKE cluster with a compatible Substrate installation and a gVisor worker pool.
- Authenticated `gcloud`, `kubectl`, Python and the deployment dependencies.
- A private image registry and writable snapshot bucket.
- `kubectl-ate` built from the release/source revision matching the control plane.
- A model endpoint and the credentials or gateway-side model bridge it requires.

Do not install a floating `kubectl-ate` version against a pinned control plane.
Build it from the matching Substrate checkout, for example:

```bash
# In the matching Substrate checkout:
go build -o /path/to/bin/kubectl-ate ./cmd/kubectl-ate
```

Use the installation instructions shipped with that Substrate version. The
integration's `cluster-up` and `substrate-install` commands are optional
installation helpers; review their configuration before running them.

## Configure and deploy

```bash
# From hermes/:
pip install -r deploy/requirements.txt
cp deploy/env.sh.example deploy/env.sh
# Edit the project, cluster, registry, storage and model settings.
deploy/hermesctl preflight
deploy/hermesctl build
deploy/hermesctl deploy
deploy/hermesctl doctor
deploy/hermesctl smoke
```

Check the target context and resource names before each mutating operation.
`deploy/env.sh` is ignored by Git. Never commit rendered credentials or logs.
Preserve the existing `API_SERVER_KEY` and the distinct `GATEWAY_INGRESS_KEY`;
rotating the actor key without replacing warmed actors can break authentication.

The deployment tools render placeholder manifests and pin actor images by digest.
ActorTemplate and Actor operations use `kubectl-ate`, not `kubectl apply` on
ActorTemplate/Actor CRDs. WorkerPool and SandboxConfig remain Kubernetes resources.
Templates are immutable: a new image requires a new template and an explicit
decision about persistent data.

## Installation checks that matter

- Verify image-pull and snapshot permissions for the Substrate components that
  use them, including snapshot garbage collection. Use the roles and scopes
  documented by your installed Substrate version.
- Keep compatible CPU platforms across worker nodes. A snapshot may fail to
  restore on an incompatible CPU.
- Ensure `atelet` is scheduled on every worker node; check node labels and
  tolerations rather than assuming a running worker means restores can succeed.
- Preserve explicit route timeout overrides. A caller timing out does not mean
  the actor stopped executing or that blindly retrying is safe.
- If using the egress MITM path, configure its trust bundle and allow the actor's
  lifecycle callbacks to reach the gateway. See [egress](../egress-plugin/README.md).
- Confirm image warm-up and readiness on the nodes used for the test.

## Validate the runtime

Use a fresh test session and record the image digests and control-plane build.

1. Send a real model turn that writes a known file under `/data` and remembers a
   unique phrase. Require a successful response, not merely a healthy pod.
2. Suspend the actor and confirm `ACTOR_STATE_SUSPENDED` through `kubectl-ate`.
3. Send another turn. Require the exact file contents and phrase after restore.
4. Start background work that lasts longer than the idle timeout. Confirm that
   the working callback holds the actor awake after the HTTP response ends.
5. Confirm completion releases the hold, the actor suspends after its idle
   window, and the next turn can read the background job's output.
6. Confirm generic HTTP ingress rejects requests with no token or with only the
   actor's API key. The trusted adapter must use `GATEWAY_INGRESS_KEY`.

These checks passed on an isolated deployment of the core runtime. They are not
a production-readiness claim or validation of every installer/platform version.

### Routing names

Current gateway actors use `agent-<hash>` derived from the registry routing ID,
or from the conversation key if no binding exists. Multiple registered channels
can share an actor, with separate conversation sessions inside it.

`conv-*` is the older conversation-based naming scheme. Compatibility helpers
and direct demos may still use it; the prefix is not a security issue and does
not need removal. Do not rename existing actors as a substitute for migration.

## Additional demo helpers

The scripts under `demo/` use their own `demo/env.sh`, copied from
`demo/env.sh.example`. Do not assume they read `deploy/env.sh`. Review their
configuration and flags before using them on an existing installation.

| Helper | Purpose |
| --- | --- |
| `demo/validate.sh` | Direct lifecycle exercise through Substrate |
| `demo/deploy_gateway.sh` | Build and deploy the gateway using demo configuration |
| `demo/demo.sh` | Gateway-driven conversation and idle-suspend demonstration |
| `demo/watch.sh` | Runtime state display; its reset action is destructive |
| `demo/telegram_go_live.sh` | Configure a Telegram bot and demo display |
| `demo/whatsapp_go_live.sh` | Configure WhatsApp and a temporary HTTPS tunnel |

For gateway-driven HTTP tests, provide `GATEWAY_INGRESS_KEY`. Direct actor API
tests use `API_SERVER_KEY`. Do not expose either key in terminal recordings.

## Channels and fleet configuration

Telegram polls from the gateway and does not need a public webhook endpoint.
Set its chat allowlist and keep one poller per bot token. The demo helper changes
the idle timeout for demonstration purposes; check it before subsequent tests.

WhatsApp uses a public HTTPS webhook with signature verification. Follow the
[WhatsApp setup guide](../docs/WHATSAPP-SETUP.md). A development tunnel is not a
durable production ingress.

The [fleet guide](fleet/README.md) covers registry generation, schedules and
sizing assumptions. Shape metadata does not automatically select templates.

## Local tests

```bash
python3 tests/selftest.py
python3 -m unittest discover -s tests -p 'test_*.py'
python3 actor/test_entrypoint.py
python3 demo/test_telegram.py
python3 demo/test_whatsapp.py
```

For a failed cluster test, inspect gateway logs, actor state and the platform
events before retrying. Do not delete a stateful actor merely to clear an error.
Use recovery procedures appropriate to the installed Substrate build.
