#!/usr/bin/env bash
# Deploy the WhatsApp demo: OpenClaw on Agent Substrate.
#
# Reproducible from a clean checkout. It (optionally) builds the two images with
# Cloud Build, pins them by digest, then deploys the WorkerPool, ActorTemplate,
# gateway, and one demo actor. See README.md for the full walkthrough.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

# --- Config (override via env) ---
PROJECT_ID="${PROJECT_ID:-}"                       # required: GCP project for gcr.io images
GCS_BUCKET="${GCS_BUCKET:-}"                        # required: bucket for golden snapshots (no gs:// prefix)
GEMINI_API_KEY="${GEMINI_API_KEY:-}"               # required: LLM provider key (prompted if unset)
IMAGE_TAG="${IMAGE_TAG:-demo}"                      # tag the images are built/pushed under
BUILD_IMAGES="${BUILD_IMAGES:-auto}"               # auto | true | false
SUBSTRATE_REPO="${SUBSTRATE_REPO:-}"               # optional: path to a substrate clone to auto-install
NAMESPACE="openclaw"
ATESPACE="openclaw-demo"
ACTOR_NAME="oc-agent"
TEMPLATE="openclaw-agent"                           # ActorTemplate name (namespace/name = openclaw/openclaw-agent)
GATEWAY_IMAGE="gcr.io/${PROJECT_ID}/openclaw-gateway"
ACTOR_IMAGE="gcr.io/${PROJECT_ID}/openclaw-actor"

echo "=== OpenClaw on Substrate: WhatsApp Demo ==="
echo ""

die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" &>/dev/null || die "$1 not found. Install it first."; }

# --- [0/8] Prerequisites ---
echo "[0/8] Checking prerequisites..."
need kubectl
need gcloud
[ -n "$PROJECT_ID" ] || die "Set PROJECT_ID (your GCP project). e.g. export PROJECT_ID=my-project"
[ -n "$GCS_BUCKET" ] || die "Set GCS_BUCKET (snapshot bucket, no gs:// prefix). e.g. export GCS_BUCKET=my-bucket"

if ! kubectl get namespace ate-system &>/dev/null; then
  if [ -n "$SUBSTRATE_REPO" ] && [ -f "$SUBSTRATE_REPO/hack/install-ate.sh" ]; then
    echo "    Substrate not found; installing from $SUBSTRATE_REPO ..."
    (cd "$SUBSTRATE_REPO" && bash hack/install-ate.sh --deploy-ate-system)
  else
    die "Agent Substrate is not installed (no ate-system namespace).
  Install it first from a clone of github.com/agent-substrate/substrate:
    cd substrate && hack/install-ate.sh --deploy-ate-system
  Or set SUBSTRATE_REPO=/path/to/substrate to have this script install it."
  fi
fi
kubectl -n ate-system get deployment ate-api-server &>/dev/null \
  || die "ate-api-server not found in ate-system — Substrate may be partially installed."
if ! command -v kubectl-ate &>/dev/null && ! kubectl ate version &>/dev/null 2>&1; then
  die "kubectl-ate CLI not found. Build it from the Substrate repo (make build-atectl) and put bin/kubectl-ate on your PATH."
fi
gcloud storage ls "gs://${GCS_BUCKET}" &>/dev/null \
  || die "Bucket gs://${GCS_BUCKET} not reachable. Create it:  gcloud storage buckets create gs://${GCS_BUCKET} --project ${PROJECT_ID}"
echo "    OK: kubectl, gcloud, Substrate, kubectl-ate, bucket."

if [ -z "$GEMINI_API_KEY" ]; then
  read -rsp "Enter your Gemini API key: " GEMINI_API_KEY; echo ""
fi
GATEWAY_TOKEN="${OPENCLAW_GATEWAY_TOKEN:-$(openssl rand -hex 32)}"

# --- [1/8] Build images (optional) ---
image_exists() { gcloud container images describe "$1:$IMAGE_TAG" &>/dev/null; }
if [ "$BUILD_IMAGES" = "true" ] || { [ "$BUILD_IMAGES" = "auto" ] && ! image_exists "$GATEWAY_IMAGE"; }; then
  echo "[1/8] Building images with Cloud Build (tag: $IMAGE_TAG)..."
  ( cd "$PARENT_DIR" && \
    sed "s|REPLACE_WITH_YOUR_PROJECT|$PROJECT_ID|g; s|:demo\"|:$IMAGE_TAG\"|g" cloudbuild-gateway.yaml >/tmp/cb-gw.yaml && \
    gcloud builds submit --project "$PROJECT_ID" --config /tmp/cb-gw.yaml . )
  ( cd "$PARENT_DIR" && \
    sed "s|REPLACE_WITH_YOUR_PROJECT|$PROJECT_ID|g; s|:demo\"|:$IMAGE_TAG\"|g" cloudbuild-actor.yaml >/tmp/cb-actor.yaml && \
    gcloud builds submit --project "$PROJECT_ID" --config /tmp/cb-actor.yaml . )
else
  echo "[1/8] Skipping build (images present; set BUILD_IMAGES=true to force)."
fi

# --- [2/8] Resolve image digests (snapshots require @sha256-pinned images) ---
echo "[2/8] Resolving image digests..."
digest_of() {
  gcloud container images describe "$1:$IMAGE_TAG" \
    --format='value(image_summary.digest)' 2>/dev/null \
    || die "Cannot resolve digest for $1:$IMAGE_TAG — build it first (BUILD_IMAGES=true)."
}
GATEWAY_DIGEST="$(digest_of "$GATEWAY_IMAGE")"
ACTOR_DIGEST="$(digest_of "$ACTOR_IMAGE")"
echo "    gateway @ ${GATEWAY_DIGEST}"
echo "    actor   @ ${ACTOR_DIGEST}"

# render: substitute project, bucket, and digests into a manifest, print to stdout.
render() {
  sed -e "s|REPLACE_WITH_YOUR_PROJECT|$PROJECT_ID|g" \
      -e "s|REPLACE_WITH_YOUR_BUCKET|$GCS_BUCKET|g" \
      -e "s|REPLACE_WITH_GATEWAY_DIGEST|$GATEWAY_DIGEST|g" \
      -e "s|REPLACE_WITH_ACTOR_DIGEST|$ACTOR_DIGEST|g" "$1"
}

# --- [3/8] Namespace + secrets ---
echo "[3/8] Namespace + secrets..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic openclaw-secrets \
  --from-literal="gateway-token=$GATEWAY_TOKEN" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic openclaw-api-keys \
  --from-literal="GEMINI_API_KEY=$GEMINI_API_KEY" --dry-run=client -o yaml | kubectl apply -f -

# --- [4/8] Substrate resources ---
echo "[4/8] Applying WorkerPool + ActorTemplate..."
render "$PARENT_DIR/openclaw-workerpool.yaml"   | kubectl apply -f -
render "$PARENT_DIR/openclaw-actortemplate.yaml" | kubectl apply -f -

# --- [5/8] Config maps ---
echo "[5/8] Applying config maps..."
kubectl apply -f "$SCRIPT_DIR/openclaw-demo-config.yaml"
kubectl apply -f "$SCRIPT_DIR/openclaw-actor-config.yaml"

# --- [6/8] Gateway ---
echo "[6/8] Deploying gateway..."
render "$PARENT_DIR/openclaw-gateway.yaml" | kubectl apply -f -
kubectl -n "$NAMESPACE" rollout status deployment/openclaw-gateway --timeout=180s

# --- [7/8] Golden + actor ---
echo "[7/8] Waiting for golden snapshot, then creating the demo actor..."
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Ready \
  actortemplate.ate.dev/"$TEMPLATE" --timeout=420s 2>/dev/null \
  || echo "    (golden not Ready yet — the actor will resume once it is)"
kubectl ate create atespace "$ATESPACE" 2>/dev/null || echo "    atespace exists, continuing..."
kubectl ate create actor "$ACTOR_NAME" --template "$NAMESPACE/$TEMPLATE" --atespace "$ATESPACE" 2>/dev/null \
  || echo "    actor exists, continuing..."

# --- [7b] Optional cron status pings (run on the always-on gateway agent) ---
WHATSAPP_PEER="${WHATSAPP_PEER:-}"
if [ -n "$WHATSAPP_PEER" ]; then
  echo "    Creating cron status pings to $WHATSAPP_PEER ..."
  GW_POD=$(kubectl -n "$NAMESPACE" get pod -l app=openclaw-gateway -o jsonpath='{.items[0].metadata.name}')
  ocaw() { kubectl -n "$NAMESPACE" exec "$GW_POD" -c gateway -- node /app/dist/index.js "$@"; }
  ocaw cron add --name status-30m --every 30m --agent main \
    --model google/gemini-3-flash-preview --light-context \
    --channel whatsapp --to "$WHATSAPP_PEER" \
    --message "Reply with a one-line system status." --announce --expect-final 2>/dev/null \
    || echo "    status-30m may already exist, continuing..."
fi

# --- [8/8] Instructions ---
echo ""
echo "[8/8] Done. Next steps:"
echo ""
echo "  1. Port-forward the gateway Control UI:"
echo "       kubectl -n $NAMESPACE port-forward svc/openclaw-gateway 18789:18789"
echo "  2. Open http://localhost:18789  (login token below)"
echo "       $GATEWAY_TOKEN"
echo "  3. In the Control UI, scan the WhatsApp QR code:"
echo "       WhatsApp > Settings > Linked Devices > Link a Device"
echo "  4. Message the linked account. Flow:"
echo "       WhatsApp -> gateway -> Substrate actor (auto-resumes) -> Gemini -> reply"
echo "  5. Watch the lifecycle:"
echo "       watch kubectl ate get actors -A"
echo "       kubectl ate logs actor $ACTOR_NAME --atespace $ATESPACE -f"
echo "     After ~10s idle the actor SUSPENDS; the next message RESUMES it."
echo ""
echo "  Cleanup:"
echo "       kubectl ate delete actor $ACTOR_NAME --atespace $ATESPACE"
echo "       kubectl delete namespace $NAMESPACE"
