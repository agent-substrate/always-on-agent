#!/usr/bin/env bash
# Build and deploy the always-on gateway (the presence / cron / idle-suspend layer).
#
#   demo/deploy_gateway.sh                     # stage binary, build, apply, wait
#   demo/deploy_gateway.sh --skip-build        # re-apply the manifest only
#   demo/deploy_gateway.sh --overwrite-agents  # also reset the agents ConfigMap
#
# The hermes-agents ConfigMap in k8s/ is an EXAMPLE with placeholder channel ids. Once you
# have a real one in the cluster, re-applying the manifest would silently replace your
# configured channel bindings and cron jobs with placeholders - so by default this script
# leaves an existing ConfigMap alone and tells you it did.
#
# This script is the ONLY supported way to deploy the gateway, because
# k8s/hermes-gateway.yaml carries a REPLACE_WITH_GATEWAY_IMAGE placeholder rather than
# a hardcoded tag. Three separate docs used to name three different tags; making one
# script own it is how that stops happening.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"
cd "$REPO_DIR"

SKIP_BUILD=0
OVERWRITE_AGENTS=0
for arg in "$@"; do
  case "$arg" in
    --skip-build)       SKIP_BUILD=1 ;;
    --overwrite-agents) OVERWRITE_AGENTS=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

log "0. Preflight"
kubectl get ns "$NS" >/dev/null 2>&1 || {
  echo "ERROR: namespace $NS does not exist. Run demo/validate.sh first, or:"
  echo "  kubectl apply -f k8s/hermes-namespace-rbac.yaml"
  exit 1
}
kubectl get secret hermes-secrets -n "$NS" >/dev/null 2>&1 || {
  echo "ERROR: secret hermes-secrets not found in ns/$NS - see demo/README.md."
  exit 1
}

if (( SKIP_BUILD )); then
  info "--skip-build: reusing $GATEWAY_IMAGE"
else
  log "1. Stage kubectl-ate into the build context"
  # The gateway shells out to kubectl-ate for actor lifecycle, so the binary has to be
  # inside the image. It is NOT committed here: it is Substrate's, it is ~66 MB, and
  # vendoring someone else's binary into a source repo ages badly.
  ATE_BIN="$(command -v "$ATE" || echo "$ATE")"
  [[ -x "$ATE_BIN" ]] || { echo "ERROR: kubectl-ate not executable at $ATE_BIN"; exit 1; }
  if ! file "$ATE_BIN" 2>/dev/null | grep -q 'ELF 64-bit.*x86-64'; then
    echo "WARNING: $ATE_BIN does not look like a linux/amd64 ELF binary."
    echo "         The gateway image is linux/amd64; build one with:"
    echo "           GOOS=linux GOARCH=amd64 go build -o gateway/kubectl-ate ./cmd/kubectl-ate"
  fi
  cp "$ATE_BIN" gateway/kubectl-ate
  info "staged $(du -h gateway/kubectl-ate | cut -f1) -> gateway/kubectl-ate (gitignored)"

  log "2. Build the gateway image"
  gcloud_build --tag "$GATEWAY_IMAGE" gateway/
fi

log "3. Apply the gateway manifest"
KEEP_AGENTS=0
if (( ! OVERWRITE_AGENTS )) && kubectl -n "$NS" get configmap hermes-agents >/dev/null 2>&1; then
  KEEP_AGENTS=1
fi

if (( KEEP_AGENTS )); then
  # Drop the ConfigMap document from the stream rather than applying it.
  render k8s/hermes-gateway.yaml \
    | python3 -c '
import sys, yaml
docs = [d for d in yaml.safe_load_all(sys.stdin)
        if d and not (d.get("kind") == "ConfigMap" and d["metadata"]["name"] == "hermes-agents")]
yaml.safe_dump_all(docs, sys.stdout)' \
    | kubectl apply -f -
  info "kept the existing hermes-agents ConfigMap (--overwrite-agents to replace it)"
else
  render k8s/hermes-gateway.yaml | kubectl apply -f -
  if (( OVERWRITE_AGENTS )); then
    info "hermes-agents ConfigMap REPLACED with the version in k8s/"
  else
    info "hermes-agents ConfigMap created from the example - edit it with your real channel ids"
  fi
fi
info "image: $GATEWAY_IMAGE"

log "4. Wait for rollout"
kubectl -n "$NS" rollout status deploy/hermes-gateway --timeout=180s
kubectl -n "$NS" logs deploy/hermes-gateway --tail=20

log "DONE"
info "smoke test  : demo/demo.sh"
info "channels    : demo/telegram_go_live.sh   (no public endpoint needed)"
info "             docs/WHATSAPP-SETUP.md"
