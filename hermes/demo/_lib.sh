# shellcheck shell=bash
# Shared bootstrap for the demo scripts. Sourced, never executed.
#
# Loads demo/env.sh and fails loudly on anything still left at its placeholder value,
# because the alternative is a `kubectl apply` that succeeds and an ActorTemplate that
# sits Pending for fifteen minutes with the reason buried in a controller log.

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$REPO_DIR/demo/env.sh}"

if [[ ! -f "$ENV_FILE" ]]; then
  cat >&2 <<EOF
ERROR: $ENV_FILE not found.

  cp demo/env.sh.example demo/env.sh
  \$EDITOR demo/env.sh

It holds the handful of values that are specific to your project: registry,
snapshot bucket, and the ateom worker image from your Substrate install.
EOF
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"


_missing=()
for _var in PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION REGISTRY ACTOR_IMAGE GATEWAY_IMAGE \
            SNAPSHOT_PREFIX ATEOM_IMAGE ATE; do
  _val="${!_var:-}"
  if [[ -z "$_val" || "$_val" == *REPLACE* || "$_val" == *your-project-id* || "$_val" == *your-substrate-cluster* ]]; then
    _missing+=("$_var=${_val:-<unset>}")
  fi
done
if (( ${#_missing[@]} )); then
  printf 'ERROR: still at placeholder values in %s:\n' "$ENV_FILE" >&2
  printf '  %s\n' "${_missing[@]}" >&2
  echo >&2
  echo "See the comments in demo/env.sh.example for how to find each one." >&2
  exit 1
fi

if [[ ! -x "$ATE" ]] && ! command -v "$ATE" >/dev/null 2>&1; then
  echo "ERROR: kubectl-ate not found at '$ATE'." >&2
  echo "  go install github.com/agent-substrate/substrate/cmd/kubectl-ate@latest" >&2
  echo "  then set ATE in demo/env.sh" >&2
  exit 1
fi

# --- pin every command to the cluster named in demo/env.sh -------------------
# env.sh names a cluster; kubectl has its own independent idea of the "current
# context". Nothing reconciles the two, so a stale context quietly deploys this
# PoC into whatever cluster you last touched - and the first sign of it is an
# ActorTemplate appearing somewhere you did not expect. Resolve the context once
# here, then route both kubectl and kubectl-ate through it.
#
# Set KUBECTL_CONTEXT in demo/env.sh if you renamed your contexts.
if [[ -z "${KUBECTL_CONTEXT:-}" ]]; then
  _derived="gke_${PROJECT_ID}_${CLUSTER_LOCATION}_${CLUSTER_NAME}"
  _current="$(command kubectl config current-context 2>/dev/null || true)"
  if command kubectl config get-contexts -o name 2>/dev/null | grep -qxF "$_derived"; then
    KUBECTL_CONTEXT="$_derived"
  elif [[ -n "$_current" && "$_current" == *"$CLUSTER_NAME"* ]]; then
    # Renamed context, but it still names the right cluster. Say so out loud.
    KUBECTL_CONTEXT="$_current"
  else
    cat >&2 <<EOF
ERROR: no kubectl context found for cluster '$CLUSTER_NAME'.

Looked for '$_derived' and found no match; the current context is
'${_current:-<none>}', which does not name that cluster either. Refusing to
guess, because the wrong guess deploys into the wrong cluster.

Fix by fetching credentials:

  gcloud container clusters get-credentials "$CLUSTER_NAME" \\
    --location "$CLUSTER_LOCATION" --project "$PROJECT_ID"

or, if your contexts are named differently, set KUBECTL_CONTEXT in demo/env.sh.
EOF
    exit 1
  fi
fi
export KUBECTL_CONTEXT

# Wrappers, so no call site has to remember the flag. `command` avoids recursing
# back into these functions. Both tools take --context as a global flag.
#
# One caveat, because it bites silently: do NOT background these wrappers. `kubectl
# ... &` on a *function* forks a bash subshell, so $! is the subshell's pid, not
# kubectl's - kill it and the real kubectl is orphaned, which for a port-forward
# means a listener that outlives the script. Background the binary directly instead:
#
#   command kubectl --context="$KUBECTL_CONTEXT" -n foo port-forward svc/bar 8000:80 &
#
# There $! is kubectl itself and the usual `trap 'kill $PID' EXIT` works.
kubectl() { command kubectl --context="$KUBECTL_CONTEXT" "$@"; }
ate()     { command "$ATE"  --context="$KUBECTL_CONTEXT" "$@"; }

NS="${NS:-hermes}"
ATESPACE="${ATESPACE:-hermes-demo}"
MODEL="${MODEL:-gemini-flash-latest}"

log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }

# Submit a Cloud Build, and believe the build's recorded status rather than the
# client's exit code.
#
# `gcloud builds submit` exits 1 when it cannot STREAM the build log - a VPC-SC
# perimeter around Cloud Logging is enough to do it - even though the build ran
# to completion and pushed the image. Under `set -e` that aborts the script at
# the build step while a perfectly good image sits in the registry, and the only
# clue is a permissions notice in output you have already lost. So: capture the
# output, and on a non-zero exit ask Cloud Build what actually happened.
#
# Args are passed straight through to `gcloud builds submit`.
gcloud_build() {
  local blog bid="" status=""
  blog="$(mktemp -t hermes-build-XXXXXX.log)"

  if gcloud builds submit --project="$PROJECT_ID" "$@" 2>&1 | tee "$blog"; then
    rm -f "$blog"
    return 0
  fi

  bid="$(grep -oE '[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}' "$blog" | head -1)"
  if [[ -n "$bid" ]]; then
    status="$(gcloud builds describe "$bid" --project="$PROJECT_ID" \
                --format='value(status)' 2>/dev/null || true)"
  fi

  if [[ "$status" == "SUCCESS" ]]; then
    info "gcloud exited non-zero, but build $bid is SUCCESS - it could not stream the"
    info "log, not build the image. Continuing."
    rm -f "$blog"
    return 0
  fi

  echo "ERROR: Cloud Build failed (id=${bid:-unknown} status=${status:-unknown})." >&2
  echo "       full output: $blog" >&2
  return 1
}

# sed replacement text is not literal: the delimiter, & and backslash all mean
# something, credentials routinely contain all three, and the failure is silent
# corruption rather than an error.
sed_escape() { printf '%s' "$1" | sed -e 's/[#&\\]/\\&/g'; }

# Render a manifest with the environment-specific values substituted in. The
# manifests carry REPLACE_WITH_* placeholders on purpose: a manifest that is valid
# YAML but points at someone else's private registry is worse than one that refuses
# to apply.
#
# ORDER MATTERS for the three model placeholders. sed has no word boundary here,
# so substituting REPLACE_WITH_MODEL first rewrites the front of
# REPLACE_WITH_MODEL_PROVIDER and REPLACE_WITH_MODEL_BASE_URL and leaves
# "gpt-5.6-luna_PROVIDER" behind. Longest first, always.
#
# This renderer handles core manifests only.
render() {
  sed -e "s#REPLACE_WITH_ACTOR_IMAGE#${ACTOR_IMAGE_PINNED:-$ACTOR_IMAGE}#g" \
      -e "s#REPLACE_WITH_GATEWAY_IMAGE#${GATEWAY_IMAGE}#g" \
      -e "s#REPLACE_WITH_ATEOM_IMAGE#${ATEOM_IMAGE}#g" \
      -e "s#REPLACE_WITH_SNAPSHOT_PREFIX#${SNAPSHOT_PREFIX}#g" \
      -e "s#REPLACE_WITH_ATESPACE#${ATESPACE}#g" \
      -e "s#REPLACE_WITH_MODEL_PROVIDER#${MODEL_PROVIDER:-custom}#g" \
      -e "s#REPLACE_WITH_MODEL_BASE_URL#${MODEL_BASE_URL:-https://api.openai.com/v1}#g" \
      -e "s#REPLACE_WITH_MODEL#${MODEL}#g" \
      -e "s#REPLACE_WITH_ACTOR_MODEL_KEY#$(sed_escape "${ACTOR_MODEL_KEY:-}")#g" \
      -e "s#REPLACE_WITH_API_SERVER_KEY#$(sed_escape "${API_KEY:-}")#g" \
      -e "s#REPLACE_WITH_PROJECT#${VERTEX_PROJECT:-$PROJECT_ID}#g" \
      -e "s#REPLACE_WITH_VERTEX_SERVICE_ACCOUNT#${VERTEX_SERVICE_ACCOUNT:-}#g" \
      -e "s#REPLACE_WITH_VERTEX_PROXY_ENABLED#${VERTEX_PROXY_ENABLED:-0}#g" \
      -e "s#REPLACE_WITH_MODEL_PROXY_BACKEND#${MODEL_PROXY_BACKEND:-vertex}#g" \
      -e "s#REPLACE_WITH_VERTEX_LOCATION#${VERTEX_LOCATION:-global}#g" \
      -e "s#REPLACE_WITH_VERTEX_MODEL#${VERTEX_MODEL:-google/gemini-3.1-pro-preview}#g" \
      -e "s#REPLACE_WITH_OPENAI_MODEL#${OPENAI_MODEL:-gpt-5.6-luna}#g" \
      "$1"
}

# Refuses to hand on a manifest that still has a placeholder in it. Comments are
# skipped: these files document their own placeholders in a header comment, and
# matching that blocks a perfectly good manifest.
assert_rendered() {
  local out="$1" name="$2" leftover
  leftover=$(grep -v '^[[:space:]]*#' <<<"$out" | grep -o "REPLACE_WITH_[A-Z][A-Z_]*" | sort -u || true)
  [[ -z "$leftover" ]] && return 0
  echo "ERROR: ${name} still has unsubstituted placeholders:" >&2
  sed 's/^/       /' <<<"$leftover" >&2
  return 1
}
