#!/usr/bin/env bash
# End-to-end lifecycle validation for Hermes-on-Substrate on a live GKE cluster.
#
# Proves, on real infra:
#   1. Provision        - ActorTemplate warms a golden snapshot -> Ready
#   2. Resume-on-demand - an inbound /v1/chat/completions through atenet cold-restores the actor
#   3. State continuity - a follow-up in the same session remembers prior context
#   4. Suspend          - actor goes SUSPENDED, its worker is freed (zero idle cost)
#   5. State survival   - after suspend, a new turn resumes and the earlier context is still there
#
# Prereqs (demo/README.md has the full list with commands):
#   * Substrate installed in the cluster (ate-system running, gvisor SandboxConfig present)
#   * a CPU-homogeneous worker node pool labelled substrate.ate.dev/worker=true
#   * kubectl-ate installed, gcloud authed to $PROJECT_ID
#   * demo/env.sh filled in   (cp demo/env.sh.example demo/env.sh)
#   * the hermes-secrets Secret applied (MODEL_API_KEY + API_SERVER_KEY)
#
# Flags:
#   --skip-build    reuse the actor image already in the registry
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"
cd "$REPO_DIR"

SKIP_BUILD=0
[[ "${1:-}" == "--skip-build" ]] && SKIP_BUILD=1

ACTOR=conv-demo1
# Bare name, not "hermes/hermes". --template-ref resolves in the actor's own
# atespace, and a slashed reference is rejected as an invalid resource name.
TEMPLATE=hermes
SESSION_ID=demo1

# The two assertions that ARE the claim. Collected rather than fail-fast, so one bad
# turn still leaves you with the actor table and metrics below it to diagnose from -
# but the script must exit non-zero, or "it printed FAIL and returned 0" quietly
# becomes "the PoC passed" in someone's CI.
FAILURES=()
PF_PORT="${PF_PORT:-8000}"

# ---------------------------------------------------------------------------
log "0. Preflight"
kubectl get ns ate-system >/dev/null 2>&1 || {
  echo "ERROR: no ate-system namespace - Substrate is not installed in this cluster."
  echo "       See demo/README.md > Prerequisites."
  exit 1
}
kubectl get nodes -l substrate.ate.dev/worker=true -o name | grep -q . || {
  echo "ERROR: no nodes labelled substrate.ate.dev/worker=true."
  echo "       The worker pool must be a SINGLE-CPU-PLATFORM machine family (c2d/c2/c4);"
  echo "       see the comment at the top of k8s/hermes-workerpool.yaml."
  exit 1
}
kubectl apply -f k8s/hermes-namespace-rbac.yaml >/dev/null
kubectl get secret hermes-secrets -n "$NS" >/dev/null 2>&1 || {
  echo "ERROR: secret hermes-secrets not found in ns/$NS. Either:"
  echo "  kubectl create secret generic hermes-secrets -n $NS \\"
  echo "    --from-literal=MODEL_API_KEY=<provider-key> \\"
  echo "    --from-literal=API_SERVER_KEY=\$(openssl rand -hex 24)"
  echo "or copy k8s/hermes-secret.example.yaml to hermes-secret.yaml, fill it in, and apply it."
  exit 1
}
API_KEY="$(kubectl get secret hermes-secrets -n "$NS" -o jsonpath='{.data.API_SERVER_KEY}' | base64 -d)"
[[ ${#API_KEY} -ge 16 ]] || { echo "ERROR: API_SERVER_KEY is ${#API_KEY} chars; Hermes needs >= 16."; exit 1; }
ACTOR_MODEL_KEY="$(kubectl get secret hermes-secrets -n "$NS" -o jsonpath='{.data.MODEL_API_KEY}' | base64 -d)"
[[ -n "$ACTOR_MODEL_KEY" ]] || {
  echo "ERROR: hermes-secrets has no MODEL_API_KEY, and the actor cannot call a model without it."
  exit 1
}
# The template used to reach the key with env.valueFrom.secretKeyRef. Substrate
# removed secretKeyRef from this API, so the key is now rendered INTO the
# template, which means anyone who can read actor templates can read it. That is
# a real exposure and it is accepted for the validation pass only; the way out is
# an in-cluster proxy holding the key, not a Secret.
info "MODEL_API_KEY is rendered into the actor template in plaintext (no secretKeyRef in the new API)"

# MODEL used to be read back out of the template file. It cannot be any more:
# that field now holds REPLACE_WITH_MODEL until render() runs, so reading it here
# put the literal placeholder in every chat request and every turn 400'd.
MODEL="${MODEL:?MODEL must be set in demo/env.sh}"
info "project=$PROJECT_ID  model=$MODEL  api_key=<${#API_KEY} chars>"

# ---------------------------------------------------------------------------
log "1. Build the actor image"
if (( SKIP_BUILD )); then
  info "--skip-build: reusing $ACTOR_IMAGE"
else
  gcloud_build --config=actor/cloudbuild.yaml \
    --substitutions=_ACTOR_IMAGE="$ACTOR_IMAGE" actor/
fi
ACTOR_IMAGE_PINNED="$(gcloud container images describe "$ACTOR_IMAGE" --project="$PROJECT_ID" \
                       --format='value(image_summary.fully_qualified_digest)')"
[[ -n "$ACTOR_IMAGE_PINNED" ]] || { echo "ERROR: could not resolve a digest for $ACTOR_IMAGE"; exit 1; }
info "pinned image: $ACTOR_IMAGE_PINNED"

# ---------------------------------------------------------------------------
log "2. Provision - apply manifests + warm the golden snapshot"
render k8s/hermes-workerpool.yaml | kubectl apply -f -

# The atespace has to exist BEFORE the template, because the template names it
# and `create actor-template` resolves it rather than creating it.
ate create atespace "$ATESPACE" 2>/dev/null || info "atespace exists"

# The ActorTemplate CRD was deleted upstream on 2026-09-02. `kubectl apply` on
# this file does nothing useful now, so it goes through the Substrate API, and
# the template is immutable: there is no update, only delete and recreate.
TEMPLATE_MANIFEST="$(render k8s/hermes-actortemplate.yaml)"
assert_rendered "$TEMPLATE_MANIFEST" k8s/hermes-actortemplate.yaml
if ate get actor-template hermes -a "$ATESPACE" >/dev/null 2>&1; then
  info "actor template exists; reusing it (it is immutable, delete it to rebuild)"
else
  printf '%s\n' "$TEMPLATE_MANIFEST" | ate create actor-template -f -
fi

# There is no `kubectl wait` for a template any more: no CRD, so no conditions.
# Readiness is goldenSnapshotStatus.goldenSnapshot becoming set, and
# errorMessage is what turns a 900-second silent timeout into an answer.
info "waiting for the golden snapshot (a Python cold start takes minutes)..."
golden=""
deadline=$((SECONDS + 900))
while [[ $SECONDS -lt $deadline ]]; do
  read -r verdict detail <<<"$(ate get actor-template hermes -a "$ATESPACE" -o json 2>/dev/null \
    | python3 -c 'import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print("pending"); sys.exit(0)
ts = d.get("actorTemplates") or ([d] if d.get("metadata") else [])
if not ts:
    print("pending"); sys.exit(0)
g = (ts[0].get("status") or {}).get("goldenSnapshotStatus") or {}
if g.get("errorMessage"):
    print("error", g["errorMessage"])
elif (g.get("goldenSnapshot") or {}).get("snapshotUri"):
    print("ready", g["goldenSnapshot"]["snapshotUri"])
elif (g.get("goldenTag") or {}).get("name"):
    print("ready", "tag:" + g["goldenTag"]["name"])
else:
    print("pending")' || echo pending)"
  case "$verdict" in
    ready) golden="$detail"; break ;;
    error) echo "ERROR: golden snapshot failed: $detail"; exit 1 ;;
  esac
  sleep 5
done
[[ -n "$golden" ]] || {
  echo "ERROR: golden snapshot not ready after 900s."
  echo "       kubectl logs -n ate-system deployment/ate-api-server | grep -i golden"
  echo "       A worker with no reported capacity is unschedulable while still showing FREE."
  exit 1
}
info "golden Ready: $golden"

# ---------------------------------------------------------------------------
log "3. Create the actor, open the atenet route"
# Bare name: --template resolves a bare name in the actor's own atespace.
ate create actor "$ACTOR" -a "$ATESPACE" --template "$TEMPLATE" 2>/dev/null || info "actor exists"
# Substrate v1 denies an actor's outbound traffic until it has an egress policy.
# The gateway creates one for every agent it provisions; this script creates its
# actor directly, so it does the same: any host over HTTP and HTTPS.
ate create egress-policy "$ACTOR" -a "$ATESPACE" -f - <<'POLICY' 2>/dev/null || info "egress policy exists"
rules:
- http: {hostnames: ["*"], ports: {all: {}}}
- https: {hostnames: ["*"]}
POLICY
ate get actors -a "$ATESPACE"

# kubectl ate proxy reaches the actor through atenet-router with its own pod
# identity certificate (v1 ingress is mTLS-only), so plain HTTP to localhost works.
# NB: the binary, not the ate() wrapper - see _lib.sh. Backgrounding a function
# would make $! a subshell pid and orphan the proxy on exit.
command "$ATE" --context="$KUBECTL_CONTEXT" proxy actor "$ACTOR" "${PF_PORT}:80" -a "$ATESPACE" >/tmp/hermes-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT
sleep 3

turn() { # $1 = user message -> prints the raw JSON response
  curl -sS --max-time 300 "localhost:${PF_PORT}/v1/chat/completions" \
    -H "Authorization: Bearer ${API_KEY}" \
    -H "X-Hermes-Session-Id: ${SESSION_ID}" \
    -H 'Content-Type: application/json' \
    -d "{\"model\":\"${MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"$1\"}]}"
}
reply_of() { python3 -c 'import sys,json
try: print(json.load(sys.stdin)["choices"][0]["message"]["content"].strip())
except Exception as e: print("<no reply: %s>" % e)'; }

# ---------------------------------------------------------------------------
log "4. Turn 1 - cold resume + real LLM answer"
t0=$(date +%s)
turn "Please remember this: my name is Maya. Reply with just 'noted'." | tee /tmp/hermes-t1.json | reply_of
t1=$(date +%s); info "turn 1 (incl. cold restore) took $((t1-t0))s"
ate get actors -a "$ATESPACE"

log "5. Turn 2 - state continuity within the live session"
a2="$(turn "What is my name? Answer with only the name." | tee /tmp/hermes-t2.json | reply_of)"
echo "   $a2"
case "$a2" in *[Mm]aya*) info "PASS - remembered within the live session." ;;
              *) info "FAIL - expected 'Maya'."; FAILURES+=("turn 2: in-session continuity") ;; esac

log "6. Suspend - free the worker (zero idle cost)"
ate suspend actor "$ACTOR" -a "$ATESPACE"
sleep 5
ate get actors -a "$ATESPACE"

log "7. Turn 3 - resume-on-demand + STATE SURVIVED suspend"
t2=$(date +%s)
a3="$(turn "What is my name? Answer with only the name." | tee /tmp/hermes-t3.json | reply_of)"
t3=$(date +%s)
echo "   $a3"
info "turn 3 (cold restore from suspend) took $((t3-t2))s"
case "$a3" in *[Mm]aya*) info "PASS - memory survived suspend -> worker freed -> resume." ;;
              *) info "FAIL - expected 'Maya' after resume."; FAILURES+=("turn 3: state survival across suspend") ;; esac
ate get actors -a "$ATESPACE"

# ---------------------------------------------------------------------------
log "8. Metrics"
info "golden snapshot: ${golden}"
# `gcloud storage`, not `gsutil`: gsutil carries its own credential path and reports
# "Your credentials are invalid" on a machine where gcloud itself is perfectly authed,
# which used to make this line vanish into 2>/dev/null and the metric never print.
gcloud storage du -s --readable-sizes "$SNAPSHOT_PREFIX" --project="$PROJECT_ID" 2>/dev/null \
  | sed 's/^/   snapshot bucket usage: /' || true

log "DONE - turns 2 and 3 must both answer 'Maya' (continuity + survival)."
info "actor logs : $ATE --context=$KUBECTL_CONTEXT logs actor $ACTOR -a $ATESPACE"
info "cleanup    : $ATE --context=$KUBECTL_CONTEXT delete actor $ACTOR -a $ATESPACE"

if (( ${#FAILURES[@]} )); then
  echo >&2
  printf '\033[1;31mFAILED (%d):\033[0m\n' "${#FAILURES[@]}" >&2
  printf '  - %s\n' "${FAILURES[@]}" >&2
  cat >&2 <<'EOF'

If the actor is sitting in ACTOR_STATE_RESUMING, this is most likely the atenet
resume-on-demand race, not a broken PoC: atenet can give up on the request before a
cold restore finishes, and cancels the restore with it. Resume explicitly and retry:

  kubectl-ate --context="$KUBECTL_CONTEXT" resume actor conv-demo1 -a hermes-demo

An actor already wedged in ACTOR_STATE_RESUMING will not clear on its own: delete and suspend
are refused, and resume does not recover it. Delete the ateom pod named in the ATEOM POD
column to drop it to ACTOR_STATE_CRASHED, then delete it.
demo/README.md > Troubleshooting has both.
EOF
  exit 1
fi
