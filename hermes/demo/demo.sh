#!/usr/bin/env bash
# Self-driving demo of Hermes-on-Substrate - the "zero manual steps" story, end to end.
#
# Unlike validate.sh (which drives atenet by hand: create actor, suspend actor), this
# demo talks ONLY to the always-on gateway. Everything else happens on its own:
#
#   fresh conversation ─POST─▶ gateway ─▶ auto-provision actor
#                                       └▶ atenet resume-on-demand ─▶ real Gemini answer
#   ... idle ...                        └▶ idle-suspender frees the worker (zero idle cost)
#   next message  ─POST─▶ gateway ─▶ atenet auto-resume ─▶ earlier memory still there
#
# It narrates each step and, alongside, shows the actor's own lifecycle
# (none → RUNNING → SUSPENDED → RUNNING) via kubectl-ate so you can see the worker
# actually being freed and restored - no hand-run create/suspend anywhere.
#
# Prereqs: demo/env.sh filled in, the gateway deployed (demo/deploy_gateway.sh), and
#          hermes-secrets holding MODEL_API_KEY + API_SERVER_KEY.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"

GW_DEPLOY=deploy/hermes-gateway
GW_PORT="${GW_PORT:-8080}"
: "${GATEWAY_INGRESS_KEY:?Set the gateway-only ingress key before running this demo}"
# A fresh conversation each run so we always start from "actor does not exist yet".
SESSION_ID="${SESSION_ID:-demo-$(date +%s)}"
# The gateway maps session -> conv-<sha256(session)[:12]>  (see gateway/actor_router.py).
ACTOR="conv-$(printf '%s' "$SESSION_ID" | sha256sum | cut -c1-12)"
# Idle-suspend fires at IDLE_TIMEOUT_SECONDS (gateway default 60) + up to one 5s poll.
IDLE_WAIT="${IDLE_WAIT:-80}"

bold()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
info()  { printf '   \033[2m%s\033[0m\n' "$*"; }
say()   { printf '   %s\n' "$*"; }
hr()    { printf '   \033[2m%s\033[0m\n' "----------------------------------------------------------------"; }

actor_state() { # prints STATUS + assigned worker pod, or "(actor does not exist yet)"
  local line
  line="$(ate get actors -a "$ATESPACE" 2>/dev/null | grep -E "(^| )${ACTOR}( |$)" || true)"
  if [[ -z "$line" ]]; then
    say "actor ${ACTOR}: (does not exist yet)"
  else
    say "actor ${ACTOR}: $(printf '%s' "$line" | awk '{$1=""; print}' | sed 's/^ *//')"
  fi
}

reply_of() { # extract assistant text from an OpenAI-compatible response body
  python3 -c 'import sys,json;
try:
    d=json.load(sys.stdin); print(d["choices"][0]["message"]["content"].strip())
except Exception as e:
    print("<no reply: %s>" % e)'
}

# ---------------------------------------------------------------------------
bold "0. Preflight - gateway must be up"
kubectl -n "$NS" get "$GW_DEPLOY" >/dev/null 2>&1 || {
  echo "ERROR: $GW_DEPLOY not found in ns/$NS. Deploy it first: demo/deploy_gateway.sh"
  exit 1
}
kubectl -n "$NS" rollout status "$GW_DEPLOY" --timeout=120s
info "session id : $SESSION_ID   (fresh conversation)"
info "maps to     : $ACTOR"
info "model       : $MODEL"

# Port-forward straight to the gateway Service surface.
# NB: the binary, not the kubectl() wrapper - see _lib.sh. Backgrounding a function
# would make $! a subshell pid and orphan the port-forward on exit.
command kubectl --context="$KUBECTL_CONTEXT" -n "$NS" port-forward "$GW_DEPLOY" "${GW_PORT}:8080" >/tmp/hermes-demo-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT
sleep 3
curl -fsS "localhost:${GW_PORT}/health" >/dev/null && info "gateway /health OK"

turn() { # $1 = user message  -> prints reply, returns via global REPLY_RAW
  REPLY_RAW="$(curl -sS --max-time 300 "localhost:${GW_PORT}/v1/chat/completions" \
    -H "X-Hermes-Session-Id: ${SESSION_ID}" \
    -H "Authorization: Bearer ${GATEWAY_INGRESS_KEY}" \
    -H 'Content-Type: application/json' \
    -d "{\"model\":\"${MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"$1\"}]}")"
  printf '%s' "$REPLY_RAW" | reply_of
}

# ---------------------------------------------------------------------------
bold "1. Before we start - the actor does not exist"
actor_state
info "No worker is allocated. Nothing is running. Cost = 0."

# ---------------------------------------------------------------------------
bold "2. First message - gateway auto-provisions + resumes on demand"
say "you  ▶ Please remember this: my name is Maya. Reply with just 'noted'."
t0=$(date +%s)
ans="$(turn 'Please remember this: my name is Maya. Reply with just: noted')"
t1=$(date +%s)
say "hermes ◀ $ans"
info "took $((t1-t0))s  (auto-provision + cold-restore from golden + first Gemini call)"
hr; actor_state
info "The gateway created the actor and atenet resumed it - no manual 'create' step."

# ---------------------------------------------------------------------------
bold "3. Follow-up - in-session continuity (warm)"
say "you  ▶ What is my name? Answer with only the name."
t0=$(date +%s)
ans="$(turn 'What is my name? Answer with only the name.')"
t1=$(date +%s)
say "hermes ◀ $ans"
info "took $((t1-t0))s  (warm - actor already running)"
case "$ans" in *[Mm]aya*) info "PASS - remembered within the live session." ;; *) info "UNEXPECTED - did not see 'Maya'." ;; esac

# ---------------------------------------------------------------------------
bold "4. Go idle - the gateway suspends the actor for us (zero idle cost)"
info "waiting ${IDLE_WAIT}s for the idle-suspender (no manual 'suspend' step)..."
for ((i=IDLE_WAIT; i>0; i-=5)); do printf '\r   ...%3ds ' "$i"; sleep 5; done; printf '\r%40s\r' ''
hr; actor_state
say "The worker is released (ATEOM POD <none>) - the actor is SUSPENDED at ~zero cost,"
say "its memory + conversation frozen in a Full snapshot in GCS."

# ---------------------------------------------------------------------------
bold "5. New message after suspend - auto-resume + STATE SURVIVED"
say "you  ▶ What is my name? Answer with only the name."
t0=$(date +%s)
ans="$(turn 'What is my name? Answer with only the name.')"
t1=$(date +%s)
say "hermes ◀ $ans"
info "took $((t1-t0))s  (auto-resume from the suspend snapshot)"
case "$ans" in
  *[Mm]aya*) info "PASS - memory survived a full suspend → worker-freed → resume cycle." ;;
  *) info "UNEXPECTED - did not see 'Maya' after resume." ;;
esac
hr; actor_state

# ---------------------------------------------------------------------------
bold "Done."
say "One conversation drove: provision → resume → answer → continuity → idle-suspend → resume,"
say "with zero manual actor lifecycle steps. The actor will idle-suspend again on its own."
info "actor logs : $ATE --context=$KUBECTL_CONTEXT logs actor $ACTOR -a $ATESPACE"
info "cleanup    : $ATE --context=$KUBECTL_CONTEXT delete actor $ACTOR -a $ATESPACE"
