#!/usr/bin/env bash
# Turn the Telegram channel on against a real bot, then lay out the recording panes.
#
# Unlike whatsapp_go_live.sh this stands up NO public endpoint and needs no tunnel:
# the gateway dials out to Telegram's getUpdates. Nothing inbound is accepted, which
# is why the only access control that matters here is the chat allowlist.
#
# The bot token is read from the environment (or prompted for silently) and goes
# straight into the cluster Secret - it is never written to a file, a manifest, or
# your shell history.
#
#   TELEGRAM_BOT_TOKEN=... TELEGRAM_ALLOWED_CHAT_IDS=123456 demo/telegram_go_live.sh
#   demo/telegram_go_live.sh --layout-only     # panes only, channel already live
#
# Steps it performs:
#   1. verify the token with getMe (fail fast on a typo, before touching the cluster)
#   2. patch hermes-secrets with the token, set the allowlist on the Deployment
#   3. shorten IDLE_TIMEOUT_SECONDS so suspend happens on camera, not off it
#   4. roll the gateway and wait for "telegram=on" in its log
#   5. open the tmux dashboard (actors top, workers bottom)
set -euo pipefail

# Sourced for one reason: it pins kubectl to the cluster named in demo/env.sh. This
# script patches a bot token into a Secret and rolls a Deployment, so "whichever
# cluster kubectl happens to point at" is the wrong default. The bold()/info() defined
# below deliberately shadow _lib.sh's - this script has its own house style.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"

NS=hermes
DEPLOY=hermes-gateway
SECRET=hermes-secrets
SESSION=${TMUX_SESSION:-hermes-demo}
# 60s of dead air is a long time on video; 20s + the suspender's 5s poll lands the
# transition inside half a minute of the reply.
DEMO_IDLE=${DEMO_IDLE:-20}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

bold() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
info() { printf '   \033[2m%s\033[0m\n' "$*"; }

layout() {
  bold "Recording layout"
  if [[ -n ${TMUX:-} ]]; then
    info "already inside tmux - splitting the current window instead"
    tmux split-window -v "$HERE/watch.sh workers"
    tmux select-pane -t 0
    tmux respawn-pane -k -t 0 "$HERE/watch.sh actors"
    return
  fi
  tmux kill-session -t "$SESSION" 2>/dev/null || true
  tmux new-session  -d -s "$SESSION" "$HERE/watch.sh actors"
  tmux split-window -v -t "$SESSION" "$HERE/watch.sh workers"
  tmux select-pane  -t "$SESSION".0
  info "put Telegram on the left half of the screen, this terminal on the right"
  info "attaching - Ctrl-b d to detach, Ctrl-c in a pane to stop it"
  tmux attach -t "$SESSION"
}

if [[ ${1:-} == --layout-only ]]; then layout; exit 0; fi

# --- 1. token -----------------------------------------------------------------
TOKEN=${TELEGRAM_BOT_TOKEN:-}
if [[ -z $TOKEN ]]; then
  read -rsp "Bot token from @BotFather (input hidden): " TOKEN; echo
fi
[[ -n $TOKEN ]] || { echo "no token given" >&2; exit 1; }

bold "Verifying the token with getMe"
me=$(curl -sS --max-time 20 "https://api.telegram.org/bot${TOKEN}/getMe")
if ! grep -q '"ok":true' <<<"$me"; then
  # Print Telegram's own description, not the request: the request contains the token.
  echo "   token rejected: $(sed -E 's/.*"description":"([^"]*)".*/\1/' <<<"$me")" >&2
  exit 1
fi
BOT=$(sed -E 's/.*"username":"([^"]*)".*/\1/' <<<"$me")
info "bot @$BOT"

# --- 2. allowlist -------------------------------------------------------------
ALLOW=${TELEGRAM_ALLOWED_CHAT_IDS:-}
if [[ -z $ALLOW ]]; then
  cat <<EOF

   No TELEGRAM_ALLOWED_CHAT_IDS set. Empty means ANY Telegram user who finds
   @$BOT can talk to the agent and spend the LLM budget.

   To get your chat id: message the bot once, then
     curl -s "https://api.telegram.org/bot\$TELEGRAM_BOT_TOKEN/getUpdates" | grep -o '"chat":{"id":[-0-9]*'

EOF
  read -rp "   Continue with an OPEN bot? [y/N] " ans
  [[ ${ans:-} == [yY] ]] || exit 1
fi

# --- 3. cluster ---------------------------------------------------------------
bold "Injecting the token into $SECRET"
kubectl -n "$NS" patch secret "$SECRET" --type merge \
  -p "{\"stringData\":{\"TELEGRAM_BOT_TOKEN\":\"${TOKEN}\"}}" >/dev/null
info "patched (stringData; kubectl encodes it, the value never hits disk here)"

bold "Configuring the gateway"
kubectl -n "$NS" set env "deploy/$DEPLOY" \
  "TELEGRAM_ALLOWED_CHAT_IDS=$ALLOW" "IDLE_TIMEOUT_SECONDS=$DEMO_IDLE" >/dev/null
info "allowlist=${ALLOW:-<open>}  idle_timeout=${DEMO_IDLE}s"
# `set env` already triggers a rollout; this just makes the wait explicit.
kubectl -n "$NS" rollout status "deploy/$DEPLOY" --timeout=180s

bold "Confirming the poll loop started"
for _ in $(seq 30); do
  line=$(kubectl -n "$NS" logs "deploy/$DEPLOY" --tail=200 2>/dev/null | grep -m1 'telegram=' || true)
  [[ -n $line ]] && break
  sleep 2
done
if grep -q 'telegram=on' <<<"${line:-}"; then
  info "${line#* }"
else
  echo "   gateway did not report telegram=on - check: kubectl -n $NS logs deploy/$DEPLOY" >&2
  exit 1
fi

cat <<EOF

   Live. Message @$BOT and watch the right-hand panes:
     1. actor appears, goes RUNNING          (auto-provision + cold restore, ~12-17s)
     2. a worker flips FREE -> ASSIGNED
     3. reply lands in Telegram
     4. ~${DEMO_IDLE}s later: SUSPENDED, worker back to FREE   <- zero idle cost
     5. message again: RUNNING, and it still remembers you     <- state survived

   Tip: demo/watch.sh reset  clears old actors so beat 1 starts from empty.
EOF

layout
