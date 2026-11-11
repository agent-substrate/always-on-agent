#!/usr/bin/env bash
# One-paste WhatsApp go-live for the Hermes-on-Substrate gateway.
#
# The gateway image already ships the WhatsApp Cloud channel (gateway/whatsapp_cloud.py);
# it just auto-enables once the credentials are present. This script does the last mile:
#
#   1. merges your Meta secrets into hermes-secrets   (never written to the repo)
#   2. sets the non-secret phone-number-id on the Deployment env
#   3. restarts the gateway and confirms "whatsapp cloud channel enabled"
#   4. opens a public HTTPS tunnel (cloudflared) to the gateway
#   5. prints the exact Callback URL + Verify token to paste into Meta
#
# Nothing here is committed. Secrets come from your environment, live only in the
# in-cluster Secret, and the tunnel URL is ephemeral (regenerated each run).
#
# Usage:
#   export WHATSAPP_CLOUD_PHONE_NUMBER_ID=...   # Meta > WhatsApp > API Setup, the "From" id
#   export WHATSAPP_CLOUD_ACCESS_TOKEN=...      # temp 24h token, or a System User token
#   export WHATSAPP_CLOUD_APP_SECRET=...        # App settings > Basic > App Secret
#   export WHATSAPP_CLOUD_VERIFY_TOKEN=...      # any string you choose (used in Meta's verify)
#   ./demo/whatsapp_go_live.sh
#
# Ctrl-C tears the tunnel + port-forward down. The credentials stay in the Secret;
# run ./demo/whatsapp_go_live.sh --disable to turn the channel back off.
set -euo pipefail

NS=hermes
GW_DEPLOY=deploy/hermes-gateway
SECRET=hermes-secrets
GW_PORT="${GW_PORT:-8080}"
CF=~/bin/cloudflared
WEBHOOK_PATH="${WHATSAPP_CLOUD_WEBHOOK_PATH:-/whatsapp/webhook}"

bold() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
err()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; }

# ---- optional: turn the channel back off -----------------------------------
if [[ "${1:-}" == "--disable" ]]; then
  bold "Disabling the WhatsApp channel"
  kubectl -n "$NS" set env "$GW_DEPLOY" WHATSAPP_CLOUD_PHONE_NUMBER_ID-  >/dev/null
  kubectl -n "$NS" set env "$GW_DEPLOY" WHATSAPP_CLOUD_PHONE_NUMBER_ID="" >/dev/null
  kubectl -n "$NS" rollout status "$GW_DEPLOY" --timeout=120s
  info "channel disabled (phone-number-id cleared; secrets left in $SECRET)."
  exit 0
fi

# ---- 0. preflight ----------------------------------------------------------
bold "0. Preflight"
: "${WHATSAPP_CLOUD_PHONE_NUMBER_ID:?set WHATSAPP_CLOUD_PHONE_NUMBER_ID (Meta 'From' id)}"
: "${WHATSAPP_CLOUD_ACCESS_TOKEN:?set WHATSAPP_CLOUD_ACCESS_TOKEN (Meta access token)}"
: "${WHATSAPP_CLOUD_VERIFY_TOKEN:?set WHATSAPP_CLOUD_VERIFY_TOKEN (any string, echoed to Meta)}"
if [[ -z "${WHATSAPP_CLOUD_APP_SECRET:-}" ]]; then
  info "WARNING: WHATSAPP_CLOUD_APP_SECRET unset - webhook signatures will NOT be verified."
fi
[[ -x "$CF" ]] || { err "cloudflared not found at $CF"; exit 1; }
kubectl -n "$NS" get "$GW_DEPLOY" >/dev/null 2>&1 || { err "$GW_DEPLOY not found in ns/$NS"; exit 1; }
kubectl -n "$NS" get secret "$SECRET" >/dev/null 2>&1 || { err "secret/$SECRET not found in ns/$NS"; exit 1; }
info "gateway + secret present."

# ---- 1. merge secrets (keeps existing MODEL_API_KEY + API_SERVER_KEY) ------
bold "1. Merge WhatsApp secrets into $SECRET"
patch_secret() { # $1=key $2=value  - patch in place, base64-encoded
  local b64; b64="$(printf '%s' "$2" | base64 -w0)"
  kubectl -n "$NS" patch secret "$SECRET" --type merge \
    -p "{\"data\":{\"$1\":\"$b64\"}}" >/dev/null
}
patch_secret WHATSAPP_CLOUD_ACCESS_TOKEN "$WHATSAPP_CLOUD_ACCESS_TOKEN"
patch_secret WHATSAPP_CLOUD_VERIFY_TOKEN "$WHATSAPP_CLOUD_VERIFY_TOKEN"
[[ -n "${WHATSAPP_CLOUD_APP_SECRET:-}" ]] && patch_secret WHATSAPP_CLOUD_APP_SECRET "$WHATSAPP_CLOUD_APP_SECRET"
info "secrets merged (values not printed)."

# ---- 2. set the non-secret phone-number-id on the Deployment ---------------
bold "2. Set WHATSAPP_CLOUD_PHONE_NUMBER_ID on the gateway"
kubectl -n "$NS" set env "$GW_DEPLOY" \
  WHATSAPP_CLOUD_PHONE_NUMBER_ID="$WHATSAPP_CLOUD_PHONE_NUMBER_ID" >/dev/null
# Bounce the pod so the new secret values are re-read (secretKeyRef is injected at start).
kubectl -n "$NS" rollout restart "$GW_DEPLOY" >/dev/null
kubectl -n "$NS" rollout status "$GW_DEPLOY" --timeout=180s

# ---- 3. confirm the channel enabled ----------------------------------------
bold "3. Confirm the channel is enabled"
sleep 3
if kubectl -n "$NS" logs "$GW_DEPLOY" --tail=50 2>/dev/null | grep -qi "whatsapp cloud channel enabled"; then
  info "gateway reports: whatsapp cloud channel enabled at $WEBHOOK_PATH"
else
  err "did not see 'channel enabled' in gateway logs - check: kubectl -n $NS logs $GW_DEPLOY"
  kubectl -n "$NS" logs "$GW_DEPLOY" --tail=20 || true
fi

# ---- 4. public HTTPS tunnel ------------------------------------------------
bold "4. Open public HTTPS tunnel to the gateway"
# NB: the binary, not the kubectl() wrapper - see _lib.sh. Backgrounding a function
# would make $! a subshell pid and orphan the port-forward on exit.
command kubectl --context="$KUBECTL_CONTEXT" -n "$NS" port-forward "$GW_DEPLOY" "${GW_PORT}:8080" >/tmp/hermes-wa-pf.log 2>&1 &
PF_PID=$!
"$CF" tunnel --url "http://localhost:${GW_PORT}" >/tmp/hermes-wa-cf.log 2>&1 &
CF_PID=$!
trap 'kill $PF_PID $CF_PID 2>/dev/null || true; echo; info "tunnel + port-forward stopped."' EXIT
info "waiting for the tunnel URL..."
PUBLIC=""
for _ in $(seq 1 30); do
  PUBLIC="$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' /tmp/hermes-wa-cf.log 2>/dev/null | head -1 || true)"
  [[ -n "$PUBLIC" ]] && break
  sleep 1
done
[[ -z "$PUBLIC" ]] && { err "tunnel URL not found - see /tmp/hermes-wa-cf.log"; exit 1; }
sleep 2
curl -fsS "${PUBLIC}/health" >/dev/null && info "public /health OK via $PUBLIC"

# ---- 5. what to paste into Meta --------------------------------------------
bold "5. Point Meta at the gateway"
cat <<EOF

   In the Meta app -> WhatsApp -> Configuration -> Edit:

     Callback URL : ${PUBLIC}${WEBHOOK_PATH}
     Verify token : ${WHATSAPP_CLOUD_VERIFY_TOKEN}

   Save (Meta GETs the URL; the gateway echoes the challenge -> "Verified"),
   then Subscribe the app to the "messages" field.

   Now message the test number from your WhatsApp. Watch the actor wake, answer,
   and suspend:

     kubectl-ate get actors -a hermes-demo
     kubectl -n ${NS} logs -f ${GW_DEPLOY}

   Send a name in one message, ask for it back in another -> the reply proves
   per-user state continuity across suspends, now over WhatsApp.

   Leave this script running to keep the tunnel up. Ctrl-C tears it down.
   Re-run with --disable to turn the channel off (secrets stay in the Secret).

EOF

# Keep the tunnel alive until the user interrupts.
wait $CF_PID
