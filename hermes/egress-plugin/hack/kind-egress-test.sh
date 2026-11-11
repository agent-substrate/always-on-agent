#!/usr/bin/env bash
#
# End to end test for the Hermes egress plugin against a real Substrate cluster.
#
# The plugin runs as a second ext_proc filter on the egress chain, behind
# Substrate's own, so it only ever sees requests whose actor certificate has
# already been validated. Identity comes from that certificate, not from
# anything the actor can set.
#
# What this proves, in four cases:
#
#   1. allow  egress-demo        -> its allowed host   : reaches the upstream
#   2. block  egress-demo        -> any other host     : refused
#   3. block  egress-quarantined -> the SAME host as 1 : refused
#   4. block  egress-quarantined -> any other host     : refused
#
# Case 1 against case 3 is the load-bearing pair. The destination is identical
# and only the calling actor differs, so nothing except per-actor policy can
# explain the two outcomes. Substrate's own filter has no knowledge of our
# registry, which rules it out as the cause.
#
# Shape of a denial: the egress listener is a CONNECT proxy, so a refusal is a
# 403 on the CONNECT and the tunnel is never established. The actor's HTTP
# client does not see an HTTP status, it sees the connection go away. We assert
# on the gateway access log for the status and on the actor for the failure.

set -o errexit -o nounset -o pipefail

CTX="${KUBECTL_CONTEXT:-kind-kind}"
ATESPACE="${ATESPACE:-demo}"
TEMPLATE="${TEMPLATE:-ate-demo-egress/egress}"
TARGET_NS="${TARGET_NS:-egress-target}"
PLUGIN_NS="${PLUGIN_NS:-hermes-egress}"
ALLOWED_ACTOR="egress-demo"
QUARANTINED_ACTOR="egress-quarantined"
CRED_HEADER="x-hermes-injected"

K="kubectl --context ${CTX}"
KATE="kubectl-ate --context ${CTX}"

log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
pass() { printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*"; FAILED=1; }
note() { printf '\033[1;33mNOTE\033[0m %s\n' "$*"; }
FAILED=0

cleanup() {
  log "cleanup"
  for a in "${ALLOWED_ACTOR}" "${QUARANTINED_ACTOR}"; do
    ${KATE} suspend actor "${a}" -a "${ATESPACE}" >/dev/null 2>&1 || true
    ${KATE} delete  actor "${a}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  done
  ${K} delete namespace "${TARGET_NS}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  info "done"
}
if [[ "${1:-}" == "--cleanup" ]]; then cleanup; exit 0; fi

log "preflight"
${K} -n ate-system rollout status deployment/atenet-egress --timeout=120s
${K} -n "${PLUGIN_NS}" rollout status deployment/hermes-egress-plugin --timeout=120s

log "deploy two in-cluster targets"
${K} create namespace "${TARGET_NS}" >/dev/null 2>&1 || true
for t in whoami-allowed whoami-blocked; do
  ${K} -n "${TARGET_NS}" create deployment "${t}" --image=traefik/whoami >/dev/null 2>&1 || true
  ${K} -n "${TARGET_NS}" expose deployment "${t}" --port=80 >/dev/null 2>&1 || true
done
${K} -n "${TARGET_NS}" rollout status deployment/whoami-allowed --timeout=180s
${K} -n "${TARGET_NS}" rollout status deployment/whoami-blocked --timeout=180s
ALLOWED_IP=$(${K} -n "${TARGET_NS}" get svc whoami-allowed -o jsonpath='{.spec.clusterIP}')
BLOCKED_IP=$(${K} -n "${TARGET_NS}" get svc whoami-blocked -o jsonpath='{.spec.clusterIP}')
info "allowed target = ${ALLOWED_IP}   other target = ${BLOCKED_IP}"

# The plugin strips the port before matching, so bare hosts are what count.
log "write the policy registry"
${K} -n "${PLUGIN_NS}" create configmap egress-registry --dry-run=client -o yaml \
  --from-literal=registry.json="$(cat <<JSON
{
  "agents": {
    "${ALLOWED_ACTOR}": { "egress": ["${ALLOWED_IP}"] },
    "${QUARANTINED_ACTOR}": { "egress": [] }
  }
}
JSON
)" | ${K} apply -f - >/dev/null
${K} -n "${PLUGIN_NS}" set env deployment/hermes-egress-plugin MODEL_HOST="${ALLOWED_IP}" >/dev/null
${K} -n "${PLUGIN_NS}" rollout status deployment/hermes-egress-plugin --timeout=180s
info "${ALLOWED_ACTOR}       -> ${ALLOWED_IP} only"
info "${QUARANTINED_ACTOR} -> nothing at all"

log "create and resume the actors"
${KATE} create atespace "${ATESPACE}" >/dev/null 2>&1 || true
for a in "${ALLOWED_ACTOR}" "${QUARANTINED_ACTOR}"; do
  # --template was renamed --template-ref when the ActorTemplate CRD went away.
  # Both failures are swallowed by the `|| true`, so a stale flag here shows up
  # as two actors that never resume rather than as an error.
  ${KATE} create actor "${a}" -a "${ATESPACE}" --template-ref "${TEMPLATE}" >/dev/null 2>&1 || true
  ${KATE} resume actor "${a}" -a "${ATESPACE}" >/dev/null 2>&1 || true
done
sleep 20

log "open a route to the actors"
${K} -n ate-system port-forward service/atenet-router 18099:80 >/tmp/egress-pf.log 2>&1 &
PF=$!
trap 'kill ${PF} >/dev/null 2>&1 || true; [[ "${KEEP:-}" == "1" ]] || cleanup' EXIT
sleep 5

# Match on the (actor, destination) pair rather than slicing the log by line
# number: the gateway Deployment has two containers and --all-containers
# interleaves them, so line offsets are not stable between reads.
gw_lines() {
  ${K} -n ate-system logs deployment/atenet-egress --all-containers --tail=-1 2>/dev/null \
    | grep '\[egress\]' | grep "authority=$2:80" | grep "actor/$1 " || true
}
gw_count() { gw_lines "$1" "$2" | grep -c . || true; }

# Ask an actor to fetch a URL, then read back what the gateway recorded for it.
# Sets: ACTOR_OUT (actor's JSON reply) and GW_CODE (status on the CONNECT).
drive() {
  local actor="$1" ip="$2" before now
  before=$(gw_count "${actor}" "${ip}")
  ACTOR_OUT=$(curl -s -m 30 -X POST http://localhost:18099/ \
    -H "Host: ${actor}.${ATESPACE}.actors.resources.substrate.ate.dev" \
    -H 'Content-Type: application/json' -d "{\"url\":\"http://${ip}:80/\"}" 2>/dev/null || true)
  GW_CODE=""
  for _ in $(seq 1 15); do
    now=$(gw_count "${actor}" "${ip}")
    if [[ "${now}" -gt "${before}" ]]; then
      GW_CODE=$(gw_lines "${actor}" "${ip}" | tail -1 | grep -o 'code=[0-9]*' | cut -d= -f2 || true)
      [[ -n "${GW_CODE}" ]] && break
    fi
    sleep 1
  done
}

expect_allowed() {
  local actor="$1" ip="$2" label="$3"
  drive "${actor}" "${ip}"
  info "gateway code=${GW_CODE:-none}"
  if [[ "${GW_CODE}" == "200" ]]; then pass "${label}: gateway established the tunnel"
  else fail "${label}: expected gateway code=200, got '${GW_CODE:-none}'"; fi
  if printf '%s' "${ACTOR_OUT}" | grep -q '"statusCode":200'; then
    pass "${label}: actor reached the upstream"
  else
    fail "${label}: actor did not reach the upstream"
    printf '%s\n' "${ACTOR_OUT}" | head -c 300 | sed 's/^/     /'
  fi
}

expect_blocked() {
  local actor="$1" ip="$2" label="$3" reason="$4"
  drive "${actor}" "${ip}"
  info "gateway code=${GW_CODE:-none}"
  if [[ "${GW_CODE}" == "403" ]]; then pass "${label}: gateway refused the CONNECT (403)"
  else fail "${label}: expected gateway code=403, got '${GW_CODE:-none}'"; fi
  if printf '%s' "${ACTOR_OUT}" | grep -q '"statusCode":200'; then
    fail "${label}: the actor reached the upstream anyway"
  else
    pass "${label}: the actor did not reach the upstream"
  fi
  # Require actor, destination and reason on the SAME line, so a denial from an
  # earlier case cannot satisfy this one.
  if ${K} -n "${PLUGIN_NS}" logs deployment/hermes-egress-plugin --tail=200 2>/dev/null \
     | grep "/actor/${actor}\"" | grep "\"authority\":\"${ip}:80\"" \
     | grep -q "${reason}"; then
    pass "${label}: our plugin is the component that refused it"
  else
    fail "${label}: no matching denial from our plugin (${reason})"
  fi
}

log "CASE 1 (allow): ${ALLOWED_ACTOR} to the host on its allowlist"
expect_allowed "${ALLOWED_ACTOR}" "${ALLOWED_IP}" "case 1"

log "CASE 2 (block): ${ALLOWED_ACTOR} to a host that is not on its allowlist"
expect_blocked "${ALLOWED_ACTOR}" "${BLOCKED_IP}" "case 2" "is not allowed to reach"

log "CASE 3 (block): ${QUARANTINED_ACTOR} to the SAME host case 1 was allowed to reach"
expect_blocked "${QUARANTINED_ACTOR}" "${ALLOWED_IP}" "case 3" "has no egress at all"
info "same destination as case 1, different actor, opposite outcome"

log "CASE 4 (block): ${QUARANTINED_ACTOR} to any other host"
expect_blocked "${QUARANTINED_ACTOR}" "${BLOCKED_IP}" "case 4" "has no egress at all"

# Header injection is reported, not asserted. The egress listener terminates
# CONNECT, so a header added to the CONNECT request is consumed establishing the
# tunnel and never reaches the target. This checks the upstream's own echo of
# what it received, which is the only honest way to tell.
log "credential injection on this chain"
drive "${ALLOWED_ACTOR}" "${ALLOWED_IP}"
if printf '%s' "${ACTOR_OUT}" | grep -qi "${CRED_HEADER}"; then
  pass "the upstream received ${CRED_HEADER}"
else
  note "the upstream did NOT receive ${CRED_HEADER}"
  info "expected on this chain: egress terminates CONNECT, so the tunnel payload"
  info "is opaque to Envoy and a header on the CONNECT never reaches the target."
  info "Destination policy is unaffected, as cases 1 to 4 show."
fi

log "what our plugin decided"
${K} -n "${PLUGIN_NS}" logs deployment/hermes-egress-plugin --tail=200 2>/dev/null \
  | grep -E '"msg":"(allowed|denied)"' | tail -6 | sed 's/^/   /' || true

log "result"
if [[ "${FAILED}" == "0" ]]; then
  pass "every case behaved as specified"
else
  fail "at least one case did not"
  exit 1
fi
