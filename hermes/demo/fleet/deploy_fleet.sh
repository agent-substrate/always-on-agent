#!/usr/bin/env bash
# Copyright 2026 Nous Research
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Deploy one fleet description to a cluster that already runs Hermes.
#
# This is the last mile only. It assumes the cluster, Substrate, the images and
# the gateway are already up, which is hermesctl's job (steps 3 to 6). What it
# does is publish the agent registry and restart the gateway so it re-reads it.
#
# It plans by default and changes nothing without --apply, because the registry
# is the file that decides which actor every inbound message reaches: publishing
# a wrong one does not error, it silently routes people to the wrong agent.
#
#   ./deploy_fleet.sh fleet.json              # plan
#   ./deploy_fleet.sh fleet.json --apply      # do it

set -o errexit -o nounset -o pipefail

FLEET="${1:-fleet.json}"
APPLY=""
[ "${2:-}" = "--apply" ] && APPLY=1

NS="${HERMES_NAMESPACE:-hermes}"
CM="${AGENTS_CONFIGMAP:-hermes-agents}"
OUT="${OUT_DIR:-out}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

c_h=$'\033[1m'; c_w=$'\033[1;33m'; c_x=$'\033[0m'
say()  { printf '  %s\n' "$*"; }
warn() { printf '  %s%s%s\n' "$c_w" "$*" "$c_x"; }
die()  { printf '  %s\n' "$*" >&2; exit 1; }

[ -f "$FLEET" ] || die "no such fleet file: $FLEET (copy fleet.example.json to fleet.json)"
command -v kubectl >/dev/null || die "kubectl not found"

printf '\n%sRender%s\n' "$c_h" "$c_x"
python3 "$HERE/render_fleet.py" "$FLEET" --out "$OUT" || die "render failed, nothing applied"
REG="$OUT/agents-registry.json"

printf '\n%sCluster%s\n' "$c_h" "$c_x"
kubectl cluster-info >/dev/null 2>&1 || die "no cluster reachable (gcloud container clusters get-credentials ...)"
say "context: $(kubectl config current-context)"
kubectl get ns "$NS" >/dev/null 2>&1 || die "namespace $NS not found, run hermesctl deploy first"

# What is about to change, before it changes. A registry swap is invisible after
# the fact: the gateway just starts routing differently.
printf '\n%sDiff%s\n' "$c_h" "$c_x"
if kubectl get cm "$CM" -n "$NS" >/dev/null 2>&1; then
  kubectl get cm "$CM" -n "$NS" -o jsonpath='{.data.agents\.json}' 2>/dev/null > "$OUT/.live.json" || true
  if [ -s "$OUT/.live.json" ]; then
    if diff -u "$OUT/.live.json" "$REG" > "$OUT/.diff" 2>/dev/null; then
      say "no change: the live registry already matches"
    else
      sed -n '3,40p' "$OUT/.diff" | sed 's/^/  /'
      L=$(python3 -c "import json;print(len(json.load(open('$OUT/.live.json'))['agents']))" 2>/dev/null || echo '?')
      N=$(python3 -c "import json;print(len(json.load(open('$REG'))['agents']))")
      say ""
      say "agents: $L live -> $N proposed"
    fi
  fi
else
  say "configmap $CM does not exist yet; it will be created"
fi

# The model is described in TWO places and neither one defers to the other: the fleet
# file says it per shape, and demo/env.sh says it for the ActorTemplate and the gateway.
# When they disagree nothing fails. The gateway's MODEL_DEFAULT is what a turn carrying
# no model of its own gets, so the fleet is measured against whatever the gateway holds
# while the fleet file, the report and the shape env all name something else.
printf '\n%sModel%s\n' "$c_h" "$c_x"
LIVE_MODEL="$(kubectl get deploy hermes-gateway -n "$NS" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MODEL_DEFAULT")].value}' \
  2>/dev/null || true)"
if [ -z "$LIVE_MODEL" ]; then
  say "no gateway deployed yet, nothing to compare against"
else
  MISMATCH=""
  for f in "$OUT"/shapes/*.env; do
    [ -f "$f" ] || continue
    SHAPE="$(basename "$f" .env)"
    WANT="$(sed -n 's/^MODEL_DEFAULT=//p' "$f")"
    if [ "$WANT" = "$LIVE_MODEL" ]; then
      say "$SHAPE wants $WANT, gateway serves $LIVE_MODEL"
    else
      warn "$SHAPE wants $WANT, but the gateway serves $LIVE_MODEL"
      MISMATCH=1
    fi
  done
  if [ -n "$MISMATCH" ]; then
    warn "turns will go to the gateway's model, not the fleet file's."
    warn "fix MODEL in demo/env.sh and redeploy the gateway and the template before"
    warn "anything measured here is quoted as a result."
  fi
fi

if [ -z "$APPLY" ]; then
  printf '\n%sPlan only.%s Re-run with --apply to publish.\n\n' "$c_w" "$c_x"
  exit 0
fi

printf '\n%sApply%s\n' "$c_h" "$c_x"
kubectl create configmap "$CM" -n "$NS" \
  --from-file=agents.json="$REG" \
  --dry-run=client -o yaml | kubectl apply -f -
say "registry published"

# The gateway reads AGENTS_CONFIG at startup, so a ConfigMap update alone changes
# nothing until it restarts. Rollout rather than delete: the replacement becomes
# Ready before the old pod goes, which is the path measured as lossless in T10.
kubectl rollout restart deploy/hermes-gateway -n "$NS"
kubectl rollout status  deploy/hermes-gateway -n "$NS" --timeout=180s
say "gateway restarted and ready"

printf '\n%sVerify%s\n' "$c_h" "$c_x"
say "registry the gateway now has:"
kubectl exec -n "$NS" deploy/hermes-gateway -- \
  sh -c 'cat "${AGENTS_CONFIG:-/etc/hermes/agents.json}"' 2>/dev/null \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print("   ",len(d["agents"]),"agents:",", ".join(a["id"] for a in d["agents"][:6]),"...")' \
  || warn "could not read it back from the pod, check by hand"

# The t0 sample is the one measurement in the whole run that cannot be taken later.
# Growth is a curve and its first point is a fleet nobody has talked to yet, which
# stops existing the moment the first message lands. Everything else here can be
# re-run against a fleet that has been up for days.
printf '\n%sBaseline%s\n' "$c_h" "$c_x"
warn "take the t0 sample NOW, before anyone sends a first message:"
say ""
say "    python3 deploy/growth_sampler.py once --run pass2 --out runs/pass2"
say ""
say "then leave the sampler running for the soak:"
say ""
say "    python3 deploy/growth_sampler.py sample --run pass2 --out runs/pass2"
say ""

printf '\n  next: hermesctl fleet shows the deployed runtime registry\n'
printf '        hermesctl fleet shows runtime state and usage\n\n'
