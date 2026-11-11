#!/usr/bin/env bash
# Keep the gateway port-forward up for the duration of an unattended run.
# A port-forward drops on apiserver hiccups, gateway pod restarts (T10 does that
# deliberately) and idle timeouts. Unsupervised, a single drop turns every
# remaining turn of the soak into a connection error and the run reads as a
# platform failure that never happened.
#
# Configure the target context explicitly and create the log directory first.
CTX=${KUBECTL_CONTEXT:-REPLACE_WITH_KUBECTL_CONTEXT}
NS=${NAMESPACE:-hermes}
PORT=${PF_PORT:-18080}
LOG=${PF_LOG:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/runs/pass2/pf-keepalive.log}
LOCK=${PF_LOCK:-/tmp/pf-keepalive-${PORT}.lock}
HEALTH_INTERVAL=${PF_HEALTH_INTERVAL:-15}
HEALTH_FAILURES=${PF_HEALTH_FAILURES:-3}
mkdir -p "$(dirname "$LOG")"

# One instance per port. Two loops racing for the same port is not a harmless
# duplicate: one wins the bind and the loser spins on its 3s sleep logging a
# bind error forever, which on 8 Sep reached 6.5 MB and buried the real events.
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "$(date -u +%FT%T%:z) another keepalive already holds $LOCK - exiting" >> "$LOG"
  exit 0
fi

# A wedged forward keeps its listener bound while the tunnel behind it is dead,
# so kubectl never exits and the restart loop below never fires. Nothing
# noticed for hours on 8 Sep. Poll the gateway instead of trusting the process,
# and kill the child so the loop can rebuild it.
watch_health() {
  local pid=$1 fails=0
  sleep 5
  while kill -0 "$pid" 2>/dev/null; do
    if curl -sf -o /dev/null --max-time 5 "http://127.0.0.1:${PORT}/health"; then
      fails=0
    else
      fails=$((fails + 1))
      if [ "$fails" -ge "$HEALTH_FAILURES" ]; then
        echo "$(date -u +%FT%T%:z) health failed ${fails}x with the listener still bound - killing $pid" >> "$LOG"
        kill "$pid" 2>/dev/null
        return
      fi
    fi
    sleep "$HEALTH_INTERVAL"
  done
}

while true; do
  echo "$(date -u +%FT%T%:z) starting port-forward" >> "$LOG"
  kubectl --context "$CTX" -n "$NS" port-forward deploy/hermes-gateway "${PORT}:8080" >> "$LOG" 2>&1 &
  pf=$!
  watch_health "$pf" &
  watcher=$!
  wait "$pf"
  rc=$?
  kill "$watcher" 2>/dev/null
  wait "$watcher" 2>/dev/null
  echo "$(date -u +%FT%T%:z) port-forward exited rc=${rc} - restarting in 3s" >> "$LOG"
  sleep 3
done
