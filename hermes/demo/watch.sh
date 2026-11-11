#!/usr/bin/env bash
# One pane of the live demo dashboard. Screen layout this is built for:
#
#   +---------------------+----------------------------+
#   |                     |  demo/watch.sh actors      |   <- the conversation
#   |   Telegram          +----------------------------+
#   |   (desktop app)     |  demo/watch.sh workers     |   <- the cost story
#   +---------------------+----------------------------+
#
# Both panes only poll kubectl-ate. Nothing here drives the system, so everything
# you see on camera is the gateway acting on its own.
#
#   actors   the actor behind the chat:  (absent) -> RUNNING -> SUSPENDED -> RUNNING
#   workers  the pool:                   FREE <-> ASSIGNED. A suspended actor holds
#            no worker at all - that is the zero-idle-cost claim, live.
#
# Usage:  demo/watch.sh actors|workers [interval_seconds]
#         demo/watch.sh reset            # delete leftover demo actors (asks first)
set -uo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"

POOL_NS=${POOL_NS:-$NS}
MODE=${1:-actors}
INTERVAL=${2:-1}

# Highlight the words the audience is meant to watch. Everything else stays dim so
# the state transition is the only thing that moves.
colorize() {
  sed -E \
    -e $'s/((ACTOR_STATE_)?RUNNING)/\033[1;32m\\1\033[0m/g' \
    -e $'s/((ACTOR_STATE_)?(SUSPENDED|SUSPENDING|PAUSED|PAUSING))/\033[1;33m\\1\033[0m/g' \
    -e $'s/((ACTOR_STATE_)?(RESUMING|DELETING)|PENDING|CREATING)/\033[1;36m\\1\033[0m/g' \
    -e $'s/((ACTOR_STATE_)?(CRASHED|FAILED)|ERROR)/\033[1;31m\\1\033[0m/g' \
    -e $'/^NAMESPACE/! s/\\bASSIGNED\\b/\033[1;32mASSIGNED\033[0m/g' \
    -e $'s/\\bFREE\\b/\033[2mFREE\033[0m/g' \
    -e $'s/<none>/\033[2m<none>\033[0m/g'
}

header() { # $1 = title, $2 = subtitle
  printf '\033[1;36m%s\033[0m  \033[2m%s\033[0m\n' "$1" "$(date +%H:%M:%S)"
  printf '\033[2m%s\033[0m\n\n' "$2"
}

actors_frame() {
  local out
  out="$(ate get actors -a "$ATESPACE" 2>&1)"
  header "ACTOR - the conversation" "atespace $ATESPACE · one actor per Telegram chat"
  # Header-only output means the atespace exists but holds nothing yet: that is the
  # "before" state of the demo, and it deserves to say so rather than look broken.
  if [[ $(wc -l <<<"$out") -le 1 ]]; then
    printf '%s\n\n   \033[2m(no actor yet - send the first Telegram message)\033[0m\n' "$out"
  else
    printf '%s\n' "$out"
  fi
}

workers_frame() {
  local out free total
  out="$(ate get workers 2>&1 | awk -v ns="$POOL_NS" 'NR==1 || $1==ns')"
  total=$(( $(wc -l <<<"$out") - 1 ))
  free=$(grep -c ' FREE ' <<<"$out")
  header "WORKERS - the cost story" \
          "pool $POOL_NS · a SUSPENDED actor holds no worker"
  printf '%s\n\n' "$out"
  printf '   \033[1m%s/%s free\033[0m  \033[2m(each ASSIGNED worker is a pod you are paying for)\033[0m\n' \
         "$free" "$total"
}

reset_actors() {
  ate get actors -a "$ATESPACE"
  printf '\nDelete ALL actors above so the demo starts from an empty atespace? [y/N] '
  read -r ans
  [[ ${ans:-} == [yY] ]] || { echo "aborted"; exit 1; }
  ate get actors -a "$ATESPACE" | awk 'NR>1 {print $2}' | while read -r a; do
    echo "deleting $a"; ate delete actor "$a" -a "$ATESPACE" || true
  done
}

case "$MODE" in
  reset) reset_actors; exit 0 ;;
  actors|workers) ;;
  *) echo "usage: $0 actors|workers [interval] | reset" >&2; exit 2 ;;
esac

# Render into a buffer and repaint in one write: a partially-drawn table on a
# 1s refresh looks like flicker on video.
trap 'printf "\033[?25h\n"; exit 0' INT TERM
printf '\033[?25l'
while true; do
  frame="$( "${MODE}_frame" | colorize )"
  printf '\033[H\033[2J%s' "$frame"
  sleep "$INTERVAL"
done
