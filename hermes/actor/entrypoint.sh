#!/bin/sh
# PID1 for the Hermes Substrate actor.
#
# The DurableDir (HERMES_HOME) is empty on the very first golden boot and persists across
# suspend/resume thereafter. We seed a minimal config.yaml only if absent, so we never clobber
# state the actor has accreted, then hand off to `hermes gateway run` via exec (so it becomes
# PID1 and receives SIGTERM directly on suspend/checkpoint).
#
# Secrets arrive as env (secretKeyRef), never baked into the image:
#   GOOGLE_API_KEY / GEMINI_API_KEY  - LLM provider key (Hermes reads these natively;
#                                      GOOGLE_API_KEY takes precedence)
#   API_SERVER_KEY                    - shared bearer token; its presence is what ENABLES the
#                                      API server (there is no API_SERVER_ENABLED flag)
set -eu

: "${HERMES_HOME:=/data/.hermes}"
: "${API_SERVER_HOST:=0.0.0.0}"
: "${API_SERVER_PORT:=80}"
: "${MODEL_PROVIDER:=gemini}"
: "${MODEL_DEFAULT:=gemini-2.5-flash}"
: "${MODEL_BASE_URL:=}"
# Hermes reads provider keys from conventionally-named env vars. MODEL_API_KEY is
# the single name the ActorTemplate injects, so map it onto whichever name the
# selected provider actually looks for. Keeps the template provider-agnostic
# instead of hardcoding one vendor's variable.
if [ -n "${MODEL_API_KEY:-}" ]; then
  case "$MODEL_PROVIDER" in
    gemini|google) export GOOGLE_API_KEY="${GOOGLE_API_KEY:-$MODEL_API_KEY}" ;;
    anthropic)     export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-$MODEL_API_KEY}" ;;
    *)             export OPENAI_API_KEY="${OPENAI_API_KEY:-$MODEL_API_KEY}"
                   export CUSTOM_API_KEY="${CUSTOM_API_KEY:-$MODEL_API_KEY}" ;;
  esac
fi

mkdir -p "$HERMES_HOME"

# When a Data-scope template uses a cold-boot resume, Substrate restores only
# /data and runs this entrypoint again. Runtime gateway markers from the previous
# boot therefore come back even though their process and advisory file locks do
# not. PID 1 is reused by this entrypoint, so Hermes can otherwise mistake the
# restored gateway.pid for a live second gateway and refuse to start. These files
# describe the old process, not durable agent state; the new gateway recreates
# them below. Golden resumes do not rerun this entrypoint.
rm -f \
  "$HERMES_HOME/gateway.pid" \
  "$HERMES_HOME/gateway.lock" \
  "$HERMES_HOME/gateway_state.json"

# Hermes only sends OPENAI_API_KEY to openai.com hosts (a guard against leaking
# it to other endpoints), so a custom endpoint, such as the gateway's model
# bridge, gets its key only from the provider's own key_env.
CUSTOM_KEY_ENV_LINE=""
if [ "$MODEL_PROVIDER" = "custom" ] && [ -n "${MODEL_BASE_URL:-}" ]; then
  CUSTOM_KEY_ENV_LINE="
    key_env: CUSTOM_API_KEY"
fi

if [ ! -f "$HERMES_HOME/config.yaml" ]; then
  echo "[entrypoint] seeding $HERMES_HOME/config.yaml (provider=$MODEL_PROVIDER model.default=$MODEL_DEFAULT)"
  cat > "$HERMES_HOME/config.yaml" <<EOF
# Seeded once by the actor entrypoint on first (golden) boot.
# provider is selected EXPLICITLY (Hermes does not infer it from the model slug); the native
# Gemini client reads GOOGLE_API_KEY / GEMINI_API_KEY from the environment.
model:
  default: ${MODEL_DEFAULT}
  provider: ${MODEL_PROVIDER}${MODEL_BASE_URL:+
providers:
  ${MODEL_PROVIDER}:
    base_url: ${MODEL_BASE_URL}}${CUSTOM_KEY_ENV_LINE}
# tools run in-process; no docker/ssh/modal child sandbox (checkpoint-friendly under gVisor)
terminal:
  backend: local
# turn-signal tells the gateway when a turn has really finished, so a long task
# is not suspended when the gateway's HTTP request times out at five minutes.
# Standalone plugins are opt-in, so it must be listed here to load at all.
plugins:
  enabled: [turn-signal]
# Memory is off by default in Hermes. For an agent it is the entire point:
# MEMORY.md accumulates how this user works, USER.md who they are, and both
# live in the DurableDir so they survive every suspend/resume cycle.
memory:
  memory_enabled: true
  user_profile_enabled: true
# No human is ever at this terminal. Hermes gates "dangerous" commands behind an
# interactive approval prompt, and with nobody to answer it the agent simply
# reports that its work is "blocked pending execution approval" - which is what
# an agent looked like from the outside before this was set.
#
# This unattended runtime cannot prompt for terminal approval. Restrict access
# to the gateway and configure sandbox/egress policy for your deployment.
approvals:
  mode: "off"
gateway:
  api_server:
    enabled: true
    host: ${API_SERVER_HOST}
    port: ${API_SERVER_PORT}
EOF
fi

# Preserve existing config and ensure unattended execution is configured.
if ! grep -q '^approvals:' "$HERMES_HOME/config.yaml"; then
  echo "[entrypoint] adding approvals.mode=off to an existing config.yaml"
  printf '\napprovals:\n  mode: "off"\n' >> "$HERMES_HOME/config.yaml"
fi

# Install the runtime briefing.
BRIEFING=/usr/local/share/hermes-skills/AGENTS.md
if [ -f "$BRIEFING" ]; then
  echo "[entrypoint] refreshing /data/AGENTS.md (always-in-context briefing)"
  cp -f "$BRIEFING" /data/AGENTS.md
fi

# Plugins, refreshed every boot for the same reason as the briefing: an update
# that stops at the DurableDir of an actor that already exists is an update that
# silently did not happen.
if [ -d /usr/local/share/hermes-plugins ]; then
  echo "[entrypoint] refreshing plugins"
  mkdir -p "$HERMES_HOME/plugins"
  cp -rf /usr/local/share/hermes-plugins/. "$HERMES_HOME/plugins/"
fi
if ! grep -q '^plugins:' "$HERMES_HOME/config.yaml"; then
  echo "[entrypoint] enabling turn-signal on an existing config.yaml"
  printf '\nplugins:\n  enabled: [turn-signal]\n' >> "$HERMES_HOME/config.yaml"
fi

echo "[entrypoint] starting: hermes gateway run  (bind ${API_SERVER_HOST}:${API_SERVER_PORT})"
# TERMINAL_CWD is how the gateway daemon is told where "the project" is. Without
# it, Hermes looks for AGENTS.md in the LAUNCH directory - which for a daemon is
# its install dir, not /data - so the briefing sits on disk, correct and
# complete, and never reaches a single prompt. Its own comment says as much:
# "the install dir for the gateway daemon, which is why the gateway sets
# TERMINAL_CWD".
#
# Safe to freeze into the golden snapshot: /data is the DurableDir mount point
# for every actor.
export TERMINAL_CWD=/data

exec hermes gateway run
