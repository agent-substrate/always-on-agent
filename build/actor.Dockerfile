# Thin overlay on the existing actor image: enable OpenClaw's OpenAI-compatible
# HTTP API (POST /v1/chat/completions) — off by default — so the gateway's
# substrate ACP backend can drive the agent loop. Auth is disabled because the
# endpoint is only reachable in-cluster via atenet.
FROM gcr.io/REPLACE_WITH_YOUR_PROJECT/openclaw-actor@sha256:e9ae3392328028898b2fcd9f602f751a77e054ac7c30bfad7668bafc90f786f3
USER 0
COPY build/actor/openclaw.json /home/node/.openclaw/openclaw.json
# Give the agent a fixed identity + plain voice so it does NOT run OpenClaw's
# persona-bootstrap flow (which invents "nano-<name>-v####" personas, emoji, and
# "pulse"/status chatter). SOUL.md/IDENTITY.md live in the agent workspace dir
# (~/.openclaw/workspace), which OpenClaw injects into agent context.
RUN mkdir -p /home/node/.openclaw/workspace
COPY build/actor/SOUL.md /home/node/.openclaw/workspace/SOUL.md
COPY build/actor/IDENTITY.md /home/node/.openclaw/workspace/IDENTITY.md
RUN chown -R 1000:1000 /home/node/.openclaw/openclaw.json /home/node/.openclaw/workspace
USER 1000
