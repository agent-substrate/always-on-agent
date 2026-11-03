# Actor image = stock public OpenClaw + the agent's config and persona.
#
# This builds from the public `ghcr.io/openclaw/openclaw` release image, so it
# needs no private base and no checkout of the OpenClaw source tree.
#
# The base is pinned by digest on purpose. A golden snapshot is only valid for
# the exact image it was taken from, so a floating tag would silently invalidate
# every snapshot the moment upstream pushes. To move to a newer OpenClaw, bump
# the digest here and let deploy-demo.sh recreate the ActorTemplate (templates
# are immutable, so it deletes and recreates), which re-warms the golden.
#
#   crane digest ghcr.io/openclaw/openclaw:2026.8.2-slim
#
# This is :slim as of 2026.8.2. Note that :slim floats: it rolled to 2026.9.1 on
# 3 Sep. Keep this digest identical to the one in build/gateway.Dockerfile, and
# bump the pinned @openclaw/whatsapp version in manifests/gateway.yaml with it.
FROM ghcr.io/openclaw/openclaw@sha256:5d25165995041caa6a7175bec82b25ad98c44eb269bb42435da8e27ec06e6be4
USER 0

# Enables OpenClaw's OpenAI-compatible HTTP API (POST /v1/chat/completions),
# which is off by default, so the gateway's substrate ACP backend can drive the
# agent loop. Auth is a shared bearer token; the endpoint is only reachable
# in-cluster via atenet.
#
# It also sets gateway.trustedProxies to the link-local range. atenet delivers
# every request into the sandbox from a link-local gateway address (169.254.17.1
# here) and forwards client headers, so OpenClaw sees "proxy-shaped" traffic and
# rejects it with proxy_attribution_required unless that source is trusted. The
# range is node-local and never routable, and the endpoint still requires the
# bearer token.
COPY build/actor/openclaw.json /home/node/.openclaw/openclaw.json

# Give the agent a fixed identity + plain voice so it does NOT run OpenClaw's
# persona-bootstrap flow (which invents "nano-<name>-v####" personas, emoji, and
# "pulse"/status chatter). SOUL.md/IDENTITY.md live in the agent workspace dir
# (~/.openclaw/workspace), which OpenClaw injects into agent context.
RUN mkdir -p /home/node/.openclaw/workspace
COPY build/actor/SOUL.md /home/node/.openclaw/workspace/SOUL.md
COPY build/actor/IDENTITY.md /home/node/.openclaw/workspace/IDENTITY.md

RUN chown -R 1000:1000 /home/node/.openclaw
USER 1000

# NOTE: the substrate plugin is deliberately NOT installed here, only in the
# gateway image. The actor has no reason to load it: it cannot call the control
# plane to suspend itself, because Substrate projects an actor's identity
# (/run/ate/actor-id, atespace, actor-uid, trust-bundle.pem) but no client
# credential, so such a call has no cert to present and fails mTLS. The gateway
# already holds the credentialed path it uses to create actors, and drives
# idle-suspend from there. See extensions/substrate/idle-suspender.ts.
