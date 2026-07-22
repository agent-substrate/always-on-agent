# Gateway image = stock OpenClaw (slim) + the compiled substrate plugin + kubectl-ate.
#
# OpenClaw's plugin installer requires compiled JS (dist/index.js) — TypeScript
# source is only honored for local dev checkouts, not installed plugins. So we
# transpile the plugin with esbuild, vendor its runtime deps, and copy the proto
# next to the compiled client (which resolves it via import.meta.url).
#
# kubectl-ate is used by the plugin (provisioner: "kubectl-ate") to create/suspend
# the per-conversation actors, since a plain Deployment on this cluster (k8s 1.35)
# has no podcert for in-band ateapi mTLS.

# --- build kubectl-ate from the pinned Substrate OSS repo ---
FROM golang:1.26-bookworm AS ate
RUN git clone --depth=1 https://github.com/agent-substrate/substrate.git /src \
 && cd /src && CGO_ENABLED=0 go build -o /out/kubectl-ate ./cmd/kubectl-ate

# --- compile the plugin (ts -> dist/*.js) + vendor runtime deps ---
FROM node:24-bookworm AS plugin
WORKDIR /plugin
COPY extensions/substrate/ ./
# Vendor prod deps (@grpc from package.json), then transpile with npx esbuild
# (npx runs from its own cache, so it never disturbs node_modules).
RUN npm install --omit=dev \
 && npx --yes esbuild@0.24.2 \
      index.ts acp-runtime.ts actor-router.ts actor-provisioner.ts \
      ateapi-client.ts kubectl-ate-client.ts idle-monitor.ts idle-suspender.ts \
      --format=esm --platform=node --target=node20 --outdir=dist \
 && cp ateapi.proto dist/ateapi.proto

# --- gateway image ---
FROM ghcr.io/openclaw/openclaw:slim
USER 0
COPY --from=ate /out/kubectl-ate /usr/local/bin/kubectl-ate
# Ship compiled dist + manifest + package.json + vendored @grpc; drop the .ts sources.
COPY --from=plugin /plugin/dist /app/extensions/substrate/dist
COPY --from=plugin /plugin/node_modules /app/extensions/substrate/node_modules
COPY extensions/substrate/package.json /app/extensions/substrate/package.json
COPY extensions/substrate/openclaw.plugin.json /app/extensions/substrate/openclaw.plugin.json
RUN chown -R 1000:1000 /app/extensions/substrate
USER 1000
