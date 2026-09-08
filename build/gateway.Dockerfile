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
#
# Pinned to a commit, not to main. kubectl-ate talks to ateapi over gRPC, so a
# floating clone lets the client drift away from the control plane the demo is
# pinned to, and the failures are opaque: an image built before the CLI learned
# to mint a bearer token fails every call with `Unauthenticated: missing bearer
# token`, and one built before ActorTemplate stopped being a CRD still passes the
# removed `--template <ns>/<name>` flag. Bump this with the pin in demo/README.md.
# Track go.mod's `go` directive in the Substrate repo, not just whatever builds
# today: the toolchain is not auto-downloadable in this build environment, so a
# repo bump to a newer Go fails the build outright.
FROM golang:1.27-bookworm AS ate
# Head of the `release-0.1` branch, pinned by SHA rather than by branch name:
# release-0.1 was cut clean off main and still moves as fixes are picked into it.
ARG SUBSTRATE_REF=c48b3a3c
RUN git clone --filter=blob:none https://github.com/agent-substrate/substrate.git /src \
 && cd /src && git checkout "$SUBSTRATE_REF" \
 && CGO_ENABLED=0 go build -o /out/kubectl-ate ./cmd/kubectl-ate

# --- compile the plugin (ts -> dist/*.js) + vendor runtime deps ---
FROM node:24-bookworm AS plugin
WORKDIR /plugin
COPY extensions/substrate/ ./
# Vendor prod deps (@grpc from package.json), then transpile with npx esbuild
# (npx runs from its own cache, so it never disturbs node_modules).
RUN npm install --omit=dev \
 && npx --yes esbuild@0.24.2 \
      index.ts acp-runtime.ts actor-router.ts actor-provisioner.ts \
      ateapi-client.ts kubectl-ate-client.ts idle-suspender.ts \
      --format=esm --platform=node --target=node20 --outdir=dist \
 && cp ateapi.proto dist/ateapi.proto

# --- gateway image ---
# Pinned by digest, and to the SAME digest as build/actor.Dockerfile. A floating
# :slim silently changes the entrypoint path and the writable-directory
# expectations underneath a manifest that hardcodes both, and gateway and actor
# drifting apart is worse still, since they speak OpenClaw's own protocol to each
# other. Bump both files together.
#
#   crane digest ghcr.io/openclaw/openclaw:2026.8.2-slim
FROM ghcr.io/openclaw/openclaw@sha256:5d25165995041caa6a7175bec82b25ad98c44eb269bb42435da8e27ec06e6be4
USER 0
COPY --from=ate /out/kubectl-ate /usr/local/bin/kubectl-ate
# Ship compiled dist + manifest + package.json + vendored @grpc; drop the .ts sources.
COPY --from=plugin /plugin/dist /app/extensions/substrate/dist
COPY --from=plugin /plugin/node_modules /app/extensions/substrate/node_modules
COPY extensions/substrate/package.json /app/extensions/substrate/package.json
COPY extensions/substrate/openclaw.plugin.json /app/extensions/substrate/openclaw.plugin.json
RUN chown -R 1000:1000 /app/extensions/substrate
USER 1000
