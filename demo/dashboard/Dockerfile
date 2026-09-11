# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# kubectl-ate is built here, pinned to the same Substrate commit the rest of the
# demo pins. It used to be copied out of the gateway image at pod start, which
# looked like the safe choice but shipped whatever that image happened to carry.
# What it carried was a July build whose auth-mode detection predates the
# server's move to --authentication-config: it looked for an --auth-mode=jwt
# argument, did not find one, concluded the server wanted no credentials, and
# sent none. The dashboard then showed no actors at all while the pod stayed
# green and the API returned a clean empty list.
FROM golang:1.27-bookworm AS ate
ARG SUBSTRATE_COMMIT=c48b3a3c1ac1122dd49da448abe39151681dc271
RUN git clone --filter=blob:none https://github.com/agent-substrate/substrate.git /src \
    && git -C /src checkout --detach "${SUBSTRATE_COMMIT}" \
    && cd /src && CGO_ENABLED=0 go build -o /out/kubectl-ate ./cmd/kubectl-ate

FROM node:24-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && mv kubectl /usr/local/bin/ && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ate /out/kubectl-ate /usr/local/bin/kubectl-ate

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY dashboard.js ./

EXPOSE 8090
CMD ["node", "dashboard.js"]
