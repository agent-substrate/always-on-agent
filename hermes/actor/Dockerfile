# The actor image: a single-process Hermes agent that runs inside a Substrate
# sandbox and suspends to zero.
#
# Reconstructed from the deployed image's build history after the working tree
# was lost. Verified against actor@sha256:8312a551… which is what the cluster runs.
FROM python:3.13-slim@sha256:70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f

# HOME and HERMES_HOME both point into /data because /data is the DurableDir:
# the only path that survives suspend/resume. Anything written elsewhere is lost
# on the next restore.
#
# MODEL_PROVIDER is a default only; the ActorTemplate overrides it, along with
# MODEL_DEFAULT and MODEL_BASE_URL, so the image is not tied to one vendor.
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/data \
    HERMES_HOME=/data/.hermes \
    API_SERVER_HOST=0.0.0.0 \
    API_SERVER_PORT=80 \
    TERMINAL_ENV=local \
    MODEL_PROVIDER=gemini

# git and ripgrep are used by Hermes' own tooling; the compilers are needed to
# build wheels that have no aarch64/amd64 binary for this Python.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git ripgrep procps \
      gcc g++ make python3-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Pinned by commit, never a branch: the golden snapshot is taken from this image,
# so a moving dependency would silently change every restored actor.
ARG HERMES_REF=5ffbea81e995aaa3b6b6b5ba81d1f453a6b88ef8
ENV HERMES_NIX_BUILD=1
RUN pip install --no-cache-dir \
      "hermes-agent[all] @ git+https://github.com/NousResearch/hermes-agent.git@${HERMES_REF}"

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY plugins/ /usr/local/share/hermes-plugins/
COPY AGENTS.md /usr/local/share/hermes-skills/AGENTS.md
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /data
EXPOSE 80
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
