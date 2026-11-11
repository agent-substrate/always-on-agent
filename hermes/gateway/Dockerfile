# The gateway: the one always-on process for the whole fleet. It owns every
# core channel, the cron clock, and actor lifecycle.
# Actors hold no connection and suspend freely, which is what keeps per-agent
# idle cost at zero rather than merely small.
FROM python:3.13-slim@sha256:70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 KUBECTL_ATE=/usr/local/bin/kubectl-ate

# Real kubectl reads worker pods and verified proxy peers for lifecycle callbacks.
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -sLo /usr/local/bin/kubectl \
       "https://dl.k8s.io/release/v1.32.0/bin/linux/amd64/kubectl" \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Substrate's own binary, staged into the build context by `hermesctl build`.
# It must be linux/amd64: a macOS copy builds an image that fails at runtime with
# "exec format error", which is a confusing way to discover the problem.
COPY kubectl-ate /usr/local/bin/kubectl-ate
RUN chmod +x /usr/local/bin/kubectl-ate

# Copy the whole tree, not `COPY *.py` plus a hand-maintained list of
# packages. The glob silently SKIPS DIRECTORIES, so adding a package and
# forgetting a COPY line yields a clean build, a green rollout, and a
# CrashLoopBackOff on ModuleNotFoundError. That has now happened twice:
# once for callbacks, once for fleet, with a comment warning about it
# sitting directly above the line that caused it. A list you have to
# remember to update is not a solution.
#
# .dockerignore keeps bytecode out.
COPY . /app/

WORKDIR /app
EXPOSE 8080
CMD ["python", "ingress.py"]
