# Substrate control-plane changes

Running a heavy, suspend/resume agent end-to-end surfaced a few gaps in
**[`agent-substrate/substrate`](https://github.com/agent-substrate/substrate)**
(the OSS control plane). Rather than carry local patches here, which bitrot,
findings go upstream as issues and PRs. This file records what came out of this
integration and where it landed.

## Upstreamed

- **PR [#487](https://github.com/agent-substrate/substrate/pull/487)** (merged):
  makes the readiness deadline a per-container setting instead of a hardcoded
  30s. `readyz.Wait` polls the container until it returns 200 or the deadline
  expires, and losing that race fails the actor start; how long a workload takes
  to bind its HTTP server is a property of that workload, not of the cluster.
  On current main this is `ContainerReadyz.timeout_seconds` (1–3600, `0` means
  the server default of 30s), alongside `http_get`.

  This PR was rescoped during review. It originally proposed a tunable
  wall-clock warmup before the golden checkpoint; that was rejected on the
  grounds that the answer for a workload which cannot report readiness is a
  readiness endpoint, or a sidecar that provides one, rather than a longer
  timer. Only the readyz deadline survived.

- **Issue [#465](https://github.com/agent-substrate/substrate/issues/465)** (open)
  covers suspend-safe actor networking via injected in-sandbox ingress/egress
  proxies. The underlying work that removes most of the need to tune timeouts
  at all.

## Findings that did not become patches

The demo does not declare a readyz probe. OpenClaw's `/healthz` reports "live"
early, before plugin pre-warm, so gating the golden checkpoint on it would
capture a half-warmed agent. The golden is instead gated on atecontroller's
wall-clock warmup, which has been sufficient in testing. A real readiness
endpoint on the actor would be the better fix and would let `timeout_seconds`
above do its job. See the note in
[`../manifests/actortemplate.yaml`](../manifests/actortemplate.yaml).

## Corrections to earlier revisions of this file

Two claims that appeared here previously were wrong and are recorded so they do
not get repeated:

- This file used to describe PR #487 as landing four environment-variable knobs
  (`ATE_GOLDEN_WARMUP_SECONDS`, `ATE_ROUTE_TIMEOUT_SECONDS`,
  `ATE_EXTPROC_TIMEOUT_SECONDS`, `ATE_RESUME_TIMEOUT_SECONDS`) plus a runsc flag
  gate (`ATEOM_RUNSC_ALLOW_CONNECTED_ON_SAVE`). None of those exist upstream;
  they were from the pre-rescope revision of the branch and never merged.
- It also claimed the public `gvisor.dev` releases "crash the sentry ~30–60s in"
  on this workload, and that a pinned `runsc.url` was therefore required. That
  is not true on current builds: the actor checkpoints and restores fine on the
  stock releases the default gVisor `SandboxConfig` ships (verified on the
  20260622, 20260803 and 20260824 builds). No `runsc.url` pin is needed.
