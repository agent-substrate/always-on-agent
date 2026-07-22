# Substrate control-plane changes (upstreamed)

Running a heavy, suspend/resume agent end-to-end surfaced a few gaps in
**[`agent-substrate/substrate`](https://github.com/agent-substrate/substrate)**
(the OSS control plane, CRD group `ate.dev`). Rather than carry local patches
here — which bitrot — the fixes are upstreamed:

- **PR [agent-substrate/substrate#487](https://github.com/agent-substrate/substrate/pull/487)** —
  makes the long-running timeouts / golden warmup configurable and gates a runsc
  flag, all **defaults unchanged** (purely additive).
- **Issue [agent-substrate/substrate#465](https://github.com/agent-substrate/substrate/issues/465)** —
  the underlying suspend-safe-networking work that will remove most of the need
  to tune these knobs.

## What changed and why

| Component | Change | Why |
|---|---|---|
| `atecontroller` golden flow | golden warmup env-configurable (`ATE_GOLDEN_WARMUP_SECONDS`, default 20s unchanged) | the 20s default can checkpoint a slow-initializing, probe-less workload before it finishes warming up, capturing a dead golden |
| `atenet` route / ext_proc timeouts | env-configurable (`ATE_ROUTE_TIMEOUT_SECONDS`, `ATE_EXTPROC_TIMEOUT_SECONDS`; defaults unchanged) | long LLM turns and a cold restore-on-demand of a large snapshot can exceed the steady-state defaults |
| `atenet` background resume timeout | env-configurable (`ATE_RESUME_TIMEOUT_SECONDS`; default unchanged) | a ~60 MiB cold restore exceeded the old ceiling; cancelling the in-flight restore surfaced as a 504 |
| `ateom-gvisor` `runsc.go` | gate `-allow-connected-on-save` behind `ATEOM_RUNSC_ALLOW_CONNECTED_ON_SAVE` (default on) | some runsc builds reject the flag (`flag provided but not defined`) on `runsc start`; this lets them opt out without a code change |

**Not upstreamed (intentionally, environment-specific):** the gVisor
`SandboxConfig` `runsc.url` pin. The generic, portable finding is only that a
runsc build which survives a heavy multi-process Node.js actor is required — the
public gvisor.dev releases crash its sentry ~30–60s in.

See PR #487 for the exact diffs, rationale, and verification.
