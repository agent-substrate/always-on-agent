# Fleet configuration

One file describes a fleet of agents. `render_fleet.py` turns it into the
registry and shape-planning files. `deploy_fleet.sh` publishes the registry only.

```
fleet.json ──► render_fleet.py ──► out/agents-registry.json   (gateway ConfigMap)
                                └► out/shapes/<name>.env      (planning/build inputs)
```

## Two axes, deliberately separate

A **shape** is *what* an agent is: persona, model, tool set.
An **agent** is a runtime instance with channels, scheduled jobs and a duty cycle.

Ten agents may share one shape or have ten of their own. Both are expressible, and
which one is being deployed is readable off the file rather than inferred.

## What is per-agent, per-shape, and global

This is the part worth reading before planning a fleet, because the boundaries are
set by Substrate's model and not by preference.

| Property | Varies by | Why |
|---|---|---|
| Agent ID | **per agent** | Stable routing key |
| Channels, timezone, idle tier | **per agent** | Rows in the gateway registry |
| Scheduled jobs and their cadence | **per agent** | Rows in the gateway registry |
| Duty cycle | **per agent** | Sizing input. Not deployed, see below |
| Model provider, base URL, default model | **per shape** | Environment on the ActorTemplate, which is immutable |
| Persona and tool set | **per shape, today per image** | Briefing in `AGENTS.md`; additional tools/skills require a custom image |

**Different runtime shapes require separate templates and deployment wiring.**
A gateway deployment currently uses one configured template for all its agents.
The renderer does not deploy templates or implement per-agent shape selection.
Use separately configured gateway deployments and registries for different
templates. Each template has its own golden snapshot and warm-up requirements.

**Persona is currently baked into the actor image.** Behaviour comes from
`AGENTS.md` in the core image. Different personas require separate
images/templates; the gateway forwards the input without adding a persona.

## Duty cycle

Recorded per agent and not deployed anywhere. It exists because fleet size is not
the number that sizes a cluster: `WorkerPool.replicas` caps how many actors are
awake *at once*, and a suspended actor holds no worker. Duty cycle is what turns
fleet size into concurrency, and therefore into density and cost.

```json
"duty_cycle": {
  "messages_per_workday": 12,
  "median_gap_minutes": 34,
  "active_hours": "09:00-18:00",
  "active_days": "Mon-Fri"
}
```

## Deploying

`deploy_fleet.sh` is the last mile only. The cluster, Substrate, the images and the
gateway come first, and that is `hermesctl`:

```
hermesctl cluster-up            GKE cluster, gVisor pool, snapshot bucket
hermesctl substrate-install     ate-system
hermesctl build                 actor + gateway images
hermesctl deploy                workerpool, ActorTemplate, gateway
```

Then:

```
./deploy_fleet.sh fleet.json              # plan: renders, diffs against live, changes nothing
./deploy_fleet.sh fleet.json --apply      # publish registry, rollout-restart the gateway
hermesctl fleet                           # runtime state and usage
```

It plans by default. A wrong registry does not error, it silently routes people to
the wrong agent, so the diff is shown before anything is written.

The restart is a rollout rather than a delete: the replacement becomes Ready before
the old pod terminates, which is the path measured as lossless for in-flight
messages. A forced delete is not.

## Validation

`render_fleet.py` refuses to emit anything it cannot vouch for. Each check maps to
a failure that is otherwise late and quiet:

| Rejected | Because |
|---|---|
| Unknown shape | The agent would deploy with no template |
| Duplicate agent id | Two rows fighting over one actor |
| `idle_tier` outside `always-on / short / medium / long` | Silently falls back to the default |
| A job with no `deliver_to` | Burns a wake and a model call writing into a log |

The renderer emits routing, scheduling and idle-tier configuration. Shape tool
lists describe intended image contents; the renderer does not install skills
or build distinct images automatically.
