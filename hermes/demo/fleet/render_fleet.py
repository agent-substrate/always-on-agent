#!/usr/bin/env python3
# Copyright 2026 Nous Research
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Render one fleet description into the artefacts that actually deploy it.

A fleet.json carries more than the gateway needs, on purpose. It describes the
fleet to a human and to whoever is sizing the cluster, and only a subset of it is
configuration the gateway reads:

  registry   id, idle_tier, timezone, channels, jobs   -> gateway AGENTS_CONFIG
  shape      persona, model, tools                     -> one ActorTemplate each
  duty cycle messages/day, median gap, active hours    -> sizing, not deployed

The gateway registry defines channel routing, schedules and idle policy.
Persona, model and tool set live in
the actor image and the ActorTemplate, which is immutable and shared, so agents
that differ on any of those need different templates rather than different rows.
Emitting them separately is what keeps that visible instead of surprising someone
at deploy time.

Only runtime registry fields are emitted; shape metadata remains in the
separate planning files. The renderer does not create templates or choose a
different template for each agent.

    python3 render_fleet.py fleet.json --out out/
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Exactly the fields gateway/agents.py reads. Anything else in fleet.json is for
# humans or for sizing, and must not reach the registry.
REGISTRY_AGENT_FIELDS = ("id", "idle_tier", "timezone", "channels", "jobs")
REGISTRY_JOB_FIELDS = ("name", "cron", "prompt", "deliver_to")
VALID_TIERS = ("always-on", "short", "medium", "long")


class FleetError(ValueError):
    pass


def _check(cond: bool, msg: str) -> None:
    if not cond:
        raise FleetError(msg)


def validate(fleet: dict) -> None:
    """Fail loudly here rather than at deploy time.

    Every check below corresponds to something that otherwise fails late and
    quietly: a shape typo produces an agent with no template, a duplicate id
    produces two rows fighting over one actor, and a job without deliver_to burns
    a wake writing into a log.
    """
    _check(isinstance(fleet.get("agents"), list) and fleet["agents"],
           "fleet has no agents")
    shapes = fleet.get("shapes") or {}
    _check(isinstance(shapes, dict) and shapes, "fleet defines no shapes")

    seen_ids: set[str] = set()
    for i, a in enumerate(fleet["agents"]):
        where = f"agents[{i}]"
        aid = a.get("id")
        _check(bool(aid), f"{where}: missing id")
        _check(aid not in seen_ids, f"{where}: duplicate id {aid!r}")
        seen_ids.add(aid)

        shape = a.get("shape")
        _check(bool(shape), f"{where} ({aid}): missing shape")
        _check(shape in shapes,
               f"{where} ({aid}): unknown shape {shape!r}; defined: {sorted(shapes)}")

        tier = a.get("idle_tier") or (fleet.get("defaults") or {}).get("idle_tier")
        if tier is not None:
            _check(tier in VALID_TIERS,
                   f"{where} ({aid}): idle_tier {tier!r} not in {VALID_TIERS}")

        for j, job in enumerate(a.get("jobs") or []):
            jw = f"{where} ({aid}).jobs[{j}]"
            for f in ("name", "cron", "prompt", "deliver_to"):
                _check(bool(job.get(f)), f"{jw}: missing {f}")


def registry(fleet: dict) -> dict:
    """The AGENTS_CONFIG document, and nothing the gateway cannot parse."""
    out_agents = []
    for a in fleet["agents"]:
        row = {k: a[k] for k in REGISTRY_AGENT_FIELDS if k in a}
        if "jobs" in row:
            row["jobs"] = [{k: j[k] for k in REGISTRY_JOB_FIELDS if k in j}
                           for j in row["jobs"]]
        out_agents.append(row)
    doc = {"agents": out_agents}
    if fleet.get("defaults"):
        doc = {"defaults": fleet["defaults"], "agents": out_agents}
    return doc


def shape_envs(fleet: dict) -> "dict[str, dict]":
    """Per-shape values that belong to an ActorTemplate, not to the registry."""
    used = {a.get("shape") for a in fleet["agents"]}
    out = {}
    for name, s in (fleet.get("shapes") or {}).items():
        if name not in used:
            continue
        m = s.get("model") or {}
        out[name] = {
            "MODEL_PROVIDER": m.get("provider", ""),
            "MODEL_BASE_URL": m.get("base_url", ""),
            "MODEL_DEFAULT": m.get("default", ""),
            "_persona_file": s.get("persona_file", ""),
            "_tools": ",".join(s.get("tools") or []),
        }
    return out


def summary(fleet: dict) -> str:
    """What someone sizing the cluster needs, in one table.

    WorkerPool.replicas caps how many actors are awake at once, so the number
    that matters is concurrency, not fleet size. Duty cycle is what turns one
    into the other, which is why it is collected at all.
    """
    lines = ["", "  agent                          shape        tier     msgs/day  median gap",
             "  " + "-" * 74]
    total = 0
    for a in fleet["agents"]:
        d = a.get("duty_cycle") or {}
        n = d.get("messages_per_workday", 0)
        total += n if isinstance(n, int) else 0
        lines.append("  {:<30} {:<12} {:<8} {:>8}  {:>10}".format(
            str(a.get("id"))[:30], str(a.get("shape"))[:12],
            str(a.get("idle_tier") or (fleet.get("defaults") or {}).get("idle_tier", "-")),
            n or "-", d.get("median_gap_minutes", "-")))
    lines.append("")
    lines.append(f"  {len(fleet['agents'])} agents over "
                 f"{len({a.get('shape') for a in fleet['agents']})} shape(s), "
                 f"{total} messages/workday across the fleet")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("fleet", help="path to fleet.json")
    ap.add_argument("--out", default="out", help="output directory")
    args = ap.parse_args()

    try:
        with open(args.fleet) as fh:
            fleet = json.load(fh)
    except (OSError, json.JSONDecodeError) as e:
        print(f"  cannot read {args.fleet}: {e}", file=sys.stderr)
        return 2

    try:
        validate(fleet)
    except FleetError as e:
        print(f"  invalid fleet: {e}", file=sys.stderr)
        return 1

    os.makedirs(args.out, exist_ok=True)
    reg_path = os.path.join(args.out, "agents-registry.json")
    with open(reg_path, "w") as fh:
        json.dump(registry(fleet), fh, indent=2, sort_keys=False)
        fh.write("\n")

    shapes = shape_envs(fleet)
    shapes_dir = os.path.join(args.out, "shapes")
    os.makedirs(shapes_dir, exist_ok=True)
    for name, env in shapes.items():
        with open(os.path.join(shapes_dir, f"{name}.env"), "w") as fh:
            for k, v in env.items():
                fh.write(f"{k}={v}\n")

    print(f"  wrote {reg_path}")
    for name in sorted(shapes):
        print(f"  wrote {shapes_dir}/{name}.env")
    print(summary(fleet))
    if len(shapes) > 1:
        print(f"\n  NOTE: {len(shapes)} shapes means {len(shapes)} ActorTemplates.")
        print("  A template is immutable and carries the model, so one template")
        print("  per shape, each with its own golden snapshot to warm.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
