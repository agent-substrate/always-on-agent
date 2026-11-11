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
"""Render the gateway's /fleet JSON as something a human can read.

Deliberately a separate file rather than inline in the shell script: the first
version interpolated JSON into a heredoc, which breaks on the first quote or
dollar sign in the data.
"""
import json, sys, time


def ago(ts):
    if not ts:
        return "never"
    s = time.time() - ts
    if s < 90:
        return f"{int(s)}s ago"
    if s < 5400:
        return f"{int(s / 60)}m ago"
    if s < 86400 * 2:
        return f"{int(s / 3600)}h ago"
    return f"{int(s / 86400)}d ago"


def main(path: str) -> int:
    with open(path) as fh:
        d = json.load(fh)
    t = d["totals"]

    if not d.get("control_plane_reachable"):
        print("  !! control plane unreachable: live state is unknown below. "
              "Counters are still accurate.\n")

    print(f"  {'AGENT':38} {'STATE':10} {'TURNS':>6} "
          f"{'FAIL':>5} {'TOKENS':>10}  LAST")
    for a in d["agents"]:
        flag = "   <-- FAILING" if a["consecutive_failures"] else ""
        name = a["agent_id"]
        print(f"  {name[:38]:38} {a['state']:10} "
              f"{a['turns']:>6} {a['failures']:>5} {a['tokens']:>10,}  "
              f"{ago(a['last_turn'])}{flag}")
        if a["consecutive_failures"]:
            print(f"      {a['consecutive_failures']} consecutive failures - "
                  f"{a['last_error'][:78]}")

    # Historical agents remain visible after removal from the registry.
    for o in d.get("unregistered_with_history", []):
        print(f"  {'(agent ' + o['agent_id'][:22] + ')':38} "
              f"{'- gone from registry -':22} {'':10} {o['turns']:>6} "
              f"{o['failures']:>5} {o['tokens']:>10,}  {ago(o['last_turn'])}"
              f"   <-- offboarded?")

    cost = f", ${t['cost']:.2f}" if t.get("cost") is not None else ""
    print(f"\n  {t['agents']} agents · {t['turns']} turns · {t['failures']} failures "
          f"· {t['tokens']:,} tokens{cost}")
    if not t.get("cost"):
        print("  (set MODEL_PRICE_PER_MTOK_IN/OUT on the gateway to see cost in money)")
    if t["failing_now"]:
        print(f"\n  {t['failing_now']} agent(s) failing right now - run: hermesctl doctor")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
