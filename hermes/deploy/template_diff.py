#!/usr/bin/env python3
"""Decides whether a live ActorTemplate already says everything a rendered one asks for.

Background. Until 2026-09-02 the template was a Kubernetes CRD and hermesctl asked
`kubectl apply --dry-run=server` whether anything immutable had moved: the API server
answered, and a rejection meant "recreate". The CRD is gone, templates are created
through the Substrate API, and that API has no update and no dry run at all. So the
question has to be answered here.

The comparison is deliberately a SUBSET test, not equality. The server fills in
defaults and adds status, so a live template is always a superset of the manifest that
created it. What we want to know is the same thing the dry run used to tell us: is
there anything we are asking for that the live template does not already say?

Exit codes, because the caller is bash:
    0  live template already satisfies the manifest, leave it alone
    1  they differ, the caller must delete actors, delete the template and recreate
    2  no live template, the caller should create one

Usage: template_diff.py <rendered-manifest> <live-json>
  <live-json> is `kubectl ate get actor-template <name> -a <space> -o json`, or the
  empty string / a file holding nothing when the template does not exist.
"""

import json
import sys

try:
    import yaml
except ImportError:
    # Deliberately fatal. The two silent alternatives are both worse than stopping:
    # assuming "unchanged" runs a stale template against a new image and nothing ever
    # says so, and assuming "changed" deletes every actor on a routine no-op deploy.
    sys.exit(
        "template_diff.py needs PyYAML to compare the rendered template against the "
        "live one.\n"
        "  pip install pyyaml\n"
        "The Substrate API has no dry run, so this comparison is the only thing "
        "standing between a changed template and a silent no-op."
    )

# Server-populated. Present on the live object, never in a manifest, and comparing
# them would report a difference on every single deploy.
IGNORED_TOP_LEVEL = {"status"}


def differences(want, live, path=""):
    """Yields a path for everything in `want` that `live` does not already match.

    Recurses into dicts, and into lists element-wise when the lengths match, so a
    bumped image reports `containers.0.image` rather than `containers`. That is the
    single most common reason this function returns anything at all, and the whole
    point of the output is to say what moved.

    Lists of differing length are reported whole. A template's lists are containers,
    volumes and env, where a missing element is the finding and pairing them up by
    index after a removal would report every later element as changed.
    """
    if isinstance(want, dict):
        if not isinstance(live, dict):
            yield path or "(root)"
            return
        for key, sub in want.items():
            if not path and key in IGNORED_TOP_LEVEL:
                continue
            here = f"{path}.{key}" if path else key
            if key not in live:
                yield here
            else:
                yield from differences(sub, live[key], here)
    elif isinstance(want, list):
        if not isinstance(live, list) or len(want) != len(live):
            yield path or "(root)"
            return
        for i, (sub, other) in enumerate(zip(want, live)):
            yield from differences(sub, other, f"{path}.{i}")
    elif want != live:
        yield path or "(root)"


def main():
    if len(sys.argv) != 3:
        sys.exit(f"usage: {sys.argv[0]} <rendered-manifest> <live-json>")
    manifest_path, live_path = sys.argv[1], sys.argv[2]

    with open(manifest_path) as f:
        want = yaml.safe_load(f)
    if not isinstance(want, dict):
        sys.exit(f"{manifest_path} is not a single YAML document holding a template")

    try:
        with open(live_path) as f:
            raw = f.read().strip()
    except FileNotFoundError:
        raw = ""
    if not raw:
        return 2

    live = json.loads(raw)
    # `get` returns a list wrapper even for a single named template. An older client,
    # or a reader still expecting a bare object, silently sees no template here and
    # recreates on every deploy.
    if isinstance(live, dict) and "actorTemplates" in live:
        templates = live["actorTemplates"]
        if not templates:
            return 2
        live = templates[0]

    diffs = list(differences(want, live))
    if not diffs:
        return 0

    print("actor template differs from the live one at:", file=sys.stderr)
    for d in diffs:
        print(f"    {d}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
