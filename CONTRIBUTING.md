# How to contribute

Contributions are welcome. A few things to know first.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
the CLA gives us permission to use and redistribute it as part of the project.
See <https://cla.developers.google.com/> for details.

You generally only need to submit a CLA once, so if you have already submitted
one (even for a different Google project), you probably do not need to do it
again.

## Code reviews

All submissions, including submissions by project members, go through a pull
request and get reviewed. See
[GitHub Help](https://help.github.com/articles/about-pull-requests/) if pull
requests are new to you.

## What belongs here, and what belongs upstream

This repo holds reference integrations, one directory each, plus the pattern
they share. Nothing here is a patch against
[`agent-substrate/substrate`](https://github.com/agent-substrate/substrate), and
that is deliberate.

A new integration is a new top-level directory with its own README. Everything
it needs to build and deploy lives inside that directory, and the commands in
its README are run from there. What it shares with the others is the pattern,
not the code.

If you hit a gap in the Substrate control plane, file it there rather than
working around it here, and add a line to
[`openclaw/substrate-patches/README.md`](openclaw/substrate-patches/README.md) pointing at the
issue or PR. Local patches bitrot, and the workaround usually outlives the bug.

Changes to OpenClaw itself go to
[`openclaw/openclaw`](https://github.com/openclaw/openclaw). The whole point of
the plugin shape is that this repo modifies no OpenClaw core file, so a PR here
that needs one is probably the wrong shape.

## Before you open a PR

CI runs on every pull request and checks that the plugin transpiles, the YAML
parses, the shell scripts pass `shellcheck` and `bash -n`, and that no file has
picked up an em dash. You can run the same checks locally:

```bash
npx esbuild@0.24.2 openclaw/extensions/substrate/*.ts --format=esm --platform=node \
  --target=node20 --outdir=/tmp/plugin-build
shellcheck openclaw/demo/deploy-demo.sh
```

Three conventions worth knowing, because they are easy to trip over:

- **Images are pinned by digest, never by tag.** Substrate snapshots require an
  `@sha256`-pinned image, and the gateway and actor must sit on the *same*
  OpenClaw base digest, since they speak OpenClaw's own protocol to each other.
  `openclaw/build/gateway.Dockerfile` and `openclaw/build/actor.Dockerfile` get bumped together.
- **The Substrate commit is pinned too.** `kubectl-ate` and `ateom` speak
  internal protos to the control plane, so a floating clone drifts and fails
  with opaque errors instead of version messages. The pin lives in
  `openclaw/build/gateway.Dockerfile` and `openclaw/demo/README.md`, and both move together.
- **No em dashes**, in prose or in comments.

## Community guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).
