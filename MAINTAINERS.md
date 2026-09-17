The current maintainers of this repository are listed below.

This repository is an integration under the `agent-substrate` organization. What
belongs here, how it is named, and how changes to Substrate itself are handled
are set by
[`docs/integration-repos.md`](https://github.com/agent-substrate/substrate/blob/main/docs/integration-repos.md)
in core. Maintainer roles and how people move between them follow the project
[Governance](https://github.com/agent-substrate/substrate/blob/main/GOVERNANCE.md).
There is no separate governance model for this repository.

<!-- Maintainer affiliations must be updated within 30 days of employment changes. -->

| Name      | GitHub ID | Company/Organization |
| --------- | --------- | -------------------- |
| Maya Wang | mayawang  | Google               |

## What maintaining this repository means

- **It builds against Substrate as released.** No fork of core, no vendored
  patch, no dependency on an unmerged change. When this integration needs
  something core does not do, the core change lands first. The running list of
  those is in [`substrate-patches/README.md`](substrate-patches/README.md).
- **Gaps go upstream as issues or PRs** on
  [`agent-substrate/substrate`](https://github.com/agent-substrate/substrate),
  not as patches carried here.
- **Every pull request runs CI.** The checks in `.github/workflows/ci.yaml` need
  no cluster and no GCP project, so anyone can run them on a fork.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).
