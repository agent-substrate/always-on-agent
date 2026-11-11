# Who wrote this

This directory arrived as a single commit rather than as its real history, so the
authorship is written down here instead of being readable from `git log`.

It was written by Google and Nous Research together over roughly two months, and
there is no clean split anywhere in it. Almost every directory has work from more
than one person, and several files were passed back and forth. Treating any part
of it as belonging to one side would be inaccurate.

| Area | Written by |
|---|---|
| `actor/`, including `plugins/turn-signal/` | Dmitriy Vasilyev, with Maya Wang |
| `console/` | Dmitriy Vasilyev |
| `deploy/` | Maya Wang, with Dmitriy Vasilyev |
| `demo/` | Maya Wang, with Dmitriy Vasilyev |
| `docs/` | Dmitriy Vasilyev, with Maya Wang |
| `egress-plugin/` | Dmitriy Vasilyev, with Maya Wang |
| `egress-plugin-example/` | Haiyan Meng |
| `gateway/` | Maya Wang, with Dmitriy Vasilyev |
| `k8s/` | Maya Wang, with Dmitriy Vasilyev |
| `tests/` | Dmitriy Vasilyev |
| `README.md` | Maya Wang, with the About Hermes Agent and Why Hermes fits Agent Substrate sections by Hervé Bizira and Dmitriy Vasilyev (Nous Research) |

`actor/plugins/turn-signal/` is the one piece with a single author throughout. It
is entirely Dmitriy Vasilyev's, which is why its `plugin.yaml` names Nous Research
as the author.

The standalone cleanup that prepared this directory for publication, including
removing everything specific to one deployment, was done by Dmitriy Vasilyev
([@Dmitry-NousResearch](https://github.com/Dmitry-NousResearch)).

## The history

The working history, with every author and date, is in a private repository and
isn't published. This table is the record of who wrote what.
