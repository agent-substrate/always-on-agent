# Egress plugin

What Substrate calls on actor egress: a per-actor destination firewall, and a
credential injector on chains that carry the real request. See Status for which
of those holds on the egress chain today.

## Why

An agent that holds an API key can leak it: anything the agent can read, a
prompt injection can exfiltrate. So the agent is given no key. It makes an
ordinary request, and on the way out Substrate asks this service what to attach.
The credential lives here and never enters the sandbox.

Substrate provides the interception. This plugin applies operator-configured
destination rules and credentials to authenticated actor requests.

## Status

The plugin supports the filter-state and certificate inputs described below.
Verify the actual Envoy chain on the installed Substrate build before enabling
credential injection; historical test results are not a compatibility guarantee.

> [!NOTE]
> Substrate renamed the actor identity filter-state object from
> `ate.actor.identity` to `dev.ate.actor.identity` on 1 September 2026. The
> plugin accepts both names so the plugin and control plane can be upgraded in
> either order. The current name is preferred when both are present.

That is a change of venue from where this started. The first version ran against
a hand-patched Envoy in kind, because
`--experimental-additional-egress-extproc-service` did not exist upstream yet.
It does now, and the real extension point differs from the hand-patched one in
every way that mattered.

### What the flag actually requires

    go run ./cmd/ate-setup deploy ate-system \
      --credential-provider='{"name":"k8s.io"}' \
      --experimental-additional-egress-extproc-service=hermes/hermes-egress-plugin:50051

(sdsmint, the MITM leg this needs, is always installed from Substrate v1.)

It reads like "point me at a Service" and it is three contracts. Each of them
fails in a way that does not name itself, so they are worth stating plainly.

**1. Identity is a request attribute, not a header.** The generated filter sets
`request_attributes: [filter_state['dev.ate.actor.identity']]`. On the inner MITM
listener there is no `x-forwarded-client-cert` at all - the actor's mutual TLS
was terminated outside, on the CONNECT leg, so there is no client certificate on
the connection to forward. A plugin that only reads XFCC refuses every request
on that leg as unidentified.

**2. The gateway dials mutual TLS, with two different signers.** The processor
must serve a `servicedns.podcert.ate.dev` certificate (the gateway pins its DNS
SAN to `<service>.<namespace>.svc`) and must verify the *client* against the
`podidentity.podcert.ate.dev` CA, because that is what signed the gateway. The
generated Envoy cluster names servicedns twice and podidentity only as a file
path, so verifying clients against the servicedns bundle is the natural mistake
- and it fails every handshake before a gRPC stream exists, which means the
processor logs nothing whatsoever. TLS 1.3 is pinned as both floor and ceiling.

Both files are projected by the kubelet and **rotate well inside this process's
lifetime**, so they are re-read rather than loaded at startup, and the rotation
window is handled explicitly: while the files are briefly unreadable - the
kubelet swaps the `..data` symlink, so `ENOENT` is the normal failure, not a rare
one - the processor keeps serving the configuration it last loaded, and logs at
warn each time it does. That is bounded by the cached certificate's own
`NotAfter`: once the cached leaf expires the handshake fails, because a component
quietly presenting an expired certificate for hours is the *worse* outage and is
the one we filed against `atenet-egress`. A rotation should trip this once and
recover; a warn line that repeats means the projection itself has broken and you
have until the leaf expires to notice.

**3. It fails closed.** `failure_mode_allow: false`, `timeout: 2s`,
`message_timeout: 2s`. No reachable processor means no egress for any actor.
Envoy logs `no healthy upstream`; inside the sandbox that arrives as *the model
provider returned HTTP 500 with an empty body*, which points diagnosis at
Gemini. Deploy the processor immediately after the install, not later.

### Verified on the acceptance cluster

Per-actor destination policy is enforced on live traffic, with identity taken
from `filter_state['dev.ate.actor.identity']` and logged as `identity_from:
filter_state`. An allowed actor's request reaches the upstream and the
upstream's own response comes back; a denied one is stopped at the proxy with a
403 naming the actor and the host.

The kind results below still hold for the CONNECT leg and are what the unit
tests cover:

| actor | destination | result |
| --- | --- | --- |
| `egress-demo` | its allowed host | tunnel established, upstream reached |
| `egress-demo` | any other host | refused, 403 on the CONNECT |
| `egress-quarantined` | the same host `egress-demo` was allowed | refused |
| `egress-quarantined` | any other host | refused |

### Credential injection: still not on the CONNECT leg, but the MITM leg is new

Substrate's egress listener terminates CONNECT (`connect_matcher`,
`upgrade_type: CONNECT`) and splices raw TCP to the target. On *that* leg the
only headers Envoy parses are the CONNECT's own, so a header added here is
consumed establishing the tunnel and never reaches the upstream. We confirmed
that rather than inferred it: the plugin reported a header mutation and the
upstream's echo of the request did not contain the header.

The MITM leg is different - it decrypts the tunnelled request, which is why the
filter can see a method and a path there at all. Injection on that leg has not
been exercised yet, because the acceptance configuration runs
`MODEL_CREDENTIAL=none` and enforces destinations only. Turning it on is a
config change, not a code change.

Until then, on the CONNECT leg the plugin detects the CONNECT at request time,
declines to claim an injection it cannot deliver, logs `credential NOT injected`
for every such call, and allows the request - nothing leaks either way, and
refusing would break every model call. Making that a refusal is a one-line
change in `Policy.Decide`.

### Addresses versus names, per leg

On the CONNECT leg the authority is the address the actor already resolved, so
`https://example.com/` arrives as `104.20.23.154:443` and the name never does.
Substrate's own egress handler says the same
(`cmd/atenet/internal/router/egress/egress.go`). On the MITM leg the authority
is the hostname, which is why the acceptance registry is written in names and
works. A policy meant to cover both legs needs both forms.

## Turning it on and off

Both switches live in `deploy/env.sh` and are applied by `hermesctl`. Nothing is
done by hand.

    hermesctl egress-status     what the cluster is actually doing right now
    hermesctl egress-plugin     build and deploy the plugin (sdsmint clusters)
    hermesctl egress-on         apply what env.sh asks for (pre-sdsmint clusters)
    hermesctl egress-off        put everything back

`egress-plugin` and `egress-on` are not interchangeable and each refuses to run
on the other's cluster. `egress-on` predates the flag: it applies the
non-sdsmint `atenet-egress.yaml` and patches Envoy itself, which on an sdsmint
cluster would replace the dataplane with one that has no MITM leg and no trust
bundle, and no actor would start. `egress-plugin` does the half that is still
needed - build the image, render the registry, roll the Deployment.

`HERMES_EGRESS=1` routes actor egress through Substrate's gateway. The gateway on
its own authenticates the actor certificate and allows every destination, so this
should not change what an agent can reach. `HERMES_EGRESS_PLUGIN=1` adds this
plugin on top, which is the part that can block traffic, so it is a separate
switch you can drop without giving up the gateway. `EGRESS_ALLOW` is the list of
addresses actors may reach.

Three things worth knowing before turning it on:

- **The allowlist has to include the gateway itself.** The turn-completion
  callback an actor makes to `hermes-gateway.hermes.svc.cluster.local` is actor
  egress like any other and policy applies to it, even though it never leaves
  the cluster. Leave the gateway off and turn signalling dies *silently*: turns
  keep working, actors stop suspending on their own signal and fall back to the
  idle timeout, and it presents exactly like the destroyed-caller-IP fault. Found
  the hard way twice - once here and once in review, on a separate cluster. The
  rendered registry carries it in `defaults`; a hand-written `EGRESS_ALLOW` will
  not unless you put it there.
- The setting reaches an actor when it is resumed, so an actor that is already
  running keeps the old behaviour until it is cycled. `egress-on` cycles them,
  and says so loudly if it cannot.
- `egress-off` takes the filter out of the chain and waits for Envoy to reload
  *before* removing the plugin. The filter is `failure_mode_allow: false`, so
  doing it the other way round denies all egress for the length of the rollout.

`egress-on` refuses to start on a cluster whose `ate-api-server` has no egress
gateway address flag (`--default-egress-gateway-address` on Substrate main,
`--egress-gateway-address` in v0.1.0 and v0.2.0), rather than half applying and
leaving a crash looping control plane.

## How identity works

Two sources, because there are two legs.

On the **MITM leg** - the one carrying the real request, and the one the
`--experimental-additional-egress-extproc-service` filter attaches to - identity
arrives as a request *attribute*:

    request_attributes:
    - filter_state['dev.ate.actor.identity']

The value is the actor's SPIFFE ID, republished by the CONNECT leg that already
validated the certificate. It is the same value the gateway's own access log
prints as `%FILTER_STATE(dev.ate.actor.identity:PLAIN)%`, which is a free way to
confirm it is populated before deploying anything. The attribute map is keyed by
the contributing filter's name and the value sits under the literal CEL
expression, so `ActorFromFilterState` checks every contributor and accepts the
bare object name too.

On the **CONNECT leg** there is no filter state to read but Envoy does populate
`x-forwarded-client-cert`, and `ActorFromXFCC` parses the presented chain.

`identify` tries filter state first. Not because the two disagree - on the
CONNECT leg both are present and identical by construction - but because it is
the only source on the MITM leg, and preferring one everywhere keeps a single
path exercised on every request instead of each being exercised half the time.

Neither source is attacker-controllable. The filter chain sets
`forward_client_cert_details: SANITIZE_SET`, so whatever an actor puts in the
header is discarded and replaced by Envoy's own value; and filter state is not
reachable from the wire at all. An actor cannot name itself, only present a
certificate.

The leaf carries `spiffe://substrate-actor.local/atespace/<atespace>/actor/<name>`,
and both readers end at the same parser.

> The sample in the Authn doc reads `req.Attributes` - right idea, and it is
> what the real filter populates. It does not compile as written: `handleCallout`
> returns a struct with `Action` while `evaluate` tests `.Allow`, and it sets
> `req.Actor` where the field is `ActorSpiffe`.

### Telling in-cluster callees who is calling: `X-Hermes-Actor`

Interception destroys the thing in-cluster services used to identify actors by:
their own network address. Before the MITM leg, an actor calling the Hermes
gateway arrived from the worker Pod's IP, and the gateway asked the control
plane which actor lives on the Pod at that address. Through the gateway every
actor arrives from one address - the egress gateway's - and Envoy does not
append `x-forwarded-for` on that leg (there is no `use_remote_address` anywhere
in the xds builder), so the actor's own address is not recoverable downstream at
all. The symptom is not subtle once you know to look for it: the Hermes
gateway's turn-completion endpoint began refusing every signal it received,
fleet-wide, the moment the plug-in was switched on.

So the plug-in restates on the inside what it verified on the outside. On every
allowed request to an **in-cluster** destination it sets

    x-hermes-actor: spiffe://substrate-actor.local/atespace/<atespace>/actor/<name>

Two rules make that safe, and both are enforced here rather than left to the
reader, because a reader that has to remember a rule eventually forgets it:

1. **It is set or removed on every allowed request, never passed through.** The
   header is `OVERWRITE_IF_EXISTS_OR_ADD` when we set it and explicitly removed
   when we do not - including on the fail-open path, where letting an
   unidentified caller assert an actor id would be a real privilege gain rather
   than a diagnostic one. An agent that writes the header itself cannot make it
   survive.
2. **Only in-cluster destinations are told.** `inCluster` matches on the
   authority's *name* - a bare Service name, or one ending `.svc`,
   `.svc.cluster.local`, `.cluster.local` - and not on address ranges: a private
   IP says nothing about who owns it, and the CONNECT leg's authority is an
   address the actor resolved itself. Third parties get the header stripped, so
   the fleet's internal naming does not leak to Gemini.

The reader's half of the contract is the other rule and it lives in the Hermes
core gateway (`gateway/caller_auth.py`): the stamp is believed **only**
when the peer address belongs to a Pod running under the `atenet-egress`
ServiceAccount in `ate-system`, and only after resolving the peer as a worker
Pod has failed. Address always wins. That ordering is what stops an actor that
reaches the gateway directly from asserting a different actor's identity, and
pinning the ServiceAccount rather than the `app` label means the address the
gateway trusts belongs to the same principal this plug-in pins on the other side
of its own connection (`spiffe://cluster.local/ns/ate-system/sa/atenet-egress`).

`stamp_test.go` covers both halves that live here, including an actor that
supplies its own `x-hermes-actor` naming someone else, and 12 `inCluster` cases -
among them `hermes-gateway.hermes.svc.cluster.local.attacker.example`, which is
a third party.

The header is trusted only with the peer checks described above. It is not a
standalone signed token and must not be accepted from arbitrary callers.

## Configuration boundaries

Credential selection, policy evaluation and request inspection are configured
separately:

- `MODEL_CREDENTIAL` selects the model credential source.
- `EGRESS_POLICY_SOURCE` selects where destination policy comes from.
- `EGRESS_EXTPROC_MODES` describes the processing phases supplied by Envoy.

Configure these for the installed Substrate version. Declaring a processing
mode does not make Envoy send that phase, and credential injection requires a
chain that exposes the actual upstream request, not only its CONNECT tunnel.

## What it can enforce depends on Envoy

`EGRESS_EXTPROC_MODES` tells the plugin which phases Envoy actually sends it, and
it is logged at startup so the gap is visible in an operator's log:

| value | consequence |
|---|---|
| `headers` (default) | allowlist and credential injection only. Calls that can only be judged from their payload (a permission change, for example) are **refused**, not allowed unread. |
| `headers,body` | payload rules become enforceable |
| `headers,body,response` | injected credentials can be stripped from responses, so an upstream cannot echo one back into the sandbox |

Substrate does not currently let the plugin's operator choose these phases.
Verify what the installed Envoy configuration supplies; do not claim payload or
response enforcement when those phases are unavailable.

## Policy sources

Selected with `EGRESS_POLICY_SOURCE`, so where policy lives can change without
touching the request path:

| value | behaviour |
|---|---|
| `static` (default) | one allowlist for every actor, compiled in |
| `registry` | per-agent rules from `EGRESS_REGISTRY_FILE`, mountable as a ConfigMap; an empty list quarantines an actor |
| `passthrough` | allow everything and only inject credentials, leaving denial to Substrate's per-actor egress policy API |

`passthrough` is the intended end state once that API lands: two engines
enforcing the same rule is two places to disagree, and the one closer to the
network should win.

## What it injects

`MODEL_CREDENTIAL` picks the source:

| value | behaviour |
|---|---|
| `oauth` (default) | mints a short lived token from a service account and refreshes it on a timer. `Authorization: Bearer`. |
| `static` | reads a plain API key from a file. |

`oauth` is preferred. If a credential leaks, whether through an echoed response
header or anything else, an hour on one scope is a smaller problem than a
standing key with no expiry.

Refreshing happens in the background, never during a request. Envoy allows a
callout 2 seconds and that is not ours to set, so the request path only reads a
value already in memory. A missing or expired token is a refusal, not a fetch:
blocking on a token endpoint that is already failing would turn a clean refusal
into stalled agent traffic.

## Failure mode

Envoy's fail-open/fail-close for the ext_proc filter is Substrate's to configure
and not ours - and it configures `failure_mode_allow: false`. So our own side is
explicit too: `EGRESS_FAIL_MODE` is `closed` by default, refusing a request whose
caller cannot be identified. `open` lets it through **uncredentialed** and exists
for diagnosis, not for running.

A refusal is written to be safe to pass on. The 403 body carries no newline and
no other control character, because an agent that gets an unexpected error from
its model provider tends to put the provider's text into something structured. In
Hermes' case a trailing newline in our body reached an aiohttp response header
and raised `Forbidden control character detected in headers` - turning a clean,
explainable 403 into a 502 with an empty body and a stack trace pointing at the
agent rather than at the policy. `sanitizeReason` keeps that from recurring
whatever a future policy puts in a reason string.

## Timeouts

Envoy allows an ext_proc callout 2 seconds per message on the egress chain
(`timeout` and `message_timeout` in the generated filter) and that is not
configurable by us. The 5s `defaultExtProcMessageTimeout` in the router source is
the *ingress* value and does not apply here. Everything on the request path is therefore local: the
credential is read from a file at startup and nothing here makes a network call.
If a future version mints per-actor tokens, they must be cached ahead of the
request rather than fetched inside it.

## Run the tests

```bash
go test ./...
```

`transport_test.go` runs real TLS 1.3 handshakes against miniature stand-ins for
the two pod-certificate signers. That is deliberate: the two-signer mistake in
Status compiles, starts, and logs a healthy listener, so only a handshake catches
it - and when it bites, the server-side error is `tls: client didn't provide a
certificate` about a client that certainly did.

## Lifecycle callbacks

The verified actor stamp also protects busy/completion callbacks to the gateway.
