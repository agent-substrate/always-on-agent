# Custom egress processor example

This separate Go module demonstrates how to plug authorization and header
mutations into Substrate's egress path. It is not the main Hermes egress policy
implementation; see [that integration](../egress-plugin/README.md) for runtime wiring.

## Where your code goes

Implement `policy.Policy` in `policy/` and pass it to `extproc.NewServer` in
`main.go`. The shipped `policy.AllowAll` permits requests; replace it before
relying on this example for destination restrictions.

`internal/extproc/` translates Envoy requests into the policy interface and
applies its result. The serving endpoint verifies the gateway's TLS connection
and reads the authenticated Substrate caller attributes. Do not replace those
checks with an actor-supplied HTTP header.

## Local validation

```bash
# From hermes/:
cd egress-plugin-example
go test ./...
go build ./...
```

Tests cover policy dispatch, caller parsing, header mutations and TLS handling.
They do not replace an end-to-end test against the installed Envoy configuration.

## Deployment

Use a Substrate release that supports the additional egress processor extension.
Check its installation flags and certificate projection support against that
release's documentation. The example manifest is
[`egress-plugin.yaml`](egress-plugin.yaml); it uses the `egress-example` namespace
and a `ko://` image reference that must be built and resolved before deployment.

1. Create the namespace in the intended cluster.
2. Build and publish this module into your image repository using `ko` from this
   module directory, resolving the image reference in the manifest.
3. Deploy the processor and verify its certificate projections and readiness.
4. Configure Substrate's additional egress processor service as
   `egress-example/egress-plugin:50051`, using the matching installer.
5. Configure actors to trust the egress MITM bundle when that path is enabled.
6. Test an allowed and a denied request with your policy and inspect processor logs.

The install scripts under `hack/` belong to a separate Substrate checkout, not
this integration. Do not run commands as though those paths exist here.

For the current template format and trust-bundle mounts, see
[`../k8s/hermes-actortemplate.yaml`](../k8s/hermes-actortemplate.yaml).
Render its placeholders and create it through `kubectl-ate`. Templates are
immutable; changing an existing template requires creating a new one.

If the platform is reinstalled, verify that the processor reloads the new
certificate material and trust bundles before allowing actor traffic. With
fail-closed processing, an unreachable processor blocks egress.
