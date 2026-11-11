// Copyright 2026 Nous Research
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// hermes-egress-plugin: the credential injector Substrate calls on actor egress.
//
// WHAT THIS IS FOR. An agent that holds an API key can leak it: anything the
// agent can read, a prompt injection can exfiltrate. So the agent is given no
// key at all. It makes an ordinary request, and on the way out Substrate asks
// this service what to attach. The credential lives here and never enters the
// sandbox.
//
// Substrate provides the interception. This plugin applies operator-configured
// destination rules and credentials to authenticated actor requests.
//
// HOW IT IS WIRED.
//
//	hack/install-ate.sh --deploy-atenet \
//	  --experimental-additional-egress-extproc-service=hermes/hermes-egress-plugin:50051
//
// That flag reads like "point me at a Service" and is three contracts. It
// generates a filter that sends the caller's identity as a request ATTRIBUTE
// rather than a header (see identity.go), an upstream cluster that dials us over
// mutual TLS with a pinned SAN (see transport.go), and it sets
// failure_mode_allow: false - so this service is in the hard path of every
// outbound call any actor makes, and its absence is a total egress outage, not a
// degraded mode.
//
// TIMEOUT. On the egress chain Envoy gives an ext_proc callout 2 SECONDS per
// message, set in manifests/ate-install/atenet-egress.yaml, and that is not
// configurable by us. The 5s defaultExtProcMessageTimeout in the router source
// is the ingress value and does not apply here. Everything on this path must
// therefore be local: the credential is read from a file at startup, and nothing
// here makes a network call. If a future version mints per-actor tokens, they
// MUST be cached ahead of the request rather than fetched inside it.
//
// STATUS. Ported to the real extension point and exercised against the Envoy
// that Substrate 4c1b37d installs, on the acceptance cluster, on the MITM leg.
// Before that it had only ever run against an Envoy in kind, and the two differ
// in both of the ways that mattered: identity source and transport.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	xfccHeader    = "x-forwarded-client-cert"
	authorityHdr  = ":authority"
	pathHdr       = ":path"
	methodHdr     = ":method"
	listenDefault = ":50051"
)

// actorHeader carries the verified caller identity to IN-CLUSTER services, and
// exists because interception destroys the thing they used to identify callers
// by: their own network address.
//
// Hermes' gateway answers "which actor is calling?" by looking up the source IP
// in the worker Pod list. With egress interception on, every actor's request -
// including its own callback to /agent/turn-ended - arrives from the egress
// proxy instead, so that lookup fails for everyone and turn signalling is
// refused fleet-wide. The symptom is not an error anywhere the operator looks:
// actors simply stop reporting that they have finished and fall back to the
// idle timeout.
//
// This header restores the answer from the one place on this leg that still
// knows it: the identity Envoy verified and handed us in filter state. It is
// therefore exactly as trustworthy as the policy decision above it, and no more.
//
// TWO RULES MAKE IT SAFE, and both are enforced here rather than in the reader:
//
//  1. It is set or REMOVED on every allowed request, never passed through. An
//     agent that sets it itself must not have its value survive, or an agent
//     could name another actor and end someone else's turn.
//  2. It is only ever set for in-cluster destinations. A receiver has to trust
//     the proxy to believe it, and a third party has no reason to - sending it
//     to a model provider would leak our actor names for nothing.
const actorHeader = "x-hermes-actor"

// failMode decides what happens when we cannot identify the caller or the
// policy errors. Substrate does not let us configure Envoy's own fail-open /
// fail-close for the ext_proc filter, so we make OUR side explicit instead.
//
// Default closed. An unidentified caller reaching the network with no credential
// is the outcome this service exists to prevent, and a refusal is loud and
// debuggable where a silent pass-through is neither.
type failMode int

const (
	failClosed failMode = iota
	failOpen
)

func (f failMode) String() string {
	if f == failOpen {
		return "open"
	}
	return "closed"
}

type server struct {
	extprocv3.UnimplementedExternalProcessorServer
	policy *Policy
	onFail failMode
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	ctx := context.Background()
	policy, err := NewPolicy(ctx)
	if err != nil {
		slog.Error("cannot start", slog.Any("err", err))
		os.Exit(1)
	}

	onFail := failClosed
	if strings.EqualFold(strings.TrimSpace(os.Getenv("EGRESS_FAIL_MODE")), "open") {
		onFail = failOpen
		slog.Warn("EGRESS_FAIL_MODE=open: unidentified callers will be ALLOWED THROUGH " +
			"without a credential. This is for diagnosis, not for running.")
	}

	addr := envOr("LISTEN_ADDR", listenDefault)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("cannot listen", slog.String("addr", addr), slog.Any("err", err))
		os.Exit(1)
	}
	opts, tlsMode, err := serverTransport()
	if err != nil {
		slog.Error("cannot configure the listener", slog.Any("err", err))
		os.Exit(1)
	}
	s := grpc.NewServer(opts...)
	extprocv3.RegisterExternalProcessorServer(s, &server{policy: policy, onFail: onFail})
	slog.Info("egress plugin up", slog.String("addr", addr), slog.String("transport", tlsMode),
		slog.String("on_failure", onFail.String()),
		slog.String("credential", policy.CredentialName()))
	// Say what this configuration does and does not police, so the gap is in
	// the log rather than only in the source.
	for _, line := range policy.caps.Explain() {
		slog.Info("capability", slog.String("detail", line))
	}
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", slog.Any("err", err))
		os.Exit(1)
	}
}

// Process is the ext_proc stream. Envoy sends one message per phase of the
// request and expects exactly one response to each, in order. Skipping a reply
// does not error, it HANGS the request until Envoy's timeout, which is the
// nastiest failure mode available here, so every branch below sends something.
func (s *server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		var resp *extprocv3.ProcessingResponse
		switch v := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			// Attributes hang off the ProcessingRequest, not off the headers
			// message, so both have to be passed down.
			resp = s.onRequestHeaders(req.GetAttributes(), v.RequestHeaders)
		default:
			// Nothing to do for body/trailer/response phases, but we must
			// still answer or the stream stalls.
			resp = continueResponse()
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *server) onRequestHeaders(attrs map[string]*structpb.Struct, h *extprocv3.HttpHeaders) *extprocv3.ProcessingResponse {
	headers := flatten(h.GetHeaders())
	actor, source, err := identify(attrs, headers[xfccHeader])
	if err != nil {
		slog.Warn("caller not identified", slog.Any("err", err),
			slog.String("authority", headers[authorityHdr]))
		if s.onFail == failOpen {
			// Still strip the identity header. Fail-open is for diagnosis, and
			// letting an unidentified caller assert an actor id to an in-cluster
			// service would be a real privilege gain, not a diagnostic one.
			return allowResponse(nil, "")
		}
		return denyResponse("caller could not be identified")
	}

	d := s.policy.Decide(actor, headers[authorityHdr], headers[pathHdr], headers[methodHdr])
	if !d.Allow {
		slog.Info("denied", slog.String("actor", actor.Spiffe), slog.String("identity_from", source),
			slog.String("authority", headers[authorityHdr]), slog.String("reason", d.Reason))
		return denyResponse(d.Reason)
	}
	if d.CredentialUndeliverable {
		// Warn every time rather than once: an operator who configured
		// credential injection needs this in the log next to the call it
		// applies to, not buried in a single line at startup.
		slog.Warn("credential NOT injected", slog.String("actor", actor.Spiffe),
			slog.String("authority", headers[authorityHdr]),
			slog.String("why", "this chain terminates CONNECT, so a header added here is "+
				"consumed establishing the tunnel and never reaches the upstream"))
	}
	// Only in-cluster callees get told who is calling; see actorHeader.
	stamp := ""
	if inCluster(headers[authorityHdr]) {
		stamp = actor.Spiffe
	}
	slog.Info("allowed", slog.String("actor", actor.Spiffe), slog.String("identity_from", source),
		slog.String("authority", headers[authorityHdr]), slog.Int("headers", len(d.Headers)),
		slog.Bool("identity_stamped", stamp != ""))
	return allowResponse(d.Headers, stamp)
}

// inCluster decides whether the destination is somewhere the actor header can be
// believed and is worth sending.
//
// Deliberately by NAME and not by address. Kubernetes DNS names are the only
// spelling we can tell apart from the outside world with certainty: a private
// address range says nothing - a VPC-peered model endpoint is 10.x too - and
// getting that wrong leaks actor identities to a third party. An actor that dials
// its own gateway by ClusterIP therefore goes unstamped and falls back to
// whatever the callee does with an unidentified caller, which is the safe
// direction to be wrong in.
func inCluster(authority string) bool {
	host := strings.ToLower(strings.TrimSpace(authority))
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return false
	}
	// A bare Service name in the same namespace, resolved by the search path.
	if !strings.Contains(host, ".") {
		return true
	}
	for _, suffix := range []string{".svc", ".svc.cluster.local", ".cluster.local"} {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

// identify works out who is calling, from whichever of the two sources this leg
// provides, and reports which one answered.
//
// Filter state is tried first because it is the only source on the MITM leg,
// which is the leg that carries the real request. XFCC is the fallback rather
// than the other way round for a reason worth stating: on the CONNECT leg BOTH
// are present, and they are the same identity by construction - the filter-state
// object is set from the certificate Envoy validated. Preferring one source
// everywhere means one code path is exercised on every request instead of each
// being exercised half the time.
//
// Neither source is attacker-controllable. XFCC is overwritten by Envoy under
// SANITIZE_SET, and filter state is not reachable from the wire at all.
func identify(attrs map[string]*structpb.Struct, xfcc string) (*Actor, string, error) {
	actor, fsErr := ActorFromFilterState(attrs)
	if fsErr == nil {
		return actor, "filter_state", nil
	}
	actor, xfccErr := ActorFromXFCC(xfcc)
	if xfccErr == nil {
		return actor, "xfcc", nil
	}
	// Report both, because "no attributes" alone reads like a misconfigured
	// filter when the real answer may be that this is the CONNECT leg and the
	// header is what is missing, or vice versa.
	return nil, "", fmt.Errorf("filter state: %v; %s: %v", fsErr, xfccHeader, xfccErr)
}

// flatten lowercases header names so lookups are case-insensitive, which HTTP/2
// guarantees for us in practice but HTTP/1 does not.
func flatten(m *corev3.HeaderMap) map[string]string {
	out := make(map[string]string, len(m.GetHeaders()))
	for _, h := range m.GetHeaders() {
		v := h.GetValue()
		if v == "" && len(h.GetRawValue()) > 0 {
			v = string(h.GetRawValue())
		}
		out[strings.ToLower(h.GetKey())] = v
	}
	return out
}

func continueResponse() *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}},
		},
	}
}

// allowResponse sets the credential headers on the outbound request, and either
// stamps the caller's identity for an in-cluster callee or strips the identity
// header entirely.
//
// The strip is not belt-and-braces, it is the whole guarantee: without it an
// agent could set x-hermes-actor itself on a request to a host we do not stamp
// for - or on any request at all, if a future edit narrows the stamping rule -
// and name any actor it liked to the receiver. Removing it on every allowed
// request means the header seen by a callee was either written here or is
// absent, with no third state.
func allowResponse(hs []Header, stampActor string) *extprocv3.ProcessingResponse {
	setters := make([]*corev3.HeaderValueOption, 0, len(hs)+1)
	for _, h := range hs {
		setters = append(setters, &corev3.HeaderValueOption{
			Header: &corev3.HeaderValue{Key: h.Key, RawValue: []byte(h.Value)},
			// OVERWRITE_IF_EXISTS_OR_ADD, so an actor cannot pre-set the
			// credential header and have its own value survive.
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		})
	}
	mutation := &extprocv3.HeaderMutation{}
	if stampActor == "" {
		mutation.RemoveHeaders = []string{actorHeader}
	} else {
		setters = append(setters, &corev3.HeaderValueOption{
			Header:       &corev3.HeaderValue{Key: actorHeader, RawValue: []byte(stampActor)},
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		})
	}
	mutation.SetHeaders = setters
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{
				Response: &extprocv3.CommonResponse{HeaderMutation: mutation},
			},
		},
	}
}

// denyResponse stops the request at the proxy with a 403.
//
// The body carries no newline and no other control character, and that is not
// cosmetic. An agent that receives an unexpected error from its model provider
// tends to put the provider's text somewhere structured - a header, a log field,
// a retry annotation - and a bare \n in it is a header-injection sequence. Hermes
// does exactly this: with a trailing newline in the body its aiohttp response
// writer raised "Forbidden control character detected in headers", turning our
// clean, explainable 403 into a 502 with an empty body and a stack trace that
// points at the agent instead of at the policy. sanitizeReason keeps that
// property no matter what a future policy puts in a reason string.
func denyResponse(reason string) *extprocv3.ProcessingResponse {
	reason = sanitizeReason(reason)
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extprocv3.ImmediateResponse{
				Status:  &typev3.HttpStatus{Code: typev3.StatusCode_Forbidden},
				Details: reason,
				Body:    []byte("egress refused: " + reason),
			},
		},
	}
}

// sanitizeReason makes a reason safe to hand to a caller that may re-emit it in
// a header: control characters become spaces, runs of whitespace collapse, and
// the result is bounded so a long policy message cannot become the response.
func sanitizeReason(reason string) string {
	cleaned := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, reason)
	cleaned = strings.Join(strings.Fields(cleaned), " ")
	if cleaned == "" {
		return "denied by egress policy"
	}
	const maxReason = 200
	if len(cleaned) > maxReason {
		cleaned = cleaned[:maxReason] + "..."
	}
	return cleaned
}
