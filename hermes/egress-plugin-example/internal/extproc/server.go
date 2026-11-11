// Copyright 2026 Google LLC
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

// Package extproc is the ext_proc glue between Envoy and a policy.
//
// It speaks the wire protocol, projects each message onto a policy.Request,
// renders the decision back into a ProcessingResponse, and logs what was
// decided. It holds no policy of its own and should not need editing to ship
// one -- that is the egress-plugin-example/policy package.
package extproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/hermes/egress-plugin-example/policy"
)

// denyAll refuses every request. It is not exported, because it exists for one
// caller: NewServer, when handed no policy at all. A server wired up wrong
// should fail visibly and closed rather than pass traffic, and there is no
// reason to deploy this deliberately.
type denyAll struct{}

// Evaluate refuses the request.
func (denyAll) Evaluate(_ context.Context, _ *policy.Request) policy.CalloutResult {
	return policy.Deny("extproc was started without a policy")
}

// Server is the ext_proc gRPC service extproc serves to the egress gateway's
// mitm_listener.
type Server struct {
	policy policy.Policy
}

// NewServer builds the service around a policy. A nil policy denies
// everything, so a wiring mistake shows up as refused egress rather than as
// unpoliced egress; pass policy.AllowAll{} to mean it.
func NewServer(p policy.Policy) *Server {
	if p == nil {
		slog.Error("extproc started with no policy; denying all egress")
		p = denyAll{}
	}
	return &Server{policy: p}
}

// Process implements the ExternalProcessor service.
//
// Envoy opens one stream per intercepted request and, with the headers-only
// processing_mode this filter is configured with, sends exactly one message on
// it. Every message must be answered with a response of the matching kind:
// Envoy treats a mismatched or extra response as a protocol error and fails the
// request with a 500, which under `failure_mode_allow: false` is a denial.
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := s.respond(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// respond answers one message with a response of its own kind.
//
// Only request headers carry a decision; the filter's processing_mode SKIPs
// every other kind, so reaching one means the mode drifted from what this
// server expects. That is a misconfiguration to fix, not a request to fail, so
// each is answered with the empty response of the matching kind: the request
// proceeds unmodified and the drift shows up in the log rather than as an
// outage.
//
// A message with no kind set at all is different. There is no matching
// response to send, and inventing one is the mismatch Envoy answers with a
// 500, so the stream ends instead - which is the same 500 for that request but
// without pretending to have processed it.
func (s *Server) respond(ctx context.Context, req *extprocv3.ProcessingRequest) (*extprocv3.ProcessingResponse, error) {
	kind := req.GetRequest()
	if headers, ok := kind.(*extprocv3.ProcessingRequest_RequestHeaders); ok {
		return s.authorize(ctx, req.GetAttributes(), headers.RequestHeaders), nil
	}

	resp := unmodifiedResponse(kind)
	if resp == nil {
		return nil, fmt.Errorf("unknown ext_proc message kind %T", kind)
	}
	slog.ErrorContext(ctx, "unexpected ext_proc message kind; check the filter's processing_mode",
		slog.String("message_kind", fmt.Sprintf("%T", kind)))
	return resp, nil
}

// authorize runs the policy over one request's headers and renders the answer.
func (s *Server) authorize(ctx context.Context, attr map[string]*structpb.Struct, reqHeaders *extprocv3.HttpHeaders) *extprocv3.ProcessingResponse {
	req := newRequest(attr, reqHeaders)

	decision := s.policy.Evaluate(ctx, req)

	if decision.Action == policy.CalloutDeny {
		// Info level, unlike the allow path: a denial is the case someone has
		// to explain afterwards, and the header set is most of the explanation.
		// This is the one log line here whose volume tracks misbehavior rather
		// than traffic.
		slog.InfoContext(ctx, "egress denied",
			append(requestAttrs(req), slog.String("reason", decision.Reason))...)
		return denyResponse()
	}

	slog.InfoContext(ctx, "egress allowed",
		// The headers in requestAttrs are the headers as they arrived, before
		// any injection: the mutation is applied by Envoy after this returns,
		// so a credential this request is about to be given cannot appear here.
		append(requestAttrs(req), slog.Any("credential_headers", loggedCredentials(decision.Credentials)))...)
	return allowResponse(ctx, decision)
}
