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

// Package policy is the part of this example you are meant to change.
//
// It holds the decision -- what an egress request looks like, what may be said
// about it, and the one method that says it. Everything else, the ext_proc
// wire protocol and the gateway's mTLS, lives under internal/ and should not
// need editing to ship a real policy.
//
// Nothing here imports Envoy. A policy is a function from a Request to a
// CalloutResult, and it can be written and tested without a gateway, a
// cluster, or a protobuf.
//
// To ship your own: implement Policy, and pass it to extproc.NewServer in
// main.go instead of AllowAll.
package policy

import "context"

// Policy decides whether one tunneled request may leave. This is the one place
// egress policy lives; everything around it is plumbing.
//
// An interface rather than a method on the server so that policy can be
// developed, tested, and swapped without touching the ext_proc plumbing.
//
// Evaluate is called once per request, on the request path, with the gateway
// holding the actor's connection open. It should not block on anything slow: a
// policy that calls out to a remote service adds that latency to every egress
// request, and a policy that hangs stalls the actor.
type Policy interface {
	Evaluate(ctx context.Context, req *Request) CalloutResult
}

// AllowAll permits every request. It is the policy this example ships with,
// and the one to replace.
type AllowAll struct{}

// Evaluate permits the request.
func (AllowAll) Evaluate(_ context.Context, _ *Request) CalloutResult { return Allow() }
