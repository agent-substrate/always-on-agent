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

package extproc

import (
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/hermes/egress-plugin-example/policy"
)

// newRequest projects one ext_proc message onto the policy.Request a Policy
// sees: the headers Envoy decrypted, plus the identity the gateway captured.
func newRequest(attr map[string]*structpb.Struct, headers *extprocv3.HttpHeaders) *policy.Request {
	req := requestFromHeaders(headers)
	req.ActorSpiffe = actorFromAttributes(attr)
	req.Atespace, req.ActorName = parseActor(req.ActorSpiffe)
	return req
}

// requestFromHeaders projects the ext_proc header map onto a policy.Request.
func requestFromHeaders(headers *extprocv3.HttpHeaders) *policy.Request {
	all := make(map[string]string)
	for _, header := range headers.GetHeaders().GetHeaders() {
		all[strings.ToLower(header.GetKey())] = headerValue(header)
	}
	return &policy.Request{
		Method:    all[":method"],
		Authority: authorityOf(all),
		Path:      all[":path"],
		Scheme:    all[":scheme"],
		Headers:   all,
	}
}

// authorityOf prefers :authority and falls back to host.
func authorityOf(headers map[string]string) string {
	if authority := headers[":authority"]; authority != "" {
		return authority
	}
	return headers["host"]
}

// headerValue reads a header value, preferring raw_value.
//
// Recent Envoy sends values in HeaderValue.raw_value (bytes) and leaves
// HeaderValue.value (string) empty.
func headerValue(header *corev3.HeaderValue) string {
	if raw := header.GetRawValue(); len(raw) > 0 {
		return string(raw)
	}
	return header.GetValue()
}
