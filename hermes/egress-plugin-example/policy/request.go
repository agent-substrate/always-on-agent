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

package policy

import (
	"net"
	"strings"
)

// Request includes all the information needed to make an authorization decision.
//
// It is built by the ext_proc glue and handed to Evaluate already parsed. A
// policy never sees a protobuf.
type Request struct {
	// Method is the method of the HTTP request from an actor.
	Method string

	// Authority comes from the `:authority` header, and falls back to the `host` header.
	Authority string

	// Path comes from the `:path` header.
	Path string

	// Scheme comes from the `:scheme` header.
	Scheme string

	// Headers include all the request headers, keys lower-cased.
	Headers map[string]string

	// ActorSpiffe is the SPIFFE ID for the actor.
	// Example: spiffe://substrate-actor.local/atespace/demo/actor/egress-demo
	ActorSpiffe string

	// Atespace and ActorName are ActorSpiffe split into its two parts, both ""
	// when ActorSpiffe is "" or does not parse.
	Atespace  string
	ActorName string
}

// Host is Authority with any port removed and the name lower-cased, which is
// the form hostname policy should match on. DNS names are case-insensitive, so
// a policy comparing Authority directly would be bypassed by an uppercase Host
// header.
func (r *Request) Host() string {
	authority := r.Authority
	if host, _, err := net.SplitHostPort(authority); err == nil {
		authority = host
	}
	return strings.ToLower(strings.TrimSuffix(authority, "."))
}
