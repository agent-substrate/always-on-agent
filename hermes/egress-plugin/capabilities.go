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

// What Envoy actually sends us, and what we can therefore enforce.
//
// This is not our choice. Envoy's ext_proc filter decides which phases of a
// request reach a plugin (headers only, headers plus body, plus the response
// phases), and Substrate's docs say the settings are not customisable by the
// user supplying the plugin. So the honest design is to be TOLD what we get and
// to refuse anything we cannot police, rather than to assume the richest case
// and silently wave through what we cannot see.
//
// EGRESS_EXTPROC_MODES is a comma-separated list. Set it to match how Substrate
// configured the filter:
//
//	headers                    the documented default, and all their sample handles
//	headers,body               adds request bodies
//	headers,body,response      adds the response phase
//
// The consequences of each are spelled out in Capabilities.Explain(), which is
// logged at startup so an operator can see in the log what the plugin is and is
// not enforcing, instead of inferring it from source.
package main

import (
	"fmt"
	"strings"
)

// Capabilities is what the filter gives us to work with.
type Capabilities struct {
	// RequestBody: Envoy streams request bodies to the plugin. Without this we
	// cannot inspect what a call is actually asking for, only where it is
	// going. Rules that depend on the payload become unenforceable, and any
	// request needing one is refused rather than allowed unread.
	RequestBody bool
	// ResponseHeaders: Envoy sends us the response phase. Without this we
	// cannot strip a credential that an upstream echoes back. A redirect or
	// an error carrying the injected header would travel into the sandbox,
	// where the agent can read it. Injecting on egress keeps the credential
	// out of the actor on the way OUT; only the response phase keeps it out on
	// the way BACK.
	ResponseHeaders bool
}

func ParseCapabilities(spec string) (Capabilities, error) {
	c := Capabilities{}
	seenHeaders := false
	for _, part := range strings.Split(spec, ",") {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case "", "headers":
			seenHeaders = true
		case "body", "request_body":
			c.RequestBody = true
		case "response", "response_headers":
			c.ResponseHeaders = true
		default:
			return c, fmt.Errorf("EGRESS_EXTPROC_MODES: %q is not one of headers, body, response", part)
		}
	}
	if !seenHeaders {
		// Request headers are the one phase we cannot work without: they carry
		// the authority we authorise on and the XFCC we identify by.
		return c, fmt.Errorf("EGRESS_EXTPROC_MODES must include headers")
	}
	return c, nil
}

// Explain states, in plain terms, what this configuration can and cannot
// enforce. Logged at startup so the gap is visible in an operator's log rather
// than discovered during a review.
func (c Capabilities) Explain() []string {
	out := []string{
		"enforcing: destination allowlist per actor, identity from the " +
			"validated client certificate",
		"credential injection: only on chains that carry the real request. " +
			"Substrate's egress listener terminates CONNECT, and on a CONNECT " +
			"the header we add is consumed establishing the tunnel and never " +
			"reaches the upstream. Each such call is logged as 'credential " +
			"NOT injected' rather than counted as enforced",
	}
	if c.RequestBody {
		out = append(out, "request bodies available for configured payload rules")
	} else {
		out = append(out, "NOT enforcing: payload rules. Envoy is not sending "+
			"request bodies, so calls whose safety depends on what they carry "+
			"are REFUSED rather than allowed unread")
	}
	if c.ResponseHeaders {
		out = append(out, "enforcing: stripping injected credentials out of "+
			"responses, so an upstream cannot echo one back into the sandbox")
	} else {
		out = append(out, "NOT enforcing: response filtering. An upstream that "+
			"echoes the injected credential in a redirect or an error will "+
			"return it to the actor, which can read it")
	}
	return out
}
