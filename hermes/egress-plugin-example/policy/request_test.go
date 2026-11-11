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

import "testing"

// Host is the form a hostname policy is meant to match on, so its
// normalization is the thing standing between a policy and its cheapest
// bypasses. Each case below is one of them.
func TestRequestHostNormalizes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		authority string
		want      string
	}{
		{"already normal", "blocked.example.com", "blocked.example.com"},
		// DNS names are case-insensitive, so a policy comparing the authority
		// verbatim is bypassed by shifting the case of a single letter.
		{"uppercase", "BLOCKED.Example.COM", "blocked.example.com"},
		// The authority may carry a port, and a policy keyed on the full
		// authority would miss every non-default one.
		{"with port", "blocked.example.com:8443", "blocked.example.com"},
		// A trailing dot is a legal absolute form of the same name.
		{"trailing dot", "blocked.example.com.", "blocked.example.com"},
		{"port and case and dot", "Blocked.Example.COM.:8443", "blocked.example.com"},
		// Subdomains and parents are different names and must stay distinct: a
		// Host that collapsed either one would silently widen every policy
		// written against it.
		{"subdomain stays distinct", "sub.blocked.example.com", "sub.blocked.example.com"},
		{"parent stays distinct", "example.com", "example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (&Request{Authority: tc.authority}).Host(); got != tc.want {
				t.Errorf("Request{Authority: %q}.Host() = %q, want %q", tc.authority, got, tc.want)
			}
		})
	}
}
