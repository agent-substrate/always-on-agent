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
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

const testActorURI = "spiffe://substrate-actor.local/atespace/demo/actor/egress-demo"

// oneAttribute builds an attribute map the way Envoy sends one: keyed by the
// name of the filter that contributed it.
func oneAttribute(key, value string) map[string]*structpb.Struct {
	return map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{key: structpb.NewStringValue(value)},
		},
	}
}

func TestActorFromAttributesReadsEverySpelling(t *testing.T) {
	// The CEL expression and the bare object name, under both the current and
	// the legacy object name. Spelled out rather than referencing the
	// constants: these strings are a contract with the gateway's Envoy config,
	// so a rename here has to break the test.
	for _, key := range []string{
		"filter_state['dev.ate.actor.identity']",
		"dev.ate.actor.identity",
		"filter_state['ate.actor.identity']",
		"ate.actor.identity",
	} {
		t.Run(key, func(t *testing.T) {
			if got := actorFromAttributes(oneAttribute(key, testActorURI)); got != testActorURI {
				t.Errorf("actorFromAttributes = %q, want %q", got, testActorURI)
			}
		})
	}

	if got := actorFromAttributes(oneAttribute("request.path", "/v1/x")); got != "" {
		t.Errorf("some other attribute read as an actor: %q", got)
	}
	if got := actorFromAttributes(nil); got != "" {
		t.Errorf("no attributes read as an actor: %q", got)
	}
}

// A gateway mid-rename can publish both objects. The current name has to win,
// or the legacy fallback outlives the thing it was added for.
func TestActorFromAttributesPrefersTheCurrentName(t *testing.T) {
	const legacyURI = "spiffe://substrate-actor.local/atespace/demo/actor/legacy"
	both := map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				actorAttribute:       structpb.NewStringValue(testActorURI),
				legacyActorAttribute: structpb.NewStringValue(legacyURI),
			},
		},
	}
	if got := actorFromAttributes(both); got != testActorURI {
		t.Errorf("actorFromAttributes = %q, want the %s value", got, actorFilterStateName)
	}
}

func TestParseActorSplitsTheSpiffeID(t *testing.T) {
	for _, tc := range []struct {
		name              string
		uri               string
		atespace, actorNm string
	}{
		{"a real one", "spiffe://substrate-actor.local/atespace/demo/actor/egress-demo", "demo", "egress-demo"},
		{"names with hyphens", "spiffe://substrate-actor.local/atespace/acme-prod/actor/invoice-agent", "acme-prod", "invoice-agent"},

		// Everything below must yield "", "".
		{"unasserted", "", "", ""},
		{"wrong trust domain", "spiffe://substrate-pod.local/atespace/demo/actor/egress-demo", "", ""},
		{"not spiffe", "https://substrate-actor.local/atespace/demo/actor/egress-demo", "", ""},
		{"no actor segment", "spiffe://substrate-actor.local/atespace/demo", "", ""},
		{"empty atespace", "spiffe://substrate-actor.local/atespace//actor/egress-demo", "", ""},
		{"empty actor name", "spiffe://substrate-actor.local/atespace/demo/actor/", "", ""},
		{"trailing path segment", "spiffe://substrate-actor.local/atespace/demo/actor/egress-demo/extra", "", ""},
		{"prefix only", "spiffe://substrate-actor.local/atespace/", "", ""},
		// A name that merely contains an actor URI is not one. This is the
		// case a prefix or substring match on Actor would get wrong.
		{"embedded, not prefixed", "spiffe://evil.example/x/spiffe://substrate-actor.local/atespace/demo/actor/root", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			atespace, actorNm := parseActor(tc.uri)
			if atespace != tc.atespace || actorNm != tc.actorNm {
				t.Errorf("parseActor(%q) = %q, %q; want %q, %q", tc.uri, atespace, actorNm, tc.atespace, tc.actorNm)
			}
		})
	}
}
