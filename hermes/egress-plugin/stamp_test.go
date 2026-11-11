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

package main

import (
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	gatewayHost = "hermes-gateway.hermes.svc.cluster.local"
	stampActor  = "spiffe://substrate-actor.local/atespace/hermes-accept/actor/agent-1"
	otherActor  = "spiffe://substrate-actor.local/atespace/hermes-accept/actor/agent-2"
)

// stampingServer is the plugin as deployed: static allowlist, no credential.
func stampingServer() *server {
	return &server{
		policy: &Policy{
			source:      &staticSource{allowed: []string{gatewayHost, "generativelanguage.googleapis.com"}},
			credentials: map[string]Credential{},
		},
		onFail: failClosed,
	}
}

// requestTo builds the RequestHeaders message Envoy would send on the MITM leg,
// with the actor identity in filter state where the real one puts it.
func requestTo(authority string, extra ...string) (*extprocv3.HttpHeaders, map[string]*structpb.Struct) {
	hs := []*corev3.HeaderValue{
		{Key: ":authority", Value: authority},
		{Key: ":path", Value: "/agent/turn-ended"},
		{Key: ":method", Value: "POST"},
	}
	for i := 0; i+1 < len(extra); i += 2 {
		hs = append(hs, &corev3.HeaderValue{Key: extra[i], Value: extra[i+1]})
	}
	return &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: hs}},
		attrs(actorAttribute, stampActor)
}

// mutation pulls the header mutation out of an allow response, failing the test
// if the plugin denied instead.
func mutation(t *testing.T, resp *extprocv3.ProcessingResponse) *extprocv3.HeaderMutation {
	t.Helper()
	rh, ok := resp.GetResponse().(*extprocv3.ProcessingResponse_RequestHeaders)
	if !ok {
		t.Fatalf("request was not allowed: %v", resp.GetResponse())
	}
	return rh.RequestHeaders.GetResponse().GetHeaderMutation()
}

func setHeader(m *extprocv3.HeaderMutation, key string) (string, bool) {
	for _, h := range m.GetSetHeaders() {
		if h.GetHeader().GetKey() == key {
			v := h.GetHeader().GetValue()
			if v == "" {
				v = string(h.GetHeader().GetRawValue())
			}
			return v, true
		}
	}
	return "", false
}

func removesHeader(m *extprocv3.HeaderMutation, key string) bool {
	for _, h := range m.GetRemoveHeaders() {
		if h == key {
			return true
		}
	}
	return false
}

func TestIdentityIsStampedForTheGateway(t *testing.T) {
	h, a := requestTo(gatewayHost)
	m := mutation(t, stampingServer().onRequestHeaders(a, h))

	got, ok := setHeader(m, actorHeader)
	if !ok {
		t.Fatalf("no %s set; the gateway cannot tell who called and turn "+
			"signalling is refused fleet-wide", actorHeader)
	}
	if got != stampActor {
		t.Errorf("stamped %q, want the verified identity %q", got, stampActor)
	}
}

// The one that matters. Without the strip, an agent names whichever actor it
// likes and ends someone else's turn.
func TestAnActorCannotStampItselfAsAnother(t *testing.T) {
	h, a := requestTo(gatewayHost, actorHeader, otherActor)
	m := mutation(t, stampingServer().onRequestHeaders(a, h))

	got, ok := setHeader(m, actorHeader)
	if !ok {
		t.Fatalf("no %s set, so the agent's own value survives", actorHeader)
	}
	if got != stampActor {
		t.Errorf("the agent's claim survived: got %q, want %q", got, stampActor)
	}
	for _, h := range m.GetSetHeaders() {
		if h.GetHeader().GetKey() == actorHeader &&
			h.GetAppendAction() != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
			t.Errorf("append action is %v, so the agent's value is kept alongside ours",
				h.GetAppendAction())
		}
	}
}

func TestIdentityIsStrippedForThirdParties(t *testing.T) {
	h, a := requestTo("generativelanguage.googleapis.com", actorHeader, otherActor)
	m := mutation(t, stampingServer().onRequestHeaders(a, h))

	if _, ok := setHeader(m, actorHeader); ok {
		t.Errorf("actor identity sent to a third party")
	}
	if !removesHeader(m, actorHeader) {
		t.Errorf("%s not removed: an agent's own value reaches the upstream, and "+
			"any in-cluster hop after it would believe it", actorHeader)
	}
}

// Fail-open exists for diagnosis. It must not also hand an unidentified caller
// the ability to assert an actor id.
func TestFailOpenStillStrips(t *testing.T) {
	s := stampingServer()
	s.onFail = failOpen
	h := &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{
		{Key: ":authority", Value: gatewayHost},
		{Key: actorHeader, Value: otherActor},
	}}}

	m := mutation(t, s.onRequestHeaders(nil, h))
	if !removesHeader(m, actorHeader) {
		t.Errorf("fail-open passed an unidentified caller's %s through", actorHeader)
	}
}

func TestInCluster(t *testing.T) {
	for _, tc := range []struct {
		authority string
		want      bool
	}{
		{"hermes-gateway.hermes.svc.cluster.local", true},
		{"hermes-gateway.hermes.svc.cluster.local.", true},
		{"hermes-gateway.hermes.svc:80", true},
		{"HERMES-GATEWAY.hermes.svc", true},
		{"hermes-gateway", true},
		{"generativelanguage.googleapis.com", false},
		{"generativelanguage.googleapis.com:443", false},
		{"github.com", false},
		// By name only, deliberately: a private address says nothing about
		// whether the callee is ours. See inCluster.
		{"10.96.0.10", false},
		{"10.96.0.10:80", false},
		{"", false},
		// The obvious way to smuggle: a public name that ends the way ours do.
		{"hermes-gateway.hermes.svc.cluster.local.attacker.example", false},
	} {
		if got := inCluster(tc.authority); got != tc.want {
			t.Errorf("inCluster(%q) = %v, want %v", tc.authority, got, tc.want)
		}
	}
}
