package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

// actorCertPEM builds a certificate shaped exactly like the one ateapi issues an
// actor: a spiffe://substrate-actor.local/atespace/<space>/actor/<name> URI SAN.
// Round-tripping a REAL certificate is the point. Hand-written header strings
// would not catch a PEM or ASN.1 mistake, which is where this will actually fail.
func actorCertPEM(t *testing.T, spiffe string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "actor"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if spiffe != "" {
		u, err := url.Parse(spiffe)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// xfcc wraps a PEM chain the way Envoy does: URL-encoded, quoted, under Chain=,
// alongside the other keys Envoy emits.
func xfcc(chainPEM string) string {
	return fmt.Sprintf(`By=spiffe://cluster.local/ns/ate-system/sa/atenet-egress;Hash=abc123;Chain="%s"`,
		url.QueryEscape(chainPEM))
}

func TestActorFromXFCC_RoundTrip(t *testing.T) {
	pemChain := actorCertPEM(t, "spiffe://substrate-actor.local/atespace/hermes/actor/agent-ce9cbcdb4edb")
	actor, err := ActorFromXFCC(xfcc(pemChain))
	if err != nil {
		t.Fatalf("round trip failed: %v", err)
	}
	if actor.Atespace != "hermes" {
		t.Errorf("atespace = %q, want hermes", actor.Atespace)
	}
	if actor.Name != "agent-ce9cbcdb4edb" {
		t.Errorf("name = %q, want agent-ce9cbcdb4edb", actor.Name)
	}
}

func TestActorFromXFCC_Rejects(t *testing.T) {
	cases := []struct {
		name   string
		header string
	}{
		{"empty header", ""},
		{"no Chain key", `By=spiffe://cluster.local/ns/x/sa/y;Hash=abc123`},
		{"chain is not PEM", `Chain="not%20a%20certificate"`},
		{"cert without any SPIFFE SAN", xfcc(actorCertPEM(t, ""))},
		{"SPIFFE from the wrong trust domain",
			xfcc(actorCertPEM(t, "spiffe://cluster.local/atespace/hermes/actor/a"))},
		{"SPIFFE that is not the actor form",
			xfcc(actorCertPEM(t, "spiffe://substrate-actor.local/ns/default/sa/thing"))},
		{"SPIFFE with a trailing slash",
			xfcc(actorCertPEM(t, "spiffe://substrate-actor.local/atespace/hermes/actor/"))},
		// Four segments, so the length check passes and only the emptiness
		// check can reject these two.
		{"SPIFFE with an empty atespace",
			xfcc(actorCertPEM(t, "spiffe://substrate-actor.local/atespace//actor/agent-x"))},
		{"SPIFFE with an empty actor segment",
			xfcc(actorCertPEM(t, "spiffe://substrate-actor.local/atespace/hermes/actor//"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if actor, err := ActorFromXFCC(tc.header); err == nil {
				t.Errorf("accepted %v, want refusal", actor)
			}
		})
	}
}

// An actor could try to name itself by sending its own XFCC. Envoy's
// SANITIZE_SET replaces it, but the parser must not be the thing relied on --
// so verify it keys on the certificate and not on any text in the header.
func TestActorFromXFCC_IgnoresUnsignedClaims(t *testing.T) {
	real := actorCertPEM(t, "spiffe://substrate-actor.local/atespace/hermes/actor/agent-real")
	header := fmt.Sprintf(`URI=spiffe://substrate-actor.local/atespace/hermes/actor/agent-IMPOSTOR;Chain="%s"`,
		url.QueryEscape(real))
	actor, err := ActorFromXFCC(header)
	if err != nil {
		t.Fatal(err)
	}
	if actor.Name != "agent-real" {
		t.Errorf("identity taken from an unsigned header field: got %q", actor.Name)
	}
}

// attrs builds an ext_proc attribute map the way Envoy sends one: keyed by the
// name of the filter that contributed it, with the value under the literal CEL
// expression that was requested.
func attrs(key, value string) map[string]*structpb.Struct {
	return map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				key: structpb.NewStringValue(value),
			},
		},
	}
}

func TestActorFromFilterState(t *testing.T) {
	const id = "spiffe://substrate-actor.local/atespace/hermes-accept/actor/agent-fb337911e2ec"

	// Both spellings, because the attribute arrives under the CEL expression
	// and it costs nothing to also accept the bare object name; and both the
	// current and legacy object names, because one build has to span the
	// rename. Spelled out rather than referencing the constants: these strings
	// are a contract with the gateway's Envoy config, so a rename here has to
	// break the test.
	for _, key := range []string{
		"filter_state['dev.ate.actor.identity']",
		"dev.ate.actor.identity",
		"filter_state['ate.actor.identity']",
		"ate.actor.identity",
	} {
		actor, err := ActorFromFilterState(attrs(key, id))
		if err != nil {
			t.Fatalf("key %q: %v", key, err)
		}
		if actor.Atespace != "hermes-accept" || actor.Name != "agent-fb337911e2ec" {
			t.Errorf("key %q: got %+v", key, actor)
		}
		if actor.Spiffe != id {
			t.Errorf("key %q: spiffe %q, want %q", key, actor.Spiffe, id)
		}
	}
}

// A gateway mid-rename can publish both objects. The current name has to win,
// or the legacy fallback outlives the thing it was added for.
func TestActorFromFilterStatePrefersTheCurrentName(t *testing.T) {
	both := map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				actorAttribute: structpb.NewStringValue(
					"spiffe://substrate-actor.local/atespace/hermes/actor/current"),
				legacyActorAttribute: structpb.NewStringValue(
					"spiffe://substrate-actor.local/atespace/hermes/actor/legacy"),
			},
		},
	}
	actor, err := ActorFromFilterState(both)
	if err != nil {
		t.Fatal(err)
	}
	if actor.Name != "current" {
		t.Errorf("read %q, want the %s value", actor.Name, actorFilterStateName)
	}
}

func TestActorFromFilterState_Rejects(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]*structpb.Struct
		// The message has to name the cause: this failure mode is invisible
		// otherwise, because it presents as a 500 from the model provider.
		wantSubstr string
	}{
		{"no attributes at all", nil, "request_attributes"},
		{"empty map", map[string]*structpb.Struct{}, "request_attributes"},
		{"some other attribute", attrs("request.path", "/v1/x"), "no filter_state"},
		{"present but empty", attrs(actorAttribute, "   "), "asserted no actor"},
		{"not a spiffe id", attrs(actorAttribute, "https://example.com/a/b/c/d"), "is not a spiffe"},
		{"wrong trust domain", attrs(actorAttribute,
			"spiffe://cluster.local/atespace/hermes/actor/a"), "is not a spiffe"},
		{"not the actor form", attrs(actorAttribute,
			"spiffe://substrate-actor.local/ns/hermes/sa/default"), "not in the actor form"},
		{"empty actor name", attrs(actorAttribute,
			"spiffe://substrate-actor.local/atespace/hermes/actor/"), "not in the actor form"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actor, err := ActorFromFilterState(tc.in)
			if err == nil {
				t.Fatalf("accepted %+v", actor)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("error %q does not mention %q", err, tc.wantSubstr)
			}
		})
	}
}

// The gateway is the only thing that sets this object and an actor cannot reach
// filter state from the wire, so the sources agree by construction. What this
// pins is the PREFERENCE: filter state wins, because it is the only source on
// the MITM leg and a single exercised path is worth more than a fallback.
func TestIdentifyPrefersFilterState(t *testing.T) {
	const fromState = "spiffe://substrate-actor.local/atespace/hermes/actor/from-filter-state"
	const fromCert = "spiffe://substrate-actor.local/atespace/hermes/actor/from-xfcc"

	actor, source, err := identify(attrs(actorAttribute, fromState), xfcc(actorCertPEM(t, fromCert)))
	if err != nil {
		t.Fatal(err)
	}
	if source != "filter_state" || actor.Name != "from-filter-state" {
		t.Errorf("got %s from %s, want from-filter-state from filter_state", actor.Name, source)
	}

	// The CONNECT leg: no attributes, XFCC present.
	actor, source, err = identify(nil, xfcc(actorCertPEM(t, fromCert)))
	if err != nil {
		t.Fatal(err)
	}
	if source != "xfcc" || actor.Name != "from-xfcc" {
		t.Errorf("got %s from %s, want from-xfcc from xfcc", actor.Name, source)
	}

	// Neither. The error must name both sources, or a misconfigured filter and
	// a request that never traversed the chain look identical.
	if _, _, err = identify(nil, ""); err == nil {
		t.Fatal("accepted a request with no identity at all")
	} else if !strings.Contains(err.Error(), "filter state") ||
		!strings.Contains(err.Error(), xfccHeader) {
		t.Errorf("error %q does not name both sources", err)
	}
}
