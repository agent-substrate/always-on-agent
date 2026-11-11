package main

import (
	"context"
	"os"
	"testing"
)

// On Substrate's egress chain every request arrives as a CONNECT, so a header
// added by the plugin is consumed establishing the tunnel and never reaches the
// upstream. The policy must report that instead of counting it as injected.
func TestConnectMakesCredentialUndeliverable(t *testing.T) {
	p := &Policy{
		source:      &staticSource{allowed: []string{"model.example"}},
		credentials: map[string]Credential{"model.example": &staticCredential{h: Header{Key: "x-key", Value: "SECRET"}}},
	}
	actor := &Actor{Name: "a", Spiffe: "spiffe://x/actor/a"}

	connect := p.Decide(actor, "model.example:443", "", "CONNECT")
	if !connect.Allow {
		t.Fatalf("CONNECT to an allowed host should still be allowed, got %q", connect.Reason)
	}
	if !connect.CredentialUndeliverable {
		t.Error("CONNECT must be reported as undeliverable, otherwise we claim an injection that never happened")
	}
	if len(connect.Headers) != 0 {
		t.Errorf("no header should be offered on a CONNECT, got %d", len(connect.Headers))
	}

	post := p.Decide(actor, "model.example:443", "/v1/generate", "POST")
	if !post.Allow || post.CredentialUndeliverable {
		t.Fatalf("a real request must still be credentialed, got allow=%v undeliverable=%v", post.Allow, post.CredentialUndeliverable)
	}
	if len(post.Headers) != 1 || post.Headers[0].Value != "SECRET" {
		t.Errorf("expected the credential header on a non-CONNECT request, got %+v", post.Headers)
	}
}

// MODEL_CREDENTIAL=none has to leave the credential map empty. If a nil landed
// in it, Decide would treat the host as one it must credential and refuse every
// call to it. Built through NewPolicy rather than by hand, because the map is
// assembled there and a hand built Policy would not exercise it.
func TestNoCredentialModeEnforcesDestinationsOnly(t *testing.T) {
	t.Setenv("MODEL_CREDENTIAL", "none")
	t.Setenv("MODEL_HOST", "model.example")
	t.Setenv("EGRESS_POLICY_SOURCE", "static")

	p, err := NewPolicy(context.Background())
	if err != nil {
		t.Fatalf("MODEL_CREDENTIAL=none must build without any credential file: %v", err)
	}
	if len(p.credentials) != 0 {
		t.Fatalf("no credential was configured, so the map must be empty, got %d entry/entries", len(p.credentials))
	}
	actor := &Actor{Name: "a", Spiffe: "spiffe://x/actor/a"}

	d := p.Decide(actor, "model.example:443", "/v1/x", "POST")
	if !d.Allow {
		t.Fatalf("an allowed host must stay allowed with no credential held, got %q", d.Reason)
	}
	if len(d.Headers) != 0 {
		t.Errorf("nothing should be attached, got %d header(s)", len(d.Headers))
	}
	if d.CredentialUndeliverable {
		t.Error("no credential was configured, so nothing is undeliverable")
	}
	if blocked := p.Decide(actor, "elsewhere.example:443", "/", "POST"); blocked.Allow {
		t.Error("destination policy must still apply when no credential is held")
	}
}

// A registry has to be able to set policy for actors it does not name, because
// enumerating actors is not always possible.
func TestRegistryDefaultsApplyToUnnamedActors(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/registry.json"
	if err := os.WriteFile(path, []byte(`{
	  "defaults": ["allowed.example"],
	  "agents": {"quarantined": {"egress": []}}
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := loadRegistry(path, []string{"fallback.example"})
	if err != nil {
		t.Fatal(err)
	}
	unnamed := &Actor{Name: "some-actor"}
	if ok, _ := src.Allows(unnamed, "allowed.example"); !ok {
		t.Error("an unnamed actor must get the file's defaults")
	}
	if ok, _ := src.Allows(unnamed, "fallback.example"); ok {
		t.Error("the file's defaults must win over the caller's fallback")
	}
	if ok, _ := src.Allows(&Actor{Name: "quarantined"}, "allowed.example"); ok {
		t.Error("a named actor with an empty list must still be denied")
	}
}

// An explicit empty defaults list is how a registry says "deny anything not
// named". It must not be mistaken for an absent key and fall back to whatever
// the caller passed in.
func TestEmptyDefaultsDenyRatherThanFallBack(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/registry.json"
	if err := os.WriteFile(path, []byte(`{"defaults": [], "agents": {}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := loadRegistry(path, []string{"fallback.example"})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := src.Allows(&Actor{Name: "some-actor"}, "fallback.example"); ok {
		t.Error("an explicit empty defaults list must deny, not fall back to the caller's list")
	}
}

// The startup log asks the policy what it holds. With no credential configured
// that must be a string, not a panic: main() logs it before serving.
func TestCredentialNameWithNoCredential(t *testing.T) {
	p := &Policy{credentials: map[string]Credential{}}
	if got := p.CredentialName(); got == "" {
		t.Error("CredentialName must describe the empty case")
	}
	withNil := &Policy{credentials: map[string]Credential{"h": nil}}
	if got := withNil.CredentialName(); got == "" {
		t.Error("a nil entry must not produce an empty name")
	}
}
