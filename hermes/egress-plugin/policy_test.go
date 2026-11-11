package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const modelHost = "generativelanguage.googleapis.com"

func staticPolicy() *Policy {
	return &Policy{
		source:      &staticSource{allowed: []string{modelHost}},
		credentials: map[string]Credential{modelHost: &staticCredential{Header{Key: "x-goog-api-key", Value: "SECRET"}}},
	}
}

var actor = &Actor{Atespace: "hermes", Name: "agent-x", Spiffe: "spiffe://x"}

func TestStaticPolicy(t *testing.T) {
	p := staticPolicy()

	t.Run("model endpoint allowed and credentialed", func(t *testing.T) {
		d := p.Decide(actor, modelHost+":443", "/v1/x", "GET")
		if !d.Allow {
			t.Fatalf("denied: %s", d.Reason)
		}
		if len(d.Headers) != 1 || d.Headers[0].Key != "x-goog-api-key" || d.Headers[0].Value != "SECRET" {
			t.Errorf("wrong credential: %+v", d.Headers)
		}
	})

	t.Run("everything else denied", func(t *testing.T) {
		for _, h := range []string{
			"api.openai.com", "evil.example.com", "metadata.google.internal",
			modelHost + ".evil.com", "notgenerativelanguage.googleapis.com",
		} {
			if d := p.Decide(actor, h, "/v1/x", "GET"); d.Allow {
				t.Errorf("%s allowed", h)
			}
		}
	})

	t.Run("no authority denied", func(t *testing.T) {
		if d := p.Decide(actor, "", "/v1/x", "GET"); d.Allow {
			t.Error("empty authority allowed")
		} else if d.Reason != "request carries no :authority" {
			// The reason matters: without the explicit guard an empty host
			// falls through to "not on the allowlist", which is the same
			// verdict for the wrong reason and hides a malformed request.
			t.Errorf("wrong reason for empty authority: %q", d.Reason)
		}
	})

	t.Run("host match ignores case and port", func(t *testing.T) {
		if d := p.Decide(actor, "GenerativeLanguage.GoogleAPIs.com:443", "/v1/x", "GET"); !d.Allow {
			t.Error("case-different host denied")
		}
	})

	t.Run("a denial never carries the credential", func(t *testing.T) {
		if d := p.Decide(actor, "evil.example.com", "/v1/x", "GET"); len(d.Headers) != 0 {
			t.Errorf("credential leaked on denial: %+v", d.Headers)
		}
	})
}

func TestRegistryPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	// agent-quarantined has an explicit empty list: no egress at all, which is
	// how Substrate's own demo describes a quarantined actor.
	err := os.WriteFile(path, []byte(`{"agents":{
	  "agent-x": {"egress": ["`+modelHost+`", "api.github.com"]},
	  "agent-quarantined": {"egress": []}
	}}`), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	src, err := loadRegistry(path, []string{modelHost})
	if err != nil {
		t.Fatal(err)
	}
	p := &Policy{source: src, credentials: map[string]Credential{modelHost: &staticCredential{Header{Key: "x-goog-api-key", Value: "SECRET"}}}}

	t.Run("per-agent host allowed", func(t *testing.T) {
		if d := p.Decide(actor, "api.github.com", "/v1/x", "GET"); !d.Allow {
			t.Errorf("denied: %s", d.Reason)
		}
	})

	t.Run("allowed host with no credential passes bare", func(t *testing.T) {
		d := p.Decide(actor, "api.github.com", "/v1/x", "GET")
		if len(d.Headers) != 0 {
			t.Errorf("invented a credential for a host we hold none for: %+v", d.Headers)
		}
	})

	t.Run("quarantined agent reaches nothing", func(t *testing.T) {
		q := &Actor{Atespace: "hermes", Name: "agent-quarantined"}
		d := p.Decide(q, modelHost, "/v1/x", "GET")
		if d.Allow {
			t.Fatal("quarantined agent allowed out")
		}
		if !strings.Contains(d.Reason, "no egress at all") {
			t.Errorf("quarantine is indistinguishable from an unlisted host: %q", d.Reason)
		}
	})

	t.Run("unlisted agent falls back to defaults", func(t *testing.T) {
		u := &Actor{Atespace: "hermes", Name: "agent-unlisted"}
		if d := p.Decide(u, modelHost, "/v1/x", "GET"); !d.Allow {
			t.Errorf("unlisted agent denied the default host: %s", d.Reason)
		}
		if d := p.Decide(u, "api.github.com", "/v1/x", "GET"); d.Allow {
			t.Error("unlisted agent got another agent's rule")
		}
	})
}

func TestCapabilities(t *testing.T) {
	t.Run("headers is the minimum", func(t *testing.T) {
		if _, err := ParseCapabilities("body"); err == nil {
			t.Error("accepted a config without headers")
		}
		if _, err := ParseCapabilities("headers,nonsense"); err == nil {
			t.Error("accepted an unknown mode")
		}
	})

	t.Run("a body-dependent call is refused when bodies are not sent", func(t *testing.T) {
		p := staticPolicy() // headers-only by default
		d := p.Decide(actor, modelHost, "/v1/files/abc/permissions", "POST")
		if d.Allow {
			t.Error("a share was allowed without the body being readable")
		}
	})

	t.Run("the same call is judged normally when bodies ARE sent", func(t *testing.T) {
		p := staticPolicy()
		p.caps = Capabilities{RequestBody: true}
		d := p.Decide(actor, modelHost, "/v1/files/abc/permissions", "POST")
		if !d.Allow {
			t.Errorf("refused even though bodies are available: %s", d.Reason)
		}
	})

	t.Run("reads are never body-dependent", func(t *testing.T) {
		p := staticPolicy()
		if d := p.Decide(actor, modelHost, "/v1/files/abc/permissions", "GET"); !d.Allow {
			t.Errorf("a GET was refused: %s", d.Reason)
		}
	})

	t.Run("every gap is stated, not just one of them", func(t *testing.T) {
		// Counting matters: asserting only that the text contains "NOT
		// enforcing" passes while one of the two gaps has quietly started
		// claiming it is enforced.
		gaps := func(c Capabilities) int {
			n := 0
			for _, line := range c.Explain() {
				if strings.HasPrefix(line, "NOT enforcing") {
					n++
				}
			}
			return n
		}
		if got := gaps(Capabilities{}); got != 2 {
			t.Errorf("headers-only reports %d gaps, want 2 (bodies and responses)", got)
		}
		if got := gaps(Capabilities{RequestBody: true}); got != 1 {
			t.Errorf("bodies-but-no-responses reports %d gaps, want 1", got)
		}
		if got := gaps(Capabilities{RequestBody: true, ResponseHeaders: true}); got != 0 {
			t.Errorf("a fully capable config claims %d gaps it does not have", got)
		}
	})
}

func TestPassthroughPolicy(t *testing.T) {
	p := &Policy{source: &passthroughSource{}, credentials: map[string]Credential{modelHost: &staticCredential{Header{Key: "x-goog-api-key", Value: "SECRET"}}}}

	t.Run("any host allowed, Substrate denies", func(t *testing.T) {
		if d := p.Decide(actor, "anything.example.com", "/v1/x", "GET"); !d.Allow {
			t.Error("passthrough denied a host")
		}
	})
	t.Run("still injects where we hold a credential", func(t *testing.T) {
		if d := p.Decide(actor, modelHost, "/v1/x", "GET"); len(d.Headers) != 1 {
			t.Errorf("no credential injected: %+v", d.Headers)
		}
	})
	t.Run("still refuses an empty authority", func(t *testing.T) {
		if d := p.Decide(actor, "", "/v1/x", "GET"); d.Allow {
			t.Error("empty authority allowed even in passthrough")
		}
	})
}
