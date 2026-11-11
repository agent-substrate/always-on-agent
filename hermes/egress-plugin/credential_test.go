package main

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStaticCredential(t *testing.T) {
	c := &staticCredential{Header{Key: "x-goog-api-key", Value: "SECRET"}}
	h, err := c.Header()
	if err != nil || h.Value != "SECRET" {
		t.Fatalf("got %+v, %v", h, err)
	}
}

func TestOAuthCredential(t *testing.T) {
	t.Run("serves the cached token, never fetching in the request path", func(t *testing.T) {
		// A source that panics if called proves the read path does no work:
		// the 2s egress ext_proc budget cannot absorb a token endpoint round trip.
		c := &oauthCredential{
			source:  panicSource{t},
			current: "tok-1",
			expires: time.Now().Add(time.Hour),
		}
		h, err := c.Header()
		if err != nil {
			t.Fatal(err)
		}
		if h.Key != "authorization" || h.Value != "Bearer tok-1" {
			t.Errorf("got %+v", h)
		}
	})

	t.Run("no token yet is an error, not an empty header", func(t *testing.T) {
		c := &oauthCredential{source: panicSource{t}}
		if _, err := c.Header(); err == nil {
			t.Error("served a request before any token existed")
		}
	})

	t.Run("an expired token is refused rather than refreshed inline", func(t *testing.T) {
		c := &oauthCredential{
			source:  panicSource{t}, // would panic if it tried to refresh
			current: "stale",
			expires: time.Now().Add(-time.Minute),
		}
		_, err := c.Header()
		if err == nil {
			t.Fatal("served an expired token")
		}
		if !strings.Contains(err.Error(), "expired") {
			t.Errorf("unhelpful error: %v", err)
		}
	})

	t.Run("refresh replaces the token", func(t *testing.T) {
		src := &fakeSource{token: "fresh", expiry: time.Now().Add(time.Hour)}
		c := &oauthCredential{source: src}
		if err := c.refresh(); err != nil {
			t.Fatal(err)
		}
		h, err := c.Header()
		if err != nil || h.Value != "Bearer fresh" {
			t.Errorf("got %+v, %v", h, err)
		}
	})

	t.Run("concurrent reads during a refresh do not race", func(t *testing.T) {
		src := &fakeSource{token: "a", expiry: time.Now().Add(time.Hour)}
		c := &oauthCredential{source: src}
		_ = c.refresh()
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _, _ = c.Header() }()
		}
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _ = c.refresh() }()
		}
		wg.Wait()
	})
}

// A denial must not carry a credential even when one is available, and a host we
// cannot credential must be refused rather than sent bare.
func TestCredentialFailureIsARefusal(t *testing.T) {
	p := &Policy{
		source:      &staticSource{allowed: []string{modelHost}},
		credentials: map[string]Credential{modelHost: &brokenCredential{}},
	}
	d := p.Decide(actor, modelHost, "/v1/x", "GET")
	if d.Allow {
		t.Error("allowed a request we could not credential")
	}
	if len(d.Headers) != 0 {
		t.Errorf("attached something anyway: %+v", d.Headers)
	}
}
