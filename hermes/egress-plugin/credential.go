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

// What we attach, and where it comes from.
//
// Two sources. A static key is the simplest thing that works and is what a
// plain API key gives you. A short lived OAuth token is better and is what we
// prefer: if a credential ever leaks, whether through an echoed response header
// or anything else, an hour of one scope is a smaller problem than a standing
// key with no expiry.
//
// THE HARD CONSTRAINT. On the egress chain Envoy gives an ext_proc callout 2
// seconds per message (manifests/ate-install/atenet-egress.yaml, timeout: 2s)
// and that is not ours to configure. The 5s defaultExtProcMessageTimeout in the
// router source is the xDS generated INGRESS value, which is not this path. So nothing here may block on the network during
// a request. The OAuth source refreshes on its own timer in the background and
// the request path only ever reads a value out of memory. If that value is
// missing or stale, we fail rather than fetch, because fetching inside the
// callout is how you turn a slow token endpoint into stalled agent traffic.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Credential is one header to attach, resolved without touching the network.
type Credential interface {
	// Header returns what to set, or an error if we have nothing usable. It
	// must not block.
	Header() (Header, error)
	Name() string
}

// ---- static key -------------------------------------------------------------

type staticCredential struct{ h Header }

func (c *staticCredential) Name() string            { return "static key" }
func (c *staticCredential) Header() (Header, error) { return c.h, nil }

func newStaticCredential(headerName, keyPath string) (Credential, error) {
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read MODEL_KEY_FILE %q: %w", keyPath, err)
	}
	key := trimmed(string(raw))
	if key == "" {
		return nil, fmt.Errorf("MODEL_KEY_FILE %q is empty", keyPath)
	}
	return &staticCredential{h: Header{Key: headerName, Value: key}}, nil
}

// ---- short lived OAuth token -------------------------------------------------

// oauthCredential holds a token refreshed on a timer, never in the request path.
type oauthCredential struct {
	source oauth2.TokenSource
	// refreshBefore is how long before expiry we replace the token. Generous
	// on purpose: a refresh that fails has several more chances before the
	// current token stops working, so a transient failure at the token
	// endpoint never becomes a failed request.
	refreshBefore time.Duration

	mu      sync.RWMutex
	current string
	expires time.Time
}

func (c *oauthCredential) Name() string { return "short lived OAuth token" }

func (c *oauthCredential) Header() (Header, error) {
	c.mu.RLock()
	tok, exp := c.current, c.expires
	c.mu.RUnlock()
	if tok == "" {
		return Header{}, fmt.Errorf("no token yet: the background refresh has not produced one")
	}
	if !exp.IsZero() && time.Now().After(exp) {
		// Deliberately not fetching here. A stale token means the refresher
		// has been failing, and blocking the request path on the same
		// endpoint that is already failing would trade a clean refusal for a
		// stalled turn.
		return Header{}, fmt.Errorf("token expired at %s and the refresh has not recovered", exp.Format(time.RFC3339))
	}
	return Header{Key: "authorization", Value: "Bearer " + tok}, nil
}

// refresh pulls one token and stores it. Returns an error rather than logging so
// the caller decides how loud to be.
func (c *oauthCredential) refresh() error {
	tok, err := c.source.Token()
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.current, c.expires = tok.AccessToken, tok.Expiry
	c.mu.Unlock()
	return nil
}

// run refreshes forever. Started before serving so the first request already has
// a token to use.
func (c *oauthCredential) run(ctx context.Context) {
	for {
		wait := time.Minute
		c.mu.RLock()
		exp := c.expires
		c.mu.RUnlock()
		if !exp.IsZero() {
			if d := time.Until(exp) - c.refreshBefore; d > wait {
				wait = d
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if err := c.refresh(); err != nil {
			// Not fatal: the current token is usually still valid, and this
			// loop gets several attempts before it is not.
			slog.Warn("token refresh failed", slog.Any("err", err))
		}
	}
}

func newOAuthCredential(ctx context.Context, keyPath string, scopes []string) (*oauthCredential, error) {
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read MODEL_SA_KEY_FILE %q: %w", keyPath, err)
	}
	creds, err := google.CredentialsFromJSON(ctx, raw, scopes...)
	if err != nil {
		return nil, fmt.Errorf("service account key at %q is not usable: %w", keyPath, err)
	}
	c := &oauthCredential{source: creds.TokenSource, refreshBefore: 5 * time.Minute}
	// Mint once before we start serving. Failing here is better than starting
	// up healthy and refusing every request.
	if err := c.refresh(); err != nil {
		return nil, fmt.Errorf("could not mint an initial token: %w", err)
	}
	return c, nil
}

func trimmed(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	return s
}
