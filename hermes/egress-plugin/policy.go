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

// What an actor may reach, and what credential it gets.
//
// The DECISION is behind an interface with three implementations, selected by
// EGRESS_POLICY_SOURCE, so that where policy lives can change without touching
// the request path:
//
//	static      (default) one allowlist for every actor, compiled in below.
//	registry    per-agent rules from a JSON file, mountable as a ConfigMap,
//	            the same shape as the agent registry the gateway already
//	            publishes, so an agent's identity and its network reach are
//	            described in one place instead of two.
//	passthrough allow any destination and only inject credentials, leaving
//	            denial to Substrate's own per-actor egress policy API. Use this
//	            once that API lands, so we are not enforcing the same rule twice
//	            in two places that can disagree.
//
// Credential injection is common to all three: it is the part only we can do,
// because only we know which agent maps to which secret.
//
// Policy source and request-body capabilities are explicit configuration.
// Actor network policy is evaluated against runtime routing information.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Decision is the answer for one request.
type Decision struct {
	Allow   bool
	Reason  string
	Headers []Header
	// CredentialUndeliverable is set when we hold a credential for this
	// destination but the request is a CONNECT, so the header we would add is
	// consumed setting up the tunnel and never reaches the target. See
	// Policy.Decide.
	CredentialUndeliverable bool
}

// Header is set on the outbound request, replacing any existing value.
type Header struct {
	Key   string
	Value string
}

// Source decides whether an actor may reach a host. It does NOT decide the
// credential. That is the injector's job, and is the same regardless.
type Source interface {
	Allows(actor *Actor, host string) (bool, string)
	Name() string
}

// ---- static: one rule for everyone -----------------------------------------

// staticSource is the default and what runs today. Hardcoded deliberately:
// there is no control plane to express per-actor rules in yet, and inventing a
// third place to describe an agent would be worse than waiting.
type staticSource struct{ allowed []string }

func (s *staticSource) Name() string { return "static" }

func (s *staticSource) Allows(_ *Actor, host string) (bool, string) {
	for _, h := range s.allowed {
		if strings.EqualFold(host, h) {
			return true, "allowed by the static allowlist"
		}
	}
	return false, fmt.Sprintf("%s is not on the static allowlist", host)
}

// ---- registry: per-agent rules from a file ---------------------------------

// registrySource reads {"agents": {"<actor>": {"egress": ["host", ...]}}}.
// Mount it from a ConfigMap. An actor with no entry gets the defaults, and an
// empty list means no egress at all, which is how an actor is quarantined.
type registrySource struct {
	perActor map[string][]string
	defaults []string
}

func (s *registrySource) Name() string { return "registry" }

func (s *registrySource) Allows(actor *Actor, host string) (bool, string) {
	hosts, named := s.perActor[actor.Name]
	if !named {
		hosts = s.defaults
	}
	for _, h := range hosts {
		if strings.EqualFold(host, h) {
			return true, fmt.Sprintf("allowed for %s by the registry", actor.Name)
		}
	}
	if named && len(hosts) == 0 {
		return false, fmt.Sprintf("%s has no egress at all", actor.Name)
	}
	return false, fmt.Sprintf("%s is not allowed to reach %s", actor.Name, host)
}

func loadRegistry(path string, defaults []string) (*registrySource, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read EGRESS_REGISTRY_FILE %q: %w", path, err)
	}
	var doc struct {
		// Defaults applies to any actor the file does not name. It matters
		// because enumerating actors is not always possible, so a registry has
		// to be able to say "everyone gets this" without listing them.
		Defaults []string `json:"defaults"`
		Agents   map[string]struct {
			Egress []string `json:"egress"`
		} `json:"agents"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("EGRESS_REGISTRY_FILE %q is not valid JSON: %w", path, err)
	}
	per := make(map[string][]string, len(doc.Agents))
	for actor, cfg := range doc.Agents {
		per[actor] = cfg.Egress
	}
	// A defaults key in the file wins over the caller's fallback, including an
	// explicit empty list, which is how a registry says "deny by default".
	if doc.Defaults != nil {
		defaults = doc.Defaults
	}
	return &registrySource{perActor: per, defaults: defaults}, nil
}

// ---- passthrough: Substrate owns denial ------------------------------------

// passthroughSource allows everything and leaves denial to Substrate's own
// per-actor egress policy. Correct once that API exists: two engines enforcing
// the same rule is two places to disagree, and the one closer to the network
// should win.
type passthroughSource struct{}

func (s *passthroughSource) Name() string { return "passthrough" }

func (s *passthroughSource) Allows(_ *Actor, _ string) (bool, string) {
	return true, "policy delegated to Substrate"
}

// ---- the policy: a source plus credential injection ------------------------

type Policy struct {
	source Source
	// caps is what Envoy actually sends us. A rule we cannot evaluate is a
	// rule we must not pretend to evaluate: see needsBody below.
	caps Capabilities
	// credentials maps destination host -> how to get the header to attach.
	// Only hosts we hold something for appear here; a host can be allowed with
	// no credential, which is a real case (the request then fails upstream
	// with 401, exactly as in Substrate's own egress demo).
	credentials map[string]Credential
}

func NewPolicy(ctx context.Context) (*Policy, error) {
	// Google's generative-language API takes a raw key in x-goog-api-key, NOT
	// a bearer token; sending Authorization: Bearer returns a 401 about OAuth
	// that sends you looking in entirely the wrong place.
	modelHost := envOr("MODEL_HOST", "generativelanguage.googleapis.com")
	modelHeader := envOr("MODEL_KEY_HEADER", "x-goog-api-key")

	// Which credential to attach. A short lived OAuth token is preferred: if
	// one ever leaks, an hour on one scope is a smaller problem than a
	// standing key that never expires. The static key stays because a plain
	// API key is what some endpoints take, and because it is one less moving
	// part when diagnosing.
	var cred Credential
	var err error
	switch mode := strings.ToLower(envOr("MODEL_CREDENTIAL", "oauth")); mode {
	case "oauth":
		path := os.Getenv("MODEL_SA_KEY_FILE")
		if path == "" {
			return nil, fmt.Errorf("MODEL_CREDENTIAL=oauth needs MODEL_SA_KEY_FILE")
		}
		scopes := strings.Fields(strings.ReplaceAll(
			envOr("MODEL_SCOPES", "https://www.googleapis.com/auth/cloud-platform"), ",", " "))
		oc, oerr := newOAuthCredential(ctx, path, scopes)
		if oerr != nil {
			return nil, oerr
		}
		go oc.run(ctx)
		cred = oc
	case "static":
		path := os.Getenv("MODEL_KEY_FILE")
		if path == "" {
			return nil, fmt.Errorf("MODEL_CREDENTIAL=static needs MODEL_KEY_FILE")
		}
		if cred, err = newStaticCredential(modelHeader, path); err != nil {
			return nil, err
		}
	case "none":
		// Hold nothing and enforce destinations only. This is the honest
		// setting on Substrate's egress chain, where the request arrives as a
		// CONNECT and no header we add can reach the upstream anyway. It also
		// lets the plugin run somewhere that has no credential to give it.
		cred = nil
	default:
		return nil, fmt.Errorf("MODEL_CREDENTIAL=%q is not one of oauth, static, none", mode)
	}

	defaults := []string{modelHost}
	var source Source
	switch mode := strings.ToLower(envOr("EGRESS_POLICY_SOURCE", "static")); mode {
	case "static":
		source = &staticSource{allowed: defaults}
	case "registry":
		path := os.Getenv("EGRESS_REGISTRY_FILE")
		if path == "" {
			return nil, fmt.Errorf("EGRESS_POLICY_SOURCE=registry needs EGRESS_REGISTRY_FILE")
		}
		if source, err = loadRegistry(path, defaults); err != nil {
			return nil, err
		}
	case "passthrough":
		source = &passthroughSource{}
	default:
		return nil, fmt.Errorf("EGRESS_POLICY_SOURCE=%q is not one of static, registry, passthrough", mode)
	}

	caps, err := ParseCapabilities(envOr("EGRESS_EXTPROC_MODES", "headers"))
	if err != nil {
		return nil, err
	}

	// A nil credential must not land in the map: a host present there means "we
	// are supposed to attach something here", and Decide refuses when it cannot.
	credentials := map[string]Credential{}
	if cred != nil {
		credentials[strings.ToLower(modelHost)] = cred
	}

	return &Policy{
		source:      source,
		caps:        caps,
		credentials: credentials,
	}, nil
}

// needsBody reports whether a request cannot be judged safe from its headers
// alone.
//
// Permission-write endpoints require inspecting their payload: a header alone
// cannot distinguish a permitted recipient from public access. This conservative
// guard rejects permission writes that need application-level authorization.
//
// If Envoy is not sending bodies, such a request is REFUSED. That is the whole
// point of tracking capabilities: the alternative is to allow it unread, which
// silently converts a policy we advertise into one we do not have.
func (p *Policy) needsBody(host, path, method string) bool {
	if method == "GET" || method == "HEAD" {
		return false
	}
	return strings.Contains(strings.ToLower(host), "googleapis.com") &&
		strings.Contains(strings.ToLower(path), "/permissions")
}

// Decide answers for one request. Default deny: an unrecognised destination is
// refused rather than forwarded uncredentialed, because a refusal is a clear
// failure and a silent pass-through is an unauthenticated call we did not mean.
func (p *Policy) Decide(actor *Actor, authority, path, method string) Decision {
	host := hostOnly(authority)
	if host == "" {
		return Decision{Allow: false, Reason: "request carries no :authority"}
	}
	allowed, reason := p.source.Allows(actor, host)
	if !allowed {
		return Decision{Allow: false, Reason: reason}
	}
	if p.needsBody(host, path, method) && !p.caps.RequestBody {
		return Decision{
			Allow: false,
			Reason: "this call can only be judged from its body, and Envoy is " +
				"not sending bodies to the plugin (EGRESS_EXTPROC_MODES has no " +
				"'body'), so it is refused rather than allowed unread",
		}
	}
	if cred, ok := p.credentials[strings.ToLower(host)]; ok {
		// Substrate's egress listener terminates CONNECT: it routes on
		// connect_matcher and then splices raw TCP to the target. On such a
		// request the only headers Envoy parses are the CONNECT's own, and the
		// actor's real request travels inside the tunnel as opaque bytes. A
		// header we add here is therefore consumed establishing the tunnel and
		// never reaches the upstream, which we verified against a live cluster:
		// the plugin reported the mutation and the upstream's echo of the
		// request it received did not contain the header.
		//
		// We allow the request and say so loudly rather than refusing. Nothing
		// leaks either way, and refusing would break every model call, which is
		// a product decision rather than one for this function. Destination
		// policy above is unaffected and still applies.
		if strings.EqualFold(method, "CONNECT") {
			return Decision{
				Allow:                   true,
				Reason:                  reason + " (credential NOT injected: CONNECT tunnel)",
				CredentialUndeliverable: true,
			}
		}
		h, err := cred.Header()
		if err != nil {
			// We are supposed to credential this host and cannot. Refuse
			// rather than forward it bare: a request that arrives without
			// the credential it needed fails upstream anyway, and an
			// explicit refusal names the real cause instead of surfacing
			// as a confusing 401 from Google.
			return Decision{Allow: false,
				Reason: fmt.Sprintf("no usable credential for %s: %v", host, err)}
		}
		return Decision{Allow: true, Reason: reason, Headers: []Header{h}}
	}
	// Allowed, but we hold nothing for it. Let it through bare rather than
	// inventing a credential; upstream decides.
	return Decision{Allow: true, Reason: reason + " (no credential held)"}
}

func hostOnly(authority string) string {
	authority = strings.TrimSpace(authority)
	if authority == "" {
		return ""
	}
	if i := strings.LastIndex(authority, ":"); i > 0 && !strings.Contains(authority[i+1:], "]") {
		return authority[:i]
	}
	return authority
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// CredentialName describes what the plugin holds, for the startup log. Reports
// "none" rather than dereferencing nothing when no credential is configured,
// which is the normal case on a chain that terminates CONNECT.
func (p *Policy) CredentialName() string {
	if len(p.credentials) == 0 {
		return "none, destination policy only"
	}
	names := make([]string, 0, len(p.credentials))
	for host, c := range p.credentials {
		if c == nil {
			continue
		}
		names = append(names, fmt.Sprintf("%s for %s", c.Name(), host))
	}
	if len(names) == 0 {
		return "none, destination policy only"
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
