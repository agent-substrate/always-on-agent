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

// Who is calling.
//
// There are two answers, because there are two legs, and which one is available
// depends on where the filter is attached.
//
// On the CONNECT leg the actor's mutual TLS is terminated by the listener the
// filter sits on, so Envoy populates x-forwarded-client-cert and
// ActorFromXFCC reads the identity out of the presented chain. That is
// trustworthy because the chain sets forward_client_cert_details: SANITIZE_SET,
// so whatever an actor puts in that header is DISCARDED and replaced by Envoy's
// own value, derived from the certificate it already validated against the
// actor-identity CA. An actor cannot name itself; it can only present a
// certificate.
//
// On the inner MITM listener there is no such header, and this is not a
// configuration we can change: the actor's mutual TLS was already terminated
// outside, on the CONNECT leg, so by the time the tunnelled request is decrypted
// there is no client certificate on the connection to forward. Measured, not
// assumed - with only the XFCC reader in place every request on that leg is
// refused as unidentified.
//
// What the MITM leg has instead is the identity the CONNECT leg already
// established, republished as a filter-state object. Substrate's generated
// ext_proc filter lists it under request_attributes:
//
//	request_attributes:
//	- filter_state['dev.ate.actor.identity']
//
// so it arrives as a request ATTRIBUTE rather than a header, carrying the same
// SPIFFE ID the certificate did. It is the same value the gateway's own access
// log prints as %FILTER_STATE(dev.ate.actor.identity:PLAIN)%, which is a cheap
// way to confirm it is populated without deploying anything.
//
// Reference implementation for the attribute read:
// egress-plugin-example/internal/extproc/actor.go in the Substrate example.
//
// Both readers end at the same place: a SPIFFE ID, parsed by actorFromSpiffeURI.
package main

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// Actor is who Envoy says is on the other end of the connection.
type Actor struct {
	Atespace string
	Name     string
	Spiffe   string
}

// spiffeHost is the trust domain ateapi issues actor certificates under. The
// URI SAN is spiffe://substrate-actor.local/atespace/<atespace>/actor/<name>.
const spiffeHost = "substrate-actor.local"

// actorFilterStateName is the filter-state object the egress gateway publishes
// the calling actor under, set on the CONNECT leg and readable on the MITM leg.
const actorFilterStateName = "dev.ate.actor.identity"

// actorAttribute is the CEL expression Substrate's generated filter lists under
// request_attributes, and therefore the key the value arrives under.
const actorAttribute = "filter_state['" + actorFilterStateName + "']"

// The gateway published these before
// https://github.com/agent-substrate/substrate/commit/e748574
// (merged 2026-09-01). Accepted so one plugin build spans the rename, which
// means the two can be upgraded in either order.
//
// Delete once no install this runs against predates that commit. The cluster
// that still does is reinstalled off the 2026-09-04 build for the second
// validation pass, so the window closes the week of 2026-09-15. Deleting these
// four lines is the whole removal; drop the note in egress-plugin/README.md
// with them.
const legacyActorFilterStateName = "ate.actor.identity"
const legacyActorAttribute = "filter_state['" + legacyActorFilterStateName + "']"

// ActorFromFilterState extracts the calling actor from an ext_proc request's
// attributes.
//
// The map is keyed by the name of the filter that contributed the attributes,
// which we do not want to depend on, so every contributor is checked. Within a
// contributor the value sits under the literal CEL expression that requested it;
// the bare object name is accepted too, because it costs one lookup and covers
// an Envoy that reports it the other way. The legacy spellings are checked
// last, so a gateway publishing both is read under the current name.
func ActorFromFilterState(attrs map[string]*structpb.Struct) (*Actor, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("the request carried no attributes: the ext_proc filter is not "+
			"configured with request_attributes: [%s]", actorAttribute)
	}
	for _, contributed := range attrs {
		fields := contributed.GetFields()
		for _, key := range []string{
			actorAttribute, actorFilterStateName,
			legacyActorAttribute, legacyActorFilterStateName,
		} {
			v, ok := fields[key]
			if !ok {
				continue
			}
			uri := strings.TrimSpace(v.GetStringValue())
			if uri == "" {
				// Present but empty means the gateway asserted no identity,
				// which is a different fault from never having been asked.
				return nil, fmt.Errorf("%s is present but empty: the gateway asserted no actor", key)
			}
			return actorFromSpiffeURI(uri)
		}
	}
	return nil, fmt.Errorf("no %s in the request attributes", actorAttribute)
}

// ActorFromXFCC extracts the calling actor from an x-forwarded-client-cert
// header value.
//
// XFCC is a comma-separated list of elements, each a semicolon-separated list of
// Key=Value pairs; values may be quoted. The full presented chain, leaf first,
// is URL-encoded PEM under the "Chain" key. Keys are case-insensitive.
func ActorFromXFCC(header string) (*Actor, error) {
	if strings.TrimSpace(header) == "" {
		return nil, fmt.Errorf("no %s header: the request did not arrive over the egress filter chain", xfccHeader)
	}
	chain, err := xfccChain(header)
	if err != nil {
		return nil, err
	}
	leaf, err := leafOf(chain)
	if err != nil {
		return nil, err
	}
	return actorFromCert(leaf)
}

// xfccChain pulls the URL-decoded PEM chain out of an XFCC header.
func xfccChain(header string) (string, error) {
	for _, element := range splitUnquoted(header, ',') {
		for _, kv := range splitUnquoted(element, ';') {
			key, value, found := strings.Cut(kv, "=")
			if !found || !strings.EqualFold(strings.TrimSpace(key), "chain") {
				continue
			}
			value = strings.TrimSpace(value)
			// Envoy quotes any value containing separators, which a PEM
			// always does once URL-encoded.
			if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
				value = value[1 : len(value)-1]
			}
			decoded, err := url.QueryUnescape(value)
			if err != nil {
				return "", fmt.Errorf("chain is not valid URL-encoding: %w", err)
			}
			return decoded, nil
		}
	}
	return "", fmt.Errorf("%s carries no Chain key, so no certificate was presented", xfccHeader)
}

// leafOf parses a PEM chain and returns the leaf, which XFCC puts first.
func leafOf(chainPEM string) (*x509.Certificate, error) {
	rest := []byte(chainPEM)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("no CERTIFICATE block in the presented chain")
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("leaf certificate does not parse: %w", err)
		}
		return cert, nil
	}
}

// actorFromCert reads the actor out of the leaf's SPIFFE URI SAN.
//
// Deliberately the SAN and not the custom ActorIdentity extension: the SAN is
// the documented contract (the plugin spec's ActorSpiffe field is exactly this
// string), it is what ateapi builds from atespace and actor name, and parsing it
// needs no Substrate-internal ASN.1 knowledge that could drift.
func actorFromCert(cert *x509.Certificate) (*Actor, error) {
	for _, uri := range cert.URIs {
		if uri.Scheme != "spiffe" || uri.Host != spiffeHost {
			continue
		}
		return actorFromSpiffeURI(uri.String())
	}
	return nil, fmt.Errorf("leaf certificate carries no spiffe://%s URI SAN", spiffeHost)
}

// actorFromSpiffeURI parses an actor's SPIFFE ID.
//
// The trust domain is checked here rather than taken on trust from the caller,
// because this is also the entry point for the filter-state string, which is
// text rather than a parsed SAN. Without the check a path of the right shape
// under some other authority would be accepted as an actor.
func actorFromSpiffeURI(raw string) (*Actor, error) {
	uri, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("identity %q is not a URI: %w", raw, err)
	}
	if uri.Scheme != "spiffe" || uri.Host != spiffeHost {
		return nil, fmt.Errorf("identity %q is not a spiffe://%s id", raw, spiffeHost)
	}
	parts := strings.Split(strings.Trim(uri.Path, "/"), "/")
	// atespace/<atespace>/actor/<name>
	if len(parts) != 4 || parts[0] != "atespace" || parts[2] != "actor" {
		return nil, fmt.Errorf("SPIFFE id %q is not in the actor form", raw)
	}
	if parts[1] == "" || parts[3] == "" {
		return nil, fmt.Errorf("SPIFFE id %q has an empty atespace or actor", raw)
	}
	return &Actor{Atespace: parts[1], Name: parts[3], Spiffe: uri.String()}, nil
}

// splitUnquoted splits on sep, ignoring separators inside double quotes.
func splitUnquoted(s string, sep rune) []string {
	var out []string
	var buf strings.Builder
	inQuotes := false
	for _, r := range s {
		switch {
		case r == '"':
			inQuotes = !inQuotes
			buf.WriteRune(r)
		case r == sep && !inQuotes:
			out = append(out, buf.String())
			buf.Reset()
		default:
			buf.WriteRune(r)
		}
	}
	out = append(out, buf.String())
	return out
}
