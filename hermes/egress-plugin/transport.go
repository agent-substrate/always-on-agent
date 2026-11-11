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

// How the egress gateway reaches this service.
//
// Not a plain gRPC listener. Substrate's
// --experimental-additional-egress-extproc-service does not generate a plaintext
// cluster; the Envoy it configures dials us over MUTUAL TLS and will not talk to
// anything else. Specifically, from
// hack/experimental-additional-egress-extproc.sh, it:
//
//   - pins TLS 1.3 as both the minimum AND the maximum. That pin exists because
//     Envoy's default ceiling for an UPSTREAM context is 1.2, only downstream
//     defaults to 1.3, while Go servers refuse to negotiate below 1.3 - left at
//     the defaults the two never agree and the handshake fails with
//     TLSV1_ALERT_PROTOCOL_VERSION on a config where both ends say 1.3;
//   - validates OUR certificate against the servicedns.podcert.ate.dev CA and
//     pins match_typed_subject_alt_names to DNS <service>.<namespace>.svc;
//   - presents ITS OWN certificate from the podidentity.podcert.ate.dev signer.
//
// TWO SIGNERS, AND THEY ARE NOT INTERCHANGEABLE. This is the part that is easy
// to get wrong, because the generated cluster names servicedns twice - once as
// the CA it verifies us against, once as the directory it watches - and never
// names podidentity except as a file path. The two roles are:
//
//	serving:  servicedns.podcert.ate.dev    the certificate WE present
//	clients:  podidentity.podcert.ate.dev   the CA that signed the GATEWAY
//
// Verifying the gateway against the servicedns bundle looks right, compiles, and
// fails every handshake. Because the failure happens before a single gRPC stream
// exists, this service logs nothing at all; the only symptom is Envoy reporting
// "upstream connect error or disconnect/reset before headers. reset reason:
// connection termination", and under failure_mode_allow: false that surfaces
// inside the sandbox as an HTTP 500 from the model provider. Measured, on the
// acceptance cluster, by making exactly this mistake.
//
// The servicedns certificate is the one that has to come from Substrate rather
// than from us: that signer issues for the Services which select the pod, so the
// DNS SAN the gateway pins is produced automatically, and a self-signed
// certificate cannot satisfy it however it is named.
//
// Three files are projected into the pod by the kubelet:
//
//	/run/servicedns.podcert.ate.dev/credential-bundle.pem    our chain AND key
//	/run/servicedns.podcert.ate.dev/trust-bundle.pem         (unused; see below)
//	/run/podidentity.podcert.ate.dev/trust-bundle.pem        the CA to verify the gateway
//
// The credential bundle really is chain and key in one file, which is why both
// halves of the keypair below are loaded from the same path. It ROTATES, well
// inside this process's lifetime, so nothing here may read it once at startup -
// see reloader. The servicedns trust bundle is projected but not read: it is what
// the gateway verifies us against, not what we verify anyone against, and it is
// left in place only because dropping it makes the volume look asymmetric next to
// the cluster config above.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Where the two projected credentials land. Overridable only so the unit tests
// can point at a temp dir.
const (
	servicednsDir  = "/run/servicedns.podcert.ate.dev"
	podidentityDir = "/run/podidentity.podcert.ate.dev"
)

const (
	credentialBundleFile = "credential-bundle.pem"
	trustBundleFile      = "trust-bundle.pem"
)

// defaultGatewaySpiffe is the identity atenet-egress runs as. Chaining to the
// podidentity CA only proves the caller is SOME pod in this cluster, and any pod
// that can reach us can authorise its own egress, so the name is pinned as well
// - the mirror image of the DNS SAN the gateway pins on us.
const defaultGatewaySpiffe = "spiffe://cluster.local/ns/ate-system/sa/atenet-egress"

// restatInterval bounds how often the files are stat-ed. The kubelet rotates
// these on the order of hours, so a second of staleness is irrelevant, but a
// stat per handshake is not free when the gateway opens a stream per request.
const restatInterval = time.Second

// serverTransport builds the gRPC options for the listener, and returns a short
// description of what it settled on for the startup log.
func serverTransport() ([]grpc.ServerOption, string, error) {
	mode := envOr("EGRESS_TLS_MODE", "mtls")
	switch mode {
	case "mtls":
		r := &reloader{
			servingDir:  envOr("POD_CERT_DIR", servicednsDir),
			clientCADir: envOr("CLIENT_CA_DIR", podidentityDir),
			peer:        envOr("EGRESS_GATEWAY_SPIFFE", defaultGatewaySpiffe),
		}
		// Load once here rather than only on first handshake: a missing or
		// malformed projection should stop the process at startup, where it is
		// obvious, not turn every egress request into a handshake failure that
		// this process never sees.
		if _, err := r.config(); err != nil {
			return nil, "", err
		}
		return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(&tls.Config{
			// Everything real is decided per handshake, so the credentials can
			// rotate under a running process.
			GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return r.config() },
			MinVersion:         tls.VersionTLS13,
		}))}, "mtls (serving servicedns, verifying podidentity)", nil
	case "insecure":
		// For kind, where there is no pod-certificate controller to issue from.
		// Loud, because a cluster that reaches this service unauthenticated has
		// handed an unauthenticated caller the ability to authorise egress.
		slog.Warn("EGRESS_TLS_MODE=insecure: serving plaintext gRPC. The egress gateway " +
			"will NOT connect to this - it dials mutual TLS - so this is for local " +
			"testing only.")
		return nil, "insecure (plaintext)", nil
	default:
		return nil, "", fmt.Errorf("EGRESS_TLS_MODE=%q: want mtls or insecure", mode)
	}
}

// reloader keeps the projected credentials fresh.
//
// The kubelet replaces these files by swapping a symlinked directory, so a
// process that read them at startup keeps serving a certificate that expired
// hours ago and fails every handshake with no log line of its own - the only
// symptom is the gateway reporting no healthy upstream, which under
// failure_mode_allow: false is a total egress outage. Hence: re-read.
type reloader struct {
	servingDir  string
	clientCADir string
	peer        string

	mu       sync.Mutex
	cached   *tls.Config
	loadedAt time.Time
	stamp    string
	// notAfter is the cached leaf's own expiry, kept because it is what bounds
	// serving the cached config when the files cannot be read. Stored rather
	// than re-derived: tls.Certificate.Leaf is not guaranteed to be populated
	// across the versions this has to build on, and re-parsing on a failure
	// path is exactly where a second bug would hide.
	notAfter time.Time
}

func (r *reloader) config() (*tls.Config, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if r.cached != nil && now.Sub(r.loadedAt) < restatInterval {
		return r.cached, nil
	}
	// Bound the retry rate on EVERY path out of here, including the failing
	// ones. This used to sit after the fingerprint check, which meant a Stat
	// error - the failure that actually happens during a rotation, when the
	// kubelet swaps the ..data symlink - was retried on every single handshake
	// for the length of the window, while the post-stamp read errors were
	// capped at one attempt a second. Apply the same bound to both paths.
	r.loadedAt = now

	stamp, err := r.fingerprint()
	if err != nil {
		return r.lastKnownGood(now, err)
	}
	if r.cached != nil && stamp == r.stamp {
		return r.cached, nil
	}

	bundlePath := filepath.Join(r.servingDir, credentialBundleFile)
	bundle, err := os.ReadFile(bundlePath)
	if err != nil {
		return r.lastKnownGood(now, fmt.Errorf("reading our serving credential %s: %w", bundlePath, err))
	}
	// Chain and key are in the one file, so it is passed as both halves.
	cert, err := tls.X509KeyPair(bundle, bundle)
	if err != nil {
		return r.lastKnownGood(now, fmt.Errorf("parsing %s as a certificate and key: %w", bundlePath, err))
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return r.lastKnownGood(now, fmt.Errorf("parsing the leaf of %s: %w", bundlePath, err))
	}

	// The CA that signed the GATEWAY, which is podidentity and not the signer
	// above. See the package comment.
	trustPath := filepath.Join(r.clientCADir, trustBundleFile)
	trust, err := os.ReadFile(trustPath)
	if err != nil {
		return r.lastKnownGood(now, fmt.Errorf("reading the client CA %s: %w", trustPath, err))
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(trust) {
		return r.lastKnownGood(now, fmt.Errorf("%s contains no usable CA certificate", trustPath))
	}

	if r.cached != nil {
		slog.Info("pod certificates rotated, reloaded")
	}
	r.cached = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		// The gateway presents its podidentity certificate. Requiring and
		// verifying it is what stops any other pod in the cluster from opening a
		// connection here; verifyPeer is what stops one that has a valid
		// podidentity certificate of its own - which is every pod in the
		// cluster - from being treated as the gateway.
		ClientAuth:            tls.RequireAndVerifyClientCert,
		VerifyPeerCertificate: verifyPeer(r.peer),
		// Matching Envoy's pin exactly. Setting only the minimum would work
		// today and would quietly start negotiating something else the day Go
		// ships a 1.4.
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		// ext_proc is gRPC, and the generated cluster configures HTTP/2
		// explicitly, so there is no reason to advertise anything else.
		NextProtos: []string{"h2"},
	}
	r.stamp = stamp
	r.notAfter = leaf.NotAfter
	return r.cached, nil
}

// lastKnownGood decides what to do when the projected credentials cannot be
// read. It serves the cached configuration, but ONLY while the certificate in
// it is still inside its own validity.
//
// The bound is the whole point. Serving last known good without one re-creates
// the fault we filed against atenet-egress: a component that keeps presenting a
// certificate that expired hours ago, failing every handshake, with no log line
// of its own - a silent, permanent outage traded for a one-second one. Failing
// closed on an expired cache is the correct end state; the cache exists to
// cover the seconds during a rotation, not to outlive the credential.
//
// Every call here logs. A rotation that trips this once a day is fine and
// invisible in metrics; a rotation that trips it continuously means the
// projection has broken, and that has to be visible before the expiry lands.
func (r *reloader) lastKnownGood(now time.Time, cause error) (*tls.Config, error) {
	if r.cached == nil {
		return nil, cause
	}
	if !now.Before(r.notAfter) {
		return nil, fmt.Errorf("credentials unreadable and the cached certificate "+
			"expired at %s: %w", r.notAfter.Format(time.RFC3339), cause)
	}
	slog.Warn("credentials unreadable, serving the last known good configuration",
		"error", cause, "cached_expires", r.notAfter.Format(time.RFC3339),
		"cached_valid_for", now.Sub(r.notAfter).Abs().Truncate(time.Second).String())
	return r.cached, nil
}

// verifyPeer checks the client's SPIFFE URI SAN against the expected gateway
// identity. Runs after the chain has already been verified against ClientCAs,
// so verifiedChains is populated and the leaf is trustworthy.
//
// An empty want disables the check, for a cluster that runs the gateway under a
// different service account than the stock install.
func verifyPeer(want string) func([][]byte, [][]*x509.Certificate) error {
	if want == "" {
		return nil
	}
	return func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
		for _, chain := range verifiedChains {
			if len(chain) == 0 {
				continue
			}
			for _, uri := range chain[0].URIs {
				if uri.String() == want {
					return nil
				}
			}
		}
		var got []string
		for _, chain := range verifiedChains {
			if len(chain) == 0 {
				continue
			}
			for _, uri := range chain[0].URIs {
				got = append(got, uri.String())
			}
		}
		return fmt.Errorf("client is %v, not the egress gateway (%s): set "+
			"EGRESS_GATEWAY_SPIFFE if this install runs atenet-egress elsewhere", got, want)
	}
}

// fingerprint is what "has it changed" is decided on. Size and mtime, not a
// hash of the contents: this runs on the handshake path and the files are only
// ever replaced wholesale.
func (r *reloader) fingerprint() (string, error) {
	var out string
	for _, p := range []string{
		filepath.Join(r.servingDir, credentialBundleFile),
		filepath.Join(r.clientCADir, trustBundleFile),
	} {
		fi, err := os.Stat(p)
		if err != nil {
			// os.Stat's own error already names the operation and the path, so
			// this adds why we were looking rather than repeating both.
			return "", fmt.Errorf("checking the projected credentials: %w", err)
		}
		out += fmt.Sprintf("%s:%d:%d;", p, fi.Size(), fi.ModTime().UnixNano())
	}
	return out, nil
}
