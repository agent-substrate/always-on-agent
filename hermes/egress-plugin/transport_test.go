package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A miniature version of the two pod-certificate signers, because the mistake
// this file exists to catch is using one where the other belongs. Nothing short
// of a real handshake catches it: the wrong CA compiles, starts, logs a healthy
// listener, and then rejects every connection before gRPC is involved, so the
// process itself never sees an error.
type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

func newCA(t *testing.T, name string) *ca {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &ca{cert: cert, key: key, pem: string(pem.EncodeToMemory(
		&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

// issue returns a credential bundle: chain and key in one file, which is the
// shape the kubelet projects and therefore the shape the loader must accept.
func (c *ca) issue(t *testing.T, dnsName, spiffe string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		},
	}
	if dnsName != "" {
		tmpl.DNSNames = []string{dnsName}
	}
	if spiffe != "" {
		u, err := url.Parse(spiffe)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})) +
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

const testGatewaySpiffe = "spiffe://cluster.local/ns/ate-system/sa/atenet-egress"

// handshake runs one real TLS 1.3 exchange against the reloader's config and
// reports whichever end objected.
//
// Over a real loopback socket rather than net.Pipe. Under TLS 1.3 the client
// finishes its handshake before the server has looked at the client
// certificate, so a rejection arrives as an alert on the next read - and on an
// unbuffered pipe both ends then block writing close_notify to a peer that is
// also writing. A kernel socket buffer makes the exchange behave the way it
// does against Envoy.
func handshake(t *testing.T, r *reloader, clientCert tls.Certificate, serverCA *ca) error {
	t.Helper()
	serverCfg, err := r.config()
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(serverCA.pem))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		s := tls.Server(conn, serverCfg)
		defer s.Close()
		if err := s.Handshake(); err != nil {
			done <- err
			return
		}
		// Read once past the handshake. Client-certificate verification has
		// already happened by here, but this is also where a server that is
		// happy proves it can carry a byte.
		_, _ = s.Write([]byte("ok"))
		done <- nil
	}()

	raw, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	c := tls.Client(raw, &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      pool,
		// The gateway pins our DNS SAN; so does this.
		ServerName: "hermes-egress-plugin.hermes.svc",
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	})
	clientErr := c.Handshake()
	if clientErr == nil {
		// Surfaces a rejection the client could not have seen at Handshake time.
		_, clientErr = c.Read(make([]byte, 2))
	}
	c.Close()

	serverErr := <-done
	if serverErr != nil {
		return serverErr
	}
	return clientErr
}

// fixture wires up the projection the way the manifest does: our serving
// credential from the servicedns signer, the client CA from podidentity.
func fixture(t *testing.T) (*reloader, *ca, *ca) {
	t.Helper()
	servicedns, podidentity := newCA(t, "servicedns"), newCA(t, "podidentity")

	servingDir, clientCADir := t.TempDir(), t.TempDir()
	writeFile(t, servingDir, credentialBundleFile,
		servicedns.issue(t, "hermes-egress-plugin.hermes.svc", ""))
	writeFile(t, clientCADir, trustBundleFile, podidentity.pem)

	return &reloader{
		servingDir:  servingDir,
		clientCADir: clientCADir,
		peer:        testGatewaySpiffe,
	}, servicedns, podidentity
}

func TestTransportAcceptsTheGateway(t *testing.T) {
	r, servicedns, podidentity := fixture(t)
	bundle := podidentity.issue(t, "", testGatewaySpiffe)
	cert, err := tls.X509KeyPair([]byte(bundle), []byte(bundle))
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(t, r, cert, servicedns); err != nil {
		t.Fatalf("the gateway was refused: %v", err)
	}
}

// The regression. A certificate from the servicedns signer is a perfectly valid
// certificate that simply is not the gateway's, and verifying clients against
// the servicedns bundle -- which is what the generated Envoy cluster names, so
// it is the natural thing to copy -- accepts it while rejecting the real
// gateway.
func TestTransportRejectsTheWrongSigner(t *testing.T) {
	r, servicedns, _ := fixture(t)
	bundle := servicedns.issue(t, "", testGatewaySpiffe)
	cert, err := tls.X509KeyPair([]byte(bundle), []byte(bundle))
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(t, r, cert, servicedns); err == nil {
		t.Fatal("accepted a client signed by the servicedns CA; only podidentity signs the gateway")
	}
}

// Chaining to the podidentity CA proves only that the caller is some pod in the
// cluster, and any pod that can open this port can authorise its own egress.
func TestTransportRejectsAnotherPod(t *testing.T) {
	r, servicedns, podidentity := fixture(t)
	bundle := podidentity.issue(t, "", "spiffe://cluster.local/ns/default/sa/anything")
	cert, err := tls.X509KeyPair([]byte(bundle), []byte(bundle))
	if err != nil {
		t.Fatal(err)
	}
	err = handshake(t, r, cert, servicedns)
	if err == nil {
		t.Fatal("accepted an unrelated pod's podidentity certificate")
	}
	// net.Pipe surfaces the server's alert to the client as a generic error, so
	// check the server-side decision directly rather than the handshake text.
	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	if verifyPeer(testGatewaySpiffe)(nil, [][]*x509.Certificate{{leaf}}) == nil {
		t.Fatal("verifyPeer accepted an unrelated identity")
	}
}

func TestVerifyPeerNamesWhatItSaw(t *testing.T) {
	_, _, podidentity := fixture(t)
	bundle := podidentity.issue(t, "", "spiffe://cluster.local/ns/default/sa/impostor")
	block, _ := pem.Decode([]byte(bundle))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyPeer(testGatewaySpiffe)(nil, [][]*x509.Certificate{{leaf}})
	if err == nil {
		t.Fatal("accepted an impostor")
	}
	// Whoever debugs this at 2am needs the identity that turned up, not just
	// the one that was wanted.
	if !strings.Contains(err.Error(), "impostor") || !strings.Contains(err.Error(), testGatewaySpiffe) {
		t.Errorf("error %q names neither the peer nor the expectation", err)
	}
	if verifyPeer("") != nil {
		t.Error("an empty expectation should disable the check, not enforce it")
	}
}

// The kubelet swaps these files under a running process, and a config cached
// forever is a certificate that expires mid-soak.
func TestReloaderPicksUpRotation(t *testing.T) {
	r, _, podidentity := fixture(t)
	first, err := r.config()
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := r.config(); again != first {
		t.Error("reloaded within the restat interval; that is a stat per handshake")
	}

	// Rotate the client CA and defeat the interval, the way an hour would.
	writeFile(t, r.clientCADir, trustBundleFile, podidentity.pem+newCA(t, "next").pem)
	r.loadedAt = time.Now().Add(-2 * restatInterval)

	rotated, err := r.config()
	if err != nil {
		t.Fatal(err)
	}
	if rotated == first {
		t.Fatal("kept serving the old config after the projection changed")
	}
}

// A rotation is not atomic from this side: the kubelet swaps the ..data symlink
// and for a moment a Stat or a read fails. Under failure_mode_allow: false a
// failed handshake in that window is a total egress outage presenting as the
// model provider being down, so the cached configuration covers it.
//
// The bug this pins: the Stat error used to return BEFORE loadedAt was updated,
// so it was retried on every handshake for the length of the window while the
// read errors after it were capped at one a second. Exactly the wrong way round.
func TestReloaderServesLastKnownGoodWhileTheFilesAreUnreadable(t *testing.T) {
	r, _, _ := fixture(t)
	first, err := r.config()
	if err != nil {
		t.Fatal(err)
	}

	// Take the whole projection away, the way the symlink swap does, and defeat
	// the interval so the next call really does go and look.
	if err := os.Remove(filepath.Join(r.servingDir, credentialBundleFile)); err != nil {
		t.Fatal(err)
	}
	r.loadedAt = time.Now().Add(-2 * restatInterval)

	during, err := r.config()
	if err != nil {
		t.Fatalf("failed the handshake mid-rotation instead of serving the cache: %v", err)
	}
	if during != first {
		t.Fatal("served something other than the cached configuration")
	}
	// And the retry is bounded on the failing path, which is the actual defect.
	if time.Since(r.loadedAt) > restatInterval {
		t.Error("loadedAt was not updated on the failure path; every handshake in " +
			"the rotation window will re-stat and re-fail")
	}
}

// The bound on the above, and the reason it is not simply "serve the cache".
// Serving a certificate past its own expiry is the atenet-egress fault we filed:
// a permanent silent outage traded for a one-second one.
func TestReloaderFailsClosedOnceTheCachedCertificateExpires(t *testing.T) {
	r, _, _ := fixture(t)
	if _, err := r.config(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(r.servingDir, credentialBundleFile)); err != nil {
		t.Fatal(err)
	}
	r.loadedAt = time.Now().Add(-2 * restatInterval)
	r.notAfter = time.Now().Add(-time.Second)

	if _, err := r.config(); err == nil {
		t.Fatal("served a cached configuration whose certificate had expired")
	} else if !strings.Contains(err.Error(), "expired") {
		t.Errorf("error %q does not say the cache expired, which is what an "+
			"operator needs to tell this apart from a bad projection", err)
	}
}

// With no cache to fall back on there is nothing to serve, and starting up
// against an unreadable projection must still fail rather than pretend.
func TestReloaderWithNoCacheStillFails(t *testing.T) {
	r, _, _ := fixture(t)
	if err := os.Remove(filepath.Join(r.servingDir, credentialBundleFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.config(); err == nil {
		t.Fatal("returned a configuration with nothing to build one from")
	}
}

func TestServerTransportRejectsAnUnknownMode(t *testing.T) {
	t.Setenv("EGRESS_TLS_MODE", "tls")
	if _, _, err := serverTransport(); err == nil {
		t.Fatal("accepted EGRESS_TLS_MODE=tls")
	}
}

// Failing at startup is the point: a missing projection must not become a
// per-request handshake failure that this process cannot see.
func TestServerTransportFailsFastOnAMissingProjection(t *testing.T) {
	t.Setenv("EGRESS_TLS_MODE", "mtls")
	t.Setenv("POD_CERT_DIR", t.TempDir())
	t.Setenv("CLIENT_CA_DIR", t.TempDir())
	_, _, err := serverTransport()
	if err == nil {
		t.Fatal("started with no credential projected")
	}
	if !strings.Contains(err.Error(), credentialBundleFile) {
		t.Errorf("error %q does not name the missing file", err)
	}
}

// A denial travels into an agent that will very likely re-emit it in a header.
// Hermes did, and a single trailing newline turned an explainable 403 into a 502
// with a stack trace pointing at the wrong component.
func TestDenyBodyCarriesNoControlCharacters(t *testing.T) {
	for _, reason := range []string{
		"agent-x is not allowed to reach example.com",
		"trailing\n",
		"embedded\r\nX-Injected: yes",
		"tab\tand\vvertical",
		"   ",
		strings.Repeat("very long reason ", 40),
	} {
		resp := denyResponse(reason)
		imm := resp.GetImmediateResponse()
		for _, s := range []string{string(imm.GetBody()), imm.GetDetails()} {
			for _, r := range s {
				if r < 0x20 || r == 0x7f {
					t.Errorf("reason %q produced control character %q in %q", reason, r, s)
				}
			}
			if s == "" {
				t.Errorf("reason %q produced an empty response part", reason)
			}
			if len(s) > 256 {
				t.Errorf("reason %q produced %d bytes; a policy message should not become the response",
					reason, len(s))
			}
		}
	}
}
