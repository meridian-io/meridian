package rest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSelfSignedCert creates a throwaway localhost cert/key pair.
func writeSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestRunnableServesAndShutsDownGracefully(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t)
	addr := freeAddr(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r := &Runnable{Server: &http.Server{Addr: addr, Handler: mux}, CertFile: certFile, KeyFile: keyFile, ShutdownTimeout: time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // test-only self-signed cert
	var resp *http.Response
	var err error
	for i := 0; i < 50; i++ {
		if resp, err = client.Get("https://" + addr + "/healthz"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("server never became reachable: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v after cancel, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
	if _, err := client.Get("https://" + addr + "/healthz"); err == nil {
		t.Error("server still accepting connections after shutdown")
	}
}

func TestRunnableReportsServeError(t *testing.T) {
	r := &Runnable{Server: &http.Server{Addr: freeAddr(t)}, CertFile: "/nonexistent.crt", KeyFile: "/nonexistent.key"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Start(ctx); err == nil {
		t.Fatal("missing certificate must surface as an error so the operator stops")
	}
}

func TestRunnableRunsOnEveryReplica(t *testing.T) {
	if (&Runnable{}).NeedLeaderElection() {
		t.Error("REST API should serve on every replica, not only the leader")
	}
}

// TestRunnableCancelsInFlightRequests verifies a slow handler (like a
// reservation waiting for a free cluster) is cancelled on shutdown.
func TestRunnableCancelsInFlightRequests(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t)
	addr := freeAddr(t)
	cancelled := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
			close(cancelled)
		case <-time.After(10 * time.Second):
		}
	})
	r := &Runnable{Server: &http.Server{Addr: addr, Handler: mux}, CertFile: certFile, KeyFile: keyFile, ShutdownTimeout: 2 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // test-only self-signed cert
	go func() {
		for i := 0; i < 50; i++ {
			if resp, err := client.Get("https://" + addr + "/slow"); err == nil {
				resp.Body.Close()
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight request was not cancelled on shutdown")
	}
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
}

func TestNewServerRejectsBadClientCA(t *testing.T) {
	if _, err := NewServer(":0", nil, nil, "ns", "/nonexistent/ca.crt"); err == nil {
		t.Error("missing CA file must be an error, not a silent fallback to no client auth")
	}
	bad := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(bad, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewServer(":0", nil, nil, "ns", bad); err == nil {
		t.Error("CA file without certificates must be an error")
	}
}

func TestNewServerRequiresClientCertWithCA(t *testing.T) {
	ca, _ := writeSelfSignedCert(t)
	srv, err := NewServer(":0", nil, nil, "ns", ca)
	if err != nil {
		t.Fatal(err)
	}
	if srv.TLSConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", srv.TLSConfig.ClientAuth)
	}
	if srv.ReadHeaderTimeout == 0 {
		t.Error("ReadHeaderTimeout must be set to limit slow-header clients")
	}
}
