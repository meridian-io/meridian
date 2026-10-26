//go:build e2e

package e2e

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ── MCP (spec 2026-07-28 client) ─────────────────────────────────────────────

const mcpProtocolVersion = "2026-07-28"

type mcpClient struct {
	url   string
	token string
	http  *http.Client
}

func newMCPClient(token string) *mcpClient {
	return &mcpClient{url: cfg.mcpURL + "/mcp", token: token, http: &http.Client{Timeout: 60 * time.Second}}
}

type rpcResponse struct {
	Result *struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// rpc sends one stateless request with the per-request _meta and the
// MCP-Protocol-Version, Mcp-Method and Mcp-Name headers the spec requires.
func (c *mcpClient) rpc(method, name string, params map[string]any) (*rpcResponse, error) {
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{
		"io.modelcontextprotocol/protocolVersion":    mcpProtocolVersion,
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
		"io.modelcontextprotocol/clientInfo":         map[string]any{"name": "meridian-e2e", "version": "1"},
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, _ := http.NewRequest(http.MethodPost, c.url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("MCP-Protocol-Version", mcpProtocolVersion)
	req.Header.Set("Mcp-Method", method)
	if name != "" {
		req.Header.Set("Mcp-Name", name)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	// The reply is a JSON object or a request-scoped SSE stream.
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "data:") {
				raw = []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
	}
	var out rpcResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode %q: %w", raw, err)
	}
	if out.Error != nil {
		return nil, fmt.Errorf("JSON-RPC error %d: %s", out.Error.Code, out.Error.Message)
	}
	return &out, nil
}

// tryCall calls a tool. A tool failure (isError) is returned as an error.
func (c *mcpClient) tryCall(tool string, args map[string]any) (string, error) {
	resp, err := c.rpc("tools/call", tool, map[string]any{"name": tool, "arguments": args})
	if err != nil {
		return "", err
	}
	if resp.Result == nil {
		return "", fmt.Errorf("no result")
	}
	var text []string
	for _, ct := range resp.Result.Content {
		text = append(text, ct.Text)
	}
	out := strings.Join(text, "\n")
	if resp.Result.IsError {
		return out, fmt.Errorf("tool error: %s", out)
	}
	return out, nil
}

func (c *mcpClient) call(t *testing.T, tool string, args map[string]any) string {
	t.Helper()
	out, err := c.tryCall(tool, args)
	if err != nil {
		t.Fatalf("%s %v: %v", tool, args, err)
	}
	return out
}

// query runs SQL through execute_query and returns the formatted result.
func (c *mcpClient) query(cluster, sql string) (string, error) {
	return c.tryCall("execute_query", map[string]any{"cluster_name": cluster, "sql": sql})
}

// nodesQuery returns e2e-nodes-<N>, where N is the number of active Trino
// nodes. A healthy pool cluster has two: the coordinator and one worker.
const nodesQuery = "SELECT 'e2e-nodes-' || CAST(count(*) AS varchar) FROM system.runtime.nodes WHERE state = 'active'"

// ── REST API (mTLS) ──────────────────────────────────────────────────────────

type restClient struct {
	http *http.Client
}

func newRESTClient(ca *x509.CertPool, cert *tls.Certificate) *restClient {
	tc := &tls.Config{RootCAs: ca, MinVersion: tls.VersionTLS12}
	if cert != nil {
		tc.Certificates = []tls.Certificate{*cert}
	}
	return &restClient{http: &http.Client{
		Timeout:   60 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tc},
	}}
}

// do sends a request and returns the status code and body.
func (r *restClient) do(method, path string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, cfg.restURL+path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, nil
}

// ── Test PKI ─────────────────────────────────────────────────────────────────

type pki struct {
	caPEM, serverCertPEM, serverKeyPEM []byte
	pool                               *x509.CertPool
	ca                                 *x509.Certificate
	caKey                              *ecdsa.PrivateKey
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "meridian-e2e-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	p := &pki{ca: ca, caKey: key, pool: x509.NewCertPool()}
	p.pool.AddCert(ca)
	p.caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	p.serverCertPEM, p.serverKeyPEM = p.issue(t, "meridian-operator", x509.ExtKeyUsageServerAuth)
	return p
}

// issue signs a leaf certificate. Server certificates are valid for the
// host-mapped NodePort (127.0.0.1, localhost).
func (p *pki) issue(t *testing.T, cn string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if usage == x509.ExtKeyUsageServerAuth {
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
}

func (p *pki) clientCert(t *testing.T, cn string) *tls.Certificate {
	t.Helper()
	c, k := p.issue(t, cn, x509.ExtKeyUsageClientAuth)
	cert, err := tls.X509KeyPair(c, k)
	if err != nil {
		t.Fatal(err)
	}
	return &cert
}
