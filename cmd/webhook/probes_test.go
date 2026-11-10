package webhook

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func writeServingCert(t *testing.T, dir string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshalling key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0o600); err != nil {
		t.Fatalf("writing certificate: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0o600); err != nil {
		t.Fatalf("writing key: %v", err)
	}
}

func probeStatus(url string) (int, error) {
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, nil
}

func TestProbesReportWebhookServerStarted(t *testing.T) {
	certDir := t.TempDir()
	writeServingCert(t, certDir)
	probePort := freePort(t)

	mgr, err := manager.New(&rest.Config{Host: "https://127.0.0.1:1"}, manager.Options{
		Scheme:                 runtime.NewScheme(),
		HealthProbeBindAddress: fmt.Sprintf("127.0.0.1:%d", probePort),
		Metrics:                server.Options{BindAddress: "0"},
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
			Host:     "127.0.0.1",
			Port:     freePort(t),
			CertDir:  certDir,
			CertName: "tls.crt",
			KeyName:  "tls.key",
		}),
	})
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}
	if err := addProbes(mgr); err != nil {
		t.Fatalf("adding probes: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager stopped with an error: %v", err)
		}
	}()

	base := fmt.Sprintf("http://127.0.0.1:%d", probePort)
	for _, path := range []string{"/healthz", "/readyz", "/readyz/webhook"} {
		deadline := time.Now().Add(10 * time.Second)
		for {
			code, err := probeStatus(base + path)
			if err == nil && code == http.StatusOK {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s did not return 200: status %d, error %v", path, code, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
