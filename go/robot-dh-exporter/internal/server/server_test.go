package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestHealthzReturnsOK(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := New(":0", reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(s.Handler)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestMetricsExposesRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "robot_dh_test_gauge", Help: "test"})
	g.Set(42)
	reg.MustRegister(g)

	s := New(":0", reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(s.Handler)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, string(body))
	}
	if !contains(string(body), "robot_dh_test_gauge 42") {
		t.Fatalf("metric not exposed: %s", string(body))
	}
}

func TestRunReturnsOnContextCancel(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := New("127.0.0.1:0", reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// 必须先用 Listen 才能拿到端口；但 Run 走 ListenAndServe 内部直接监听。
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	if err := Run(ctx, s, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(sub) > 0 && (indexOf(s, sub) >= 0)))
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
