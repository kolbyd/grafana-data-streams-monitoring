package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func configuredApp(t *testing.T, base, key string) *App {
	t.Helper()
	settings, err := json.Marshal(map[string]string{"serviceUrl": base})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := NewApp(context.Background(), backend.AppInstanceSettings{
		JSONData: settings, DecryptedSecureJSONData: map[string]string{"apiKey": key},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := instance.(*App)
	t.Cleanup(a.Dispose)
	return a
}

func TestNewAppURLValidation(t *testing.T) {
	for _, base := range []string{"file:///etc/passwd", "ftp://example.com", "http://user:secret@example.com", "https://example.com?token=secret", "https://example.com#fragment", "//example.com", "http://"} {
		t.Run(base, func(t *testing.T) {
			settings, _ := json.Marshal(map[string]string{"serviceUrl": base})
			if _, err := NewApp(context.Background(), backend.AppInstanceSettings{JSONData: settings}); err == nil {
				t.Fatal("expected invalid service URL to be rejected")
			}
		})
	}
	if _, err := NewApp(context.Background(), backend.AppInstanceSettings{JSONData: []byte(`not-json`)}); err == nil {
		t.Fatal("expected malformed app settings to be rejected")
	}
	for _, base := range []string{"", "http://127.0.0.1:4318", "https://example.com/flow/"} {
		t.Run("valid-"+base, func(t *testing.T) { configuredApp(t, base, "") })
	}
}

type resourceSender struct{ response *backend.CallResourceResponse }

func (s *resourceSender) Send(response *backend.CallResourceResponse) error {
	s.response = response
	return nil
}

func TestResourceAllowlistAndAuthentication(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Authorization") != "Bearer test-secret" {
			t.Error("configured secure API key was not forwarded")
		}
		if r.Header.Get("Cookie") != "" {
			t.Error("Grafana browser credentials must not be forwarded upstream")
		}
		if r.Method != http.MethodGet || (r.URL.Path != "/flow/api/status" && r.URL.Path != "/flow/api/topology") {
			t.Errorf("unexpected upstream request %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"instanceId":"test","nodes":[],"edges":[],"events":[]}`))
	}))
	defer server.Close()
	a := configuredApp(t, server.URL+"/flow", "test-secret")
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "topology", 200}, {"GET", "/status", 200},
		{"GET", "../../v1/traces", 404}, {"GET", "topology?url=http://other.invalid", 404}, {"GET", "unknown", 404},
		{"POST", "topology", 405}, {"POST", "v1/traces", 405}, {"DELETE", "status", 405},
	} {
		t.Run(tc.method+"-"+tc.path, func(t *testing.T) {
			before := requests.Load()
			sender := &resourceSender{}
			err := a.CallResource(context.Background(), &backend.CallResourceRequest{Method: tc.method, Path: tc.path,
				Headers: map[string][]string{"Cookie": {"grafana_session=must-not-leave"}}}, sender)
			if err != nil {
				t.Fatal(err)
			}
			if sender.response == nil || sender.response.Status != tc.status {
				t.Fatalf("expected status %d, got %#v", tc.status, sender.response)
			}
			if !json.Valid(sender.response.Body) {
				t.Fatal("resource response must be JSON")
			}
			if strings.Contains(string(sender.response.Body), "test-secret") {
				t.Fatal("secret was reflected in resource response")
			}
			if sender.response.Headers["Cache-Control"][0] != "no-store" {
				t.Fatal("resource must not be cached")
			}
			if tc.status != 200 && requests.Load() != before {
				t.Fatal("disallowed resource called upstream")
			}
		})
	}
	if requests.Load() != 2 {
		t.Fatalf("expected only two allowed upstream calls, got %d", requests.Load())
	}
}

func TestUpstreamFailuresAreBoundedAndSanitized(t *testing.T) {
	var redirected atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1); _, _ = w.Write([]byte(`{}`)) }))
	defer redirectTarget.Close()
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"redirect", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, redirectTarget.URL, 302) }},
		{"unauthorized", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(401)
			_, _ = w.Write([]byte("upstream secret details"))
		}},
		{"invalid-json", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("not json")) }},
		{"oversized", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"data":"` + strings.Repeat("x", maxResponse) + `"}`))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			a := configuredApp(t, server.URL, "test-secret")
			body, status, err := a.get(context.Background(), "/api/status")
			if err == nil || status != 502 || len(body) != 0 {
				t.Fatalf("expected sanitized 502 failure, got status %d, error %v", status, err)
			}
			if strings.Contains(err.Error(), "test-secret") || strings.Contains(err.Error(), "upstream secret details") {
				t.Fatal("upstream details leaked")
			}
		})
	}
	if redirected.Load() != 0 {
		t.Fatal("authenticated requests must not follow upstream redirects")
	}
}

func TestMissingConfigurationAndCancelledRequest(t *testing.T) {
	a := configuredApp(t, "", "")
	_, status, err := a.get(context.Background(), "/api/status")
	if status != 503 || err == nil {
		t.Fatal("missing configuration must produce actionable 503")
	}
	health, err := a.CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil || health.Status != backend.HealthStatusError {
		t.Fatal("missing configuration must fail health check")
	}
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	a = configuredApp(t, server.URL, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if _, _, err := a.get(ctx, "/api/status"); err == nil {
		t.Fatal("cancelled request must fail")
	}
	if time.Since(started) > time.Second {
		t.Fatal("cancelled request did not stop promptly")
	}
}

func TestSubscribeFreshSnapshotAndReadOnlyPublication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/topology" {
			t.Errorf("unexpected snapshot endpoint %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"type":"snapshot","instanceId":"current","cursor":42}`))
	}))
	defer server.Close()
	a := configuredApp(t, server.URL, "")
	result, err := a.SubscribeStream(context.Background(), &backend.SubscribeStreamRequest{Path: "flow"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != backend.SubscribeStreamStatusOK || result.InitialData == nil || !strings.Contains(string(result.InitialData.Data()), `"cursor":42`) {
		t.Fatal("subscribers need a fresh current snapshot")
	}
	missing, err := a.SubscribeStream(context.Background(), &backend.SubscribeStreamRequest{Path: "arbitrary"})
	if err != nil || missing.Status != backend.SubscribeStreamStatusNotFound {
		t.Fatal("unknown stream must be rejected")
	}
	published, err := a.PublishStream(context.Background(), &backend.PublishStreamRequest{Path: "flow", Data: []byte(`{"inject":true}`)})
	if err != nil || published.Status != backend.PublishStreamStatusPermissionDenied {
		t.Fatal("browser publications must be denied")
	}
}

type packetSender struct {
	packets chan []byte
	failure error
}

func (s *packetSender) Send(packet *backend.StreamPacket) error {
	if s.failure != nil {
		return s.failure
	}
	s.packets <- append([]byte(nil), packet.Data...)
	return nil
}

func waitPacket(t *testing.T, packets <-chan []byte) []byte {
	t.Helper()
	select {
	case packet := <-packets:
		return packet
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stream packet")
		return nil
	}
}

func TestStreamForwardsAndReconnectsWithFreshSnapshot(t *testing.T) {
	var connections atomic.Int32
	upgrader := websocket.Upgrader{Subprotocols: []string{"liveflow.v1"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/flow/api/stream" || r.Header.Get("Authorization") != "Bearer stream-secret" {
			t.Error("WebSocket path or server-side authorization missing")
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		n := connections.Add(1)
		if err := conn.WriteJSON(map[string]any{"type": "snapshot", "instanceId": "test", "cursor": n}); err != nil {
			t.Error(err)
			return
		}
		if n > 1 {
			_, _, _ = conn.ReadMessage()
		}
	}))
	defer server.Close()
	a := configuredApp(t, server.URL+"/flow", "stream-secret")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	packets := &packetSender{packets: make(chan []byte, 20)}
	done := make(chan error, 1)
	go func() {
		done <- a.RunStream(ctx, &backend.RunStreamRequest{Path: "flow"}, backend.NewStreamSender(packets))
	}()
	if packet := waitPacket(t, packets.packets); !strings.Contains(string(packet), `"cursor":1`) {
		t.Fatalf("expected first snapshot, got %s", packet)
	}
	if packet := waitPacket(t, packets.packets); !strings.Contains(string(packet), `"status":"reconnecting"`) {
		t.Fatalf("expected explicit reconnect notice, got %s", packet)
	}
	if packet := waitPacket(t, packets.packets); !strings.Contains(string(packet), `"cursor":2`) {
		t.Fatalf("expected fresh snapshot after reconnect, got %s", packet)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled stream did not close the blocked upstream reader")
	}
}

func TestStreamStopsWhenDisposedOrSenderFails(t *testing.T) {
	for _, failSender := range []bool{false, true} {
		t.Run(map[bool]string{false: "dispose", true: "sender-error"}[failSender], func(t *testing.T) {
			upgrader := websocket.Upgrader{Subprotocols: []string{"liveflow.v1"}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"snapshot"}`))
				_, _, _ = conn.ReadMessage()
			}))
			defer server.Close()
			a := configuredApp(t, server.URL, "")
			packets := &packetSender{packets: make(chan []byte, 10)}
			if failSender {
				packets.failure = errors.New("client departed")
			}
			done := make(chan error, 1)
			go func() {
				done <- a.RunStream(context.Background(), &backend.RunStreamRequest{Path: "flow"}, backend.NewStreamSender(packets))
			}()
			if !failSender {
				waitPacket(t, packets.packets)
				a.Dispose()
			}
			select {
			case err := <-done:
				if failSender && err == nil {
					t.Fatal("sender failure must terminate stream")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("stream did not terminate")
			}
		})
	}
}

func TestStreamRejectsInvalidAndOversizedMessages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"invalid-json", []byte("not-json")},
		{"oversized", []byte(`{"data":"` + strings.Repeat("x", maxResponse) + `"}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{Subprotocols: []string{"liveflow.v1"}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.WriteMessage(websocket.TextMessage, tc.payload)
				_, _, _ = conn.ReadMessage()
			}))
			defer server.Close()
			a := configuredApp(t, server.URL, "")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			packets := &packetSender{packets: make(chan []byte, 10)}
			done := make(chan error, 1)
			go func() {
				done <- a.RunStream(ctx, &backend.RunStreamRequest{Path: "flow"}, backend.NewStreamSender(packets))
			}()
			packet := waitPacket(t, packets.packets)
			if !strings.Contains(string(packet), `"status":"reconnecting"`) {
				t.Fatal("invalid or oversized upstream payload must be discarded before delivery")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cancelled reconnect did not stop")
			}
		})
	}
}
