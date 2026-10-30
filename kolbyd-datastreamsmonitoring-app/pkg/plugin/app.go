package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
)

const maxResponse = 8 * 1024 * 1024

var (
	_ backend.CallResourceHandler   = (*App)(nil)
	_ backend.CheckHealthHandler    = (*App)(nil)
	_ backend.StreamHandler         = (*App)(nil)
	_ instancemgmt.InstanceDisposer = (*App)(nil)
)

type App struct {
	base     *url.URL
	key      string
	client   *http.Client
	lifetime context.Context
	cancel   context.CancelFunc
}

func NewApp(_ context.Context, settings backend.AppInstanceSettings) (instancemgmt.Instance, error) {
	var config struct {
		ServiceURL string `json:"serviceUrl"`
	}
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &config); err != nil {
			return nil, errors.New("invalid app settings")
		}
	}
	var base *url.URL
	if config.ServiceURL != "" {
		var err error
		base, err = url.Parse(strings.TrimRight(config.ServiceURL, "/"))
		if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
			return nil, errors.New("service URL must be HTTP(S), without credentials, query or fragment")
		}
	}
	lifetime, cancel := context.WithCancel(context.Background())
	return &App{base: base, key: settings.DecryptedSecureJSONData["apiKey"], lifetime: lifetime, cancel: cancel,
		client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (a *App) Dispose() { a.cancel(); a.client.CloseIdleConnections() }

func (a *App) endpoint(path string) (string, error) {
	if a.base == nil {
		return "", errors.New("configure the flow service URL in plugin settings")
	}
	u := *a.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	return u.String(), nil
}

func (a *App) get(ctx context.Context, path string) ([]byte, int, error) {
	target, err := a.endpoint(path)
	if err != nil {
		return nil, 503, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, 502, errors.New("invalid service request")
	}
	if a.key != "" {
		req.Header.Set("Authorization", "Bearer "+a.key)
	}
	res, err := a.client.Do(req)
	if err != nil {
		return nil, 502, errors.New("flow service unavailable; check its address and connectivity")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, 502, errors.New("flow service rejected the request; check its API key and health")
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxResponse+1))
	if err != nil || len(body) > maxResponse || !json.Valid(body) {
		return nil, 502, errors.New("invalid or oversized flow service response")
	}
	return body, 200, nil
}

func (a *App) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	_, _, err := a.get(ctx, "/api/status")
	if err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: "Flow service connected"}, nil
}

func (a *App) SubscribeStream(ctx context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	if req.Path != "flow" {
		return &backend.SubscribeStreamResponse{Status: backend.SubscribeStreamStatusNotFound}, nil
	}
	body, _, err := a.get(ctx, "/api/topology")
	if err != nil {
		return nil, err
	}
	initial, err := backend.NewInitialData(body)
	if err != nil {
		return nil, err
	}
	return &backend.SubscribeStreamResponse{Status: backend.SubscribeStreamStatusOK, InitialData: initial}, nil
}

// The browser can subscribe but cannot publish observations through Grafana.
func (a *App) PublishStream(context.Context, *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return &backend.PublishStreamResponse{Status: backend.PublishStreamStatusPermissionDenied}, nil
}

func (a *App) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	if req.Path != "flow" {
		return errors.New("unknown stream")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(a.lifetime, cancel)
	defer stop()
	target, err := a.endpoint("/api/stream")
	if err != nil {
		return err
	}
	u, _ := url.Parse(target)
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	headers := http.Header{}
	if a.key != "" {
		headers.Set("Authorization", "Bearer "+a.key)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second, Subprotocols: []string{"liveflow.v1"}}
	delay := 250 * time.Millisecond
	for ctx.Err() == nil {
		conn, response, dialErr := dialer.DialContext(ctx, u.String(), headers)
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if dialErr == nil {
			delay = 250 * time.Millisecond
			conn.SetReadLimit(maxResponse)
			closeOnCancel := context.AfterFunc(ctx, func() { conn.Close() })
			for ctx.Err() == nil {
				_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
				_, payload, readErr := conn.ReadMessage()
				if readErr != nil {
					break
				}
				if !json.Valid(payload) {
					break
				}
				if err := sender.SendJSON(payload); err != nil {
					closeOnCancel()
					conn.Close()
					return err
				}
			}
			closeOnCancel()
			conn.Close()
		}
		if ctx.Err() != nil {
			return nil
		}
		if err := sender.SendJSON([]byte(`{"type":"connection","status":"reconnecting","message":"Flow service disconnected; reconnecting for a fresh snapshot"}`)); err != nil {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if delay < 8*time.Second {
			delay *= 2
		}
	}
	return nil
}
