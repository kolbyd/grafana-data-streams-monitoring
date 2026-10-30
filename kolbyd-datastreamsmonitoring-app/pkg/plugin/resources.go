package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func (a *App) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	status := http.StatusMethodNotAllowed
	body := []byte(`{"message":"Only GET is supported"}`)
	if req.Method == http.MethodGet {
		path := map[string]string{"topology": "/api/topology", "status": "/api/status"}[strings.Trim(req.Path, "/")]
		if path == "" {
			status = http.StatusNotFound
			body = []byte(`{"message":"Unknown resource"}`)
		} else {
			var err error
			body, status, err = a.get(ctx, path)
			if err != nil {
				body, _ = json.Marshal(map[string]string{"message": err.Error()})
			}
		}
	}
	return sender.Send(&backend.CallResourceResponse{Status: status, Body: body, Headers: map[string][]string{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}}})
}
