# Grafana integration boundary

The service is ready to receive OTLP and stream a normalized live graph. The bundled viewer is a working local review client. **No Grafana plugin has been installed, published, or modified by this delivery.**

## Review first

1. Run the local service and choose Start demo.
2. Inspect `/api/topology` and the `/api/stream` WebSocket frames.
3. Point a small real .NET pipeline or its existing collector at `/v1/traces`.
4. Verify resource and Kafka attributes before choosing a rendering adapter.

## Existing Grafana Cloud catalog components

The [Business Charts panel](https://grafana.com/grafana/plugins/volkovlabs-echarts-panel/) supports ECharts rendering, streaming data sources, and Grafana Live. The [WebSocket API datasource](https://grafana.com/grafana/plugins/golioth-websocket-datasource/) is a catalog candidate for directly reaching the live service. Compatibility, frame mapping, credentials, and browser network access still need validation in the user's stack; the WebSocket datasource's catalog version has not been updated recently.

The service emits graph snapshots/deltas and observation events together. A consumer adapter must implement the rules in [stream-protocol.md](stream-protocol.md), retain graph state, and present data to a panel. It should not blindly append all rows forever or treat `matched` as another received message. Updates are bounded; click-through traces can use the event's `traceId` and `producerTraceId` in the user's Tempo datasource.

For an HTTP prototype, a catalog JSON-capable datasource can query `/api/topology` and extract the `nodes` and `edges` arrays. Map node fields `id → id`, `name → title`, and `kind → subtitle`; map edge `id`, `source`, `target`, and `observations → mainstat`. This provides topology polling, not the live particle stream.

## Grafana Live

Grafana Live is an alternate transport between a Grafana datasource backend and its panel. This service is **not** a Grafana Live server. It does not pretend that `/api/live/push` accepts OTLP or custom graph JSON.

A future streaming datasource backend could subscribe to this service's WebSocket, fold snapshots/deltas into node/edge data frames, and publish through Grafana Live. A custom panel could consume observation events for individually timed particles. Initial snapshots, reconnect cursors, and visual omissions should remain explicit through that adapter.

Grafana Cloud requires plugins published in the catalog; privately signed or unsigned custom plugins cannot simply be uploaded. A custom datasource/panel therefore needs the appropriate review/publication route. See [Grafana Cloud plugin policy](https://grafana.com/docs/grafana-cloud/learn-and-build/get-started/learn/find-and-use-plugins/).

## Useful boundaries to preserve

- Keep the OTLP → live-service branch separate from the user's existing Grafana Cloud export branch.
- Apply collector tail sampling only after the branch point; app-side sampling has already removed spans.
- A browser-facing direct WebSocket needs a network-reachable TLS endpoint, an authentication approach supported by the chosen datasource, and the exact Grafana origin allowed by this service. This is not automatically provided by PDC or by setting an allowed origin.
- Keep particles bounded and render locally between updates. The local viewer caps concurrent particles; a Grafana renderer should have its own budget.
- Recompute layout on topology changes, not for every operation. Preserve separate consumer-group edges.
- Store history in Tempo or a separate event store if durable replay is required. Grafana Live and this service's in-memory buffer do not provide it.
