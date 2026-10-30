# Stream protocol v1

All HTTP JSON uses lowerCamelCase. All event times are Unix milliseconds. Kafka offsets and partitions are strings, not floating-point numbers. The protocol transports observations, not Kafka payloads.

## Connect

Connect to `ws://127.0.0.1:4318/api/stream` (use `wss` behind HTTPS). No client messages are needed. The server emits a current snapshot immediately, then pushes updates at the configured cadence when state changes, and a heartbeat frame at least every two seconds while connected.

On reconnect, pass both:

```text
/api/stream?instanceId=INSTANCE_FROM_LAST_FRAME&after=LAST_CURSOR
```

The server keeps the cursor/state lookup consistent under its state lock. Cursor values refer to service events, not Kafka offsets.

## Frame shape

```json
{
  "type": "snapshot",
  "instanceId": "random-id-for-this-service-process",
  "cursor": 120,
  "topologyVersion": 8,
  "replaceTopology": true,
  "resetEvents": true,
  "reason": null,
  "serverTime": 1793361600000,
  "nodes": [],
  "edges": [],
  "events": [],
  "stats": {},
  "visualEventsSkipped": 0
}
```

This is a structural example; numbers are illustrative.

- If `replaceTopology` is true, replace local node and edge maps with the supplied arrays. This occurs on initial connect, topology changes (including expiration), or a reset. Otherwise upsert supplied nodes and edges by `id`.
- If `resetEvents` is true, clear the viewer's old event buffer/correlation display before inserting the supplied observations. Initial/reset snapshots include the entire retained event ring (up to `MaxEvents`, default 5,000), subject to the frame byte budget. A newly connected viewer can calculate a retained-window rate immediately instead of warming up a 150-event tail. This does not promise a full minute when the collector itself has just started or retention/capacity has removed that history. Internal callers can explicitly request `Snapshot(recentEvents)` to limit history.
- Otherwise append only new events. A reconnect can receive `type: update` with `replaceTopology: true` and `resetEvents: false`: preserve the old event buffer and restore the full current graph.
- Always advance to the returned `cursor`, even if animation events were omitted. Aggregate `stats` includes all accepted observations in this process session.
- `visualEventsSkipped` counts events omitted from this frame to enforce the animation budget. This is a lossy visualization feed, not an audit log or guaranteed delivery stream.
- `reason` can be `instance_changed`, `cursor_expired`, or `invalid_cursor`. A valid reconnect cursor preserves new event delivery within the ring's retention/capacity, subject to the visual limit.
- `topologyVersion` changes when nodes/edges are created or removed. Activity counters can change without a topology version change.
- Additive `pathways` and `causality` fields are full replacements on every frame, including incremental/heartbeat frames. Never append pathway snapshots as new samples. See [otel-causality.md](otel-causality.md).
- Additive `coverage` includes `retainedEvents`, `returnedEvents`, nullable `oldestRetainedObservedAt`/`oldestReturnedObservedAt`, `eventsOmitted`, `nodesOmitted`, `edgesOmitted`, `pathwaysOmitted`, and `truncated`. A reset with `eventsOmitted > 0` is partial history; a rate calculated from it must be labeled partial. On incremental frames, event omissions describe the new event range only, including the animation limit; previously retained client history is not counted as omitted.
- Every serialized frame is bounded by `MaxFrameBytes` (default and maximum 3 MiB), leaving headroom below Grafana Live's default **4 MiB outbound client queue**, which is tighter than the app backend's 8 MiB transport limit. Oversized frames retain a smaller recent event tail, then reduce pathways/topology if necessary, reporting all omissions through `coverage`. A snapshot drops matched enrichments whose publication was removed by this byte reduction, so a displayed matched hop has its publication timing available. Counting uses a capped JSON stream rather than allocating an oversized JSON buffer. Incremental animation remains limited to `MaxVisualEventsPerFrame` (default 100).

## Nodes and edges

Nodes contain `id`, `kind` (`service` or `topic`), `name`, `serviceNamespace`, `environment`, `cluster`, `lastSeen`, `published`, `received`, and `errors`. Service identity is environment + namespace + service. Topic identity is environment + cluster + topic. IDs are stable hashes across restarts, while counters/cursors are not.

Nodes, edges and `stats` also contain additive `processed` counts. Processing updates incoming topology but never increments a wire receive/publish count or `edge.observations`. There is no synthetic `process` event in the legacy observation ring. Do not add received and processed counts as if they were distinct delivered messages.

Edges contain `id`, `source`, `target`, `operation`, `environment`, `cluster`, `topic`, `consumerGroup`, `lastSeen`, `observations`, and `errors`. Consumer group is part of edge identity. Multiple groups reaching one service remain distinct edges. Counts are observed operations; batch observations are not silently multiplied into individual messages.

## Events

`kind` is `publish`, `receive`, or `matched`.

Publish/receive fields include `sequence`, `observationId` (`traceId:spanId`), `source`, `target`, `topicNodeId`, `service`, `environment`, `cluster`, `topic`, nullable `consumerGroup`, `partition`, `offset`, `error`, `isBatch`, `occurredAt`, `observedAt`, `traceId`, and `spanId`.

`occurredAt` is the span completion time. In the .NET auto-instrumentation, a consume span covers the polling call; its start can precede message availability. Do not treat consume span duration as broker queue time or application processing duration.

A `matched` event refers to an existing receive `observationId` and adds `producerObservationId`, `producerService`, `producerTraceId`, and `correlationMethod` (`record` or `span_link`). It is an enrichment, not another receive. Do not increment message/operation counts when rendering it. It can arrive after the original receive, and multiple consumer groups can refer to the same publication. No calculated latency is asserted by the match itself.

Events can include `parent` and bounded allowlisted `dsmAttributes`. Complete parent/link vectors are held only in the globally bounded causal store, not duplicated into the event ring; inspect a retained span through `/api/causality/spans/{traceId}/{spanId}`. Cursors can advance for causal enrichment even when a frame contains no new animation events.

## Authentication

When `Flow__ApiKey` is set, ordinary clients can set `Authorization: Bearer KEY` on the WebSocket upgrade request. Browser JavaScript cannot set that header; it can offer these protocols instead:

```js
const protocols = ["liveflow.v1", "liveflow.auth." + base64urlUtf8(apiKey)];
const socket = new WebSocket(url, protocols);
```

The server selects only `liveflow.v1`; it never echoes the credential as its selected protocol. `base64urlUtf8` means base64 of the UTF-8 bytes, replacing `+`/`/` with `-`/`_` and removing padding. This encoding is not encryption: use TLS for network access and redact credential-bearing request headers in proxy logging. The included viewer keeps the key in tab memory and never places it in a URL or browser storage.

Cross-origin browser connections must have their exact `Origin` allowed in `Flow__AllowedOrigins__0`, etc. This is separate from bearer authentication. HTTP browser CORS access is not enabled; use a backend datasource for cross-origin HTTP queries. Static assets and `/healthz` are public.

## Flow control and lifecycle

At most `MaxViewers` concurrent sockets are admitted; further upgrade attempts return HTTP 429. Each socket has one receive loop and one send loop. Writes have a five-second deadline; slow viewers disconnect and recover with a snapshot/reconnect. Client messages are not an ingestion or command protocol. Long fragmented client messages are not accumulated.

Persist the latest instance ID and cursor only for the lifetime/history policy of your viewer. Graph and event state disappear when the service process exits. A disconnected stream cannot establish that Kafka traffic stopped; represent connection state separately from inactivity.
