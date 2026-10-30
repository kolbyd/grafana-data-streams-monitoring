# Review results

Verified locally on 2026-10-30 using .NET SDK 10.0.403 and the ASP.NET Core 10 runtime on Windows x64.

## Service

- Locked NuGet restore succeeded.
- Release build succeeded with zero warnings and errors.
- All **34** named tests in the dependency-free test runner passed after the standard-OTel causal processor addition.
- The subsequent retained-history snapshot fix passed a clean Release build and **3 focused checks** (`--filter=snapshot`): a full minute with 360 publish/receive/matched events including reset reconnects and explicit count overrides; expired cursors with the full bounded ring and unchanged incremental animation limit; and JSON byte-budget/coverage enforcement with valid matched-publication pairs. The prior broad suite was not rerun for this focused change.
- A live continuity probe then found a tighter Grafana limit: direct collector frames were one snapshot followed by 14 continuous updates, but a ~4.56 MB Grafana subscription snapshot caused a WebSocket close code 3008 (`slow`). The running Grafana defaults confirmed its outbound client queue is 4 MiB. The collector default/maximum frame cap was corrected to 3 MiB; the focused byte-budget test also checks this default against a large snapshot. Grafana's 8 MiB incoming/backend limits alone do not protect its outbound queue.
- Integration tests launched real service processes and exercised HTTP/protobuf, OTLP JSON, gzip, malformed requests, partial success, compressed/decompressed request limits, WebSocket updates/reconnect cursors, bearer authentication, browser subprotocol authentication, and origin restrictions.
- Core tests covered aliases, exact large offsets, invalid/oversized identity rejection, ignored operation boundaries, fan-out, receive-before-produce, link correlation, conflicting/unknown cluster boundaries, deduplication, errors/batches, capacity/TTL cleanup, cursor expiry/restart, viewer admission, visual event limits, and concurrent ingestion.
- Additional causal tests covered all-link and parent retention, cross-trace relations, out-of-order multi-hop processing, linked batches, receive/process non-doublecounting, fan-out, missing processing, clock skew, identity contradictions, span/relation/depth/work/output budgets, duplicate/late spans, expiry, safe extension enrichment and non-messaging ancestry bridges.
- Both synthetic .NET SDK fixtures were ingested through the actual OTLP HTTP endpoint. Each produced a connected source-to-target observed range of 45 ms, two publishes, two process spans and zero synthetic receives. Raw span inspection also retained their parent/link metadata. These are protocol fixtures, not real Kafka traffic or a production latency claim.
- The Python standard-library demo sender completed a separate smoke test and sent 20 synthetic observations through the real OTLP endpoint.

## Viewer

Verified using a real headless Microsoft Edge browser at 1440px desktop and 390px mobile widths:

- WebSocket connection and synthetic OTLP activity.
- Moving particles, pause/resume, short replay, service/topic filters, empty-filter state, node details, and failed-operation wording.
- Keyboard row focus survives incoming activity; Enter opens details.
- Actual rendered mobile subtitle size is 12 CSS pixels, including after entering and leaving Fit mode.
- Stale nodes preserve text opacity; dashed borders/status marks convey staleness.
- No uncaught page errors and no document-level mobile horizontal overflow. The graph and table intentionally scroll within their own regions on narrow screens.

The finish reviewer scored the original contrast, font scaling, and focus findings resolved. That verdict covers the viewer findings, not a production audit of the backend.

Desktop/mobile evidence: `review/desktop.png` and `review/mobile.png`. All displayed traffic in these captures is synthetic and labeled as such.

## Not established

No actual Kafka applications, broker, Grafana stack, or deployment were changed or connected. No representative production Kafka span sample, production throughput benchmark, latency SLO, persistent replay, or HA behavior has been validated. Collector/Alloy examples were checked against official configuration documentation, but their binaries were not run against a real deployment.
