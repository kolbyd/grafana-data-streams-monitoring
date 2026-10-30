# Data Streams Monitoring

<!-- impeccable:product-schema 1 -->

## Platform
web

## Stack
Official `@grafana/create-plugin` 7.17.0 app scaffold, React 19, Grafana 13.2.4, native `@grafana/ui` controls and `@grafana/scenes` 9.2.0 variable selectors/time-series panels. The Go backend uses plugin SDK 0.298.0; signing tooling uses sign-plugin 4. A separate ASP.NET Core flow collector normalizes OTLP traces and derives retained topology/causal pathways. The Go plugin backend bridges its WebSocket to Grafana Live and proxies snapshots. The Data Streams data source remains; no prebuilt dashboards are bundled.

## Users
Engineers operating Kafka and instrumented applications in Grafana. .NET/Confluent.Kafka is the first integration focus; standard OpenTelemetry is required input rather than a private protocol.

## Product Purpose
Inspect active producer → topic → consumer relationships, observed operations and complete causal pathway ranges on full Grafana dashboard-style app canvases.

## Operating Context
Local review runs isolated Grafana and collector processes. The separate `../dotnet-kafka-lab` has supplied actual Kafka traffic from ten .NET services with cross-trace linked joins. A bounded one-hour background run started at 2026-10-30 18:10:29 UTC; this is local evidence, not Cloud installation or production coverage. Final run/review outcomes belong in `docs/review-results.md`. The publishing target is GitHub then Grafana catalog; a first unsigned submission is distinct from catalog approval or installation.

## Brand Commitments
Grafana's native components, typography and active theme own the interface. Grafana's sidebar is the only top-level navigation. There is no in-page app title or primary page-tab row. Every page uses a dashboard-style canvas. The DSM reference informs topology/task structure without overriding native conventions. Custom Dagre SVG remains for domain shapes, node sparklines and live observation particles.

## Capabilities and Constraints
- **Map/List:** one compact native controls row: 160px Filter, Environment, Cluster, Services and Queues selectors, a text-only 32px Map/List toggle and Grafana TimeRangePicker. Scenes multiselect pills default to All and refresh options/callbacks after Setup navigation. The picker supports relative/absolute ranges and timezone selection.
- **Map canvas:** fills remaining space; pointer/touch drag pans, wheel zoom anchors at the cursor, scale is bounded to 0.25–3, and native zoom/Fit controls remain available. Organized arrow ports/curves show direction. Hover/focus native tooltips expose observed ops/s and error statistics. Blue/error particles traverse observed edges over 1.8 seconds; reduced motion is automatic. There is no Pause button, facet drawer or repeated Kafka/Topic caption.
- **List:** Services/Queues inventories with native sortable tables and column selection. Incoming/outgoing rates are labeled Operations in/out (ops/s), not message counts. Content reflects the active time range and filters.
- **Pipelines:** personal browser-local start/end definitions with catalog create/edit/remove and Definition/Summary/Components/Monitors sections. Native time-series panels show rates, hop timing and complete causal ranges; monitor creation is not implemented.
- **Measure:** choose/swap start/end, inspect connectivity, matched-hop p95, completed-pathway count and end-to-end p95 when complete, then save a pipeline.
- **Kafka Console:** active observed Clusters/Topics/Consumer Groups with native charts, sortable/paginated inventories and map links. Broker/connector inventory, health, partition metrics, rebalances and lag are uncollected.
- **Setup:** discovered-service inventory with Observed ops/s, collection guidance, backend connection configuration and the only entry point for example-data activation. No Map toolbar Preview shortcut or Updated timestamp is rendered.

Saved Views code remains dormant and is not rendered. There is no saved-view UI or saved-filter CRUD feature in the current app. Pipeline definitions remain scoped in browser storage by Grafana organization/user and live/example source, not shared server storage.

The selected range prunes inactive edges and orphan nodes, including workers and consumer-group relationships, and limits selector options to active entities. Relative live ranges combine last-arrival edge activity with retained events/process completions. Historical ranges use retained event/process completion evidence; they cannot reconstruct discarded history.

The backend frame budget is 3 MiB to fit Grafana Live's 4 MiB outbound queue; truncation/partial coverage remains reported. Counts are observed operations, not broker-authoritative messages or complete traffic from sampled traces. Sampling, gaps, batches, retention and frame limits affect coverage. Missing measurements remain unavailable. Credentials stay backend-side. The collector has no OTLP metrics receiver.

Complete standard send/process causal chains preserve parents, all valid links and cross-trace joins. Timing spans first retained send start to terminal processing completion. Incomplete/conflicting/failed/clock-skewed samples do not contribute complete values. Batch pathways measure operation ranges, not individual-message latency. Optional bounded dsm.* attributes enrich evidence without a private header/checkpoint requirement.

The separate `../live-flow-dotnet` library defaults to ActivitySource/Meter and W3C propagation, with explicit application adoption and normal OTel SDK/exporters. Private pathway/checkpoint extensions are opt-in and their separate sink schema is not ingested here. No runtime agent, retry policy, DLQ router, database outbox or Kafka commit behavior is implemented by that library.

## Evidence on Hand
Source evidence includes Explorer, Native/DashboardControls, Topology, Pipelines, KafkaConsole and `src/flow/activity.ts`. Datadog reference screenshots remain outside shipping assets. The plugin metadata declares current Map/Measure catalog screenshots captured from this app's own local Kafka lab. The two former dashboard definitions/provisioning and menu links are removed. Final automated/browser/Kafka evidence belongs in `docs/review-results.md`; earlier checks do not establish this latest UI.

Catalog publisher account: kolbyd1; organization: eageraspen2909. The app/data-source IDs are `eageraspen2909-datastreamsmonitoring-app` and `eageraspen2909-datastreamsmonitoring-datasource`. The physical scaffold directory retains its original name. Public source is `github.com/kolbyd/grafana-data-streams-monitoring`; repository creation is distinct from a release or catalog approval.

## Product Principles
- Keep standard OTel input and preserve existing telemetry pipelines.
- Separate dependency, matched hop and complete causal pathway evidence.
- Use native dashboard controls with sidebar-only top-level navigation.
- Let the map occupy remaining canvas; keep pan/zoom and automatic motion accessibility.
- Scope topology and selector values to active time-range evidence.
- Report partial coverage and missing broker metrics honestly.

