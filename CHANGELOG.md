# Changelog

## Unreleased

### Security

- Update the .NET instrumentation/sample/lab OpenTelemetry packages to 1.15.3 for GHSA-g94r-2vxg-569j; audited locked restores and the 33 library/10 lab checks pass without audit suppression.

### CI and documentation

- Apply [Grafana's voice and tone guidelines](https://grafana.com/docs/writers-toolkit/write/style-guide/voice-tone-guidelines/) to current task documentation, with action headings, short steps, clear outcomes and an explicit writing reference in CONTRIBUTING.md.
- Add `.github/workflows/ci.yml` for frontend/backend/.NET checks, Linux Mage packaging, plugin validation and Grafana 13.2.4/13.3.0 browser coverage, with Node 24.15.0/Go 1.27.3 toolchains. Actual workflow status is separate from historical local results.
- Add future-tag draft-release automation that calls CI and can sign through an existing configured secret; it does not replace the frozen v1.0.0 artifacts or create credentials.
- Expand the root README with real Map/Measure screenshots, four-project architecture, local startup and measurement/deployment boundaries.
- Add contribution prerequisites and verified existing check commands for the plugin, collector, .NET library and Kafka lab.
- Document release immutability and distinguish main-branch improvements from the frozen v1.0.0 submission and archives.

## 1.0.0 — 2026-10-30

Initial public source release and unsigned Linux AMD64 plugin package for first Grafana catalog review. Catalog approval, public signing and Cloud installation are separate.

### Grafana app

- Native dashboard-style app canvases with Grafana-sidebar navigation and active native theme; no duplicate app title/page tabs.
- Compact native Filter, Environment, Cluster and Services/Queues variable controls, Map/List and relative/absolute/timezone selection. Services/Queues use Scenes multiselect pills with All.
- Automatically arranged producer → topic → consumer topology, organized arrow ports/curves, pointer/touch pan, cursor-anchored zoom, Fit, hover/focus operation statistics and observed-flow particles. Reduced motion is automatic.
- Native sortable List inventories labeled Operations in/out (ops/s), plus Setup discovery rates labeled Observed ops/s.
- Browser-local pipeline definitions with catalog/create/edit/remove and Definition, Summary, Components and Monitors sections. Measure selects endpoints and saves a pipeline; native charts display observed rates, matched hops and complete retained causal ranges.
- Observed Kafka cluster/topic/consumer-group inventories, collection Setup, secure backend connection settings and Setup-only synthetic examples.
- A Go backend bridges the collector stream into Grafana Live and proxies snapshots to the nested Data Streams data source.
- Active time ranges prune inactive edges/orphan nodes and selector values, including worker/group relationships. Partial retained/stream coverage remains explicit.
- Published IDs `eageraspen2909-datastreamsmonitoring-app` and `eageraspen2909-datastreamsmonitoring-datasource`; app folder retains its original scaffold name.
- Removed the former bundled Overview/Consumer Groups dashboards and links. Saved Views code is dormant and not rendered.

### Collector

- ASP.NET Core OTLP/HTTP protobuf/JSON trace input, including gzip, with standard Kafka attribute normalization.
- Bounded in-memory topology, exact offset strings, retained observations, out-of-order producer/receive matching, reset/reconnect streaming and authenticated API access.
- Parent/all-link preservation and cross-trace joins derive observed send/process ranges when retained causal evidence is complete. Missing/conflicting/failed/clock-skewed evidence does not become a complete latency sample.
- A 3 MiB frame budget accommodates Grafana Live's 4 MiB outbound queue.

### .NET instrumentation preview

- Explicit Confluent.Kafka publish/receive/processing helpers with ActivitySource/Meter and W3C trace-context/baggage propagation through an application-owned OTel SDK.
- Standard-context preservation through persisted outbox metadata and linked dispatcher jobs.
- Optional retry/dead-letter attributes and opt-in private pathway/checkpoint extensions; no private transport is required for standard spans.
- Fake-producer/listener tests and compiled standard-OTLP examples.

### Real Kafka lab and local evidence

- Isolated local Kafka with ten .NET workers, separate downstream consumer groups, fan-out/fan-in, cross-trace two-input joins, retries and dead-letter routes.
- A finite 24-order smoke workload, xUnit tests and bounded background lifecycle controls.
- Recorded local Grafana/browser, .NET, Go, validator and actual-Kafka evidence; app-owned Map/Measure catalog screenshots and runtime notices.
- Local smoke results include 22 completed joins, 22 observations in each of three downstream groups, three retries and two DLQ records. These are workload-specific evidence, not throughput guarantees.

### Known limitations

- Observed rates are not authoritative broker message counts or complete traffic coverage. Sampling, batching, missing instrumentation, retention and budgets reduce visibility.
- Complete timing covers retained instrumented operation boundaries. Graph connectivity/receive alone does not prove business completion; batch timing is not per-record latency.
- No broker lag/health/partition/rebalance collection, OTLP metrics receiver or durable historical store. Library metrics require a separate standard metrics destination.
- No rendered Saved Views, prebuilt dashboards, monitor management, CLR profiler or automatic Kafka-call interception.
- Pipeline definitions are browser-local; collector state and lab joins are volatile. Application transactions, Kafka commits, retry/DLQ policy, durable outboxes and exactly-once behavior are application responsibilities.
- The plugin requires a separately reachable collector. A workstation's loopback address is not reachable from Grafana Cloud.
- No catalog approval, public signing, Cloud deployment, production capacity or high availability is established by this initial release.

See the [recorded plugin evidence](kolbyd-datastreamsmonitoring-app/docs/review-results.md), [lab evidence](dotnet-kafka-lab/VALIDATION.md) and [v1.0.0 release](https://github.com/kolbyd/grafana-data-streams-monitoring/releases/tag/v1.0.0). Its tag/submission/archives remain unchanged by later main-branch documentation or CI work.
