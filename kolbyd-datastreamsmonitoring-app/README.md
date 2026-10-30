# Data Streams Monitoring for Grafana

A complete Kafka topology and causal-pathway app with standard OpenTelemetry input and .NET as its first integration focus. Grafana's sidebar is the only top-level navigation. Each page uses a dashboard-style canvas with native controls, tables and charts; the custom live SVG map fills its remaining space.

The project uses the official `@grafana/create-plugin` 7.17.0 App + Go backend scaffold, Grafana 13.2.4, React 19 and Scenes 9.2.0. The generated `.config/` directory is unchanged.

## Review locally

Requirements: Docker Desktop with Linux containers, Node.js and Microsoft Edge. Builds also need Go/Mage; the launcher can use the workspace's isolated toolchain.

```powershell
./build-review.ps1
./start-review.ps1
./start-review.ps1 -RunTests
```

Open [Map](http://127.0.0.1:3001/a/eageraspen2909-datastreamsmonitoring-app/explore). Activate synthetic example data only from **Setup**. It is labeled and never contacts Kafka or changes collector data.

The isolated Compose project exposes Grafana on loopback 3001 and the flow collector on loopback 4318. Grafana reaches the collector through its private network. Anonymous users are Viewers; connection configuration requires administrator login. The launcher supplies random credentials through in-memory environment values/Docker secrets. Optional `GRAFANA_ADMIN_PASSWORD` and `FLOW_SERVICE_API_KEY` overrides belong in the launching process before first startup, never a saved env file.

```powershell
./start-review.ps1 -Stop
```

This stops only the named review project. See [build and review instructions](docs/build-review.md).

## Operating workflow

- **Map/List:** compact native Filter, Environment, Cluster, Services and Queues selectors; Services/Queues are real Scenes multiselect pills defaulting to All. A Map/List toggle and Grafana TimeRangePicker support relative/absolute ranges and timezone selection.
- **Map:** drag with pointer/touch to pan; wheel zoom anchors at the cursor. Native zoom/Fit controls operate over scale 0.25–3. Organized arrow ports and curves show direction. Hover/focus native tooltips expose observed ops/s and errors. Blue/error dots animate observations over 1.8 seconds; reduced motion is automatic.
- **List:** native sortable Services/Queues inventories and column selection, with Operations in/out reported in ops/s.
- **Pipelines:** personal browser-local start/end definitions with create/edit/remove, Definition/Summary/Components/Monitors sections and native charts for observed rates, hops and complete causal ranges. Monitor creation is not implemented.
- **Measure:** choose/swap start/end, inspect matched-hop p95 and complete end-to-end ranges, then save a pipeline.
- **Kafka Console:** active observed clusters, topics and consumer groups with native tables/charts and map links.
- **Setup:** discovered-service inventory with Observed ops/s, collection guidance, connection settings and example-data activation.

There is no in-page app title, duplicate primary navigation, saved-view UI, Services/Queues drawer, Pause button or Map Preview/Updated shortcut. Dormant SavedViews source is not rendered. Pipeline definitions remain browser-local and scoped by Grafana organization/user and live/example source. The Data Streams data source remains available; no prebuilt dashboards or dashboard links are shipped.

## Time and coverage

The selected interval prunes inactive edges and orphan nodes, including workers and consumer-group relationships. Selector options show active entities. Relative live ranges combine last-arrival activity with retained event/process-completion evidence. Historical ranges use retained events/process completions; changing the picker cannot recover discarded history.

The bridge uses a 3 MiB frame budget to fit Grafana Live's 4 MiB outbound queue. Partial/truncated coverage is reported. Rates count observed operations, not authoritative Kafka messages or full traffic from sampled traces. Sampling, batching, missing instrumentation and retention affect coverage. Broker health, lag, partitions and rebalances are uncollected; the collector has no OTLP metrics receiver.

## Architecture

```text
Kafka application + standard OpenTelemetry instrumentation
  → OTLP/HTTP traces → separate ASP.NET Core flow collector
  → Go plugin WebSocket bridge → Grafana Live → app
  → Go snapshot proxy → Data Streams data source
```

This is not backend-free. Applications can use existing standard instrumentation/collectors; preserve their current pipeline and fan out traces where needed. No runtime instrumentation agent is implemented here.

The collector preserves parents, all valid links and cross-trace joins. Complete send/process chains can yield first-retained-send to terminal-processing ranges. Incomplete, conflicting, failed or clock-skewed evidence does not contribute complete timing. A graph path or receive span alone is not business completion. Batch pathways measure observed operation ranges, not individual-message latency. Optional bounded `dsm.*` attributes enrich evidence without requiring a private protocol.

The separate `../live-flow-dotnet` preview uses explicit Confluent.Kafka APIs, ActivitySource/Meter and W3C propagation by default. Applications own SDK/exporters and sampling. Custom pathway/checkpoint extensions are opt-in; their separate checkpoint schema is not ingested by this service. Standard exported spans can provide causal evidence without it. Metrics require another standard metrics destination.

The separate `../dotnet-kafka-lab` provides actual isolated Kafka fan-out/fan-in traffic, ten .NET services, cross-trace joins and retry/DLQ routes. Its README documents bounded background runs and tests. Final execution/live evidence is recorded in `docs/review-results.md`; a local run is not Cloud or production validation.

## Development and distribution

The managed build uses Webpack/Jest in a pinned Linux Node container on this host, Go plugin SDK 0.298.0 with Mage targets and sign-plugin 4. Keep `.config/` unchanged. Manifest changes require rebuild/restart. Development unsigned-plugin allowance is scoped to this review. No Grafana Cloud installation or production deployment is performed by the launcher.

Catalog publisher account is kolbyd1, organization eageraspen2909. The app ID is `eageraspen2909-datastreamsmonitoring-app`; the nested data-source ID is `eageraspen2909-datastreamsmonitoring-datasource`. The physical source directory retains its original scaffold name. Catalog screenshots in plugin metadata capture this app's own local Kafka lab.

The source repository is [kolbyd/grafana-data-streams-monitoring](https://github.com/kolbyd/grafana-data-streams-monitoring); report bugs through its [issue tracker](https://github.com/kolbyd/grafana-data-streams-monitoring/issues). The current source layout retains the sibling plugin, collector, .NET library and Kafka-lab folders. GitHub publication is followed by a Grafana catalog submission. The first catalog submission may use an unsigned archive; repository creation does not imply a published release, catalog approval, signing or Cloud installation.

