# Data Streams Monitoring

Inspect Kafka producer → topic → consumer relationships and causal operation ranges from standard OpenTelemetry traces, with .NET prioritized. Native Grafana controls/tables/Scenes charts surround a custom live topology. Grafana's sidebar is the only top-level navigation; every page uses a dashboard-style canvas.

![Observed local Kafka topology](https://raw.githubusercontent.com/kolbyd/grafana-data-streams-monitoring/v1.0.0/kolbyd-datastreamsmonitoring-app/src/img/screenshots/map.png)

![Measured causal operation range](https://raw.githubusercontent.com/kolbyd/grafana-data-streams-monitoring/v1.0.0/kolbyd-datastreamsmonitoring-app/src/img/screenshots/measure.png)

Screenshots capture this app's own local Kafka lab; they are not traffic-coverage or performance guarantees.

## Connect the collector

1. Enable the app and open **Configuration**.
2. Enter the separate flow collector's server-side HTTP(S) address and optional API key.
3. Select **Save and test** to check the connection.

After the connection succeeds, open **Map** to inspect available telemetry. Credentials stay in Grafana secure settings. The Go backend connects the collector's snapshots and WebSocket stream to Grafana Live.

## Explore Map and List

The single compact native row provides Filter, Environment, Cluster and Services/Queues multiselect pills (All by default), Map/List and the normal Grafana TimeRangePicker with relative/absolute ranges and timezone selection.

Drag the map with a pointer or touch to pan. Use the wheel to zoom around the cursor, or use the native zoom and **Fit** controls. Scale is limited to 0.25–3. Arrows and curves show direction; hover or focus to inspect observed ops/s and errors. Blue/error particles animate observations over 1.8 seconds. The map follows your reduced-motion preference automatically.

List shows sortable Services/Queues tables and column selection, with Operations in/out in ops/s. Setup labels its service rates Observed ops/s. The chosen interval removes inactive edges and orphan nodes, including worker/group relationships, and limits selector options to active entities. Relative live ranges also use last-arrival edge activity; historical ranges depend on retained events/process completions.

There is no in-page app title, duplicate primary page tabs, saved-view UI, Services/Queues drawer, Pause button, Updated label or Map Preview shortcut. Example data can be activated only in Setup and remains clearly labeled.

## Inspect pipelines and Kafka

Open **Pipelines** to create, edit or remove personal browser-local start/end definitions. Definition, Summary, Components and Monitors sections organize the details; native charts show rates, matched hops and complete causal ranges. Use **Measure** to choose or swap endpoints and save a pipeline. Monitor creation isn't implemented.

Open **Kafka Console** for observed clusters, topics and groups in native tables and charts. Open **Setup** for discovered services, collection guidance and configuration. The configured backend also provides the Data Streams data source. No prebuilt dashboards or dashboard links are bundled.

## Interpret the measurements

The backend frame budget is 3 MiB for Grafana Live's 4 MiB outbound queue; partial coverage remains reported. Counts are observed operations, not authoritative message counts or full sampled-trace traffic. Historical data is bounded and in memory. Broker health/lag are uncollected; this collector has no OTLP metrics receiver.

Complete standard send/process chains with parent/link context and cross-trace joins support first-retained-send to terminal-processing ranges. Missing/conflicting/failed/clock-skewed evidence suppresses complete values. Batch pathways measure operation ranges, not individual-message latency. Optional dsm.* enrichment does not require a private protocol.

The separate .NET library defaults to standard ActivitySource/Meter/W3C propagation with explicit application integration. Custom pathway/checkpoint extensions are opt-in, and their separate checkpoint schema is not ingested here. The isolated .NET Kafka lab supplies actual fan-out/fan-in and linked joins; local validation does not establish Cloud or production readiness.

## Prepare a deployment

Use Grafana 13.2.4 or newer with the separately running collector. The backend package targets Linux AMD64. App ID: eageraspen2909-datastreamsmonitoring-app; data-source ID: eageraspen2909-datastreamsmonitoring-datasource. Catalog publisher account: kolbyd1; organization: eageraspen2909. Signing and a supported distribution route remain required for installation beyond the isolated development setup.

[Source repository](https://github.com/kolbyd/grafana-data-streams-monitoring) · [Report an issue](https://github.com/kolbyd/grafana-data-streams-monitoring/issues). Catalog approval and Cloud installation are separate from source and release availability.
