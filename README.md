# Data Streams Monitoring for Grafana

Explore Kafka producer → topic → consumer relationships from standard OpenTelemetry traces. The app combines native Grafana controls, tables and charts with an interactive topology, observed-flow animations and causal pathway timing. .NET and Confluent.Kafka are the first integration focus.

## Projects

- [Grafana app](kolbyd-datastreamsmonitoring-app): frontend, Go bridge, nested snapshot datasource, provisioning and browser checks. Catalog ID: `eageraspen2909-datastreamsmonitoring-app`.
- [Flow collector](live-flow): ASP.NET Core OTLP/HTTP trace ingestion, bounded topology/causal aggregation and authenticated streaming.
- [.NET instrumentation](live-flow-dotnet): preview Confluent.Kafka helpers using standard ActivitySource, Meter and W3C propagation; applications configure their OTel SDK/exporters.
- [Real Kafka lab](dotnet-kafka-lab): isolated local Kafka with ten .NET workers, fan-out/fan-in, cross-trace joins, retries and dead-letter routing.

The app's physical source folder retains its original scaffold name. The published app and datasource IDs use the Grafana publisher organization `eageraspen2909`.

## Try locally

Install Docker Desktop with Linux containers, Node.js 24.15+ and Go/Mage. From `kolbyd-datastreamsmonitoring-app`, run `./build-review.ps1`, then `./start-review.ps1` in PowerShell. The launcher provisions an isolated local Grafana and collector using random Docker secrets. Open [the map](http://127.0.0.1:3001/a/eageraspen2909-datastreamsmonitoring-app/explore). Setup includes an explicitly labeled synthetic example. Follow the lab README for actual Kafka traffic.

The GitHub release archive contains the unsigned Linux AMD64 plugin for initial catalog review. Grafana catalog approval and public signing are separate steps. The separate collector must be reachable by Grafana's backend; Grafana Cloud does not gain access to a workstation's loopback address.

## Measurement boundaries

Rates are observed operations, affected by sampling and batching. Complete retained parent/link chains can establish observed causal operation ranges; they do not prove business completion or per-record batch latency. Kafka lag, broker health and partition metrics are not collected yet. The collector keeps bounded in-memory history rather than durable historical storage. Pipeline definitions are browser-local. This includes no CLR auto-instrumentation profiler.

See [validation evidence](kolbyd-datastreamsmonitoring-app/docs/review-results.md), [configuration and local review](kolbyd-datastreamsmonitoring-app/docs/build-review.md), and each project's README. No prebuilt dashboards are included.

## License

Apache-2.0. Runtime third-party notices accompany the plugin package.
