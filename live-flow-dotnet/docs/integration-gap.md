# Standard telemetry and remaining integration boundaries

The default library uses normal OpenTelemetry .NET tracing, metrics, W3C propagation and OTLP exporters. A private checkpoint receiver is not a prerequisite. The compiled SDK sample can send standard traces/metrics to an existing Collector/Alloy and onward to the application's configured backends. Exporter configuration, authentication, resources, sampling and deployment belong to that pipeline.

The optional advanced flow collector/Grafana view should consume the same ordinary OTLP Kafka spans, including spans from other conforming .NET auto instrumentation. Its contract is:

- `messaging.system=kafka`, destination/group, operation name/type and known Kafka partition/offset coordinates.
- `send` Producer spans and separate `process` Consumer spans; retained parent IDs and all message links, including cross-trace links.
- Resource `service.name` and environment attributes from the application's SDK.
- A process span may parent an active application job and link to a producer in another trace. An outgoing send parents that process. Standard fixtures in `samples/fixtures/` cover both this case and a single-trace chain.

Process duration means handler duration. It is not broker transit or successful Kafka offset commit. A correlated first-send-start to terminal-process-end interval describes observed trace boundaries, not authoritative total business latency. Partial/sampled traces, missing propagation, skew and out-of-order ingestion limit inference. Do not fabricate missing receive spans or broker transit from handler duration. This library observes delivery to the application as a metric; it does not intercept the earlier broker poll.

Standard messaging metrics use seconds for duration and distinguish attempted sends from consumed observations. `messaging.client.sent.messages` includes failed/cancelled attempts by OTel convention. Broker-confirmed delivery is a separate optional extension fact. A trace-only receiver is not automatically an OTLP metrics receiver; route metrics through an endpoint supporting that signal. Broker lag/backlog needs broker/commit metrics from the normal Kafka metrics pipeline; it is not established by these spans.

Additional `dsm.*` scalar span attributes can enrich explicit retry/DLQ/outbox operations. They are optional properties, not required inputs or alternative causal links. A receiver should bound/filter them and keep measured standard parent/link timing authoritative.

`EnablePathwayExtensions=true` separately enables private lineage/checkpoints alongside standard telemetry. Its in-memory `ICheckpointSink` handoff still has no dedicated transport/receiver, durable store or aggregation service in this package. Implementing one is necessary only for applications choosing that independent checkpoint extension, not for the app's standard OTel path. Datadog DSM protocol compatibility is not claimed.

The current library tests establish API behavior with fake producers and deterministic clocks, plus a compiled official SDK/OTLP sample. They do not establish real-broker, exporter/backend, database-transaction, production-load or end-to-end Grafana integration. No CLR profiler or automatic Kafka interception has been implemented.
