# Datadog DSM reference

Reviewed public source at commit **2ad6d5913b9193efc51026c30d40bdb7b451f29f**. The repository's root license and source notices identify Apache-2.0. No implementation code was copied into this library. This reference applies to the **optional** pathway extension; the default implementation follows standard OpenTelemetry .NET/W3C conventions and requires none of that extension's protocol.

- [PathwayContext](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/DataStreamsMonitoring/PathwayContext.cs#L20-L43): compact pathway identity plus origin and preceding checkpoint times.
- [Checkpoint manager](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/DataStreamsMonitoring/DataStreamsManager.cs#L280-L352): derives route identity, computes edge/origin latency and records statistics independently of span retention. Its produce fallback inherits the last consumed pathway through AsyncLocal.
- [Hashing](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/DataStreamsMonitoring/Hashes/HashHelper.cs#L16-L70): FNV-1 over service/environment and sorted edge tags, then node plus parent hash. This preview uses its own length-prefixed SHA-256-derived 64-bit route fingerprint instead.
- [Encoding](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/DataStreamsMonitoring/PathwayContextEncoder.cs#L18-L50): Datadog's little-endian hash and zigzag-varint millisecond timestamps. This preview uses bounded, versioned JSON in a distinct Kafka header.
- [Aggregation](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/DataStreamsMonitoring/Aggregation/DataStreamsAggregator.cs#L184-L212): DDSketch distributions keyed by pathway, distinct from individual APM traces. This preview exposes checkpoint records and ordinary .NET instruments; a compatible aggregation/receiver remains future work.
- [Kafka propagation](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/ClrProfiler/AutoInstrumentation/Kafka/KafkaHelper.cs#L420-L458): checkpoint and header injection occur before delivery. This preview deliberately separates attempts, delivery confirmation and failures.
- [Consumer lifecycle](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/ClrProfiler/AutoInstrumentation/Kafka/KafkaConsumerConsumeIntegration.cs#L38-L100): the default scope spans handling between consume calls. This preview uses explicit handler scopes so asynchronous/concurrent processing is not guessed.
- [Batch pairing caveat](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/SpanContext.cs#L535-L543): consumed messages processed later need their own restored context.
- [Commit tracking](https://github.com/DataDog/dd-trace-dotnet/blob/2ad6d5913b9193efc51026c30d40bdb7b451f29f/tracer/src/Datadog.Trace/ClrProfiler/AutoInstrumentation/Kafka/KafkaConsumerCommitIntegration.cs#L35-L53): offset observations are separate from processing. This preview does not implement lag or commit interception.

The reference's internal DSM types are not a standalone public NuGet instrumentation API. A truly transparent zero-code adapter would require profiler/agent interception; this first package uses explicit public Confluent.Kafka calls.

Current OpenTelemetry references were fetched through Context7 before the OTel-first refactor:

- [.NET library API and standard propagation](https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Api/README.md).
- [.NET instrumentation-library guidance](https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/docs/trace/extending-the-sdk/README.md).
- [Official OTLP exporter and OTEL environment configuration](https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Exporter.OpenTelemetryProtocol/README.md).
- [Kafka span attributes](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md).
- [Messaging duration/count conventions](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/messaging-metrics.md).

In particular, OTel's sent-message metric counts attempted sends. The optional checkpoint extension keeps broker-confirmed delivery as a separate fact rather than changing the standard metric's meaning.
