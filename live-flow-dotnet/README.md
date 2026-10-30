# LiveFlow.Kafka — OpenTelemetry-first .NET preview

.NET 8+ instrumentation for Confluent.Kafka 2.12.0, using the standard OpenTelemetry .NET API, `ActivitySource`, `Meter`, and W3C trace-context/baggage propagation. The default path needs no private header, checkpoint sink, custom receiver or Grafana plugin. Applications export through their normal OpenTelemetry SDK and OTLP pipeline.

This is an explicit instrumentation library. It does not implement a CLR profiler, intercept arbitrary Kafka calls, run Kafka, commit offsets, retry work or implement an outbox/DLQ. Applications choose their own client, handler, transaction and delivery policies. Optional pathway extensions add properties when standard telemetry is insufficient; they are disabled by default.

## Use the normal OpenTelemetry pipeline

The compiled [OTLP Kafka sample](samples/OtlpKafka/Program.cs) registers the library source/meter and the official `OpenTelemetry.Exporter.OpenTelemetryProtocol` 1.15.3 package:

```csharp
using LiveFlow.Kafka;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var resource = ResourceBuilder.CreateDefault().AddService("payments");
using var traces = Sdk.CreateTracerProviderBuilder()
    .SetResourceBuilder(resource)
    .AddSource(FlowInstrumentation.ActivitySourceName)
    .AddOtlpExporter()
    .Build();
using var metrics = Sdk.CreateMeterProviderBuilder()
    .SetResourceBuilder(resource)
    .AddMeter(FlowInstrumentation.MeterName)
    .AddOtlpExporter()
    .Build();
using var flow = new FlowInstrumentation(new FlowOptions { Cluster = "configured-kafka-cluster-id" });
```

The application owns exporters, resources, sampling, credentials and shutdown. Registering the tracing source alone does not register/export metrics. There is no exporter embedded in the library. Its OpenTelemetry.Api dependency provides standard propagators; the SDK/exporter dependency belongs to the application. The default propagator is the OTel API's W3C trace-context + baggage composite; applications can supply another standard `TextMapPropagator` through `FlowOptions.Propagator`.

Point the sample at an existing Kafka broker and an OTLP Collector/Alloy endpoint that accepts both signals:

```powershell
$env:OTEL_SERVICE_NAME = "payments"
$env:OTEL_RESOURCE_ATTRIBUTES = "deployment.environment.name=dev"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4318"
$env:OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_TOPIC = "orders"
dotnet run --project samples/OtlpKafka -- send

# Consumer processing, with an optional outgoing send inside its process span:
$env:KAFKA_OUTPUT_TOPIC = "payments"
dotnet run --project samples/OtlpKafka -- consume
```

These are example local endpoints, not provisioned services. Use normal `OTEL_EXPORTER_OTLP_TRACES_*` / `OTEL_EXPORTER_OTLP_METRICS_*` configuration when signals go to different endpoints. `--help` starts neither a Kafka client nor an exporter. Basic tracing/metrics can use an existing OTel backend; advanced flow correlation is an optional downstream consumer of ordinary OTLP spans. See [integration-gap.md](docs/integration-gap.md).

## Send and process messages

```csharp
await flow.ProduceAsync(producer, "orders", message, cancellationToken: token);

// The application owns Consume and Commit. Observe promptly after Consume returns.
var record = consumer.Consume(token);
var received = flow.ObserveReceived(record, "payments-v1");
await flow.ProcessAsync(received, async cancellationToken =>
{
    await HandleOrder(record.Message.Value, cancellationToken);
    await flow.ProduceAsync(producer, "payments", outputMessage,
        cancellationToken: cancellationToken);
}, token);
consumer.Commit(record); // Only if this matches the application's policy.
```

The wrapper emits `send {topic}` Producer Activities and `process {topic}` Consumer Activities. Spans carry `messaging.system=kafka`, `messaging.operation.name`, `messaging.operation.type`, destination/group and known Kafka partition/offset coordinates. Configured cluster ID is included when supplied. Payloads, keys and exception messages are not inspected/exported.

For a single message, processing uses the active application span as its parent when present; otherwise it uses the extracted producer context. It also links to that message's producer context, including across traces. Outgoing sends become children of the process span; parallel/fan-out outputs remain siblings. The library copies the message and header byte arrays before injecting standard headers. Invalid trace context is ignored; it cannot block the handler. Standard baggage is propagated, scoped to handling and restored afterwards. Apply the application's normal baggage/privacy policy.

`ObserveReceived` records a delivered-to-application metric; it does not invent a wire-level receive span. The `ProcessAsync(record, group, handler)` convenience overload observes receipt when invoked. Handler return, failure and cancellation remain distinct; the original exception is rethrown. Handler completion does not assert business success or committed offsets. No Kafka-transit duration is inferred from handler duration. There are no broker high-watermark/commit/lag metrics in this library.

Sampling affects retained spans, not metric collection or standard propagation through an unsampled processing boundary. Activity listener and propagator failures are isolated from application work. Exceptions during sampling/start/stop cannot replace a Kafka result or handler exception, and ambient Activity/context state is restored.

## Avoid duplicate instrumentation

Choose one owner for each Kafka boundary. When a profiler/auto-instrumentation package already records producer operations, prefer its producer call and propagation rather than wrapping the same operation again. Use this library's explicit processing boundary only when another instrumentor does not already own that boundary. Do not enable overlapping Kafka instrumentations and assume they deduplicate each other.

For mixed adapters, `CreateProducerActivity=false` and/or `CreateProcessingActivity=false` disable the corresponding library span creation. The wrappers still record metrics and propagate the available standard context, so also select one owner for overlapping metrics. No CLR profiler or automatic discovery is supplied here.

## Persist W3C context through an outbox

```csharp
// Inside the business handler/transaction, capture the active standard context.
var metadata = flow.CaptureOutboxContext();
await SaveBusinessChangeAndOutboxRowInOneTransaction(payload, metadata.Serialized, token);
flow.RecordOutboxPersisted(metadata); // Only after known commit success; optional observation.

// In a later dispatcher process, restore and send using the persisted W3C parent.
if (flow.TryRestoreOutboxContext(row.FlowMetadata, out var restored))
    await flow.PublishOutboxAsync(producer, row.Topic, message, restored!,
        cancellationToken: token);
```

The default envelope contains ordinary `traceparent`, `tracestate` and `baggage` carrier fields plus an outbox ID/capture timestamp. It contains no pathway context. `TraceHeaders` exposes those standard carrier fields for applications with their own persistence schema. Persist the metadata in the application's transaction; capture itself asserts no commit. A dispatcher send uses the saved W3C parent and links to an active dispatcher job, preserving both causes. Without a captured trace, it can start a normal fresh trace.

`PublishOutboxAsync` adds optional `dsm.outbox.id`/`dsm.outbox.wait_ms` span attributes. Wait means capture-to-dispatch, including transaction time; it is not a database commit timestamp or Kafka transit. The postcommit observation adds an event to the active span, if any. Repeated dispatches retain the captured context but are separate attempts. Row locking, durable delivery, retries, idempotence and marking rows sent remain application responsibilities. Invalid serialized metadata returns false so the application can choose its fallback/quarantine policy.

## Optional retry/DLQ and pathway enrichment

Passing `PublishOptions.Classification`, `DeadLetterReason` and `Attempt` adds bounded `dsm.publish.classification`, `dsm.deadletter.reason` and `dsm.retry.attempt` attributes to the ordinary send span. These are optional extra properties, not a second telemetry transport. A normal send emits no `dsm.*` attributes. No retry or DLQ-routing policy runs automatically.

For independent logical IDs, origin timestamps and pathway checkpoints across sampled-out traces, explicitly opt in on participating services:

```csharp
var checkpoints = new BufferedCheckpointSink(capacity: 10_000); // Optional handoff, not an exporter.
using var extended = new FlowInstrumentation(new FlowOptions
{
    ServiceName = "payments", Environment = "dev", Cluster = "cluster-id",
    EnablePathwayExtensions = true,
}, checkpoints);
var context = extended.CreateMessageContext(); // Reuse for a root retry; do not recreate per attempt.
await extended.ProduceAsync(producer, "orders.dlq", message, new PublishOptions
{
    Context = context, Classification = PublishClassification.DeadLetter,
    DeadLetterReason = DeadLetterReason.RetryLimitExceeded, Attempt = 4,
}, token);
```

Only this opt-in adds the bounded/versioned `liveflow-pathway-v1` header, extension checkpoint measurements and optional `ICheckpointSink` callbacks. W3C propagation remains primary. Immutable `AsyncLocal` lineage is scoped to processing; pass the extension `Context` explicitly across execution-context suppression or root retries. An outbox captured with extensions also stores that lineage. A default dispatcher ignores custom lineage and restores W3C context normally. Version-1 preview outbox metadata is accepted only in extension mode and cannot recover W3C context that was never stored.

`PublishAttempt`, broker-confirmed `PublishDelivered`, failed, cancelled and unconfirmed outcomes remain separate extension facts. Cancellation does not prove the broker rejected a message. Confirmed delivery requires persisted status and usable partition/offset. There is no exactly-once deduplication. Unknown/malformed/missing extension metadata starts fresh unknown-origin lineage. Every required wire field is validated; extension headers are at most 1024 UTF-8 bytes, outbox envelopes at most 16384. Negative cross-host intervals are flagged/omitted; processing uses a monotonic clock. Synchronized clocks remain necessary for remote timing.

The optional checkpoint sink is not connected to a dedicated receiver in this preview. Custom sinks must be fast/nonblocking; exceptions and full/closed buffers cannot interrupt work. An application-owned worker can drain `ReadAllAsync()`. At shutdown stop producers, `Complete()` the buffer, drain it and dispose instrumentation. A crash loses buffered checkpoints. None of this transport is needed for standard OTLP spans/metrics.

## Metrics and limits

Default instruments follow OTel's messaging conventions (currently marked development upstream):

| Instrument | Unit | Meaning |
| --- | --- | --- |
| `messaging.client.sent.messages` | `{message}` | Actual send attempts, including failed/cancelled operations; not broker-confirmed deliveries. |
| `messaging.client.consumed.messages` | `{message}` | Records explicitly observed as delivered to the application. |
| `messaging.client.operation.duration` | `s` | Monotonic duration of the wrapped send operation. |
| `messaging.process.duration` | `s` | Monotonic handler duration, separately from client operations. |

Failures use the same bounded `error.type=error` or `cancelled` on spans and metrics; successful operations omit it. Destination/group identities have a per-instance budget (`MaxMetricIdentities`, default 256), then aggregate as `_other`. Message/trace/pathway IDs, offsets, attempts, payloads, keys and exception text are never metric dimensions. Application resource attributes provide service/environment identity.

Extension mode additionally records the `liveflow.*` checkpoint, confirmed-publication, rejected-context, clock-skew and latency instruments. Their checkpoint IDs belong in optional telemetry records, not metric dimensions. These extensions are independent of trace sampling and are not compatible with Datadog DSM's wire/intake protocol; see [reference notes](docs/datadog-reference.md).

## Build, test and artifacts

With .NET SDK 10 (library/sample target .NET 8):

```powershell
dotnet restore LiveFlow.DotNet.slnx --locked-mode
dotnet build LiveFlow.DotNet.slnx -c Release --no-restore
dotnet run --project tests/LiveFlow.Kafka.Tests -c Release --no-build
dotnet run --project samples/OtlpKafka -c Release --no-build -- --help
dotnet pack src/LiveFlow.Kafka -c Release --no-build -o artifacts
```

Tests use fake producers, collecting listeners/sinks and deterministic clocks. They contact neither Kafka nor an exporter. [Standard OTLP fixtures](samples/fixtures/standard-chain.otlp.json) cover `send -> process -> send -> process`; [the second fixture](samples/fixtures/cross-trace-chain.otlp.json) uses an active application parent plus a cross-trace producer link. They have no private attributes or header dependency. Regenerate timestamps using `node samples/fixtures/create-fixtures.mjs`; no telemetry is sent by that generator.

See [review results](docs/review-results.md) for checks and limits. NuGet/source artifacts are local under `artifacts/`; nothing is published automatically.
