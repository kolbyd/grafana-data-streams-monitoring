# Library review results

Verified 2026-10-30 on Windows, .NET SDK 10.0.403 and .NET 8 runtime 8.0.33.

- Locked restore passed with exact Confluent.Kafka 2.12.0, OpenTelemetry.Api 1.15.0 and checked-in dependency lock files. The compiled sample uses the official OpenTelemetry OTLP exporter 1.15.0. NuGet audit was disabled for the reproducible restore; this check is not an online vulnerability audit.
- Release solution build passed with zero warnings and zero errors.
- All **33 deterministic tests passed** using fake `IProducer` implementations, collecting/rejecting/bounded sinks, Activity/Meter listeners, standard/custom propagators and an injectable `TimeProvider`.
- NuGet packaging succeeded as `LiveFlow.Kafka.0.1.0-preview.1.nupkg`, targeting `net8.0`, with the library DLL, README, Apache-2.0 license and exact Confluent/OpenTelemetry API dependencies. Source ZIP includes the SDK sample and standard OTLP fixtures.

Coverage includes header propagation without mutating the caller's message, broker-confirmed coordinates versus failed/unconfirmed/cancelled attempts, separate receive/processing completion, sibling fan-out, parallel/nested async context isolation, malformed/future/oversized metadata, failing/full sinks, Activity sampling independence, metric privacy/cardinality limits, retry/DLQ identity preservation, outbox persistence signals and serialization across instrumentation instances, distinct outbox/Kafka timing, clock skew and W3C trace propagation. Processing spans explicitly identify `process` rather than a wire-level receive operation.

The follow-up review corrected two issues: missing JSON fields could previously become trusted default timestamps, and throwing Activity listeners could prevent application work or mask its original exception. Regression tests now remove every pathway, outbox and nested context field in turn, and exercise throwing sampler/start/stop listeners across publication and processing success/failure with ambient Activity restoration.

The OTel-first revision verifies that default operations emit standard send/process Activities and messaging instruments with no private header, checkpoint measurements or sink callbacks. Additional tests cover process parents/message links, sibling sends, cross-trace application parents, unsampled W3C/tracestate/baggage propagation and restoration, W3C outbox persistence across dispatcher instances, explicit extension opt-in, boundary ownership switches, throwing propagators, attempted-send semantics (including failure/cancellation/unconfirmed results), and bounded standard metric destination/group identities. Activity listener failures run with both standard defaults and extensions enabled.

The official SDK/exporter sample builds and its `--help` path runs without starting Kafka/exporters. Standard and cross-trace OTLP JSON fixtures describe a synthetic send/process/send/process chain without custom attributes; these are contract examples, not real-broker measurements.

This is not a real-broker, exporter/backend, production-load or profiler integration test. A private checkpoint transport is optional and remains unimplemented. Standard telemetry uses normal OTLP without it. See [integration-gap.md](integration-gap.md) for pipeline and inference limits.
