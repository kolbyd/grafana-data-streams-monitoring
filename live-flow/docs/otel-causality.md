# Standard OTel causal processing

Ingress remains OTLP/HTTP protobuf or JSON at `/v1/traces`. No custom message header, unsampled checkpoint stream or proprietary DSM format is required. The source is normal OTel span identity, parent context, links and messaging attributes. Processing spans must cover actual handler work; a polling `Consume` span cannot establish its completion.

The official [messaging conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) distinguish send, receive, process, create and settle, and permit links to producers in other traces. A process span can have an ambient HTTP parent plus a message producer link. The processor uses the producer relation for the message input, and can traverse retained non-messaging parent bridges between a process and a downstream send. It never joins outputs merely because their timestamps or service names are nearby.

## Frame additions

`pathways` is a bounded array, replaced completely on every snapshot/update. Each item contains:

| Field | Meaning |
| --- | --- |
| `id` | Stable hash of the retained causal branch's span IDs |
| `originObservationId`, `terminalObservationId` | Earliest retained send and terminal process IDs; origin can be null for incomplete branches |
| `originService`, `terminalService`, `environment` | Display metadata; service names alone are not identities |
| `startNodeId`, `endNodeId` | Existing stable service-ID scheme; start can be null when incomplete |
| `nodeIds`, `edgeIds` | Ordered topology references; edge IDs include only currently retained graph edges |
| `startedAt`, `completedAt` | Send start and terminal process completion, Unix milliseconds; start can be null |
| `durationMs` | `completedAt - startedAt` only for a complete, non-skewed observed branch |
| `status` | `complete`, `incomplete`, or `clock_skew` |
| `incompleteReason` | Missing send/processing/ancestor, invalid relationship, incompatible message scope, cycle or depth limit |
| `evidence` | `otel_parent_links`, or `record_assisted` when an unambiguous Kafka record supplied a missing message relationship |
| `isBatch`, `error` | Explicit batch-dependency and span-error flags |
| `steps` | Ordered observation/trace/span IDs, operation, service/node/topic/cluster/group, span start/completion, error |
| `missingSpanIds` | Missing references on an incomplete displayed branch |
| `dsmAttributes` | Optional bounded extension metadata; never establishes causality or overrides measured duration |

A complete item means the displayed range is connected by retained evidence. It does **not** prove this is the true start/end of every business pipeline. Unknown context before an earliest observed send is a boundary. A late ancestor can extend a prior shorter range, replacing its prior row/ID. A known processing hop with missing input remains incomplete; an unexplained send→send transition without a retained process is not assigned an invented processing interval.

Linked batches can yield multiple input dependency branches sharing processing/output spans. Complete batch ranges may be displayed as explicitly labeled causal pathway samples; they do not establish which individual input record caused a particular output. Summaries must distinguish them from individual-message samples, filter complete/non-skew evidence and selected endpoints/environment/time, and must not multiply wire counts. A batch send without per-message creation context still supplies operation-range timing, not record-level mapping. Do not add per-hop p95 values to estimate end-to-end p95.

A valid multi-topic process span may omit `messaging.destination.name`. Its causal metadata is retained with an empty topic, and incoming topology is derived only from the actual topics on retained linked producers/receives/creation contexts. No `unknown` topic is invented. Those derived multi-topic dependency edges do not claim attributable per-topic receive/processing counts; the service's processing span is counted once. Late producer arrival can fill in the topology.

`causality` contains `retainedSpans`, `retainedRelations`, `evictedSpans`, `truncatedRelations`, `incompleteChains`, `clockSkewedChains`, `duplicateSpans`, `truncatedPathways`, and cumulative `processedSpans`. Result counts describe the bounded current output, not global loss-free message totals.

## Bounds and retention

Defaults: 10,000 causal spans, 50,000 parent/link references, 128 pathway results, depth 64, 50,000 derivation visits per rebuild, and 1 MiB serialized pathway output. Configure `Flow__MaxCausalSpans`, `Flow__MaxCausalRelations`, `Flow__MaxPathways`, `Flow__MaxPathwayDepth`, `Flow__MaxCausalDerivationWork`, and `Flow__MaxPathwayOutputBytes`.

The same correlation-retention setting applies to causal spans. Full valid link vectors are preserved without a per-span `Take(16)` truncation. Capacity pressure evicts whole oldest retained spans; a single vector larger than the global relation budget is omitted as a whole and counted. Invalid references are counted and prevent that branch from claiming a complete duration. Derivation/output bounds truncate results, not wire counters. Deduplication is bounded, not permanent or durable.

The legacy wire correlation caches keep only the single link they actually need. Full vectors live only in the causal store so repeated exports cannot multiply an arbitrary number of retained links across event/correlation windows. The authenticated `/api/causality/spans/{traceId}/{spanId}` endpoint exposes a retained normalized span and every valid link for inspection; it returns 404 after omission, eviction or expiry.

## Optional extensions

The allowlist accepts scalar strings/integers/booleans/finite doubles, with at most 256 UTF-8 bytes per value: `dsm.logical_message_id`, `dsm.pathway_hash`, `dsm.origin_unix_ms`, `dsm.publish.classification`, `dsm.retry.attempt`, `dsm.deadletter.reason`, `dsm.outbox.id`, `dsm.outbox.wait_ms`, and compatibility spellings `dsm.message.id`, `dsm.pathway.hash`, `dsm.pathway.parent_hash`, `dsm.origin.timestamp_ms`, `dsm.dead_letter.reason`, `dsm.outbox.enqueued_at_unix_ms`, `dsm.processing.outcome`. Arbitrary payload/body attributes are not retained. These properties enrich details only and never silently replace standard OTel ancestry/timestamps.

## Standard Grafana path

This service is optional. [Tempo service graphs](https://grafana.com/docs/tempo/latest/metrics-from-traces/service_graphs/) already support producer/consumer service edges. Tempo can additionally emit `traces_service_graph_request_messaging_system_seconds` when `enable_messaging_system_latency_histogram: true` is enabled in its metrics-generator configuration. Those native service graphs/Prometheus metrics and Tempo queries are valid foundations for Grafana. This processor provides this prototype's topic-oriented live snapshot protocol and retained multi-hop branch evidence; it is not a prerequisite for standard Grafana messaging visibility.

This processor does not collect broker metrics, committed offsets or authoritative consumer lag. It cannot recover spans lost to sampling, instrument business processing automatically or prove durable delivery/transaction success from a dependency alone.

## Synthetic protocol fixtures

`examples/otel/standard-chain.otlp.json` and `cross-trace-chain.otlp.json` are generated .NET SDK protocol fixtures. Both describe send A → process B → send B → process C with a 45 ms illustrative range. Tests post them through the actual HTTP endpoint and verify 2 publish spans, 2 processing spans, 0 synthetic receives and a connected 45 ms branch. They do not contact Kafka.

For a fresh local review, `python tools/send_otel_fixture.py` refreshes IDs/timestamps and posts one explicitly synthetic fixture to loopback. This is not cluster orchestration or production traffic generation.
