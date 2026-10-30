using System.Globalization;
using System.Text;
using Google.Protobuf;
using LiveFlow.Flow;
using OpenTelemetry.Proto.Collector.Trace.V1;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace LiveFlow.Ingestion;

public sealed class SpanNormalizer(FlowOptions options)
{
    private static readonly HashSet<string> DsmKeys = new(StringComparer.Ordinal)
    {
        "dsm.logical_message_id", "dsm.pathway_hash", "dsm.origin_unix_ms", "dsm.publish.classification",
        "dsm.retry.attempt", "dsm.deadletter.reason", "dsm.outbox.id", "dsm.outbox.wait_ms",
        "dsm.message.id", "dsm.pathway.hash", "dsm.pathway.parent_hash", "dsm.origin.timestamp_ms",
        "dsm.dead_letter.reason", "dsm.outbox.enqueued_at_unix_ms", "dsm.processing.outcome"
    };
    public NormalizedBatch Normalize(ExportTraceServiceRequest request)
    {
        var total = request.ResourceSpans.Sum(r => r.ScopeSpans.Sum(s => s.Spans.Count));
        if (total > options.MaxSpansPerRequest) throw new BadHttpRequestException("Too many spans in one export.", 413);
        var observations = new List<Observation>();
        var causal = new List<Observation>();
        var ignored = 0;
        var invalid = 0;
        foreach (var resource in request.ResourceSpans)
        {
            var resourceAttributes = ReadAttributes(resource.Resource?.Attributes ?? []);
            foreach (var span in resource.ScopeSpans.SelectMany(s => s.Spans))
            {
                var attributes = ReadAttributes(span.Attributes);
                string? Get(params string[] keys)
                {
                    foreach (var key in keys)
                        if (attributes.TryGetValue(key, out var value) && value.Length > 0) return value;
                    foreach (var key in keys)
                        if (resourceAttributes.TryGetValue(key, out var value) && value.Length > 0) return value;
                    return null;
                }
                var kafka = string.Equals(Get("messaging.system"), "kafka", StringComparison.OrdinalIgnoreCase);
                var operation = Get("messaging.operation.type", "messaging.operation.name", "messaging.operation")?.ToLowerInvariant();
                var role = kafka ? operation switch
                {
                    "publish" or "send" or "produce" => "publish",
                    "receive" or "consume" or "poll" => "receive",
                    "process" => "process",
                    "create" => "create",
                    "settle" or "commit" or "ack" or "subscribe" => "internal",
                    _ when span.Kind == Span.Types.SpanKind.Producer => "publish",
                    _ when span.Kind == Span.Types.SpanKind.Consumer => "receive",
                    _ => "internal"
                } : "internal";
                var wire = role is "publish" or "receive";
                var topic = kafka ? Get("messaging.destination.name", "messaging.destination") ?? "" : "";
                var service = resourceAttributes.GetValueOrDefault("service.name");
                var env = Get("deployment.environment.name", "deployment.environment", "env") ?? "unknown";
                var ns = Get("service.namespace") ?? "";
                var cluster = Get("messaging.kafka.cluster.id", "kafka.cluster.id", "kafka.cluster.name", "liveflow.cluster") ?? options.DefaultCluster;
                var group = kafka ? Get("messaging.consumer.group.name", "messaging.kafka.consumer.group", "messaging.kafka.consumer_group") : null;
                var valid = !string.IsNullOrWhiteSpace(service)
                    && new[] { topic, service, ns, env, cluster, group }.All(label => label?.Length is not > 256)
                    && ValidId(span.TraceId, 16) && ValidId(span.SpanId, 8)
                    && span.EndTimeUnixNano != 0 && span.EndTimeUnixNano >= span.StartTimeUnixNano
                    && span.EndTimeUnixNano / 1_000_000 <= (ulong)DateTimeOffset.MaxValue.ToUnixTimeMilliseconds();
                if (!valid || wire && string.IsNullOrWhiteSpace(topic))
                {
                    if (wire) invalid++; else ignored++;
                    continue;
                }
                var invalidRelationships = 0;
                SpanReference? parent = null;
                if (span.ParentSpanId.Length > 0)
                {
                    if (ValidId(span.ParentSpanId, 8)) parent = new(Hex(span.TraceId), Hex(span.ParentSpanId));
                    else invalidRelationships++;
                }
                // Preserve every valid link, including cross-trace and per-message link attributes.
                // Global causal-state bounds are applied later to whole spans, never a silent Take(16).
                var links = new List<SpanReference>(span.Links.Count);
                foreach (var link in span.Links)
                {
                    if (!ValidId(link.TraceId, 16) || !ValidId(link.SpanId, 8)) { invalidRelationships++; continue; }
                    var linkAttributes = ReadAttributes(link.Attributes).Where(p => p.Key is
                        "messaging.destination.name" or "messaging.destination.partition.id" or "messaging.kafka.offset"
                        or "messaging.message.id" or "messaging.consumer.group.name")
                        .Where(p => p.Value.Length <= 256 && Encoding.UTF8.GetByteCount(p.Value) <= 256).ToDictionary(p => p.Key, p => p.Value);
                    links.Add(new(Hex(link.TraceId), Hex(link.SpanId)) { Attributes = linkAttributes.Count == 0 ? null : linkAttributes });
                }
                var dsm = attributes.Where(p => DsmKeys.Contains(p.Key) && p.Value.Length <= 256 && Encoding.UTF8.GetByteCount(p.Value) <= 256)
                    .Take(16).ToDictionary(p => p.Key, p => p.Value);
                var observation = new Observation(Hex(span.TraceId), Hex(span.SpanId), role,
                    service!, ns, env, cluster, topic, role is "receive" or "process" ? group : null,
                    NonNegativeInteger(Get("messaging.destination.partition.id", "messaging.kafka.destination.partition")),
                    NonNegativeInteger(Get("messaging.kafka.offset", "messaging.kafka.message.offset")),
                    (long)(span.StartTimeUnixNano / 1_000_000), (long)(span.EndTimeUnixNano / 1_000_000),
                    span.Status?.Code == Status.Types.StatusCode.Error || Get("error.type") is not null,
                    long.TryParse(Get("messaging.batch.message_count"), out var count) && count > 1, links)
                {
                    Parent = parent, InvalidRelationships = invalidRelationships, DsmAttributes = dsm,
                    MessageId = Get("messaging.message.id") is { Length: <= 256 } messageId ? messageId : null
                };
                if (wire) observations.Add(observation);
                else
                {
                    // Preserve the established wire-count semantics. Process/internal spans enrich causality,
                    // but are not a second receive or another publish operation.
                    ignored++;
                    causal.Add(observation);
                }
            }
        }
        return new NormalizedBatch(total, ignored, invalid, observations) { CausalSpans = causal };
    }

    private static string? NonNegativeInteger(string? value) =>
        long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var number) && number >= 0
            ? number.ToString(CultureInfo.InvariantCulture) : null;
    private static string Hex(ByteString value) => Convert.ToHexStringLower(value.Span);
    private static bool ValidId(ByteString value, int length) => value.Length == length && value.Span.IndexOfAnyExcept((byte)0) >= 0;

    private static Dictionary<string, string> ReadAttributes(IEnumerable<KeyValue> values)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in values)
        {
            var value = pair.Value;
            if (value is null) continue;
            var text = value.ValueCase switch
            {
                AnyValue.ValueOneofCase.StringValue => value.StringValue,
                AnyValue.ValueOneofCase.IntValue => value.IntValue.ToString(CultureInfo.InvariantCulture),
                AnyValue.ValueOneofCase.BoolValue => value.BoolValue ? "true" : "false",
                AnyValue.ValueOneofCase.DoubleValue when double.IsFinite(value.DoubleValue) => value.DoubleValue.ToString("R", CultureInfo.InvariantCulture),
                _ => null
            };
            if (text is not null) result[pair.Key] = text;
        }
        return result;
    }
}
