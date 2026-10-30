using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

namespace LiveFlow.Kafka;

internal static class ContextCodec
{
    public const string HeaderName = "liveflow-pathway-v1";
    public const int MaxContextBytes = 1024;
    public const int MaxOutboxBytes = 16384;
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase, MaxDepth = 8,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
    };
    private sealed record Wire([property: JsonRequired] int Version, [property: JsonRequired] string MessageId,
        [property: JsonRequired] long Origin, [property: JsonRequired] bool OriginKnown, [property: JsonRequired] long At,
        [property: JsonRequired] string Checkpoint, [property: JsonRequired] string Pathway, [property: JsonRequired] CheckpointKind Kind);
    private sealed record OutboxWire([property: JsonRequired] int Version, [property: JsonRequired] string OutboxId,
        [property: JsonRequired] long EnqueuedAt, [property: JsonRequired] Wire Context);
    private sealed record OutboxCarrier([property: JsonRequired] int Version, [property: JsonRequired] string OutboxId,
        [property: JsonRequired] long EnqueuedAt, [property: JsonRequired] Dictionary<string, string> Headers,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] JsonElement? Context = null);
    private static Wire ToWire(FlowContext context) => new(1, context.LogicalMessageId, context.OriginUnixMs, context.OriginKnown,
        context.CheckpointUnixMs, context.CheckpointId, context.PathwayHash, context.Kind);

    internal static bool ValidIdentity(string? text, int limit) => !string.IsNullOrWhiteSpace(text)
        && text.Length <= limit && Encoding.UTF8.GetByteCount(text) <= limit && !text.Any(char.IsControl);
    private static bool Hex(string? text, int length) => text?.Length == length && text.All(Uri.IsHexDigit);

    internal static byte[] Encode(FlowContext context)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(ToWire(context), Json);
        if (bytes.Length > MaxContextBytes) throw new InvalidOperationException("Context exceeds the wire limit.");
        return bytes;
    }

    private static ContextReadStatus Validate(Wire? wire, long now, long futureSkew, out FlowContext? context)
    {
        context = null;
        if (wire is null) return ContextReadStatus.Malformed;
        if (wire.Version != 1) return ContextReadStatus.UnsupportedVersion;
        if (!ValidIdentity(wire.MessageId, 128) || !Hex(wire.Checkpoint, 32) || !Hex(wire.Pathway, 16) || !Enum.IsDefined(wire.Kind))
            return ContextReadStatus.InvalidIdentity;
        if (wire.Origin < 0 || wire.At < 0 || wire.Origin > now + futureSkew || wire.At > now + futureSkew)
            return ContextReadStatus.InvalidTimestamp;
        context = new(wire.MessageId, wire.Origin, wire.OriginKnown, wire.At, wire.Checkpoint, wire.Pathway, wire.Kind);
        return ContextReadStatus.Valid;
    }

    internal static ContextReadStatus Decode(Headers? headers, long now, long futureSkew, out FlowContext? context)
    {
        context = null;
        byte[]? bytes = null;
        if (headers is not null)
            foreach (var header in headers)
                if (header.Key == HeaderName) bytes = header.GetValueBytes();
        if (bytes is null) return ContextReadStatus.Missing;
        if (bytes.Length > MaxContextBytes) return ContextReadStatus.TooLarge;
        try { return Validate(JsonSerializer.Deserialize<Wire>(bytes, Json), now, futureSkew, out context); }
        catch (Exception ex) when (ex is JsonException or ArgumentException or NotSupportedException) { return ContextReadStatus.Malformed; }
    }

    internal static string EncodeOutbox(OutboxContext value)
    {
        var text = JsonSerializer.Serialize(new OutboxCarrier(2, value.OutboxId, value.EnqueuedAtUnixMs,
            new Dictionary<string, string>(value.TraceHeaders), value.HasPathwayExtension ? JsonSerializer.SerializeToElement(ToWire(value.Context), Json) : null), Json);
        if (Encoding.UTF8.GetByteCount(text) > MaxOutboxBytes) throw new InvalidOperationException("Outbox metadata exceeds the wire limit.");
        return text;
    }

    internal static bool TryDecodeOutbox(string? text, long now, long futureSkew, bool enableExtensions, out OutboxContext? context)
    {
        context = null;
        if (text is null || text.Length > MaxOutboxBytes || Encoding.UTF8.GetByteCount(text) > MaxOutboxBytes) return false;
        try
        {
            using var document = JsonDocument.Parse(text);
            if (!document.RootElement.TryGetProperty("version", out var version) || !version.TryGetInt32(out var number)) return false;
            if (number == 1 && enableExtensions)
            {
                var legacy = JsonSerializer.Deserialize<OutboxWire>(text, Json);
                if (legacy is null || !Hex(legacy.OutboxId, 32) || legacy.EnqueuedAt < 0 || legacy.EnqueuedAt > now + futureSkew
                    || Validate(legacy.Context, now, futureSkew, out var legacyFlow) != ContextReadStatus.Valid || legacyFlow is null) return false;
                context = new(legacy.OutboxId, legacy.EnqueuedAt, legacyFlow);
                return true;
            }
            if (number != 2) return false;
            var wire = JsonSerializer.Deserialize<OutboxCarrier>(text, Json);
            if (wire is null || !Hex(wire.OutboxId, 32) || wire.EnqueuedAt < 0 || wire.EnqueuedAt > now + futureSkew || wire.Headers is null) return false;
            if (wire.Headers.Count > 3 || wire.Headers.Any(entry => entry.Key is not ("traceparent" or "tracestate" or "baggage")
                    || entry.Value is null || Encoding.UTF8.GetByteCount(entry.Value) > 8192)) return false;
            var propagation = TracePropagation.FromCarrier(wire.Headers);
            if (wire.Headers.ContainsKey("traceparent") && !TracePropagation.Valid(propagation.ActivityContext)) return false;
            if (wire.Headers.ContainsKey("tracestate") && !wire.Headers.ContainsKey("traceparent")) return false;
            FlowContext? flow = null;
            if (enableExtensions && wire.Context is { } extension
                && Validate(extension.Deserialize<Wire>(Json), now, futureSkew, out flow) != ContextReadStatus.Valid) return false;
            var hasPathway = flow is not null;
            flow ??= new(Guid.NewGuid().ToString("N"), now, false, now, Guid.NewGuid().ToString("N"), "0000000000000000", CheckpointKind.Origin);
            context = new(wire.OutboxId, wire.EnqueuedAt, flow, propagation, hasPathway);
            return true;
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException or NotSupportedException or InvalidOperationException) { return false; }
    }
}
