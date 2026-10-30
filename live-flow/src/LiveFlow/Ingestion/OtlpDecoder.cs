using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Nodes;
using Google.Protobuf;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace LiveFlow.Ingestion;

public static class OtlpDecoder
{
    private static readonly JsonParser Parser = new(JsonParser.Settings.Default.WithIgnoreUnknownFields(true));

    public static async Task<byte[]> ReadBoundedAsync(HttpRequest request, int limit, CancellationToken cancellation)
    {
        if (request.ContentLength > limit) throw new BadHttpRequestException("Request is too large.", 413);
        var encoding = request.Headers.ContentEncoding.ToString();
        if (encoding.Length > 0 && !encoding.Equals("gzip", StringComparison.OrdinalIgnoreCase))
            throw new BadHttpRequestException("Only gzip content encoding is supported.", 415);
        using var gzip = encoding.Length > 0 ? new GZipStream(request.Body, CompressionMode.Decompress, leaveOpen: true) : null;
        var input = gzip is null ? request.Body : gzip;
        using var output = new MemoryStream();
        var buffer = new byte[16_384];
        int read;
        while ((read = await input.ReadAsync(buffer, cancellation)) != 0)
        {
            if (output.Length + read > limit) throw new BadHttpRequestException("Decoded request is too large.", 413);
            await output.WriteAsync(buffer.AsMemory(0, read), cancellation);
        }
        return output.ToArray();
    }

    public static ExportTraceServiceRequest Decode(ReadOnlySpan<byte> bytes, bool json)
    {
        if (!json) return ExportTraceServiceRequest.Parser.ParseFrom(bytes);
        var node = JsonNode.Parse(bytes, documentOptions: new JsonDocumentOptions { MaxDepth = 64 })
            ?? throw new InvalidDataException("Expected an OTLP JSON object.");
        if (node is not JsonObject) throw new InvalidDataException("Expected an OTLP JSON object.");
        // OTLP uses hexadecimal IDs, unlike the generic protobuf JSON parser's base64 IDs.
        if (node["resourceSpans"] is JsonArray resources)
            foreach (var resource in resources.OfType<JsonObject>())
                if (resource["scopeSpans"] is JsonArray scopes)
                    foreach (var scope in scopes.OfType<JsonObject>())
                        if (scope["spans"] is JsonArray spans)
                            foreach (var span in spans.OfType<JsonObject>())
                            {
                                ConvertIds(span);
                                if (span["links"] is JsonArray links)
                                    foreach (var link in links.OfType<JsonObject>()) ConvertIds(link);
                            }
        return Parser.Parse<ExportTraceServiceRequest>(node.ToJsonString());
    }

    private static void ConvertIds(JsonNode node)
    {
        if (node is JsonObject obj)
        {
            foreach (var key in new[] { "traceId", "spanId", "parentSpanId" })
            {
                if (obj[key] is not JsonValue value || !value.TryGetValue<string>(out var hex)) continue;
                if (hex.Length != 0 && (hex.Length != (key == "traceId" ? 32 : 16) || !hex.All(Uri.IsHexDigit)))
                    throw new InvalidDataException($"Invalid {key}.");
                obj[key] = Convert.ToBase64String(Convert.FromHexString(hex));
            }
        }
    }
}
