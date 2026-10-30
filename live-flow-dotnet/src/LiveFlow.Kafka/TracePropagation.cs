using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry.Context.Propagation;

namespace LiveFlow.Kafka;

internal static class TracePropagation
{
    internal static TextMapPropagator W3C { get; } = new CompositeTextMapPropagator(
        [new TraceContextPropagator(), new BaggagePropagator()]);
    internal static bool Valid(ActivityContext context) => context.TraceId != default && context.SpanId != default;

    internal static IEnumerable<string> HeaderValues(Headers? headers, string key)
    {
        if (headers is null) yield break;
        foreach (var header in headers)
        {
            if (!string.Equals(header.Key, key, StringComparison.OrdinalIgnoreCase)) continue;
            var bytes = header.GetValueBytes();
            if (bytes is { Length: <= 8192 }) yield return Encoding.UTF8.GetString(bytes);
        }
    }

    internal static IReadOnlyDictionary<string, string> ToCarrier(PropagationContext context)
    {
        var carrier = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        W3C.Inject(context, carrier, static (headers, name, value) => headers[name] = value);
        return new ReadOnlyDictionary<string, string>(carrier);
    }

    internal static PropagationContext FromCarrier(IReadOnlyDictionary<string, string> carrier)
        => W3C.Extract(default, carrier, static (headers, key) => headers.TryGetValue(key, out var value) ? [value] : []);
}
