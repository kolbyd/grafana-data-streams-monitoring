using System.Diagnostics;
using System.Text.Json;
using LiveFlow.Kafka;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace DotNetKafkaLab;

public sealed class Statistics
{
    public string Role { get; init; } = "";
    public string RunId { get; init; } = "";
    public long Processed { get; set; }
    public long Published { get; set; }
    public long Retry { get; set; }
    public long DeadLetter { get; set; }
    public long PrivateHeaders { get; set; }
    public long StandardHeaders { get; set; }
    public long JoinSpans { get; set; }
    public long TwoInputLinks { get; set; }
    public long CrossTraceJoins { get; set; }
    public long RootJoins { get; set; }
    public long SendSpans { get; set; }
    public long ProcessSpans { get; set; }
    public long ReceiveSpans { get; set; }
    public long TimedReceiveSpans { get; set; }
    public int PendingJoins { get; set; }
    public int UniqueOrders { get; set; }
    public bool Finished { get; set; }
    public string? Error { get; set; }
    private readonly object _gate = new();
    private readonly HashSet<string> _orders = new();
    private DateTime _lastWrite;
    public void ObserveOrder(string id) { _orders.Add(id); UniqueOrders = _orders.Count; Processed++; }
    public void Write(Options options, bool force = false)
    {
        lock (_gate)
        {
            if (!force && DateTime.UtcNow - _lastWrite < TimeSpan.FromSeconds(1)) return;
            _lastWrite = DateTime.UtcNow;
            var path = options.ProgressFile(Role);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.WriteAllText(path + ".tmp", JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true }));
            File.Move(path + ".tmp", path, overwrite: true);
        }
    }
    public static Statistics? Read(string path)
    {
        try { return JsonSerializer.Deserialize<Statistics>(File.ReadAllText(path)); }
        catch (IOException) { return null; }
    }
}

public sealed class AuditProcessor(Statistics stats) : BaseProcessor<Activity>
{
    public override void OnEnd(Activity activity)
    {
        lock (stats)
        {
        if (Equals(activity.GetTagItem("messaging.operation.type"), "send")) stats.SendSpans++;
        if (Equals(activity.GetTagItem("messaging.operation.type"), "process")) stats.ProcessSpans++;
        if (Equals(activity.GetTagItem("messaging.operation.type"), "receive"))
        {
            stats.ReceiveSpans++;
            if (activity.Duration > TimeSpan.Zero) stats.TimedReceiveSpans++;
        }
        if (!Equals(activity.GetTagItem("messaging.batch.message_count"), 2)) return;
        stats.JoinSpans++;
        var links = activity.Links.Select(link => link.Context).ToArray();
        if (links.Length == 2 && links[0].SpanId != links[1].SpanId) stats.TwoInputLinks++;
        if (links.Select(link => link.TraceId).Distinct().Count() == 2) stats.CrossTraceJoins++;
        if (activity.ParentSpanId == default) stats.RootJoins++;
        }
    }
}

public sealed class WorkerTelemetry : IDisposable
{
    public const string SourceName = "DotNetKafkaLab.Workload";
    public ActivitySource Work { get; } = new(SourceName);
    public FlowInstrumentation Flow { get; }
    private readonly TracerProvider _traces;
    private readonly MeterProvider? _metrics;
    public WorkerTelemetry(Options options, Statistics statistics)
    {
        var resource = ResourceBuilder.CreateDefault().AddService($"dotnet-lab-{options.Role}")
            .AddAttributes([new("deployment.environment.name", "local-lab")]);
        var traceBuilder = Sdk.CreateTracerProviderBuilder().SetResourceBuilder(resource)
            .SetSampler(new AlwaysOnSampler()).AddSource(FlowInstrumentation.ActivitySourceName, SourceName)
            .AddProcessor(new AuditProcessor(statistics));
        if (options.Export) traceBuilder.AddOtlpExporter();
        _traces = traceBuilder.Build();
        if (options.ExportMetrics && options.Export)
            _metrics = Sdk.CreateMeterProviderBuilder().SetResourceBuilder(resource)
                .AddMeter(FlowInstrumentation.MeterName).AddOtlpExporter().Build();
        Flow = new FlowInstrumentation(new FlowOptions { Cluster = Options.ClusterId });
    }
    public void Dispose()
    {
        Flow.Dispose(); Work.Dispose();
        _traces.ForceFlush(5000); _metrics?.ForceFlush(5000);
        _traces.Dispose(); _metrics?.Dispose();
    }
}
