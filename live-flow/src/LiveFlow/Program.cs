using System.Net;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using LiveFlow.Flow;
using LiveFlow.Ingestion;
using LiveFlow.Streaming;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;
using OpenTelemetry.Proto.Collector.Trace.V1;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddProblemDetails();
builder.Services.AddOptions<FlowOptions>().Bind(builder.Configuration.GetSection("Flow")).ValidateDataAnnotations().ValidateOnStart();
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<FlowOptions>>().Value);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<FlowEngine>();
builder.Services.AddSingleton<SpanNormalizer>();
builder.Services.AddHostedService<StateSweeper>();
builder.WebHost.ConfigureKestrel(server => server.Limits.MaxRequestBodySize = builder.Configuration.GetValue("Flow:MaxRequestBytes", 4_194_304));
var app = builder.Build();
var options = app.Services.GetRequiredService<FlowOptions>();
var urls = builder.Configuration["urls"] ?? "http://127.0.0.1:4318";
if (string.IsNullOrEmpty(options.ApiKey) && urls.Split(';').Any(url =>
    !Uri.TryCreate(url, UriKind.Absolute, out var uri) || !(uri.IsLoopback || IPAddress.TryParse(uri.Host, out var ip) && IPAddress.IsLoopback(ip))))
    throw new InvalidOperationException("Set Flow__ApiKey before binding beyond loopback.");

app.UseExceptionHandler();
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });
app.Use(async (context, next) =>
{
    context.Response.Headers["X-Content-Type-Options"] = "nosniff";
    context.Response.Headers["Referrer-Policy"] = "no-referrer";
    if (context.Request.Path.StartsWithSegments("/api") || context.Request.Path.StartsWithSegments("/v1") || context.Request.Path == "/metrics")
    {
        context.Response.Headers.CacheControl = "no-store";
        if (!string.IsNullOrEmpty(options.ApiKey))
        {
            var supplied = context.Request.Headers.Authorization.ToString();
            supplied = supplied.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? supplied[7..] : "";
            // Browser WebSocket APIs cannot set Authorization; never put a long-lived key in the URL.
            if (supplied.Length == 0 && context.WebSockets.IsWebSocketRequest)
            {
                var protocol = context.WebSockets.WebSocketRequestedProtocols.FirstOrDefault(p => p.StartsWith("liveflow.auth.", StringComparison.Ordinal));
                if (protocol is not null)
                    try { supplied = Encoding.UTF8.GetString(WebEncoders.Base64UrlDecode(protocol[14..])); }
                    catch (FormatException) { supplied = ""; }
            }
            if (!CryptographicOperations.FixedTimeEquals(SHA256.HashData(Encoding.UTF8.GetBytes(supplied)), SHA256.HashData(Encoding.UTF8.GetBytes(options.ApiKey))))
            { context.Response.StatusCode = 401; context.Response.Headers.WWWAuthenticate = "Bearer"; return; }
        }
        var origin = context.Request.Headers.Origin.ToString();
        if (origin.Length > 0 && !origin.Equals($"{context.Request.Scheme}://{context.Request.Host}", StringComparison.OrdinalIgnoreCase)
            && !options.AllowedOrigins.Contains(origin, StringComparer.OrdinalIgnoreCase))
        { context.Response.StatusCode = 403; return; }
    }
    await next(context);
});
app.UseDefaultFiles();
app.UseStaticFiles(new StaticFileOptions
{
    OnPrepareResponse = ctx => ctx.Context.Response.Headers["Content-Security-Policy"] =
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:; frame-ancestors 'self'; base-uri 'self'"
});

app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));
app.MapGet("/api/status", (FlowEngine engine) => Results.Ok(new { instanceId = engine.InstanceId, stats = engine.Stats,
    streamIntervalMs = options.StreamIntervalMs, staleAfterSeconds = options.StaleAfterSeconds,
    authenticated = options.ApiKey.Length > 0, persistence = "memory", protocolVersion = 1 }));
app.MapGet("/api/topology", (FlowEngine engine) => engine.Snapshot());
app.MapGet("/api/causality/spans/{traceId}/{spanId}", (FlowEngine engine, string traceId, string spanId) =>
    engine.CausalSpan(traceId, spanId) is { } span ? Results.Ok(span) : Results.NotFound());
app.MapGet("/api/events", (FlowEngine engine, string? instanceId, long? after) => engine.Since(instanceId, after ?? 0, -1));
app.Map("/api/stream", StreamEndpoint.HandleAsync);
app.MapGet("/metrics", (FlowEngine engine) =>
{
    var s = engine.Stats;
    var metrics = new Dictionary<string, (double Value, string Type)>
    {
        ["accepted_total"] = (s.Accepted, "counter"), ["publish_total"] = (s.Published, "counter"),
        ["receive_total"] = (s.Received, "counter"), ["error_total"] = (s.Errors, "counter"),
        ["process_total"] = (s.Processed, "counter"),
        ["matched_total"] = (s.Matched, "counter"), ["duplicate_total"] = (s.Duplicates, "counter"),
        ["ignored_total"] = (s.Ignored, "counter"), ["rejected_total"] = (s.Rejected, "counter"),
        ["correlation_evictions_total"] = (s.CorrelationEvictions, "counter"), ["expired_receives_total"] = (s.ExpiredReceives, "counter"),
        ["waiting_receives"] = (s.WaitingReceives, "gauge"), ["cached_publications"] = (s.CachedPublications, "gauge"),
        ["buffered_events"] = (s.BufferedEvents, "gauge"), ["nodes"] = (s.Nodes, "gauge"),
        ["edges"] = (s.Edges, "gauge"), ["viewers"] = (s.Viewers, "gauge"), ["slow_viewers_total"] = (s.SlowViewers, "counter")
    };
    return Results.Text(string.Join('\n', metrics.Select(p => $"# TYPE liveflow_{p.Key} {p.Value.Type}\nliveflow_{p.Key} {p.Value.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)}")) + "\n", "text/plain; version=0.0.4");
});

app.MapPost("/v1/traces", async (HttpContext context, SpanNormalizer normalizer, FlowEngine engine) =>
{
    var contentType = context.Request.ContentType?.Split(';')[0].Trim();
    var json = string.Equals(contentType, "application/json", StringComparison.OrdinalIgnoreCase);
    if (!json && !string.Equals(contentType, "application/x-protobuf", StringComparison.OrdinalIgnoreCase))
        return Results.Problem("Use application/x-protobuf or application/json.", statusCode: 415);
    try
    {
        var bytes = await OtlpDecoder.ReadBoundedAsync(context.Request, options.MaxRequestBytes, context.RequestAborted);
        var request = OtlpDecoder.Decode(bytes, json);
        var result = engine.Ingest(normalizer.Normalize(request));
        var response = new ExportTraceServiceResponse();
        if (result.Rejected > 0)
            response.PartialSuccess = new ExportTracePartialSuccess { RejectedSpans = result.Rejected,
                ErrorMessage = "Kafka spans were rejected: missing required identity/timestamps or topology capacity reached. Inspect /api/status." };
        return json ? Results.Text(response.ToString(), "application/json")
            : Results.Bytes(response.ToByteArray(), "application/x-protobuf");
    }
    catch (BadHttpRequestException ex) { return Results.Problem(ex.Message, statusCode: ex.StatusCode); }
    catch (Exception ex) when (ex is InvalidProtocolBufferException or System.Text.Json.JsonException or InvalidDataException or FormatException)
    { return Results.Problem("Malformed OTLP trace export.", statusCode: 400); }
});

app.Run();

public partial class Program;

internal sealed class StateSweeper(FlowEngine engine) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try { while (await timer.WaitForNextTickAsync(stoppingToken)) engine.Sweep(); }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
    }
}
