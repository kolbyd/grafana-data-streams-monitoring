using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Google.Protobuf;
using LiveFlow.Flow;
using LiveFlow.Ingestion;
using OpenTelemetry.Proto.Collector.Trace.V1;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;

var passed = 0; var failed = 0;
var testFilter = args.FirstOrDefault(a => a.StartsWith("--filter="))?[9..];
await Run("protobuf and old .NET semantic attributes", () => {
    var b = Normalize(OtlpDecoder.Decode(Export(SpanFor(1), "producer").ToByteArray(), false));
    Eq(1, b.Observations.Count); Eq("42", b.Observations[0].Offset); Eq("prod", b.Observations[0].Environment);
});
await Run("OTLP JSON hex IDs, timestamps and unknown fields", () => {
    var json = JsonExport(2).Replace("\"kind\":4", "\"kind\":4,\"futureField\":{\"traceId\":\"unknown-field-content\"}");
    var b = Normalize(OtlpDecoder.Decode(Encoding.UTF8.GetBytes(json), true)); Eq(1, b.Observations.Count); Eq(32, b.Observations[0].TraceId.Length);
});
await Run("new semantic attributes and offsets beyond JavaScript integer precision", () => {
    var s = SpanFor(3, true); s.Attributes.Clear(); s.Attributes.AddRange([Attr("messaging.system", "kafka"), Attr("messaging.operation.type", "receive"), Attr("messaging.destination.name", "orders.created"), Attr("messaging.consumer.group.name", "payments"), Attr("messaging.destination.partition.id", "0"), Attr("messaging.kafka.offset", "9007199254740993")]);
    var o = Normalize(Export(s, "consumer")).Observations.Single(); Eq("9007199254740993", o.Offset); Eq("payments", o.ConsumerGroup);
});
await Run("invalid identities rejected and unrelated spans ignored", () => {
    var s = SpanFor(4); s.TraceId = ByteString.Empty; var request = Export(s, "producer"); request.ResourceSpans[0].ScopeSpans[0].Spans.Add(new Span());
    var b = Normalize(request); Eq(1, b.Invalid); Eq(1, b.Ignored);
});
await Run("create/process operations do not double count boundaries", () => {
    foreach (var op in new[] { "create", "process", "settle" }) { var s = SpanFor(5); s.Attributes.Add(Attr("messaging.operation.type", op)); Eq(1, Normalize(Export(s, "producer")).Ignored); }
});
await Run("oversized identities reject instead of colliding", () => {
    var request = Export(SpanFor(6), "producer"); request.ResourceSpans[0].Resource.Attributes[0] = Attr("service.name", new string('a', 257));
    Eq(1, Normalize(request).Invalid);
});
await Run("viewer admission is bounded and recovers after disconnect", () => {
    var engine = Engine(new() { MaxViewers = 1 }); Assert(engine.TryAddViewer()); Assert(!engine.TryAddViewer());
    engine.RemoveViewer(); Assert(engine.TryAddViewer()); engine.RemoveViewer(); Eq(0, engine.Stats.Viewers);
});
await Run("record matching and fan-out preserve consumer groups", () => {
    var e = Engine(); Put(e, O(10, "publish"), O(11, "receive", "payments"), O(12, "receive", "analytics"));
    Eq(2L, e.Stats.Matched); Eq(3, e.Stats.Edges); Eq(0, e.Stats.WaitingReceives); Eq("record", e.Snapshot().Events.Last().CorrelationMethod);
});
await Run("receive-before-produce is matched", () => {
    var e = Engine(); Put(e, O(20, "receive")); Eq(1, e.Stats.WaitingReceives); Put(e, O(21, "publish")); Eq(1L, e.Stats.Matched); Eq(0, e.Stats.WaitingReceives);
});
await Run("span links match without record coordinates", () => {
    var e = Engine(); var p = O(30, "publish") with { Offset = null, Cluster = "unknown" };
    Put(e, O(31, "receive") with { Offset = null, Cluster = "unknown", Links = [new(p.TraceId, p.SpanId)] }, p);
    Eq(1L, e.Stats.Matched); Eq("span_link", e.Snapshot().Events.Last().CorrelationMethod);
});
await Run("unknown cluster and cluster/environment boundaries prevent false matches", () => {
    var e = Engine(); Put(e, O(40, "publish"), O(41, "receive") with { Cluster = "other" }, O(42, "receive") with { Environment = "staging" }); Eq(0L, e.Stats.Matched);
    var unknown = Engine(); Put(unknown, O(43, "publish") with { Cluster = "unknown" }, O(44, "receive") with { Cluster = "unknown" }); Eq(0L, unknown.Stats.Matched);
});
await Run("conflicting coordinates cannot match through a span link", () => {
    var e = Engine(); var p = O(50, "publish"); Put(e, p, O(51, "receive") with { Offset = "43", Links = [new(p.TraceId, p.SpanId)] }); Eq(0L, e.Stats.Matched);
});
await Run("export retry deduplication, failed and batch observations", () => {
    var e = Engine(); var p = O(60, "publish"); Put(e, p, p); Eq(1L, e.Stats.Accepted); Eq(1L, e.Stats.Duplicates);
    Put(e, O(61, "receive") with { Error = true }, O(62, "receive") with { IsBatch = true }); Eq(0L, e.Stats.Matched); Eq(1L, e.Stats.Errors);
});
await Run("bounded state and topology capacity rejection", () => {
    var e = Engine(new() { MaxCorrelationEntries = 3, MaxSeenSpans = 10, MaxEvents = 10, MaxNodes = 2, MaxEdges = 1 });
    for (var i = 100; i < 150; i++) Put(e, O(i, "publish") with { Offset = i.ToString() });
    Assert(e.Stats.CachedPublications <= 3 && e.Stats.SeenSpans <= 10 && e.Stats.BufferedEvents <= 10);
    Eq(1, e.Ingest(new(1, 0, 0, [O(999, "receive")])).Rejected);
});
await Run("TTL expiration cleans graph, replay and correlation indices", () => {
    var clock = new Clock(); var e = new FlowEngine(new() { EventRetentionMinutes = 1, CorrelationRetentionMinutes = 1, TopologyRetentionMinutes = 1 }, clock);
    Put(e, O(200, "publish"), O(201, "receive") with { Offset = "999" }); clock.Advance(TimeSpan.FromMinutes(2)); e.Sweep();
    Eq(0, e.Stats.Nodes); Eq(0, e.Stats.Edges); Eq(0, e.Stats.CachedPublications); Eq(0, e.Stats.SeenSpans); Eq(1L, e.Stats.ExpiredReceives);
});
await Run("cursor replay and restart/expired cursor recovery", () => {
    var e = Engine(new() { MaxEvents = 10 }); Put(e, O(300, "publish")); var cursor = e.Snapshot().Cursor; Put(e, O(301, "receive"));
    var frame = e.Since(e.InstanceId, cursor, 0); Eq(2, frame.Events.Count); Assert(!frame.ResetEvents); Eq("instance_changed", e.Since("old", cursor, 0).Reason);
    for (var i = 302; i < 330; i++) Put(e, O(i, "publish")); Eq("cursor_expired", e.Since(e.InstanceId, cursor, 0).Reason);
});
await Run("visual reduction preserves counts and parallel ingestion", () => {
    var e = Engine(new() { MaxVisualEventsPerFrame = 2 }); Parallel.For(400, 900, i => Put(e, O(i, "publish")));
    var frame = e.Since(e.InstanceId, 0, 0); Eq(2, frame.Events.Count); Eq(498, frame.VisualEventsSkipped); Eq(500L, frame.Stats.Published);
});
await RunAsync("HTTP protobuf, JSON, gzip, partial success, limits and WebSocket replay", async () => {
    await using var host = await Host.Start(); using var client = new HttpClient { BaseAddress = host.Url };
    var response = await client.PostAsync("v1/traces", Proto(Export(SpanFor(1000), "producer"))); Eq(HttpStatusCode.OK, response.StatusCode); Eq("application/x-protobuf", response.Content.Headers.ContentType!.MediaType);
    Eq(HttpStatusCode.OK, (await client.PostAsync("v1/traces", new StringContent(JsonExport(1001), Encoding.UTF8, "application/json"))).StatusCode);
    using var compressed = new MemoryStream(); using (var zip = new GZipStream(compressed, CompressionMode.Compress, true)) zip.Write(Export(SpanFor(1002), "producer").ToByteArray());
    using var body = new ByteArrayContent(compressed.ToArray()); body.Headers.ContentType = new("application/x-protobuf"); body.Headers.ContentEncoding.Add("gzip"); Eq(HttpStatusCode.OK, (await client.PostAsync("v1/traces", body)).StatusCode);
    using var bomb = new MemoryStream(); using (var zip = new GZipStream(bomb, CompressionMode.Compress, true)) zip.Write(new byte[4_194_305]);
    using var bombBody = new ByteArrayContent(bomb.ToArray()); bombBody.Headers.ContentType = new("application/x-protobuf"); bombBody.Headers.ContentEncoding.Add("gzip");
    Eq(HttpStatusCode.RequestEntityTooLarge, (await client.PostAsync("v1/traces", bombBody)).StatusCode);
    Eq(HttpStatusCode.BadRequest, (await client.PostAsync("v1/traces", new StringContent("not json", Encoding.UTF8, "application/json"))).StatusCode);
    Eq(HttpStatusCode.UnsupportedMediaType, (await client.PostAsync("v1/traces", new StringContent(""))).StatusCode);
    var bad = SpanFor(1004); bad.TraceId = ByteString.Empty;
    var partial = ExportTraceServiceResponse.Parser.ParseFrom(await (await client.PostAsync("v1/traces", Proto(Export(bad, "producer")))).Content.ReadAsByteArrayAsync()); Eq(1L, partial.PartialSuccess.RejectedSpans);
    using var snapshot = JsonDocument.Parse(await client.GetStringAsync("api/topology")); var instance = snapshot.RootElement.GetProperty("instanceId").GetString(); var cursor = snapshot.RootElement.GetProperty("cursor").GetInt64();
    using var socket = new ClientWebSocket(); using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await socket.ConnectAsync(new Uri(host.Url.ToString().Replace("http:", "ws:") + $"api/stream?instanceId={instance}&after={cursor}"), timeout.Token);
    Eq("update", (await ReadFrame(socket, timeout.Token)).GetProperty("type").GetString());
    await client.PostAsync("v1/traces", Proto(Export(SpanFor(1003), "producer"))); Assert((await ReadFrame(socket, timeout.Token)).GetProperty("cursor").GetInt64() > cursor);
    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "test", timeout.Token);
    Assert((await client.GetStringAsync("metrics")).Contains("liveflow_publish_total 4"));
    var origin = new HttpRequestMessage(HttpMethod.Get, "api/status"); origin.Headers.Add("Origin", "https://evil.example"); Eq(HttpStatusCode.Forbidden, (await client.SendAsync(origin)).StatusCode);
    using var large = new ByteArrayContent(new byte[4_194_305]); large.Headers.ContentType = new("application/x-protobuf");
    using var largeRequest = new HttpRequestMessage(HttpMethod.Post, "v1/traces") { Content = large }; largeRequest.Headers.ExpectContinue = true;
    Eq(HttpStatusCode.RequestEntityTooLarge, (await client.SendAsync(largeRequest)).StatusCode);
});
await RunAsync("HTTP bearer and browser WebSocket authentication", async () => {
    await using var host = await Host.Start("review-test-key"); using var client = new HttpClient { BaseAddress = host.Url };
    Eq(HttpStatusCode.Unauthorized, (await client.GetAsync("api/status")).StatusCode); client.DefaultRequestHeaders.Authorization = new("Bearer", "review-test-key"); Eq(HttpStatusCode.OK, (await client.GetAsync("api/status")).StatusCode);
    using var socket = new ClientWebSocket(); socket.Options.AddSubProtocol("liveflow.v1"); socket.Options.AddSubProtocol("liveflow.auth." + Convert.ToBase64String(Encoding.UTF8.GetBytes("review-test-key")).TrimEnd('=').Replace('+', '-').Replace('/', '_'));
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5)); await socket.ConnectAsync(new Uri(host.Url.ToString().Replace("http:", "ws:") + "api/stream"), timeout.Token);
    Eq("snapshot", (await ReadFrame(socket, timeout.Token)).GetProperty("type").GetString()); await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "test", timeout.Token);
});
await SnapshotTests.RunAll(Run);
await CausalTests.RunAll(Run);
foreach (var fixture in new[] { "standard-chain.otlp.json", "cross-trace-chain.otlp.json" })
{
    await RunAsync("SDK standard OTLP fixture over HTTP: " + fixture, async () =>
    {
        await using var host = await Host.Start(); using var client = new HttpClient { BaseAddress = host.Url };
        var json = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", fixture));
        var response = await client.PostAsync("v1/traces", new StringContent(json, Encoding.UTF8, "application/json"));
        Eq(HttpStatusCode.OK, response.StatusCode);
        using var topology = JsonDocument.Parse(await client.GetStringAsync("api/topology"));
        var root = topology.RootElement;
        var path = root.GetProperty("pathways").EnumerateArray().Single(p => p.GetProperty("status").GetString() == "complete"
            && p.GetProperty("originService").GetString() == "fixture-source" && p.GetProperty("terminalService").GetString() == "fixture-target");
        Eq(45L, path.GetProperty("durationMs").GetInt64());
        Eq(2L, root.GetProperty("stats").GetProperty("published").GetInt64());
        Eq(0L, root.GetProperty("stats").GetProperty("received").GetInt64());
        Eq(2L, root.GetProperty("stats").GetProperty("processed").GetInt64());
        Assert(path.GetProperty("nodeIds").GetArrayLength() >= 5);
        var processStep = path.GetProperty("steps").EnumerateArray().First(s => s.GetProperty("operation").GetString() == "process");
        var detailUrl = "api/causality/spans/" + processStep.GetProperty("traceId").GetString() + "/" + processStep.GetProperty("spanId").GetString();
        using var detail = JsonDocument.Parse(await client.GetStringAsync(detailUrl));
        Assert(detail.RootElement.GetProperty("links").GetArrayLength() >= 1);
        Assert(detail.RootElement.GetProperty("parent").GetProperty("spanId").GetString() is { Length: 16 });
        Assert(root.GetProperty("events").EnumerateArray().All(e => e.GetProperty("kind").GetString() != "process"));
        Assert((await client.GetStringAsync("metrics")).Contains("liveflow_process_total 2"));
    });
}
Console.WriteLine($"\n{passed} passed; {failed} failed."); return failed == 0 ? 0 : 1;

async Task Run(string name, Action action) => await RunAsync(name, () => { action(); return Task.CompletedTask; });
async Task RunAsync(string name, Func<Task> action) { if (testFilter is not null && !name.Contains(testFilter, StringComparison.OrdinalIgnoreCase)) return; try { await action(); passed++; Console.WriteLine("PASS  " + name); } catch (Exception e) { failed++; Console.Error.WriteLine("FAIL  " + name + "\n" + e); } }
static void Assert(bool value) { if (!value) throw new Exception("Expected true."); }
static void Eq<T>(T expected, T actual) { if (!EqualityComparer<T>.Default.Equals(expected, actual)) throw new Exception($"Expected {expected}; got {actual}."); }
static FlowEngine Engine(FlowOptions? options = null) => new(options ?? new(), new Clock());
static void Put(FlowEngine e, params Observation[] observations) => e.Ingest(new(observations.Length, 0, 0, observations));
static Observation O(int id, string op, string? group = null) => new(id.ToString("x32"), id.ToString("x16"), op, op == "publish" ? "producer" : "consumer", "shop", "prod", "cluster-a", "orders.created", op == "receive" ? group ?? "payments" : null, "0", "42", 1000, 1010, false, false, []);
static KeyValue Attr(string key, string value) => new() { Key = key, Value = new AnyValue { StringValue = value } };
static Span SpanFor(int id, bool consumer = false) => new() {
    TraceId = ByteString.CopyFrom(Convert.FromHexString(id.ToString("x32"))), SpanId = ByteString.CopyFrom(Convert.FromHexString(id.ToString("x16"))), Kind = consumer ? Span.Types.SpanKind.Consumer : Span.Types.SpanKind.Producer,
    StartTimeUnixNano = 1_700_000_000_000_000_000UL, EndTimeUnixNano = 1_700_000_000_010_000_000UL,
    Attributes = { Attr("messaging.system", "kafka"), Attr("messaging.operation", consumer ? "receive" : "publish"), Attr("messaging.destination.name", "orders.created"), Attr("messaging.kafka.destination.partition", "0"), Attr("messaging.kafka.message.offset", "42") }
};
static ExportTraceServiceRequest Export(Span s, string service) => new() { ResourceSpans = { new ResourceSpans { Resource = new Resource { Attributes = { Attr("service.name", service), Attr("deployment.environment.name", "prod"), Attr("kafka.cluster.id", "cluster-a") } }, ScopeSpans = { new ScopeSpans { Spans = { s } } } } } };
static NormalizedBatch Normalize(ExportTraceServiceRequest request) => new SpanNormalizer(new()).Normalize(request);
static string JsonExport(int id) => JsonSerializer.Serialize(new { resourceSpans = new[] { new { resource = new { attributes = new[] { new { key = "service.name", value = new { stringValue = "producer" } } } }, scopeSpans = new[] { new { spans = new[] { new { traceId = id.ToString("x32"), spanId = id.ToString("x16"), kind = 4, startTimeUnixNano = "1700000000000000000", endTimeUnixNano = "1700000000010000000", attributes = new[] { new { key = "messaging.system", value = new { stringValue = "kafka" } }, new { key = "messaging.destination.name", value = new { stringValue = "orders.created" } } } } } } } } } });
static ByteArrayContent Proto(ExportTraceServiceRequest request) { var result = new ByteArrayContent(request.ToByteArray()); result.Headers.ContentType = new("application/x-protobuf"); return result; }
static async Task<JsonElement> ReadFrame(ClientWebSocket socket, CancellationToken token) {
    using var output = new MemoryStream(); var buffer = new byte[65536]; WebSocketReceiveResult result;
    do { result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), token); output.Write(buffer, 0, result.Count); } while (!result.EndOfMessage);
    using var doc = JsonDocument.Parse(output.ToArray()); return doc.RootElement.Clone();
}
sealed class Clock : TimeProvider { private DateTimeOffset now = new(2026, 10, 30, 12, 0, 0, TimeSpan.Zero); public override DateTimeOffset GetUtcNow() => now; public void Advance(TimeSpan duration) => now += duration; }
sealed class Host(Process process, Uri url) : IAsyncDisposable {
    public Uri Url => url;
    public static async Task<Host> Start(string? key = null) {
        var root = new DirectoryInfo(AppContext.BaseDirectory); while (root is not null && !File.Exists(Path.Combine(root.FullName, "LiveFlow.slnx"))) root = root.Parent;
        var source = Path.Combine(root!.FullName, "src", "LiveFlow"); var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start(); var port = ((IPEndPoint)listener.LocalEndpoint).Port; listener.Stop();
        var config = AppContext.BaseDirectory.Contains("Release") ? "Release" : "Debug";
        var start = new ProcessStartInfo("dotnet") { WorkingDirectory = source, UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        start.ArgumentList.Add(Path.Combine(source, "bin", config, "net10.0", "LiveFlow.dll")); start.ArgumentList.Add("--urls"); start.ArgumentList.Add($"http://127.0.0.1:{port}"); start.Environment["Flow__ApiKey"] = key ?? "";
        var process = Process.Start(start)!; var logs = new StringBuilder(); process.OutputDataReceived += (_, e) => { if (e.Data is not null) lock (logs) logs.AppendLine(e.Data); }; process.ErrorDataReceived += (_, e) => { if (e.Data is not null) lock (logs) logs.AppendLine(e.Data); }; process.BeginOutputReadLine(); process.BeginErrorReadLine();
        var url = new Uri($"http://127.0.0.1:{port}/"); using var client = new HttpClient { BaseAddress = url, Timeout = TimeSpan.FromSeconds(1) };
        for (var i = 0; i < 100; i++) { if (process.HasExited) throw new Exception("Service exited: " + logs); try { if ((await client.GetAsync("healthz")).IsSuccessStatusCode) return new Host(process, url); } catch (Exception error) when (error is HttpRequestException or TaskCanceledException) { } await Task.Delay(100); }
        process.Kill(true); throw new Exception("Service startup timeout: " + logs);
    }
    public async ValueTask DisposeAsync() { if (!process.HasExited) process.Kill(true); await process.WaitForExitAsync(); process.Dispose(); }
}
