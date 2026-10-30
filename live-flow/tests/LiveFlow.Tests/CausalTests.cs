using Google.Protobuf;
using LiveFlow.Flow;
using LiveFlow.Ingestion;
using OpenTelemetry.Proto.Collector.Trace.V1;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;

internal static class CausalTests
{
    public static async Task RunAll(Func<string, Action, Task> run)
    {
        await run("OTel normalizer preserves parent, every cross-trace link and processing separately", () =>
        {
            var span = ProtoSpan(1, "process");
            span.ParentSpanId = Bytes(99, 8);
            for (var i = 0; i < 25; i++) span.Links.Add(new Span.Types.Link
            { TraceId = Bytes(100 + i, 16), SpanId = Bytes(200 + i, 8), Attributes = { Attr("messaging.message.id", "message-" + i) } });
            span.Attributes.Add(Attr("messaging.consumer.group.name", "workers"));
            span.Attributes.Add(Attr("dsm.publish.classification", "dead_letter"));
            span.Attributes.Add(Attr("dsm.payload", "must-not-be-retained"));
            span.Attributes.Add(new KeyValue { Key = "dsm.outbox.wait_ms", Value = new AnyValue { DoubleValue = 12.25 } });
            var normalized = Normalize(span);
            Eq(0, normalized.Observations.Count); Eq(1, normalized.Ignored); Eq(1, normalized.CausalSpans.Count);
            var process = normalized.CausalSpans.Single();
            Eq("process", process.Operation); Eq(25, process.Links.Count); Eq(99.ToString("x16"), process.Parent!.SpanId);
            Eq("message-24", process.Links[24].Attributes!["messaging.message.id"]);
            Eq("12.25", process.DsmAttributes["dsm.outbox.wait_ms"]); Eq("workers", process.ConsumerGroup);
            Assert(!process.DsmAttributes.ContainsKey("dsm.payload"));
        });
        await run("out-of-order send-process-send-process yields measured full causal range", () =>
        {
            var chain = Chain(); var engine = Engine();
            foreach (var span in new[] { chain[3], chain[2], chain[0] }) Put(engine, span);
            // A shorter send→process range can already be measured; the missing processing
            // ancestor must never be replaced by an invented connection to the earlier send.
            Assert(!engine.Snapshot().Pathways.Any(p => p.TerminalObservationId == chain[3].Id
                && p.OriginObservationId == chain[0].Id && p.Status == "complete"));
            Put(engine, chain[1]);
            var path = engine.Snapshot().Pathways.Single(p => p.TerminalObservationId == chain[3].Id && p.Status == "complete");
            Eq(200L, path.DurationMs); Eq(chain[0].Id, path.OriginObservationId); Eq(4, path.Steps.Count);
            Eq(FlowEngine.Id("service", "prod", "shop", "source"), path.StartNodeId);
            Eq(FlowEngine.Id("service", "prod", "shop", "target"), path.EndNodeId);
            Eq(5, path.NodeIds.Count); Eq(4, path.EdgeIds.Count);
            Eq(2L, engine.Stats.Published); Eq(0L, engine.Stats.Received); Eq(2L, engine.Stats.Processed);
            Assert(engine.Snapshot().Events.All(e => e.Kind != "process"));
        });
        await run("receive and process for the same delivery are not double-counted", () =>
        {
            var engine = Engine(); var send = S(10, "publish", "source", "orders", 100, 110);
            var receive = S(11, "receive", "worker", "orders", 90, 125) with { ConsumerGroup = "workers" };
            var process = S(12, "process", "worker", "orders", 130, 190) with { ConsumerGroup = "workers", Parent = Ref(receive) };
            Put(engine, process, send, receive);
            Eq(1L, engine.Stats.Received); Eq(1L, engine.Stats.Processed); Eq(1L, engine.Stats.Matched);
            var edge = engine.Snapshot().Edges.Single(e => e.Target == FlowEngine.Id("service", "prod", "shop", "worker"));
            Eq(1L, edge.Observations); Eq(1L, edge.Processed);
            var path = engine.Snapshot().Pathways.Single(p => p.Status == "complete");
            Eq(90L, path.DurationMs); Eq("record_assisted", path.Evidence);
        });
        await run("linked batches retain all producer causes without multiplying message counts", () =>
        {
            var engine = Engine();
            var sends = Enumerable.Range(20, 24).Select(i => S(i, "publish", "source-" + i, "orders", 100 + i, 130 + i) with { Offset = i.ToString(), TraceId = i.ToString("x32") }).ToArray();
            var batch = S(50, "process", "batch-worker", "orders", 200, 400) with { IsBatch = true, Offset = null, Links = sends.Select(Ref).ToArray() };
            Put(engine, batch); Put(engine, sends);
            var paths = engine.Snapshot().Pathways.Where(p => p.Status == "complete").ToArray();
            Eq(24, paths.Length); Assert(paths.All(p => p.IsBatch)); Eq(24, paths.Select(p => p.OriginObservationId).Distinct().Count());
            Eq(0L, engine.Stats.Received); Eq(1L, engine.Stats.Processed); Eq(24, engine.Snapshot().Causality.RetainedRelations);
        });
        await run("fan-out produces separate causal paths and does not chain unrelated outputs", () =>
        {
            var engine = Engine(); var chain = Chain();
            var otherSend = S(60, "publish", "worker", "other-topic", 160, 170) with { TraceId = chain[1].TraceId, Parent = Ref(chain[1]), Offset = "99" };
            var otherProcess = S(61, "process", "other-target", "other-topic", 190, 330) with { Links = [Ref(otherSend)], Offset = "99" };
            Put(engine, chain.Concat([otherSend, otherProcess]).ToArray());
            var paths = engine.Snapshot().Pathways.Where(p => p.Status == "complete" && p.Steps.Count == 4).ToArray();
            Eq(2, paths.Length); Assert(paths.All(p => p.OriginObservationId == chain[0].Id));
            Assert(paths.Single(p => p.TerminalObservationId == otherProcess.Id).Steps.All(s => s.ObservationId != chain[2].Id));
        });
        await run("multi-topic linked fan-in retains true topics without inventing counts", () =>
        {
            var engine = Engine();
            var a = S(65, "publish", "payments", "payment.results", 100, 110);
            var b = S(66, "publish", "inventory", "inventory.results", 105, 115) with { TraceId = 2.ToString("x32") };
            var join = S(67, "process", "joiner", "", 130, 180) with { IsBatch = true, Offset = null, Links = [Ref(a), Ref(b)] };
            Put(engine, join); Put(engine, a, b);
            var frame = engine.Snapshot();
            Eq(2, frame.Pathways.Count(p => p.Status == "complete"));
            var joinId = FlowEngine.Id("service", "prod", "shop", "joiner");
            var incoming = frame.Edges.Where(e => e.Target == joinId).ToArray(); Eq(2, incoming.Length);
            Assert(incoming.All(e => e.Topic is "payment.results" or "inventory.results" && e.Observations == 0 && e.Processed == 0));
            Eq(1L, frame.Nodes.Single(n => n.Id == joinId).Processed); Eq(0L, frame.Stats.Received);
            Assert(frame.Pathways.All(p => p.IsBatch && p.EdgeIds.Count == 2));
            Assert(frame.Nodes.Where(n => n.Kind == "topic").All(n => n.Name.Length > 0));
        });
        await run("a missing origin or missing processing bridge never receives fabricated latency", () =>
        {
            var chain = Chain(); var missingOrigin = Engine(); Put(missingOrigin, chain.Skip(1).ToArray());
            Assert(missingOrigin.Snapshot().Pathways.Where(p => p.TerminalObservationId == chain[3].Id).All(p => p.DurationMs is null && p.Status == "incomplete"));
            var noProcessing = Engine(); var receive = chain[1] with { Operation = "receive" }; Put(noProcessing, chain[0], receive, chain[2], chain[3]);
            Assert(noProcessing.Snapshot().Pathways.Where(p => p.TerminalObservationId == chain[3].Id).All(p => p.DurationMs is null && p.IncompleteReason == "missing_processing_span"));
        });
        await run("clock skew suppresses negative or inconsistent pathway timing", () =>
        {
            var engine = Engine(); var send = S(70, "publish", "source", "orders", 200, 210);
            var process = S(71, "process", "target", "orders", 100, 150) with { Links = [Ref(send)] };
            Put(engine, send, process); var path = engine.Snapshot().Pathways.Single();
            Eq("clock_skew", path.Status); Assert(path.DurationMs is null); Eq(1, engine.Snapshot().Causality.ClockSkewedChains);
        });
        await run("cluster, topic and environment contradictions prevent false causal joins", () =>
        {
            foreach (var changed in new[] { "cluster", "topic", "environment" })
            {
                var engine = Engine(); var send = S(80, "publish", "source", "orders", 100, 110);
                var process = S(81, "process", "target", "orders", 130, 150) with { Links = [Ref(send)] };
                process = changed switch { "cluster" => process with { Cluster = "other" }, "topic" => process with { Topic = "other" }, _ => process with { Environment = "staging" } };
                Put(engine, send, process); Assert(engine.Snapshot().Pathways.All(p => p.DurationMs is null));
            }
        });
        await run("causal span, relationship, result and depth budgets remain bounded", () =>
        {
            var options = new FlowOptions { MaxCausalSpans = 10, MaxCausalRelations = 10, MaxPathways = 3, MaxPathwayDepth = 4, MaxCausalDerivationWork = 100 };
            var engine = Engine(options);
            for (var i = 100; i < 150; i++) Put(engine, S(i, "publish", "source", "orders", 100, 110) with { Offset = i.ToString() });
            var tooMany = S(151, "process", "target", "orders", 130, 200) with { Links = Enumerable.Range(100, 11).Select(i => new SpanReference(i.ToString("x32"), i.ToString("x16"))).ToArray() };
            Put(engine, tooMany); var snapshot = engine.Snapshot();
            Assert(snapshot.Causality.RetainedSpans <= 10 && snapshot.Causality.RetainedRelations <= 10 && snapshot.Pathways.Count <= 3);
            Assert(snapshot.Causality.EvictedSpans > 0 && snapshot.Causality.TruncatedRelations >= 11);
            Assert(engine.CausalSpan(tooMany.TraceId, tooMany.SpanId) is null);
            var byteBound = Engine(new FlowOptions { MaxPathwayOutputBytes = 16_384, MaxPathways = 100 });
            var origins = Enumerable.Range(200, 30).Select(i => S(i, "publish", "source-" + i, "orders", 100, 110) with { Offset = i.ToString() }).ToArray();
            Put(byteBound, origins); Put(byteBound, S(250, "process", "target", "orders", 150, 200) with { IsBatch = true, Offset = null, Links = origins.Select(Ref).ToArray() });
            var boundedOutput = byteBound.Snapshot();
            Assert(System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(boundedOutput.Pathways).Length <= 16_384);
            Assert(boundedOutput.Causality.TruncatedPathways > 0);
            var depth = Engine(new FlowOptions { MaxPathwayDepth = 4 }); var chain = Chain();
            var bridge = S(152, "internal", "worker", "", 140, 180) with { TraceId = chain[1].TraceId, Parent = Ref(chain[1]) };
            Put(depth, chain[0], chain[1], bridge, chain[2] with { Parent = Ref(bridge) }, chain[3]);
            Assert(depth.Snapshot().Pathways.Where(p => p.TerminalObservationId == chain[3].Id).All(p => p.DurationMs is null));
        });
        await run("late ancestry updates pathways, duplicate spans do not duplicate them, expiry removes them", () =>
        {
            var clock = new CausalClock(); var engine = new FlowEngine(new FlowOptions { CorrelationRetentionMinutes = 1 }, clock); var chain = Chain();
            Put(engine, chain); var initial = engine.Snapshot(); var count = initial.Pathways.Count;
            Put(engine, chain); Eq(count, engine.Snapshot().Pathways.Count); Eq(2L, engine.Stats.Processed);
            Assert(engine.Snapshot().Causality.DuplicateSpans >= 2);
            clock.Advance(TimeSpan.FromMinutes(2)); engine.Sweep(); var expired = engine.Snapshot(); Eq(0, expired.Pathways.Count); Eq(0, expired.Causality.RetainedSpans);
        });
        await run("optional dsm attributes enrich, but never override standard measured timing", () =>
        {
            var engine = Engine(); var chain = Chain(); chain[2] = chain[2] with { DsmAttributes = new Dictionary<string, string> { ["dsm.origin.timestamp_ms"] = "1", ["dsm.publish.classification"] = "dead_letter" } };
            Put(engine, chain); var path = engine.Snapshot().Pathways.Single(p => p.TerminalObservationId == chain[3].Id && p.Status == "complete");
            Eq(200L, path.DurationMs); Eq("dead_letter", path.DsmAttributes["dsm.publish.classification"]);
        });
        await run("ordinary non-messaging spans can bridge a standard parent chain", () =>
        {
            var engine = Engine(); var chain = Chain(); var bridge = S(160, "internal", "worker", "", 140, 170) with { TraceId = chain[1].TraceId, Parent = Ref(chain[1]) };
            Put(engine, chain[0], chain[1], bridge, chain[2] with { Parent = Ref(bridge) }, chain[3]);
            var path = engine.Snapshot().Pathways.Single(p => p.TerminalObservationId == chain[3].Id && p.Status == "complete");
            Eq(200L, path.DurationMs); Assert(path.Steps.Any(s => s.Operation == "internal" && s.NodeId is null));
        });
    }

    private static Observation[] Chain()
    {
        var send = S(1, "publish", "source", "orders", 100, 110) with { TraceId = 100.ToString("x32"), Parent = new(100.ToString("x32"), 99.ToString("x16")) };
        var process = S(2, "process", "worker", "orders", 130, 220) with { TraceId = 200.ToString("x32"), Links = [Ref(send)] };
        var outgoing = S(3, "publish", "worker", "payments", 150, 160) with { TraceId = process.TraceId, Parent = Ref(process), Offset = "43" };
        var terminal = S(4, "process", "target", "payments", 180, 300) with { TraceId = 300.ToString("x32"), Links = [Ref(outgoing)], Offset = "43" };
        return [send, process, outgoing, terminal];
    }
    private static Observation S(int id, string op, string service, string topic, long start, long end)
        => new(1.ToString("x32"), id.ToString("x16"), op, service, "shop", "prod", "cluster-a", topic,
            op is "receive" or "process" ? "workers" : null, "0", "42", start, end, false, false, []);
    private static SpanReference Ref(Observation span) => new(span.TraceId, span.SpanId);
    private static FlowEngine Engine(FlowOptions? options = null) => new(options ?? new(), new CausalClock());
    private static void Put(FlowEngine engine, params Observation[] spans)
    {
        var wire = spans.Where(s => s.Operation is "publish" or "receive").ToArray();
        var others = spans.Where(s => s.Operation is not ("publish" or "receive")).ToArray();
        engine.Ingest(new(spans.Length, others.Length, 0, wire) { CausalSpans = others });
    }
    private static ByteString Bytes(int value, int size) => ByteString.CopyFrom(Convert.FromHexString(value.ToString(size == 16 ? "x32" : "x16")));
    private static KeyValue Attr(string key, string value) => new() { Key = key, Value = new AnyValue { StringValue = value } };
    private static Span ProtoSpan(int id, string op) => new()
    {
        TraceId = Bytes(id, 16), SpanId = Bytes(id, 8), ParentSpanId = ByteString.Empty,
        Kind = op == "send" ? Span.Types.SpanKind.Producer : Span.Types.SpanKind.Consumer,
        StartTimeUnixNano = 100_000_000, EndTimeUnixNano = 200_000_000,
        Attributes = { Attr("messaging.system", "kafka"), Attr("messaging.operation.type", op), Attr("messaging.destination.name", "orders"), Attr("messaging.kafka.cluster.id", "cluster-a") }
    };
    private static NormalizedBatch Normalize(Span span) => new SpanNormalizer(new()).Normalize(new ExportTraceServiceRequest
    { ResourceSpans = { new ResourceSpans { Resource = new Resource { Attributes = { Attr("service.name", "worker") } }, ScopeSpans = { new ScopeSpans { Spans = { span } } } } } });
    private static void Assert(bool condition) { if (!condition) throw new Exception("Expected true."); }
    private static void Eq<T>(T expected, T actual) { if (!EqualityComparer<T>.Default.Equals(expected, actual)) throw new Exception($"Expected {expected}; got {actual}."); }
    private sealed class CausalClock : TimeProvider
    {
        private DateTimeOffset now = new(2026, 10, 30, 12, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => now;
        public void Advance(TimeSpan duration) => now += duration;
    }
}
