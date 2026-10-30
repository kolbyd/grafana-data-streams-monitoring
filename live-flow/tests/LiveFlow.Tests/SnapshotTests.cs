using System.Text.Json;
using LiveFlow.Flow;

internal static class SnapshotTests
{
    public static async Task RunAll(Func<string, Action, Task> run)
    {
        await run("snapshot retains a full minute above 150 events and reset reconnect needs no rate warmup", () =>
        {
            var clock = new SnapshotClock();
            var engine = new FlowEngine(new(), clock);
            for (var i = 0; i < 120; i++)
            {
                PutPair(engine, i, clock.GetUtcNow().ToUnixTimeMilliseconds());
                clock.Advance(TimeSpan.FromMilliseconds(500));
            }
            var first = engine.Snapshot();
            Equal(360, first.Events.Count);
            Equal(120, first.Events.Count(e => e.Kind == "publish"));
            Equal(120, first.Events.Count(e => e.Kind == "matched"));
            Equal(2d, first.Events.Count(e => e.Kind == "publish") / 60d);
            Assert(!first.Coverage.Truncated);
            foreach (var reset in new[] { engine.Since("previous-instance", first.Cursor, 0), engine.Since(engine.InstanceId, -1, 0) })
            {
                Assert(reset.ResetEvents);
                Equal(first.Events.Count, reset.Events.Count);
                Equal(2d, reset.Events.Count(e => e.Kind == "publish") / 60d);
                Equal(first.Events.First().Sequence, reset.Events.First().Sequence);
            }
            Equal(200, engine.Snapshot(200).Events.Count);
            Equal(360, engine.Snapshot(10_000).Events.Count);
            Equal(0, engine.Snapshot(0).Events.Count);
            var continuation = engine.Since(engine.InstanceId, first.Cursor, first.TopologyVersion);
            Assert(!continuation.ResetEvents); Equal(0, continuation.Events.Count);
        });
        await run("snapshot cursor expiration restores retained history while incremental animation stays bounded", () =>
        {
            var engine = new FlowEngine(new() { MaxEvents = 600, MaxVisualEventsPerFrame = 100 }, new SnapshotClock());
            for (var i = 0; i < 250; i++) PutPair(engine, i, 1_000 + i * 10);
            var reset = engine.Since(engine.InstanceId, 0, 0);
            Assert(reset.ResetEvents); Equal("cursor_expired", reset.Reason); Equal(600, reset.Events.Count);
            Equal(0, reset.Coverage.EventsOmitted);
            var initial = engine.Snapshot();
            for (var i = 250; i < 300; i++) PutPair(engine, i, 1_000 + i * 10);
            var update = engine.Since(engine.InstanceId, initial.Cursor, initial.TopologyVersion);
            Assert(!update.ResetEvents); Equal(100, update.Events.Count); Equal(50, update.VisualEventsSkipped);
        });
        await run("snapshot JSON byte budget reports coverage and keeps matched hop publications", () =>
        {
            const int budget = 16_384;
            var engine = new FlowEngine(new() { MaxFrameBytes = budget }, new SnapshotClock());
            var attributes = Enumerable.Range(0, 10).ToDictionary(i => "dsm.test." + i, _ => new string('界', 200));
            for (var i = 0; i < 100; i++) PutPair(engine, i, 1_000 + i);
            var frame = engine.Snapshot();
            Assert(JsonSerializer.SerializeToUtf8Bytes(frame, new JsonSerializerOptions(JsonSerializerDefaults.Web)).Length <= budget);
            Assert(frame.Coverage.Truncated && frame.Coverage.EventsOmitted > 0);
            Equal(300, frame.Coverage.RetainedEvents);
            Equal(frame.Events.Count, frame.Coverage.ReturnedEvents);
            var publications = frame.Events.Where(e => e.Kind == "publish").Select(e => e.ObservationId).ToHashSet();
            Assert(frame.Events.Any(e => e.Kind == "matched"));
            Assert(frame.Events.Where(e => e.Kind == "matched").All(e => publications.Contains(e.ProducerObservationId!)));
            var reset = engine.Since("old-instance", 0, 0);
            Assert(JsonSerializer.SerializeToUtf8Bytes(reset, new JsonSerializerOptions(JsonSerializerDefaults.Web)).Length <= budget);
            Assert(reset.Coverage.Truncated);
            // The actual Grafana Live default queue is 4 MiB, not the bridge's
            // 8 MiB input limit. A single initial response must fit below it.
            var defaults = new FlowOptions();
            Equal(3_145_728, defaults.MaxFrameBytes);
            var large = new FlowEngine(defaults, new SnapshotClock());
            for (var i = 0; i < 100; i++) PutPair(large, i, 1_000 + i, attributes);
            var safe = large.Snapshot();
            Assert(JsonSerializer.SerializeToUtf8Bytes(safe, new JsonSerializerOptions(JsonSerializerDefaults.Web)).Length <= defaults.MaxFrameBytes);
            Assert(safe.Events.Count > 0 && safe.Coverage.Truncated);
        });
    }

    private static void PutPair(FlowEngine engine, int i, long at, IReadOnlyDictionary<string, string>? attributes = null)
    {
        var p = new Observation((i * 2 + 1).ToString("x32"), (i * 2 + 1).ToString("x16"), "publish", "producer", "shop",
            "prod", "cluster-a", "orders", null, "0", i.ToString(), at - 10, at, false, false, [])
        { DsmAttributes = attributes ?? new Dictionary<string, string>() };
        var r = p with { TraceId = (i * 2 + 2).ToString("x32"), SpanId = (i * 2 + 2).ToString("x16"), Operation = "receive", Service = "consumer", ConsumerGroup = "workers", OccurredAt = at + 5 };
        engine.Ingest(new(2, 0, 0, [p, r]));
    }
    private static void Assert(bool value) { if (!value) throw new Exception("Expected true."); }
    private static void Equal<T>(T expected, T actual) { if (!EqualityComparer<T>.Default.Equals(expected, actual)) throw new Exception($"Expected {expected}; got {actual}."); }
    private sealed class SnapshotClock : TimeProvider
    {
        private DateTimeOffset now = new(2026, 10, 30, 12, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => now;
        public void Advance(TimeSpan duration) => now += duration;
    }
}
