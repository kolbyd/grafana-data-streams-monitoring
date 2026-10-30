using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using LiveFlow.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

var tests = new (string Name, Func<Task> Run)[]
{
    ("Propagation preserves caller message and confirms delivery coordinates", Propagation),
    ("Delivery failures preserve the original exception and never claim delivery", DeliveryFailure),
    ("Missing delivery reports remain unconfirmed", UnconfirmedDelivery),
    ("Receive and handler completion are separate checkpoints", ProcessingBoundary),
    ("Fan-out publishes share their inbound processing parent", FanOut),
    ("Parallel asynchronous handlers keep their message contexts isolated", ParallelIsolation),
    ("Nested failing handlers restore the outer context", NestedRestoration),
    ("Malformed, oversized and future headers do not interrupt processing", MalformedHeaders),
    ("Sink failures cannot interrupt Kafka work", SinkFailures),
    ("Checkpoint and metric collection survives Activity sampling None", UnsampledCheckpoints),
    ("Metric labels exclude sensitive identifiers and cap topic/group cardinality", MetricPrivacy),
    ("Dead-letter publications preserve lineage and bounded reason/attempt metadata", DeadLetter),
    ("Root retries preserve identity when reusing their explicit message context", RetryIdentity),
    ("Outbox metadata survives another process and separates wait from Kafka transit", OutboxRoundTrip),
    ("Outbox capture does not assert persistence", OutboxNoFalsePersistence),
    ("Outbox decoder rejects malformed, oversized and future metadata", MalformedOutbox),
    ("Clock skew is flagged without negative latency measurements", ClockSkew),
    ("Processing failures and cancellation remain distinct outcomes", HandlerFailures),
    ("Publish cancellation remains unknown rather than confirmed or failed delivery", PublishCancellation),
    ("W3C trace context propagates alongside independent pathway metadata", TracePropagation),
    ("A completed handler cannot contaminate an unrelated later publish", NoAmbientLeak),
    ("Invalid caller configuration is rejected before Kafka is called", InvalidArguments),
    ("A full checkpoint buffer stays bounded, reports drops, and can drain on shutdown", BoundedBuffer),
    ("Every pathway and outbox wire member must be present", RequiredWireMembers),
    ("Throwing Activity listeners cannot prevent work or mask original failures", ActivityListenerFailures),
    ("Default emits standard spans/metrics with no private header or checkpoint sink", DefaultOpenTelemetry),
    ("Standard message parents and links connect processing and sibling output sends", StandardCausality),
    ("Unsampled processing preserves W3C context and isolates baggage", UnsampledStandardPropagation),
    ("Outbox persistence restores W3C context across dispatcher instances", StandardOutboxPropagation),
    ("Private pathway enrichment is an explicit opt-in on top of W3C", OptionalPathwayExtensions),
    ("Existing instrumentation can own spans while wrappers retain metrics", ActivityOwnership),
    ("Propagator failures cannot interrupt application work", PropagatorFailures),
    ("Standard sent-message counts include attempted failures and unconfirmed sends", StandardMetricOutcomes),
};
var failed = 0;
foreach (var test in tests)
{
    try { await test.Run(); Console.WriteLine("PASS " + test.Name); }
    catch (Exception error) { failed++; Console.WriteLine("FAIL " + test.Name + "\n" + error); }
}
Console.WriteLine($"{tests.Length - failed}/{tests.Length} tests passed");
return failed == 0 ? 0 : 1;

static FlowInstrumentation Flow(string name, Sink? sink = null, TestClock? clock = null, int metricCap = 256)
    => new(new FlowOptions { ServiceName = name, Environment = "test", Cluster = "cluster-a", MaxMetricIdentities = metricCap, EnablePathwayExtensions = true }, sink, clock);
static Message<string, string> Message(string text = "private-payload") => new() { Key = "private-key", Value = text };
static ConsumeResult<string, string> Record(Message<string, string> message, string topic = "orders")
    => new() { Topic = topic, Partition = new Partition(2), Offset = new Offset(7), Message = message };
static void Check(bool condition, string message) { if (!condition) throw new InvalidOperationException(message); }
static FlowCheckpoint One(Sink sink, CheckpointKind kind) => sink.Items.Single(p => p.Kind == kind);
static FlowContext Extract(FlowInstrumentation flow, Message<string, string> message)
{
    Check(flow.TryExtractContext(message.Headers, out var context) == ContextReadStatus.Valid && context is not null, "Expected valid propagation metadata");
    return context!;
}
static async Task<Message<string, string>> Incoming(string identity = "message-a", TestClock? clock = null)
{
    using var source = Flow("source", clock: clock);
    var producer = ProducerProxy.Create();
    await source.ProduceAsync(producer.Client, "orders", Message(), new PublishOptions { Context = source.CreateMessageContext(identity) });
    return producer.Calls.Single().Message;
}

static async Task Propagation()
{
    var clock = new TestClock(); var sink = new Sink(); using var flow = Flow("producer", sink, clock);
    var producer = ProducerProxy.Create(); var input = Message(); input.Headers = new Headers { { "existing", [1, 2, 3] } };
    var context = flow.CreateMessageContext("stable-id");
    var result = await flow.ProduceAsync(producer.Client, "orders", input, new PublishOptions { Context = context });
    var sent = producer.Calls.Single().Message; var propagated = Extract(flow, sent);
    Check(propagated.LogicalMessageId == "stable-id" && propagated.OriginUnixMs == context.OriginUnixMs, "Identity/origin changed");
    Check(propagated.CheckpointId == One(sink, CheckpointKind.PublishAttempt).Id, "Header must represent the pre-delivery checkpoint");
    Check(input.Headers.Count == 1 && sent.Headers.Count == 2 && !ReferenceEquals(sent, input), "Caller message/headers were mutated");
    Check(sent.Key == input.Key && sent.Value == input.Value, "Payload or key changed");
    var delivered = One(sink, CheckpointKind.PublishDelivered);
    Check(delivered.Partition == 2 && delivered.Offset == 7 && delivered.ParentCheckpointId == propagated.CheckpointId, "Delivery confirmation is missing coordinates or parent");
    Check(result.Offset.Value == 7 && sink.Items.Count == 2, "Unexpected result/checkpoint count");
}

static async Task DeliveryFailure()
{
    var sink = new Sink(); using var flow = Flow("failed-producer", sink);
    var expected = new InvalidOperationException("secret-error-message");
    var producer = ProducerProxy.Create((_, _, _) => Task.FromException<DeliveryResult<string, string>>(expected));
    try { await flow.ProduceAsync(producer.Client, "orders", Message()); throw new Exception("Expected failure"); }
    catch (InvalidOperationException actual) { Check(ReferenceEquals(actual, expected), "Original failure was replaced"); }
    Check(sink.Items.Select(p => p.Kind).SequenceEqual([CheckpointKind.PublishAttempt, CheckpointKind.PublishFailed]), "Failed publication claimed delivery");
    var text = JsonSerializer.Serialize(sink.Items);
    Check(!text.Contains("secret-error-message") && !text.Contains("private-payload") && !text.Contains("private-key"), "Checkpoint leaked application content");
}

static async Task UnconfirmedDelivery()
{
    var sink = new Sink(); using var flow = Flow("unconfirmed-producer", sink);
    var producer = ProducerProxy.Create((topic, message, _) => Task.FromResult(new DeliveryResult<string, string>
        { Topic = topic, Message = message, Partition = Partition.Any, Offset = Offset.Unset, Status = PersistenceStatus.NotPersisted }));
    await flow.ProduceAsync(producer.Client, "orders", Message());
    Check(sink.Items.Any(p => p.Kind == CheckpointKind.PublishUnconfirmed) && !sink.Items.Any(p => p.Kind == CheckpointKind.PublishDelivered), "Unset offsets must not count as confirmed delivery");
}

static async Task ProcessingBoundary()
{
    var clock = new TestClock(); var incoming = await Incoming(clock: clock); clock.Advance(80);
    var sink = new Sink(); using var flow = Flow("consumer", sink, clock);
    var received = flow.ObserveReceived(Record(incoming), "consumer-v1");
    Check(One(sink, CheckpointKind.Receive).EdgeLatencyMs == 80, "Queue transit should use the propagated publish timestamp");
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var task = flow.ProcessAsync(received, async (context, _) =>
    {
        Check(flow.CurrentContext?.CheckpointId == context.CheckpointId, "Handler context not active");
        entered.SetResult(); await release.Task;
    });
    await entered.Task.WaitAsync(TimeSpan.FromSeconds(3));
    Check(!sink.Items.Any(p => p.Kind == CheckpointKind.ProcessingCompleted), "Receive was mistaken for completion");
    Check(flow.CurrentContext is null, "An in-flight asynchronous handler leaked into its caller");
    clock.Advance(20); release.SetResult(); await task;
    Check(One(sink, CheckpointKind.ProcessingCompleted).ProcessingDurationMs == 20, "Handler duration must use the monotonic clock");
    Check(flow.CurrentContext is null, "Handler context was not restored");
}

static async Task FanOut()
{
    var sink = new Sink(); using var flow = Flow("fanout", sink);
    var input = flow.ObserveReceived(Record(await Incoming()), "fanout-v1"); var producer = ProducerProxy.Create();
    await flow.ProcessAsync(input, async (_, token) =>
    {
        await flow.ProduceAsync(producer.Client, "output-a", Message(), cancellationToken: token);
        await flow.ProduceAsync(producer.Client, "output-b", Message(), cancellationToken: token);
    });
    var parent = One(sink, CheckpointKind.ProcessingStarted).Id;
    var publishes = sink.Items.Where(p => p.Kind == CheckpointKind.PublishAttempt).ToArray();
    Check(publishes.Length == 2 && publishes.All(p => p.ParentCheckpointId == parent), "Fan-out was chained output-a to output-b");
    Check(publishes[0].PathwayHash != publishes[1].PathwayHash && publishes.All(p => p.LogicalMessageId == "message-a"), "Branches need distinct pathway hashes and shared logical origin");
}

static async Task ParallelIsolation()
{
    var sink = new Sink(); using var flow = Flow("parallel", sink); var producer = ProducerProxy.Create();
    var left = flow.ObserveReceived(Record(await Incoming("left")), "workers");
    var right = flow.ObserveReceived(Record(await Incoming("right")), "workers");
    var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously); var arrived = 0;
    Task Handler(ReceivedMessage<string, string> received) => flow.ProcessAsync(received, async (context, token) =>
    {
        if (Interlocked.Increment(ref arrived) == 2) ready.SetResult();
        await ready.Task.WaitAsync(TimeSpan.FromSeconds(3)); await Task.Yield();
        Check(flow.CurrentContext?.LogicalMessageId == context.LogicalMessageId, "Parallel handler inherited a sibling's identity");
        await flow.ProduceAsync(producer.Client, context.LogicalMessageId + "-output", Message(), cancellationToken: token);
    });
    await Task.WhenAll(Handler(left), Handler(right));
    var publishes = sink.Items.Where(p => p.Kind == CheckpointKind.PublishAttempt).ToArray();
    Check(publishes.Length == 2 && publishes.All(p => p.Topic == p.LogicalMessageId + "-output"), "Parallel publication lineage crossed");
    Check(flow.CurrentContext is null, "Parallel tasks contaminated the caller");
}

static async Task NestedRestoration()
{
    using var flow = Flow("nested");
    var outer = flow.ObserveReceived(Record(await Incoming("outer")), "workers");
    var inner = flow.ObserveReceived(Record(await Incoming("inner")), "workers");
    await flow.ProcessAsync(outer, async (context, _) =>
    {
        try { await flow.ProcessAsync(inner, (_, _) => throw new InvalidOperationException("expected")); }
        catch (InvalidOperationException) { }
        Check(flow.CurrentContext?.CheckpointId == context.CheckpointId, "Inner failure did not restore the outer handler");
    });
    Check(flow.CurrentContext is null, "Outer context leaked");
}

static async Task MalformedHeaders()
{
    var clock = new TestClock(); var valid = await Incoming(clock: clock);
    var original = valid.Headers.Single(h => h.Key == FlowInstrumentation.PropagationHeader).GetValueBytes();
    static byte[] Edit(byte[] value, string property, JsonNode replacement)
    { var json = JsonNode.Parse(value)!.AsObject(); json[property] = replacement; return Encoding.UTF8.GetBytes(json.ToJsonString()); }
    var samples = new[] { Encoding.UTF8.GetBytes("{"), new byte[FlowInstrumentation.MaxContextBytes + 1], new byte[] { 0xff, 0xff },
        Edit(original, "version", JsonValue.Create(2)!), Edit(original, "checkpoint", JsonValue.Create("invalid")!),
        Edit(original, "at", JsonValue.Create(clock.GetUtcNow().ToUnixTimeMilliseconds() + 61_000)!) };
    foreach (var sample in samples)
    {
        var sink = new Sink(); using var flow = Flow("invalid-context", sink, clock);
        var message = Message(); message.Headers = new Headers { { FlowInstrumentation.PropagationHeader, sample } };
        var received = flow.ObserveReceived(Record(message), "workers");
        Check(received.ContextStatus != ContextReadStatus.Valid && !received.Context.OriginKnown, "Invalid context was trusted");
        var called = false; await flow.ProcessAsync(received, (_, _) => { called = true; return Task.CompletedTask; });
        Check(called && sink.Items.Any(p => p.Kind == CheckpointKind.ProcessingCompleted), "Malformed metadata prevented business processing");
    }
}

static async Task SinkFailures()
{
    foreach (var throws in new[] { false, true })
    {
        using var flow = new FlowInstrumentation(new FlowOptions { ServiceName = "sink-failure", EnablePathwayExtensions = true }, new RejectingSink(throws));
        var producer = ProducerProxy.Create();
        await flow.ProduceAsync(producer.Client, "orders", Message());
        var processed = false;
        await flow.ProcessAsync(Record(Message()), "workers", (_, _) => { processed = true; return Task.CompletedTask; });
        Check(producer.Calls.Count == 1 && processed, "A sink failure interrupted business work");
    }
}

static async Task UnsampledCheckpoints()
{
    var activities = 0; long published = 0;
    using var activityListener = new ActivityListener { ShouldListenTo = s => s.Name == FlowInstrumentation.ActivitySourceName,
        Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None,
        ActivityStarted = _ => Interlocked.Increment(ref activities) };
    ActivitySource.AddActivityListener(activityListener);
    using var metrics = new MeterListener { InstrumentPublished = (instrument, listener) =>
    { if (instrument.Meter.Name == FlowInstrumentation.MeterName) listener.EnableMeasurementEvents(instrument); } };
    metrics.SetMeasurementEventCallback<long>((instrument, value, _, _) => { if (instrument.Name == "liveflow.kafka.published") Interlocked.Add(ref published, value); });
    metrics.Start();
    var sink = new Sink(); using var flow = Flow("unsampled", sink); var producer = ProducerProxy.Create();
    await flow.ProduceAsync(producer.Client, "orders", Message());
    Check(activities == 0 && sink.Items.Count == 2 && published == 1, "Activity sampling removed checkpoint/metric observations");
}

static async Task MetricPrivacy()
{
    var tags = new ConcurrentBag<KeyValuePair<string, object?>>();
    using var listener = new MeterListener { InstrumentPublished = (instrument, owner) =>
    { if (instrument.Meter.Name == FlowInstrumentation.MeterName) owner.EnableMeasurementEvents(instrument); } };
    listener.SetMeasurementEventCallback<long>((_, _, values, _) => { foreach (var tag in values) tags.Add(tag); });
    listener.SetMeasurementEventCallback<double>((_, _, values, _) => { foreach (var tag in values) tags.Add(tag); }); listener.Start();
    using var flow = Flow("privacy", metricCap: 1); var producer = ProducerProxy.Create();
    var context = flow.CreateMessageContext("sensitive-logical-id");
    await flow.ProduceAsync(producer.Client, "first-topic", Message(), new PublishOptions { Context = context });
    await flow.ProduceAsync(producer.Client, "second-topic", Message(), new PublishOptions { Context = context, Attempt = 123 });
    Check(tags.Any(t => t.Key == "messaging.destination.name" && Equals(t.Value, "_other")), "Metric identity budget was not enforced");
    Check(tags.All(t => !t.Key.Contains("id", StringComparison.OrdinalIgnoreCase) || t.Key == "cardinality.limited"), "An identifier became a metric label");
    Check(tags.All(t => !t.Key.Contains("attempt") && !t.Key.Contains("offset") && !t.Key.Contains("hash")), "A high-cardinality field became a metric label");
    Check(tags.All(t => !new[] { "sensitive-logical-id", "private-payload", "private-key", "123" }.Contains(t.Value?.ToString())), "Sensitive message data became a metric label");
}

static async Task DeadLetter()
{
    var clock = new TestClock(); var incoming = await Incoming("deadletter-id", clock); clock.Advance(100);
    var sink = new Sink(); using var flow = Flow("dlq-worker", sink, clock); var producer = ProducerProxy.Create();
    var received = flow.ObserveReceived(Record(incoming), "workers");
    await flow.ProcessAsync(received, (_, token) => flow.ProduceAsync(producer.Client, "orders.dlq", Message(), new PublishOptions
        { Classification = PublishClassification.DeadLetter, DeadLetterReason = DeadLetterReason.RetryLimitExceeded, Attempt = 4 }, token));
    var context = Extract(flow, producer.Calls.Single().Message); var initial = Extract(flow, incoming);
    Check(context.LogicalMessageId == initial.LogicalMessageId && context.OriginUnixMs == initial.OriginUnixMs, "DLQ lost the original message identity or timestamp");
    var attempt = One(sink, CheckpointKind.PublishAttempt);
    Check(attempt.Classification == PublishClassification.DeadLetter && attempt.DeadLetterReason == DeadLetterReason.RetryLimitExceeded && attempt.Attempt == 4, "DLQ facts missing");
}

static async Task RetryIdentity()
{
    var sink = new Sink(); using var flow = Flow("retrying", sink); var count = 0;
    var producer = ProducerProxy.Create((topic, message, _) => ++count == 1
        ? Task.FromException<DeliveryResult<string, string>>(new InvalidOperationException("expected"))
        : Task.FromResult(ProducerProxy.Delivered(topic, message)));
    var context = flow.CreateMessageContext("retry-id");
    try { await flow.ProduceAsync(producer.Client, "orders", Message(), new PublishOptions { Context = context, Attempt = 1 }); } catch (InvalidOperationException) { }
    await flow.ProduceAsync(producer.Client, "orders", Message(), new PublishOptions { Context = context, Classification = PublishClassification.Retry, Attempt = 2 });
    var sent = producer.Calls.Select(c => Extract(flow, c.Message)).ToArray();
    Check(sent.Length == 2 && sent.All(c => c.LogicalMessageId == context.LogicalMessageId && c.OriginUnixMs == context.OriginUnixMs), "Retry replaced its origin");
    Check(sink.Items.Count(p => p.Kind == CheckpointKind.PublishFailed) == 1 && sink.Items.Count(p => p.Kind == CheckpointKind.PublishDelivered) == 1, "Retry outcome facts incorrect");
}

static async Task OutboxRoundTrip()
{
    var clock = new TestClock(); var sink = new Sink(); using var source = Flow("outbox-source", sink, clock);
    var origin = source.CreateMessageContext("outbox-id"); var outbox = source.CaptureOutboxContext(origin);
    Check(sink.Items.IsEmpty, "Capturing outbox metadata asserted persistence");
    clock.Advance(20); source.RecordOutboxPersisted(outbox);
    Check(One(sink, CheckpointKind.OutboxPersisted).OutboxId == outbox.OutboxId, "Explicit persistence observation missing");
    var serialized = outbox.Serialized; clock.Advance(4980);
    var dispatchSink = new Sink(); using var dispatcher = Flow("outbox-dispatcher", dispatchSink, clock);
    Check(dispatcher.TryRestoreOutboxContext(serialized, out var restored) && restored is not null, "Outbox context did not survive persistence/restart");
    var producer = ProducerProxy.Create(); await dispatcher.PublishOutboxAsync(producer.Client, "orders", Message(), restored!);
    Check(One(dispatchSink, CheckpointKind.OutboxDispatched).OutboxWaitMs == 5000, "Outbox wait did not start at capture/enqueue time");
    var propagated = Extract(dispatcher, producer.Calls.Single().Message);
    Check(propagated.LogicalMessageId == origin.LogicalMessageId && propagated.OriginUnixMs == origin.OriginUnixMs, "Outbox publication lost origin");
    clock.Advance(37); var receiveSink = new Sink(); using var consumer = Flow("outbox-consumer", receiveSink, clock);
    consumer.ObserveReceived(Record(producer.Calls.Single().Message), "workers");
    var receive = One(receiveSink, CheckpointKind.Receive);
    Check(receive.EdgeLatencyMs == 37 && receive.PathwayLatencyMs == 5037, "Outbox wait was conflated with Kafka transit or lost from origin latency");
}

static async Task OutboxNoFalsePersistence()
{
    var sink = new Sink(); using var flow = Flow("outbox-uncommitted", sink); var outbox = flow.CaptureOutboxContext();
    Check(sink.Items.IsEmpty, "Capture must be metadata only");
    await flow.PublishOutboxAsync(ProducerProxy.Create().Client, "orders", Message(), outbox);
    Check(!sink.Items.Any(p => p.Kind == CheckpointKind.OutboxPersisted), "Dispatcher guessed transaction success");
}

static Task MalformedOutbox()
{
    var clock = new TestClock(); using var flow = Flow("outbox-decode", clock: clock);
    foreach (var text in new[] { "{", "null", new string('x', FlowInstrumentation.MaxOutboxBytes + 1) })
        Check(!flow.TryRestoreOutboxContext(text, out _), "Invalid outbox metadata was accepted");
    var json = JsonNode.Parse(flow.CaptureOutboxContext().Serialized)!;
    json["enqueuedAt"] = clock.GetUtcNow().ToUnixTimeMilliseconds() + 61_000;
    Check(!flow.TryRestoreOutboxContext(json.ToJsonString(), out _), "Future outbox enqueue timestamp was accepted");
    return Task.CompletedTask;
}

static async Task ClockSkew()
{
    var clock = new TestClock(); var incoming = await Incoming(clock: clock); clock.ShiftWall(-5000);
    var sink = new Sink(); using var flow = Flow("skewed-consumer", sink, clock);
    var received = flow.ObserveReceived(Record(incoming), "workers"); var receive = One(sink, CheckpointKind.Receive);
    Check(received.ContextStatus == ContextReadStatus.Valid && receive.ClockSkewDetected && receive.EdgeLatencyMs is null && receive.PathwayLatencyMs is null, "Negative remote latency must be flagged and omitted");
    await flow.ProcessAsync(received, (_, _) => { clock.Advance(10); return Task.CompletedTask; });
    Check(One(sink, CheckpointKind.ProcessingCompleted).ProcessingDurationMs == 10, "Clock skew damaged monotonic processing duration");
}

static async Task HandlerFailures()
{
    foreach (var cancelled in new[] { false, true })
    {
        var sink = new Sink(); using var flow = Flow("handler-outcomes", sink);
        var expected = cancelled ? (Exception)new OperationCanceledException() : new InvalidOperationException("private-error");
        try { await flow.ProcessAsync(Record(Message()), "workers", (_, _) => Task.FromException(expected)); throw new Exception("Expected handler failure"); }
        catch (Exception actual) when (ReferenceEquals(actual, expected)) { }
        Check(!sink.Items.Any(p => p.Kind == CheckpointKind.ProcessingCompleted), "Failed handler was marked complete");
        Check(sink.Items.Any(p => p.Kind == (cancelled ? CheckpointKind.ProcessingCancelled : CheckpointKind.ProcessingFailed)), "Handler outcome not distinguished");
        Check(flow.CurrentContext is null, "Failure leaked processing context");
    }
}

static async Task PublishCancellation()
{
    var sink = new Sink(); using var flow = Flow("cancelled-producer", sink);
    var producer = ProducerProxy.Create((_, _, _) => Task.FromCanceled<DeliveryResult<string, string>>(new CancellationToken(true)));
    try { await flow.ProduceAsync(producer.Client, "orders", Message()); } catch (OperationCanceledException) { }
    Check(sink.Items.Any(p => p.Kind == CheckpointKind.PublishCancelled) && !sink.Items.Any(p => p.Kind is CheckpointKind.PublishDelivered or CheckpointKind.PublishFailed), "Cancellation was treated as a definite delivery failure/success");
}

static async Task TracePropagation()
{
    using var listener = new ActivityListener { ShouldListenTo = s => s.Name == FlowInstrumentation.ActivitySourceName,
        Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded };
    ActivitySource.AddActivityListener(listener);
    using var producerFlow = Flow("trace-source"); var producer = ProducerProxy.Create();
    await producerFlow.ProduceAsync(producer.Client, "orders", Message());
    var message = producer.Calls.Single().Message;
    Check(message.Headers.Any(h => h.Key == "traceparent") && message.Headers.Any(h => h.Key == FlowInstrumentation.PropagationHeader), "Trace and pathway headers were not both present");
    using var consumer = Flow("trace-consumer");
    await consumer.ProcessAsync(Record(message), "workers", (_, _) =>
    {
        var remoteParent = Encoding.UTF8.GetString(message.Headers.Single(h => h.Key == "traceparent").GetValueBytes());
        Check(ActivityContext.TryParse(remoteParent, null, out var context) && Activity.Current?.ParentSpanId == context.SpanId, "Processing Activity did not use the propagated remote parent");
        Check(Activity.Current?.GetTagItem("messaging.operation.type")?.ToString() == "process", "Processing spans must identify process rather than masquerade as wire-level receives");
        return Task.CompletedTask;
    });
}

static async Task NoAmbientLeak()
{
    using var flow = Flow("no-leak");
    await flow.ProcessAsync(Record(await Incoming("previous-message")), "workers", (_, _) => Task.CompletedTask);
    var producer = ProducerProxy.Create(); await flow.ProduceAsync(producer.Client, "unrelated", Message());
    Check(Extract(flow, producer.Calls.Single().Message).LogicalMessageId != "previous-message", "An unrelated publish reused a completed handler's message context");
}

static async Task InvalidArguments()
{
    using var flow = Flow("validation"); var producer = ProducerProxy.Create();
    try { flow.CreateMessageContext(new string('x', 129)); throw new Exception("Expected ID validation"); } catch (ArgumentException) { }
    try { await flow.ProduceAsync(producer.Client, "orders", Message(), new PublishOptions { Attempt = -1 }); throw new Exception("Expected attempt validation"); } catch (ArgumentOutOfRangeException) { }
    Check(producer.Calls.IsEmpty, "Invalid caller options reached Kafka");
}

static async Task BoundedBuffer()
{
    var sink = new BufferedCheckpointSink(capacity: 1);
    long dropped = 0;
    using var listener = new MeterListener { InstrumentPublished = (instrument, owner) =>
    { if (instrument.Meter.Name == FlowInstrumentation.MeterName) owner.EnableMeasurementEvents(instrument); } };
    listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
    { if (instrument.Name == "liveflow.checkpoints.dropped") Interlocked.Add(ref dropped, value); });
    listener.Start();
    using var flow = new FlowInstrumentation(new FlowOptions { ServiceName = "buffered", EnablePathwayExtensions = true }, sink);
    var producer = ProducerProxy.Create();
    await flow.ProduceAsync(producer.Client, "orders", Message());
    Check(dropped == 1 && producer.Calls.Count == 1, "Full buffer did not report the dropped confirmation or blocked Kafka");
    sink.Complete();
    var retained = new List<FlowCheckpoint>();
    await foreach (var checkpoint in sink.ReadAllAsync()) retained.Add(checkpoint);
    Check(retained.Count == 1 && retained[0].Kind == CheckpointKind.PublishAttempt, "Bounded buffer failed to drain its retained checkpoint");
}

static async Task RequiredWireMembers()
{
    using var flow = Flow("required-wire");
    var producer = ProducerProxy.Create();
    await flow.ProduceAsync(producer.Client, "orders", Message());
    var encoded = producer.Calls.Single().Message.Headers.Single(h => h.Key == FlowInstrumentation.PropagationHeader).GetValueBytes();
    var wire = JsonNode.Parse(encoded)!.AsObject();
    foreach (var name in wire.Select(property => property.Key).ToArray())
    {
        var missing = wire.DeepClone().AsObject(); missing.Remove(name);
        var headers = new Headers { { FlowInstrumentation.PropagationHeader, Encoding.UTF8.GetBytes(missing.ToJsonString()) } };
        Check(flow.TryExtractContext(headers, out var context) == ContextReadStatus.Malformed && context is null,
            $"Missing pathway member {name} was trusted");
    }
    var outbox = JsonNode.Parse(flow.CaptureOutboxContext().Serialized)!.AsObject();
    foreach (var name in outbox.Select(property => property.Key).Where(name => name != "context").ToArray())
    {
        var missing = outbox.DeepClone().AsObject(); missing.Remove(name);
        Check(!flow.TryRestoreOutboxContext(missing.ToJsonString(), out _), $"Missing outbox member {name} was trusted");
    }
    foreach (var name in outbox["context"]!.AsObject().Select(property => property.Key).ToArray())
    {
        var missing = outbox.DeepClone().AsObject(); missing["context"]!.AsObject().Remove(name);
        Check(!flow.TryRestoreOutboxContext(missing.ToJsonString(), out _), $"Missing nested outbox member {name} was trusted");
    }
}

static async Task ActivityListenerFailures()
{
    foreach (var extensions in new[] { false, true })
    foreach (var failure in new[] { "sample", "started", "stopped" })
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == FlowInstrumentation.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => failure == "sample"
                ? throw new InvalidOperationException("listener sample failed") : ActivitySamplingResult.AllData,
            ActivityStarted = _ => { if (failure == "started") throw new InvalidOperationException("listener start failed"); },
            ActivityStopped = _ => { if (failure == "stopped") throw new InvalidOperationException("listener stop failed"); }
        };
        ActivitySource.AddActivityListener(listener);
        using var parent = new Activity("application-parent").Start();
        var sink = new Sink(); using var flow = new FlowInstrumentation(new FlowOptions
            { ServiceName = "listener-failure", EnablePathwayExtensions = extensions }, sink);
        var producer = ProducerProxy.Create();
        var result = await flow.ProduceAsync(producer.Client, "orders", Message());
        Check(result.Offset.Value == 7 && producer.Calls.Count == 1 && ReferenceEquals(Activity.Current, parent),
            $"{failure} listener prevented publication or leaked Activity.Current");
        if (failure != "stopped")
            Check(Encoding.UTF8.GetString(producer.Calls.Single().Message.Headers.Single(h => h.Key == "traceparent").GetValueBytes()) == parent.Id,
                $"{failure} listener left a partial Activity in outgoing trace propagation");
        var handled = false;
        await flow.ProcessAsync(Record(Message()), "workers", (_, _) =>
        {
            if (failure != "stopped") Check(ReferenceEquals(Activity.Current, parent), $"{failure} listener left a partial Activity in the handler");
            handled = true; return Task.CompletedTask;
        });
        Check(handled && ReferenceEquals(Activity.Current, parent) && flow.CurrentContext is null,
            $"{failure} listener prevented processing or leaked ambient context");
        var expected = new InvalidOperationException("original handler failure");
        try
        {
            await flow.ProcessAsync(Record(Message()), "workers", (_, _) => Task.FromException(expected));
            throw new Exception("Expected original handler failure");
        }
        catch (InvalidOperationException actual) { Check(ReferenceEquals(actual, expected), $"{failure} listener replaced handler exception"); }
        var kafkaFailure = new InvalidOperationException("original Kafka failure");
        var failingProducer = ProducerProxy.Create((_, _, _) => Task.FromException<DeliveryResult<string, string>>(kafkaFailure));
        try
        {
            await flow.ProduceAsync(failingProducer.Client, "orders", Message());
            throw new Exception("Expected original Kafka failure");
        }
        catch (InvalidOperationException actual) { Check(ReferenceEquals(actual, kafkaFailure), $"{failure} listener replaced Kafka exception"); }
        Check(ReferenceEquals(Activity.Current, parent) && flow.CurrentContext is null, $"{failure} listener failure leaked context");
        Check(!extensions ? sink.Items.IsEmpty : sink.Items.Count(p => p.Kind == CheckpointKind.PublishDelivered) == 1
            && sink.Items.Count(p => p.Kind == CheckpointKind.PublishFailed) == 1
            && sink.Items.Count(p => p.Kind == CheckpointKind.ProcessingCompleted) == 1
            && sink.Items.Count(p => p.Kind == CheckpointKind.ProcessingFailed) == 1,
            $"{failure} listener corrupted checkpoint outcomes");
    }
}

static ActivityListener TraceListener(ConcurrentQueue<Activity> finished, ActivitySamplingResult sampling = ActivitySamplingResult.AllDataAndRecorded)
{
    var listener = new ActivityListener
    {
        ShouldListenTo = source => source.Name == FlowInstrumentation.ActivitySourceName,
        Sample = (ref ActivityCreationOptions<ActivityContext> _) => sampling,
        ActivityStopped = finished.Enqueue
    };
    ActivitySource.AddActivityListener(listener);
    return listener;
}

static ActivityContext TraceHeader(Message<string, string> message)
{
    var text = Encoding.UTF8.GetString(message.Headers.Single(header => header.Key == "traceparent").GetValueBytes());
    Check(ActivityContext.TryParse(text, null, true, out var context), "Expected standard W3C traceparent");
    return context;
}

static async Task DefaultOpenTelemetry()
{
    var spans = new ConcurrentQueue<Activity>(); using var traces = TraceListener(spans);
    var measurements = new ConcurrentQueue<(string Instrument, object Value, KeyValuePair<string, object?>[] Tags)>();
    using var metrics = new MeterListener { InstrumentPublished = (instrument, listener) =>
    { if (instrument.Meter.Name == FlowInstrumentation.MeterName) listener.EnableMeasurementEvents(instrument); } };
    metrics.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Enqueue((instrument.Name, value, tags.ToArray())));
    metrics.SetMeasurementEventCallback<double>((instrument, value, tags, _) => measurements.Enqueue((instrument.Name, value, tags.ToArray())));
    metrics.Start();
    var sink = new Sink(); using var flow = new FlowInstrumentation(sink: sink);
    var producer = ProducerProxy.Create(); var original = Message(); original.Headers = new Headers { { "application", [1] } };
    await flow.ProduceAsync(producer.Client, "orders", original);
    var sent = producer.Calls.Single().Message;
    await flow.ProcessAsync(Record(sent), "workers", token => flow.ProduceAsync(producer.Client, "processed", Message(), cancellationToken: token));
    Check(sink.Items.IsEmpty, "Default path wrote proprietary checkpoints");
    Check(producer.Calls.All(call => call.Message.Headers.All(header => header.Key != FlowInstrumentation.PropagationHeader)), "Default path emitted a proprietary header");
    Check(original.Headers.Count == 1 && original.Headers.Single().Key == "application", "Default propagation mutated caller headers");
    Check(spans.Count == 3 && spans.Count(span => span.Kind == ActivityKind.Producer) == 2
        && spans.Count(span => span.Kind == ActivityKind.Consumer) == 1, "Standard send/process spans missing");
    Check(spans.All(span => Equals(span.GetTagItem("messaging.system"), "kafka")
        && span.GetTagItem("messaging.operation.name") is not null && span.GetTagItem("messaging.operation.type") is not null), "Standard messaging attributes missing");
    Check(spans.All(span => !span.TagObjects.Any(tag => tag.Key.StartsWith("dsm.", StringComparison.Ordinal))), "Default normal operations unexpectedly added custom attributes");
    var process = spans.Single(span => span.Kind == ActivityKind.Consumer);
    Check(Equals(process.GetTagItem("messaging.destination.partition.id"), "2")
        && Equals(process.GetTagItem("messaging.kafka.offset"), 7L), "Standard Kafka coordinates missing");
    Check(measurements.Any(item => item.Instrument == "messaging.client.sent.messages")
        && measurements.Any(item => item.Instrument == "messaging.client.consumed.messages")
        && measurements.Any(item => item.Instrument == "messaging.client.operation.duration")
        && measurements.Any(item => item.Instrument == "messaging.process.duration"), "Standard Meter measurements missing");
    Check(measurements.All(item => !item.Instrument.StartsWith("liveflow.", StringComparison.Ordinal)), "Default path measured private checkpoint metrics");
    Check(measurements.SelectMany(item => item.Tags).All(tag => !tag.Key.Contains("offset") && !tag.Key.Contains("id") && !tag.Key.Contains("attempt")), "High-cardinality data leaked into standard metrics");
}

static async Task StandardCausality()
{
    var spans = new ConcurrentQueue<Activity>(); using var traces = TraceListener(spans);
    using var flow = new FlowInstrumentation(); var producer = ProducerProxy.Create();
    await flow.ProduceAsync(producer.Client, "orders", Message());
    var input = producer.Calls.Single().Message; var remote = TraceHeader(input);
    await flow.ProcessAsync(Record(input), "workers", async token =>
    {
        await flow.ProduceAsync(producer.Client, "left", Message(), cancellationToken: token);
        await flow.ProduceAsync(producer.Client, "right", Message(), cancellationToken: token);
    });
    var process = spans.Single(span => span.Kind == ActivityKind.Consumer);
    Check(process.TraceId == remote.TraceId && process.ParentSpanId == remote.SpanId
        && process.Links.Single().Context.SpanId == remote.SpanId, "Single-message process did not parent/link to the producer");
    Check(spans.Where(span => span.DisplayName is "send left" or "send right").All(span => span.ParentSpanId == process.SpanId), "Output sends did not remain siblings under processing");
    using var job = new Activity("application-job").SetIdFormat(ActivityIdFormat.W3C).Start();
    await flow.ProcessAsync(Record(input), "workers", _ => Task.CompletedTask);
    var nested = spans.Last(span => span.Kind == ActivityKind.Consumer);
    Check(nested.TraceId == job.TraceId && nested.ParentSpanId == job.SpanId
        && nested.Links.Single().Context.TraceId == remote.TraceId && nested.Links.Single().Context.SpanId == remote.SpanId,
        "An active application span lost parentage or the cross-trace producer link");
}

static async Task UnsampledStandardPropagation()
{
    var spans = new ConcurrentQueue<Activity>(); using var traces = TraceListener(spans, ActivitySamplingResult.None);
    var originalBaggage = Baggage.Current;
    Baggage.SetBaggage("caller", "before");
    try
    {
        var input = Message(); input.Headers = new Headers
        {
            { "traceparent", Encoding.UTF8.GetBytes("00-0123456789abcdef0123456789abcdef-0123456789abcdef-00") },
            { "tracestate", Encoding.UTF8.GetBytes("vendor=opaque") },
            { "baggage", Encoding.UTF8.GetBytes("tenant=example") }
        };
        using var flow = new FlowInstrumentation(); var producer = ProducerProxy.Create();
        await flow.ProcessAsync(Record(input), "workers", async token =>
        {
            Check(Baggage.GetBaggage("tenant") == "example" && Baggage.GetBaggage("caller") is null, "Incoming baggage was not isolated");
            await Task.Yield();
            await flow.ProduceAsync(producer.Client, "output", Message(), cancellationToken: token);
        });
        var sent = producer.Calls.Single().Message;
        Check(TraceHeader(sent).TraceId == TraceHeader(input).TraceId && TraceHeader(sent).SpanId == TraceHeader(input).SpanId,
            "Sampling None cut standard propagation");
        Check(Encoding.UTF8.GetString(sent.Headers.Single(header => header.Key == "tracestate").GetValueBytes()) == "vendor=opaque"
            && sent.Headers.Any(header => header.Key == "baggage"), "Standard tracestate/baggage were lost");
        Check(spans.IsEmpty && Baggage.GetBaggage("caller") == "before" && Baggage.GetBaggage("tenant") is null, "Sampling/baggage state leaked");
    }
    finally { Baggage.Current = originalBaggage; }
}

static async Task StandardOutboxPropagation()
{
    var spans = new ConcurrentQueue<Activity>(); using var traces = TraceListener(spans);
    var savedBaggage = Baggage.Current;
    string serialized; ActivityContext captured;
    try
    {
        using var source = new FlowInstrumentation();
        using (var parent = new Activity("business-transaction").SetIdFormat(ActivityIdFormat.W3C).Start())
        {
            parent.TraceStateString = "vendor=opaque";
            Baggage.SetBaggage("tenant", "example");
            var outbox = source.CaptureOutboxContext(); captured = parent.Context; serialized = outbox.Serialized;
            var wire = JsonNode.Parse(serialized)!.AsObject();
            Check(!wire.ContainsKey("context") && wire["headers"]!["traceparent"] is not null,
                "Default outbox required proprietary lineage instead of standard W3C fields");
        }
        Baggage.Current = default;
        using var dispatcher = new FlowInstrumentation();
        Check(dispatcher.TryRestoreOutboxContext(serialized, out var restored) && restored is not null, "Standard persisted outbox did not restore");
        var producer = ProducerProxy.Create();
        using (var job = new Activity("dispatcher-job").SetIdFormat(ActivityIdFormat.W3C).Start())
        {
            await dispatcher.PublishOutboxAsync(producer.Client, "orders", Message(), restored!);
            var send = spans.Last();
            Check(send.TraceId == captured.TraceId && send.ParentSpanId == captured.SpanId
                && send.Links.Single().Context.SpanId == job.SpanId, "Dispatcher lost persisted parent or ambient job link");
        }
        var sent = producer.Calls.Single().Message;
        Check(TraceHeader(sent).TraceId == captured.TraceId && sent.Headers.All(header => header.Key != FlowInstrumentation.PropagationHeader), "Standard outbox dispatch emitted private wire metadata or broke trace continuity");
        Check(sent.Headers.Any(header => header.Key == "tracestate") && sent.Headers.Any(header => header.Key == "baggage"), "Outbox lost tracestate/baggage");
        await dispatcher.ProcessAsync(Record(sent), "workers", _ => Task.CompletedTask);
        Check(spans.Last().ParentSpanId == TraceHeader(sent).SpanId, "Dispatched message could not continue through normal processing");
        var malformed = JsonNode.Parse(serialized)!; malformed["headers"]!["traceparent"] = "invalid";
        Check(!dispatcher.TryRestoreOutboxContext(malformed.ToJsonString(), out _), "Invalid persisted W3C context was accepted");
    }
    finally { Baggage.Current = savedBaggage; }
}

static async Task OptionalPathwayExtensions()
{
    var spans = new ConcurrentQueue<Activity>(); using var traces = TraceListener(spans);
    var defaultSink = new Sink(); using var standard = new FlowInstrumentation(sink: defaultSink);
    var extendedSink = new Sink(); using var extended = new FlowInstrumentation(new FlowOptions { EnablePathwayExtensions = true }, extendedSink);
    var producer = ProducerProxy.Create();
    await standard.ProduceAsync(producer.Client, "plain", Message());
    await extended.ProduceAsync(producer.Client, "extended", Message(), new PublishOptions
        { Context = extended.CreateMessageContext("logical-example"), Classification = PublishClassification.DeadLetter,
            DeadLetterReason = DeadLetterReason.ValidationFailed, Attempt = 2 });
    Check(defaultSink.Items.IsEmpty && extendedSink.Items.Count == 2, "Checkpoint delivery did not obey explicit opt-in");
    Check(producer.Calls.First().Message.Headers.All(header => header.Key != FlowInstrumentation.PropagationHeader)
        && producer.Calls.Last().Message.Headers.Any(header => header.Key == FlowInstrumentation.PropagationHeader)
        && producer.Calls.All(call => call.Message.Headers.Any(header => header.Key == "traceparent")), "Custom extension replaced or became mandatory for standard propagation");
    Check(Equals(spans.Last().GetTagItem("dsm.publish.classification"), "DeadLetter")
        && Equals(spans.Last().GetTagItem("dsm.deadletter.reason"), "ValidationFailed"), "Explicit DLQ enrichment missing from standard span");
    var privateOnly = producer.Calls.Last().Message;
    var received = standard.ObserveReceived(Record(privateOnly), "workers");
    Check(received.ContextStatus == ContextReadStatus.Missing && received.TraceContext.TraceId != default, "Default required/consumed custom lineage");
}

static async Task ActivityOwnership()
{
    var spans = new ConcurrentQueue<Activity>(); using var traces = TraceListener(spans);
    using var flow = new FlowInstrumentation(new FlowOptions { CreateProducerActivity = false, CreateProcessingActivity = false });
    var producer = ProducerProxy.Create();
    using var application = new Activity("already-instrumented").SetIdFormat(ActivityIdFormat.W3C).Start();
    await flow.ProduceAsync(producer.Client, "orders", Message());
    await flow.ProcessAsync(Record(producer.Calls.Single().Message), "workers", _ => Task.CompletedTask);
    Check(spans.IsEmpty && TraceHeader(producer.Calls.Single().Message).SpanId == application.SpanId,
        "Disabled boundaries emitted duplicate Activities or lost existing context");
}

static async Task PropagatorFailures()
{
    using var flow = new FlowInstrumentation(new FlowOptions { Propagator = new ThrowingPropagator() });
    var producer = ProducerProxy.Create(); var handled = false;
    await flow.ProduceAsync(producer.Client, "orders", Message());
    await flow.ProcessAsync(Record(Message()), "workers", _ => { handled = true; return Task.CompletedTask; });
    Check(handled && producer.Calls.Count == 1, "Custom propagator failure interrupted business work");
}

static async Task StandardMetricOutcomes()
{
    var counts = new ConcurrentQueue<(long Value, KeyValuePair<string, object?>[] Tags)>();
    using var listener = new MeterListener { InstrumentPublished = (instrument, owner) =>
    { if (instrument.Meter.Name == FlowInstrumentation.MeterName) owner.EnableMeasurementEvents(instrument); } };
    listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
    { if (instrument.Name == "messaging.client.sent.messages") counts.Enqueue((value, tags.ToArray())); });
    listener.Start();
    using var flow = new FlowInstrumentation(new FlowOptions { MaxMetricIdentities = 2 });
    var producer = ProducerProxy.Create();
    await flow.ProduceAsync(producer.Client, "confirmed", Message());
    var failed = ProducerProxy.Create((_, _, _) => Task.FromException<DeliveryResult<string, string>>(new InvalidOperationException("example")));
    try { await flow.ProduceAsync(failed.Client, "failed", Message()); } catch (InvalidOperationException) { }
    var cancelled = ProducerProxy.Create((_, _, _) => Task.FromCanceled<DeliveryResult<string, string>>(new CancellationToken(true)));
    try { await flow.ProduceAsync(cancelled.Client, "cancelled", Message()); } catch (OperationCanceledException) { }
    var unconfirmed = ProducerProxy.Create((topic, message, _) => Task.FromResult(new DeliveryResult<string, string>
        { Topic = topic, Message = message, Partition = Partition.Any, Offset = Offset.Unset, Status = PersistenceStatus.NotPersisted }));
    await flow.ProduceAsync(unconfirmed.Client, "unconfirmed", Message());
    Check(counts.Sum(item => item.Value) == 4, "Standard attempted-message count was conflated with broker delivery confirmation");
    Check(counts.Count(item => item.Tags.Any(tag => tag.Key == "error.type" && Equals(tag.Value, "error"))) == 1
        && counts.Count(item => item.Tags.Any(tag => tag.Key == "error.type" && Equals(tag.Value, "cancelled"))) == 1,
        "Standard attempt errors/cancellation were lost or fabricated");
    Check(counts.Count(item => item.Tags.Any(tag => tag.Key == "messaging.destination.name" && Equals(tag.Value, "_other"))) == 2,
        "Standard metric destination cardinality budget was bypassed");
}

public sealed class Sink : ICheckpointSink
{
    public ConcurrentQueue<FlowCheckpoint> Items { get; } = new();
    public bool TryWrite(FlowCheckpoint checkpoint) { Items.Enqueue(checkpoint); return true; }
}
public sealed class RejectingSink(bool throws) : ICheckpointSink
{
    public bool TryWrite(FlowCheckpoint checkpoint) => throws ? throw new InvalidOperationException("sink unavailable") : false;
}
public sealed class ThrowingPropagator : TextMapPropagator
{
    public override ISet<string> Fields { get; } = new HashSet<string>();
    public override PropagationContext Extract<T>(PropagationContext context, T carrier, Func<T, string, IEnumerable<string>?> getter)
        => throw new InvalidOperationException("propagator unavailable");
    public override void Inject<T>(PropagationContext context, T carrier, Action<T, string, string> setter)
        => throw new InvalidOperationException("propagator unavailable");
}
public sealed class TestClock : TimeProvider
{
    private long _wall = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();
    private long _ticks;
    public override long TimestampFrequency => 1000;
    public override DateTimeOffset GetUtcNow() => DateTimeOffset.FromUnixTimeMilliseconds(Interlocked.Read(ref _wall));
    public override long GetTimestamp() => Interlocked.Read(ref _ticks);
    public void Advance(long milliseconds) { Interlocked.Add(ref _wall, milliseconds); Interlocked.Add(ref _ticks, milliseconds); }
    public void ShiftWall(long milliseconds) => Interlocked.Add(ref _wall, milliseconds);
}
public class ProducerProxy : DispatchProxy
{
    public sealed record Call(string Topic, Message<string, string> Message);
    public ConcurrentQueue<Call> Calls { get; } = new();
    public IProducer<string, string> Client { get; private set; } = null!;
    private Func<string, Message<string, string>, CancellationToken, Task<DeliveryResult<string, string>>> _send = null!;
    public static ProducerProxy Create(Func<string, Message<string, string>, CancellationToken, Task<DeliveryResult<string, string>>>? send = null)
    {
        var client = DispatchProxy.Create<IProducer<string, string>, ProducerProxy>();
        var proxy = (ProducerProxy)client;
        proxy.Client = client; proxy._send = send ?? ((topic, message, _) => Task.FromResult(Delivered(topic, message)));
        return proxy;
    }
    public static DeliveryResult<string, string> Delivered(string topic, Message<string, string> message)
        => new() { Topic = topic, Message = message, Partition = new Partition(2), Offset = new Offset(7), Status = PersistenceStatus.Persisted };
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        if (targetMethod?.Name == "ProduceAsync" && args is { Length: 3 } && args[0] is string topic && args[1] is Message<string, string> message)
        { Calls.Enqueue(new(topic, message)); return _send(topic, message, (CancellationToken)args[2]!); }
        if (targetMethod?.Name == "Dispose") return null;
        throw new NotSupportedException(targetMethod?.Name);
    }
}
