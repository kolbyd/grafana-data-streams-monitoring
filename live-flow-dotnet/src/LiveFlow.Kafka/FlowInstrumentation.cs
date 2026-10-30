using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace LiveFlow.Kafka;

/// <summary>Explicit instrumentation of message boundaries; it never consumes or commits Kafka records.</summary>
public sealed class FlowInstrumentation : IDisposable
{
    public const string ActivitySourceName = "LiveFlow.DataStreams";
    public const string MeterName = "LiveFlow.DataStreams";
    public const string PropagationHeader = "liveflow-pathway-v1";
    public const int MaxContextBytes = 1024;
    public const int MaxOutboxBytes = 16384;

    private readonly FlowOptions _options;
    private readonly TimeProvider _clock;
    private readonly ICheckpointSink? _sink;
    private readonly AsyncLocal<FlowContext?> _current = new();
    private readonly AsyncLocal<PropagationContext?> _propagation = new();
    private readonly TextMapPropagator _propagator;
    private readonly ActivitySource _activities = new(ActivitySourceName, "0.1.0-preview.1");
    private readonly Meter _meter = new(MeterName, "0.1.0-preview.1");
    private readonly Counter<long> _checkpoints, _published, _received, _dropped, _invalid, _skew;
    private readonly Histogram<double> _edgeLatency, _pathwayLatency, _processing, _outboxWait;
    private readonly Counter<long> _sentMessages, _consumedMessages;
    private readonly Histogram<double> _operationDuration, _processDuration;
    private readonly ConcurrentDictionary<(string Topic, string Group), byte> _metricIdentities = new();
    private readonly object _metricIdentityLock = new();
    private readonly long _futureSkewMs;

    public FlowInstrumentation(FlowOptions? options = null, ICheckpointSink? sink = null, TimeProvider? timeProvider = null)
    {
        options ??= new FlowOptions();
        ValidateLabel(options.ServiceName, 128, nameof(options.ServiceName));
        ValidateLabel(options.Environment, 64, nameof(options.Environment));
        ValidateLabel(options.Cluster, 128, nameof(options.Cluster));
        if (options.MaxMetricIdentities is < 1 or > 4096) throw new ArgumentOutOfRangeException(nameof(options.MaxMetricIdentities));
        if (options.AllowedFutureClockSkew < TimeSpan.Zero || options.AllowedFutureClockSkew > TimeSpan.FromMinutes(5))
            throw new ArgumentOutOfRangeException(nameof(options.AllowedFutureClockSkew));
        _options = options; _sink = sink; _clock = timeProvider ?? TimeProvider.System;
        _propagator = options.Propagator ?? TracePropagation.W3C;
        _futureSkewMs = (long)options.AllowedFutureClockSkew.TotalMilliseconds;
        _checkpoints = _meter.CreateCounter<long>("liveflow.checkpoints", "{checkpoint}");
        _published = _meter.CreateCounter<long>("liveflow.kafka.published", "{message}", "Broker-confirmed successful deliveries with usable offsets.");
        _received = _meter.CreateCounter<long>("liveflow.kafka.received", "{message}", "Records explicitly observed by the instrumentation.");
        _dropped = _meter.CreateCounter<long>("liveflow.checkpoints.dropped", "{checkpoint}");
        _invalid = _meter.CreateCounter<long>("liveflow.context.rejected", "{context}");
        _skew = _meter.CreateCounter<long>("liveflow.clock_skew", "{checkpoint}");
        _edgeLatency = _meter.CreateHistogram<double>("liveflow.edge.latency", "ms");
        _pathwayLatency = _meter.CreateHistogram<double>("liveflow.pathway.latency", "ms");
        _processing = _meter.CreateHistogram<double>("liveflow.processing.duration", "ms");
        _outboxWait = _meter.CreateHistogram<double>("liveflow.outbox.wait", "ms");
        _sentMessages = _meter.CreateCounter<long>("messaging.client.sent.messages", "{message}");
        _consumedMessages = _meter.CreateCounter<long>("messaging.client.consumed.messages", "{message}");
        _operationDuration = _meter.CreateHistogram<double>("messaging.client.operation.duration", "s");
        _processDuration = _meter.CreateHistogram<double>("messaging.process.duration", "s");
    }

    public FlowContext? CurrentContext => _current.Value;
    private long Now => _clock.GetUtcNow().ToUnixTimeMilliseconds();

    /// <summary>Keep this immutable context to preserve identity across root-level retries. Emits no checkpoint.</summary>
    public FlowContext CreateMessageContext(string? logicalMessageId = null)
    {
        logicalMessageId ??= Guid.NewGuid().ToString("N");
        ValidateLabel(logicalMessageId, 128, nameof(logicalMessageId));
        var now = Now;
        return new(logicalMessageId, now, true, now, Guid.NewGuid().ToString("N"), "0000000000000000", CheckpointKind.Origin);
    }

    public ContextReadStatus TryExtractContext(Headers? headers, out FlowContext? context)
        => ContextCodec.Decode(headers, Now, _futureSkewMs, out context);

    /// <summary>Call immediately after Consume returns. Malformed lineage is discarded without losing the record.</summary>
    public ReceivedMessage<TKey, TValue> ObserveReceived<TKey, TValue>(ConsumeResult<TKey, TValue> record, string? consumerGroup = null)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (record.IsPartitionEOF || record.Message is null) throw new ArgumentException("Expected a message record, not a partition EOF.", nameof(record));
        ValidateTopic(record.Topic);
        ValidateGroup(consumerGroup);
        var status = ContextReadStatus.Missing;
        FlowContext? parent = null;
        if (_options.EnablePathwayExtensions) status = TryExtractContext(record.Message.Headers, out parent);
        if (parent is null)
        {
            var now = Now;
            parent = new(Guid.NewGuid().ToString("N"), now, false, now, Guid.NewGuid().ToString("N"), "0000000000000000", CheckpointKind.Origin);
        }
        if (status is not ContextReadStatus.Valid and not ContextReadStatus.Missing)
            Safe(() => { var tags = MetricTags(record.Topic, consumerGroup, CheckpointKind.Receive); tags.Add("context.status", status.ToString()); _invalid.Add(1, tags); });
        var step = Step(parent, CheckpointKind.Receive, record.Topic, consumerGroup);
        Emit(step.Checkpoint with { Partition = ValidPartition(record.Partition), Offset = ValidOffset(record.Offset) });
        Safe(() => _consumedMessages.Add(1, StandardMetricTags(record.Topic, consumerGroup, "receive")));
        PropagationContext propagation = default;
        Safe(() => propagation = _propagator.Extract(default, record.Message.Headers, TracePropagation.HeaderValues));
        return new(record, step.Context, consumerGroup, status, propagation);
    }

    /// <summary>Convenience boundary: receipt is observed when this method is called, not at the earlier Consume return.</summary>
    public Task ProcessAsync<TKey, TValue>(ConsumeResult<TKey, TValue> record, string? consumerGroup,
        Func<FlowContext, CancellationToken, Task> handler, CancellationToken cancellationToken = default)
        => ProcessAsync(ObserveReceived(record, consumerGroup), handler, cancellationToken);

    public Task ProcessAsync<TKey, TValue>(ConsumeResult<TKey, TValue> record, string? consumerGroup,
        Func<CancellationToken, Task> handler, CancellationToken cancellationToken = default)
        => ProcessAsync(ObserveReceived(record, consumerGroup), handler, cancellationToken);

    public Task ProcessAsync<TKey, TValue>(ReceivedMessage<TKey, TValue> received,
        Func<CancellationToken, Task> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return ProcessAsync(received, (_, token) => handler(token), cancellationToken);
    }

    /// <summary>Executes a handler with isolated ambient lineage, restoring the caller's context in every outcome.</summary>
    public async Task ProcessAsync<TKey, TValue>(ReceivedMessage<TKey, TValue> received,
        Func<FlowContext, CancellationToken, Task> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(received); ArgumentNullException.ThrowIfNull(handler);
        var start = Step(received.Context, CheckpointKind.ProcessingStarted, received.Record.Topic, received.ConsumerGroup);
        Emit(start.Checkpoint);
        var previous = _current.Value;
        var previousPropagation = _propagation.Value;
        var previousBaggage = Baggage.Current;
        _current.Value = _options.EnablePathwayExtensions ? start.Context : null;
        var timestamp = _clock.GetTimestamp();
        var parent = CurrentTraceContext() ?? received.TraceContext;
        using var activityScope = StartActivity($"process {received.Record.Topic}", ActivityKind.Consumer, parent,
            MessageLinks(received.TraceContext), _options.CreateProcessingActivity,
            ActivityTags(received.Record.Topic, received.ConsumerGroup, "process"));
        var activity = activityScope.Activity;
        Baggage.Current = received.PropagationContext.Baggage;
        _propagation.Value = new PropagationContext(activity?.Context ?? parent, Baggage.Current);
        TagActivity(activity, received.Record.Topic, received.ConsumerGroup, "process");
        TagCoordinates(activity, ValidPartition(received.Record.Partition), ValidOffset(received.Record.Offset));
        string? error = null;
        try
        {
            await handler(start.Context, cancellationToken).ConfigureAwait(false);
            var end = Step(start.Context, CheckpointKind.ProcessingCompleted, received.Record.Topic, received.ConsumerGroup);
            Emit(end.Checkpoint with { ProcessingDurationMs = Math.Max(0, _clock.GetElapsedTime(timestamp).TotalMilliseconds) });
        }
        catch (OperationCanceledException)
        {
            error = "cancelled";
            TagError(activity, error);
            var end = Step(start.Context, CheckpointKind.ProcessingCancelled, received.Record.Topic, received.ConsumerGroup);
            Emit(end.Checkpoint with { ProcessingDurationMs = Math.Max(0, _clock.GetElapsedTime(timestamp).TotalMilliseconds) });
            throw;
        }
        catch (Exception)
        {
            error = "error";
            TagError(activity, error);
            var end = Step(start.Context, CheckpointKind.ProcessingFailed, received.Record.Topic, received.ConsumerGroup);
            Emit(end.Checkpoint with { ProcessingDurationMs = Math.Max(0, _clock.GetElapsedTime(timestamp).TotalMilliseconds) });
            throw;
        }
        finally
        {
            Safe(() => _processDuration.Record(_clock.GetElapsedTime(timestamp).TotalSeconds,
                StandardMetricTags(received.Record.Topic, received.ConsumerGroup, "process", error)));
            _current.Value = previous;
            _propagation.Value = previousPropagation;
            Baggage.Current = previousBaggage;
        }
    }

    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync<TKey, TValue>(IProducer<TKey, TValue> producer, string topic,
        Message<TKey, TValue> message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(producer); ArgumentNullException.ThrowIfNull(message);
        ValidateTopic(topic); ValidatePublish(options);
        (FlowContext Context, FlowCheckpoint Checkpoint)? step = null;
        if (_options.EnablePathwayExtensions)
        {
            var parent = options?.Context ?? _current.Value ?? CreateMessageContext();
            step = Step(parent, CheckpointKind.PublishAttempt, topic, null);
            Emit(Classify(step.Value.Checkpoint, options));
        }
        var timestamp = _clock.GetTimestamp();
        var explicitTrace = options?.ParentContext?.ActivityContext;
        var traceParent = explicitTrace is { } captured && TracePropagation.Valid(captured) ? captured
            : CurrentTraceContext() ?? _propagation.Value?.ActivityContext ?? default;
        using var activityScope = StartActivity($"send {topic}", ActivityKind.Producer, traceParent,
            options?.ParentContext is { } explicitParent ? MessageLinks(CurrentTraceContext() ?? default, explicitParent.ActivityContext) : null,
            _options.CreateProducerActivity, ActivityTags(topic, null, "send"));
        var activity = activityScope.Activity;
        TagActivity(activity, topic, null, "send");
        TagPublication(activity, options, step?.Context);
        var outgoing = new Message<TKey, TValue>
        {
            Key = message.Key, Value = message.Value, Timestamp = message.Timestamp,
            Headers = CopyAndInject(message.Headers, step?.Context,
                new PropagationContext(activity?.Context ?? traceParent, options?.ParentContext?.Baggage ?? Baggage.Current))
        };
        string? error = null;
        try
        {
            var result = await producer.ProduceAsync(topic, outgoing, cancellationToken).ConfigureAwait(false);
            var confirmed = result.Status == PersistenceStatus.Persisted && ValidOffset(result.Offset) is not null && ValidPartition(result.Partition) is not null;
            TagCoordinates(activity, ValidPartition(result.Partition), ValidOffset(result.Offset));
            if (step is { } attempt)
            {
                var end = Step(attempt.Context, confirmed ? CheckpointKind.PublishDelivered : CheckpointKind.PublishUnconfirmed, topic, null);
                Emit(Classify(end.Checkpoint, options) with { Partition = ValidPartition(result.Partition), Offset = ValidOffset(result.Offset) });
            }
            return result;
        }
        catch (OperationCanceledException)
        {
            error = "cancelled";
            TagError(activity, error);
            if (step is { } attempt) Emit(Classify(Step(attempt.Context, CheckpointKind.PublishCancelled, topic, null).Checkpoint, options));
            throw;
        }
        catch (Exception)
        {
            error = "error";
            TagError(activity, error);
            if (step is { } attempt) Emit(Classify(Step(attempt.Context, CheckpointKind.PublishFailed, topic, null).Checkpoint, options));
            throw;
        }
        finally
        {
            Safe(() => _sentMessages.Add(1, StandardMetricTags(topic, null, "send", error)));
            Safe(() => _operationDuration.Record(_clock.GetElapsedTime(timestamp).TotalSeconds, StandardMetricTags(topic, null, "send", error)));
        }
        // Do not update _current: output fan-out must share the input parent rather than chain siblings.
    }

    /// <summary>Creates metadata only. Persist Serialized beside the outbox row in the application's own transaction.</summary>
    public OutboxContext CaptureOutboxContext(FlowContext? context = null)
        => new(Guid.NewGuid().ToString("N"), Now, context ?? _current.Value ?? CreateMessageContext(),
            new PropagationContext(CurrentTraceContext() ?? _propagation.Value?.ActivityContext ?? default, Baggage.Current), _options.EnablePathwayExtensions);

    /// <summary>Call only after the application knows its transaction committed. Never called automatically.</summary>
    public void RecordOutboxPersisted(OutboxContext outbox)
    {
        ArgumentNullException.ThrowIfNull(outbox);
        Safe(() => Activity.Current?.AddEvent(new ActivityEvent("outbox.persisted", tags: new ActivityTagsCollection
            { { "dsm.outbox.id", outbox.OutboxId } })));
        Emit(Step(outbox.Context, CheckpointKind.OutboxPersisted, "", null).Checkpoint with
        { OutboxId = outbox.OutboxId, OutboxEnqueuedAtUnixMs = outbox.EnqueuedAtUnixMs });
    }

    public bool TryRestoreOutboxContext(string? serialized, out OutboxContext? context)
        => ContextCodec.TryDecodeOutbox(serialized, Now, _futureSkewMs, _options.EnablePathwayExtensions, out context);

    public Task<DeliveryResult<TKey, TValue>> PublishOutboxAsync<TKey, TValue>(IProducer<TKey, TValue> producer, string topic,
        Message<TKey, TValue> message, OutboxContext outbox, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(outbox); ArgumentNullException.ThrowIfNull(producer); ArgumentNullException.ThrowIfNull(message);
        ValidateTopic(topic); ValidatePublish(options);
        if (options?.Context is not null) throw new ArgumentException("Outbox publication restores the persisted lineage; do not override Context.", nameof(options));
        var step = Step(outbox.Context, CheckpointKind.OutboxDispatched, topic, null);
        var wait = Now - outbox.EnqueuedAtUnixMs;
        Emit(step.Checkpoint with { OutboxId = outbox.OutboxId, OutboxEnqueuedAtUnixMs = outbox.EnqueuedAtUnixMs,
            OutboxWaitMs = wait < 0 ? null : wait, ClockSkewDetected = step.Checkpoint.ClockSkewDetected || wait < 0 });
        return ProduceAsync(producer, topic, message, new PublishOptions { Context = step.Context,
            ParentContext = outbox.PropagationContext,
            Outbox = outbox,
            Classification = options?.Classification ?? PublishClassification.Normal,
            DeadLetterReason = options?.DeadLetterReason, Attempt = options?.Attempt ?? 0 }, cancellationToken);
    }

    private (FlowContext Context, FlowCheckpoint Checkpoint) Step(FlowContext parent, CheckpointKind kind, string topic, string? group)
    {
        var now = Now;
        var id = Guid.NewGuid().ToString("N");
        var hash = PathwayHash(parent.PathwayHash, kind.ToString(), _options.ServiceName, _options.Environment, _options.Cluster, topic, group ?? "");
        var isRoot = parent.Kind == CheckpointKind.Origin;
        var edge = isRoot ? (double?)null : now - parent.CheckpointUnixMs;
        var pathway = parent.OriginKnown ? (double?)(now - parent.OriginUnixMs) : null;
        var skew = edge < 0 || pathway < 0;
        return (new(parent.LogicalMessageId, parent.OriginUnixMs, parent.OriginKnown, now, id, hash, kind), new FlowCheckpoint
        {
            Id = id, ParentCheckpointId = isRoot ? null : parent.CheckpointId, LogicalMessageId = parent.LogicalMessageId,
            PathwayHash = hash, ParentPathwayHash = isRoot ? null : parent.PathwayHash, Kind = kind,
            Service = _options.ServiceName, Environment = _options.Environment, Cluster = _options.Cluster, Topic = topic, ConsumerGroup = group,
            TimestampUnixMs = now, OriginUnixMs = parent.OriginUnixMs, OriginKnown = parent.OriginKnown,
            EdgeLatencyMs = edge < 0 ? null : edge, PathwayLatencyMs = pathway < 0 ? null : pathway, ClockSkewDetected = skew
        });
    }

    private static FlowCheckpoint Classify(FlowCheckpoint checkpoint, PublishOptions? options) => checkpoint with
    {
        Classification = options?.Classification ?? PublishClassification.Normal, Attempt = options?.Attempt ?? 0,
        DeadLetterReason = options?.Classification == PublishClassification.DeadLetter ? options.DeadLetterReason ?? DeadLetterReason.Unspecified : null
    };

    private void Emit(FlowCheckpoint checkpoint)
    {
        if (!_options.EnablePathwayExtensions) return;
        Safe(() =>
        {
            var tags = MetricTags(checkpoint.Topic, checkpoint.ConsumerGroup, checkpoint.Kind);
            tags.Add("publish.classification", checkpoint.Classification.ToString());
            if (checkpoint.DeadLetterReason is not null) tags.Add("deadletter.reason", checkpoint.DeadLetterReason.Value.ToString());
            _checkpoints.Add(1, tags);
            if (checkpoint.Kind == CheckpointKind.PublishDelivered) _published.Add(1, tags);
            if (checkpoint.Kind == CheckpointKind.Receive) _received.Add(1, tags);
            if (checkpoint.EdgeLatencyMs is { } edge) _edgeLatency.Record(edge, tags);
            if (checkpoint.PathwayLatencyMs is { } pathway) _pathwayLatency.Record(pathway, tags);
            if (checkpoint.ProcessingDurationMs is { } processing) _processing.Record(processing, tags);
            if (checkpoint.OutboxWaitMs is { } wait) _outboxWait.Record(wait, tags);
            if (checkpoint.ClockSkewDetected) _skew.Add(1, tags);
        });
        if (_sink is null) return;
        bool accepted;
        try { accepted = _sink.TryWrite(checkpoint); } catch { accepted = false; }
        if (!accepted) Safe(() => _dropped.Add(1, MetricTags("", null, checkpoint.Kind)));
    }

    private TagList MetricTags(string topic, string? group, CheckpointKind kind)
    {
        var identity = (topic, group ?? "");
        var limited = false;
        if (!_metricIdentities.ContainsKey(identity))
        {
            lock (_metricIdentityLock)
            {
                if (!_metricIdentities.ContainsKey(identity))
                {
                    if (_metricIdentities.Count >= _options.MaxMetricIdentities) limited = true;
                    else _metricIdentities.TryAdd(identity, 0);
                }
            }
        }
        return new TagList
        {
            { "service.name", _options.ServiceName }, { "deployment.environment.name", _options.Environment },
            { "messaging.kafka.cluster", _options.Cluster }, { "messaging.destination.name", limited ? "_other" : topic },
            { "messaging.consumer.group.name", limited ? "_other" : group ?? "" }, { "checkpoint.kind", kind.ToString() },
            { "cardinality.limited", limited }
        };
    }

    private Headers CopyAndInject(Headers? original, FlowContext? context, PropagationContext propagation)
    {
        var headers = new Headers();
        if (original is not null)
            foreach (var header in original)
                if (header.Key != PropagationHeader)
                    headers.Add(header.Key, header.GetValueBytes()?.ToArray());
        Safe(() =>
        {
            if (TracePropagation.Valid(propagation.ActivityContext))
            { headers.Remove("traceparent"); headers.Remove("tracestate"); }
            _propagator.Inject(propagation, headers, static (carrier, key, value) =>
            {
                if (Encoding.UTF8.GetByteCount(value) > 8192) return;
                carrier.Remove(key);
                carrier.Add(key, Encoding.UTF8.GetBytes(value));
            });
        });
        if (_options.EnablePathwayExtensions && context is not null) Safe(() => headers.Add(PropagationHeader, ContextCodec.Encode(context)));
        return headers;
    }

    private static ActivityContext? CurrentTraceContext()
    {
        var context = Activity.Current?.Context ?? default;
        return TracePropagation.Valid(context) ? context : null;
    }

    private static IEnumerable<ActivityLink>? MessageLinks(ActivityContext context, ActivityContext exclude = default)
        => TracePropagation.Valid(context) && context != exclude ? [new ActivityLink(context)] : null;

    private void TagActivity(Activity? activity, string topic, string? group, string operation)
    {
        activity?.SetTag("messaging.system", "kafka"); activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.operation.type", operation);
        activity?.SetTag("messaging.operation.name", operation);
        if (_options.Cluster != "unknown") activity?.SetTag("messaging.kafka.cluster.id", _options.Cluster);
        if (group is not null) activity?.SetTag("messaging.consumer.group.name", group);
    }

    private ActivityTagsCollection ActivityTags(string topic, string? group, string operation)
    {
        var tags = new ActivityTagsCollection
        {
            { "messaging.system", "kafka" }, { "messaging.destination.name", topic },
            { "messaging.operation.type", operation }, { "messaging.operation.name", operation }
        };
        if (_options.Cluster != "unknown") tags.Add("messaging.kafka.cluster.id", _options.Cluster);
        if (group is not null) tags.Add("messaging.consumer.group.name", group);
        return tags;
    }

    private static void TagCoordinates(Activity? activity, int? partition, long? offset)
    {
        if (partition is { } p) activity?.SetTag("messaging.destination.partition.id", p.ToString(System.Globalization.CultureInfo.InvariantCulture));
        if (offset is { } o) activity?.SetTag("messaging.kafka.offset", o);
    }

    private static void TagError(Activity? activity, string error)
    { activity?.SetTag("error.type", error); activity?.SetStatus(ActivityStatusCode.Error); }

    private void TagPublication(Activity? activity, PublishOptions? options, FlowContext? extensionContext)
    {
        if (options?.Classification is { } classification && classification != PublishClassification.Normal) activity?.SetTag("dsm.publish.classification", classification.ToString());
        if (options?.Attempt is > 0) activity?.SetTag("dsm.retry.attempt", options.Attempt);
        if (options?.Classification == PublishClassification.DeadLetter)
            activity?.SetTag("dsm.deadletter.reason", (options.DeadLetterReason ?? DeadLetterReason.Unspecified).ToString());
        if (options?.Outbox is { } outbox)
        {
            activity?.SetTag("dsm.outbox.id", outbox.OutboxId);
            var wait = Now - outbox.EnqueuedAtUnixMs;
            if (wait >= 0) activity?.SetTag("dsm.outbox.wait_ms", wait);
        }
        if (_options.EnablePathwayExtensions && extensionContext is { } context)
        {
            activity?.SetTag("dsm.logical_message_id", context.LogicalMessageId);
            activity?.SetTag("dsm.pathway_hash", context.PathwayHash);
            if (context.OriginKnown) activity?.SetTag("dsm.origin_unix_ms", context.OriginUnixMs);
        }
    }

    private TagList StandardMetricTags(string topic, string? group, string operation, string? error = null)
    {
        var bounded = MetricTags(topic, group, CheckpointKind.Origin);
        var tags = new TagList
        {
            { "messaging.system", "kafka" }, { "messaging.destination.name", bounded[3].Value },
            { "messaging.consumer.group.name", bounded[4].Value },
            { "messaging.operation.name", operation }, { "messaging.operation.type", operation }
        };
        if (error is not null) tags.Add("error.type", error);
        return tags;
    }

    private static string PathwayHash(params string[] values)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> length = stackalloc byte[4];
        foreach (var value in values)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length); hash.AppendData(length); hash.AppendData(bytes);
        }
        return Convert.ToHexString(hash.GetHashAndReset().AsSpan(0, 8)).ToLowerInvariant();
    }

    private static void ValidatePublish(PublishOptions? options)
    {
        if (options is null) return;
        if (!Enum.IsDefined(options.Classification) || options.DeadLetterReason is { } reason && !Enum.IsDefined(reason)) throw new ArgumentException("Unknown publication classification or reason.", nameof(options));
        if (options.Attempt is < 0 or > 1_000_000) throw new ArgumentOutOfRangeException(nameof(options.Attempt));
    }
    private static void ValidateLabel(string? value, int limit, string name)
    { if (!ContextCodec.ValidIdentity(value, limit)) throw new ArgumentException($"{name} must be nonempty, free of control characters, and at most {limit} UTF-8 bytes.", name); }
    private static void ValidateTopic(string topic) => ValidateLabel(topic, 249, nameof(topic));
    private static void ValidateGroup(string? group) { if (group is not null) ValidateLabel(group, 255, nameof(group)); }
    private static int? ValidPartition(Partition partition) => partition.Value >= 0 ? partition.Value : null;
    private static long? ValidOffset(Offset offset) => offset.Value >= 0 ? offset.Value : null;
    private static void Safe(Action action) { try { action(); } catch { /* Instrumentation callbacks cannot fail application work. */ } }

    private ActivityScope StartActivity(string name, ActivityKind kind, ActivityContext? parent = null,
        IEnumerable<ActivityLink>? links = null, bool enabled = true, IEnumerable<KeyValuePair<string, object?>>? tags = null)
    {
        var previous = Activity.Current;
        Activity? activity = null;
        try
        {
            if (enabled) activity = _activities.StartActivity(name, kind, parent ?? default, tags, links);
        }
        catch
        {
            // ActivityStarted may throw after StartActivity has installed its new ambient Activity.
            var partial = Activity.Current;
            if (!ReferenceEquals(partial, previous)) Safe(() => partial?.Dispose());
            Safe(() => Activity.Current = previous);
        }
        return new ActivityScope(activity, previous);
    }

    private sealed class ActivityScope(Activity? activity, Activity? previous) : IDisposable
    {
        public Activity? Activity { get; } = activity;
        public void Dispose()
        {
            try { Activity?.Dispose(); }
            catch { /* ActivityStopped observers cannot replace a Kafka result or handler exception. */ }
            finally { Safe(() => System.Diagnostics.Activity.Current = previous); }
        }
    }

    public void Dispose() { _activities.Dispose(); _meter.Dispose(); }
}
