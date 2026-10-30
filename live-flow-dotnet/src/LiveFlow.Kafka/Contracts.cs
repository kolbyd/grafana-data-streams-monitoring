using System.Diagnostics;
using Confluent.Kafka;
using OpenTelemetry.Context.Propagation;

namespace LiveFlow.Kafka;

public enum CheckpointKind { Origin, PublishAttempt, PublishDelivered, PublishFailed, PublishCancelled, PublishUnconfirmed, Receive, ProcessingStarted, ProcessingCompleted, ProcessingFailed, ProcessingCancelled, OutboxPersisted, OutboxDispatched }
public enum PublishClassification { Normal, Retry, DeadLetter }
public enum DeadLetterReason { Unspecified, ProcessingFailed, ValidationFailed, RetryLimitExceeded, Expired }
public enum ContextReadStatus { Valid, Missing, Malformed, TooLarge, UnsupportedVersion, InvalidIdentity, InvalidTimestamp }

/// <summary>Immutable message lineage. Identifiers belong in checkpoints, never metric labels.</summary>
public sealed class FlowContext
{
    public string LogicalMessageId { get; }
    public long OriginUnixMs { get; }
    public bool OriginKnown { get; }
    public long CheckpointUnixMs { get; }
    public string CheckpointId { get; }
    public string PathwayHash { get; }
    public CheckpointKind Kind { get; }
    internal FlowContext(string messageId, long origin, bool originKnown, long checkpointAt, string checkpointId, string pathwayHash, CheckpointKind kind)
        => (LogicalMessageId, OriginUnixMs, OriginKnown, CheckpointUnixMs, CheckpointId, PathwayHash, Kind)
        = (messageId, origin, originKnown, checkpointAt, checkpointId, pathwayHash, kind);
}

public sealed record FlowCheckpoint
{
    public required string Id { get; init; }
    public string? ParentCheckpointId { get; init; }
    public required string LogicalMessageId { get; init; }
    public required string PathwayHash { get; init; }
    public string? ParentPathwayHash { get; init; }
    public required CheckpointKind Kind { get; init; }
    public required string Service { get; init; }
    public required string Environment { get; init; }
    public required string Cluster { get; init; }
    public required string Topic { get; init; }
    public string? ConsumerGroup { get; init; }
    public required long TimestampUnixMs { get; init; }
    public required long OriginUnixMs { get; init; }
    public required bool OriginKnown { get; init; }
    public double? EdgeLatencyMs { get; init; }
    public double? PathwayLatencyMs { get; init; }
    public double? ProcessingDurationMs { get; init; }
    public double? OutboxWaitMs { get; init; }
    public string? OutboxId { get; init; }
    public long? OutboxEnqueuedAtUnixMs { get; init; }
    public int? Partition { get; init; }
    public long? Offset { get; init; }
    public PublishClassification Classification { get; init; }
    public DeadLetterReason? DeadLetterReason { get; init; }
    public int Attempt { get; init; }
    public bool ClockSkewDetected { get; init; }
}

/// <summary>Implementations must be fast and nonblocking. False means dropped; exceptions are isolated.</summary>
public interface ICheckpointSink { bool TryWrite(FlowCheckpoint checkpoint); }

public sealed class FlowOptions
{
    public string ServiceName { get; init; } = "unknown_service";
    public string Environment { get; init; } = "unknown";
    public string Cluster { get; init; } = "unknown";
    public int MaxMetricIdentities { get; init; } = 256;
    public TimeSpan AllowedFutureClockSkew { get; init; } = TimeSpan.FromSeconds(60);
    /// <summary>Opt into private pathway headers/checkpoints in addition to standard OpenTelemetry.</summary>
    public bool EnablePathwayExtensions { get; init; }
    /// <summary>Disable when existing Kafka instrumentation already owns producer spans.</summary>
    public bool CreateProducerActivity { get; init; } = true;
    public bool CreateProcessingActivity { get; init; } = true;
    /// <summary>Defaults to the OpenTelemetry W3C trace-context and baggage propagators.</summary>
    public TextMapPropagator? Propagator { get; init; }
}

public sealed class PublishOptions
{
    internal OutboxContext? Outbox { get; init; }
    public FlowContext? Context { get; init; }
    public PropagationContext? ParentContext { get; init; }
    public PublishClassification Classification { get; init; }
    public DeadLetterReason? DeadLetterReason { get; init; }
    public int Attempt { get; init; }
}

public sealed class ReceivedMessage<TKey, TValue>
{
    public ConsumeResult<TKey, TValue> Record { get; }
    public FlowContext Context { get; }
    public string? ConsumerGroup { get; }
    public ContextReadStatus ContextStatus { get; }
    public ActivityContext TraceContext => PropagationContext.ActivityContext;
    public PropagationContext PropagationContext { get; }
    internal ReceivedMessage(ConsumeResult<TKey, TValue> record, FlowContext context, string? group, ContextReadStatus status, PropagationContext propagation)
        => (Record, Context, ConsumerGroup, ContextStatus, PropagationContext) = (record, context, group, status, propagation);
}

/// <summary>Persist Serialized alongside the application's outbox row, in its transaction.</summary>
public sealed class OutboxContext
{
    public string OutboxId { get; }
    public long EnqueuedAtUnixMs { get; }
    public FlowContext Context { get; }
    public PropagationContext PropagationContext { get; }
    public IReadOnlyDictionary<string, string> TraceHeaders => TracePropagation.ToCarrier(PropagationContext);
    internal bool HasPathwayExtension { get; }
    public string Serialized => ContextCodec.EncodeOutbox(this);
    internal OutboxContext(string id, long at, FlowContext context, PropagationContext propagation = default, bool hasPathwayExtension = true)
        => (OutboxId, EnqueuedAtUnixMs, Context, PropagationContext, HasPathwayExtension) = (id, at, context, propagation, hasPathwayExtension);
}
