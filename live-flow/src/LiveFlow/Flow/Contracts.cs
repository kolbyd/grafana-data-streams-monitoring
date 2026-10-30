using System.ComponentModel.DataAnnotations;

namespace LiveFlow.Flow;

public sealed class FlowOptions
{
    public string ApiKey { get; set; } = "";
    public string[] AllowedOrigins { get; set; } = [];
    public string DefaultCluster { get; set; } = "unknown";
    [Range(1024, 16_777_216)] public int MaxRequestBytes { get; set; } = 4_194_304;
    [Range(1, 50_000)] public int MaxSpansPerRequest { get; set; } = 10_000;
    [Range(10, 100_000)] public int MaxEvents { get; set; } = 5_000;
    [Range(10, 200_000)] public int MaxSeenSpans { get; set; } = 50_000;
    [Range(1, 10_000)] public int MaxNodes { get; set; } = 2_000;
    [Range(1, 20_000)] public int MaxEdges { get; set; } = 4_000;
    [Range(1, 100_000)] public int MaxCorrelationEntries { get; set; } = 10_000;
    [Range(1, 240)] public int EventRetentionMinutes { get; set; } = 10;
    [Range(1, 240)] public int CorrelationRetentionMinutes { get; set; } = 10;
    [Range(1, 1440)] public int TopologyRetentionMinutes { get; set; } = 60;
    [Range(1, 3600)] public int StaleAfterSeconds { get; set; } = 30;
    [Range(50, 5000)] public int StreamIntervalMs { get; set; } = 200;
    [Range(1, 256)] public int MaxViewers { get; set; } = 32;
    [Range(1, 1000)] public int MaxVisualEventsPerFrame { get; set; } = 100;
    // Grafana Live's default outbound client queue is 4 MiB, tighter than the
    // app backend's 8 MiB HTTP/WebSocket limit. Leave space for queued updates.
    [Range(16_384, 3_145_728)] public int MaxFrameBytes { get; set; } = 3_145_728;
    [Range(10, 100_000)] public int MaxCausalSpans { get; set; } = 10_000;
    [Range(10, 500_000)] public int MaxCausalRelations { get; set; } = 50_000;
    [Range(1, 1000)] public int MaxPathways { get; set; } = 128;
    [Range(4, 128)] public int MaxPathwayDepth { get; set; } = 64;
    [Range(100, 500_000)] public int MaxCausalDerivationWork { get; set; } = 50_000;
    [Range(16_384, 4_194_304)] public int MaxPathwayOutputBytes { get; set; } = 1_048_576;
}

public sealed record SpanReference(string TraceId, string SpanId)
{
    public string Id => $"{TraceId}:{SpanId}";
    public IReadOnlyDictionary<string, string>? Attributes { get; init; }
}

public sealed record Observation(
    string TraceId, string SpanId, string Operation, string Service, string ServiceNamespace,
    string Environment, string Cluster, string Topic, string? ConsumerGroup,
    string? Partition, string? Offset, long StartedAt, long OccurredAt, bool Error,
    bool IsBatch, IReadOnlyList<SpanReference> Links)
{
    public string Id => $"{TraceId}:{SpanId}";
    public SpanReference? Parent { get; init; }
    public string? MessageId { get; init; }
    public IReadOnlyDictionary<string, string> DsmAttributes { get; init; } = new Dictionary<string, string>();
    public int InvalidRelationships { get; init; }
}

public sealed record NormalizedBatch(int Total, int Ignored, int Invalid, IReadOnlyList<Observation> Observations)
{
    public IReadOnlyList<Observation> CausalSpans { get; init; } = [];
}
public sealed record IngestResult(int Accepted, int Duplicates, int Ignored, int Rejected);

public sealed record FlowNode(string Id, string Kind, string Name, string ServiceNamespace,
    string Environment, string Cluster, long LastSeen, long Published, long Received, long Errors)
{
    public long Processed { get; init; }
}

public sealed record FlowEdge(string Id, string Source, string Target, string Operation,
    string Environment, string Cluster, string Topic, string? ConsumerGroup,
    long LastSeen, long Observations, long Errors)
{
    public long Processed { get; init; }
}

public sealed record FlowEvent(long Sequence, string Kind, string ObservationId, string Source,
    string Target, string TopicNodeId, string Service, string Environment, string Cluster,
    string Topic, string? ConsumerGroup, string? Partition, string? Offset, bool Error,
    bool IsBatch, long OccurredAt, long ObservedAt, string TraceId, string SpanId,
    string? ProducerObservationId = null, string? ProducerService = null,
    string? CorrelationMethod = null, string? ProducerTraceId = null)
{
    public SpanReference? Parent { get; init; }
    public IReadOnlyDictionary<string, string> DsmAttributes { get; init; } = new Dictionary<string, string>();
}

public sealed record FlowStats(long Accepted, long Published, long Received, long Errors,
    long Matched, long Duplicates, long Ignored, long Rejected, long CorrelationEvictions,
    long ExpiredReceives, int WaitingReceives, int CachedPublications, int SeenSpans,
    int BufferedEvents, int Nodes, int Edges, int Viewers, long SlowViewers)
{
    public long Processed { get; init; }
}

public sealed record StreamFrame(string Type, string InstanceId, long Cursor, long TopologyVersion,
    bool ReplaceTopology, bool ResetEvents, string? Reason, long ServerTime,
    IReadOnlyList<FlowNode> Nodes, IReadOnlyList<FlowEdge> Edges,
    IReadOnlyList<FlowEvent> Events, FlowStats Stats, int VisualEventsSkipped)
{
    public IReadOnlyList<FlowPathway> Pathways { get; init; } = [];
    public CausalityStats Causality { get; init; } = new(0, 0, 0, 0, 0, 0, 0, 0, 0);
    public FrameCoverage Coverage { get; init; } = new(0, 0, null, null, 0, 0, 0, 0, false);
}

public sealed record FrameCoverage(int RetainedEvents, int ReturnedEvents,
    long? OldestRetainedObservedAt, long? OldestReturnedObservedAt,
    int EventsOmitted, int NodesOmitted, int EdgesOmitted, int PathwaysOmitted, bool Truncated);

public sealed record PathwayStep(string ObservationId, string TraceId, string SpanId, string Operation,
    string Service, string? NodeId, string Topic, string Cluster, string? ConsumerGroup,
    long StartedAt, long CompletedAt, bool Error);

public sealed record FlowPathway(string Id, string? OriginObservationId, string TerminalObservationId,
    string OriginService, string TerminalService, string Environment, string? StartNodeId, string EndNodeId,
    IReadOnlyList<string> NodeIds, IReadOnlyList<string> EdgeIds, long? StartedAt, long CompletedAt,
    long? DurationMs, string Status, string? IncompleteReason, string Evidence, bool IsBatch, bool Error,
    IReadOnlyList<PathwayStep> Steps, IReadOnlyList<string> MissingSpanIds,
    IReadOnlyDictionary<string, string> DsmAttributes);

public sealed record CausalityStats(int RetainedSpans, int RetainedRelations, long EvictedSpans,
    long TruncatedRelations, int IncompleteChains, int ClockSkewedChains, long DuplicateSpans,
    int TruncatedPathways, long ProcessedSpans);
