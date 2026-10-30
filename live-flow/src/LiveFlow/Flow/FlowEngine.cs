using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace LiveFlow.Flow;

/// <summary>Single-process, bounded observational state. No Kafka connection or message payloads.</summary>
public sealed class FlowEngine(FlowOptions options, TimeProvider clock)
{
    private readonly object gate = new();
    private readonly Dictionary<string, NodeState> nodes = [];
    private readonly Dictionary<string, EdgeState> edges = [];
    private readonly Queue<FlowEvent> events = new();
    private readonly Dictionary<string, long> seen = [];
    private readonly Queue<(string Id, long At)> seenOrder = new();
    private readonly Dictionary<string, CachedObservation> publications = [];
    private readonly Dictionary<string, string> producerIndex = [];
    private readonly Dictionary<string, CachedObservation> waiting = [];
    private readonly Dictionary<string, HashSet<string>> waitingIndex = [];
    private readonly CausalStore causal = new(options);
    private long sequence, topologyVersion, accepted, published, received, errors, matched, duplicates, ignored, rejected, nextPruneAt;
    private long correlationEvictions, expiredReceives, slowViewers;
    private int viewers;
    public string InstanceId { get; } = Guid.NewGuid().ToString("N");

    public IngestResult Ingest(NormalizedBatch batch)
    {
        lock (gate)
        {
            var now = Now;
            Prune(now);
            ignored += batch.Ignored;
            rejected += batch.Invalid;
            var batchAccepted = 0;
            var batchDuplicates = 0;
            var batchRejected = batch.Invalid;
            foreach (var observation in batch.Observations)
            {
                if (seen.ContainsKey(observation.Id)) { duplicates++; batchDuplicates++; continue; }
                var serviceId = Id("service", observation.Environment, observation.ServiceNamespace, observation.Service);
                var topicId = Id("topic", observation.Environment, observation.Cluster, observation.Topic);
                var source = observation.Operation == "publish" ? serviceId : topicId;
                var target = observation.Operation == "publish" ? topicId : serviceId;
                var edgeId = Id("edge", source, target, observation.ConsumerGroup ?? "");
                var newNodes = (nodes.ContainsKey(serviceId) ? 0 : 1) + (nodes.ContainsKey(topicId) ? 0 : 1);
                if (nodes.Count + newNodes > options.MaxNodes || (!edges.ContainsKey(edgeId) && edges.Count >= options.MaxEdges))
                { rejected++; batchRejected++; continue; }

                Remember(observation.Id, now);
                accepted++; batchAccepted++;
                if (observation.Operation == "publish") published++; else received++;
                if (observation.Error) errors++;
                var evt = new FlowEvent(++sequence, observation.Operation, observation.Id, source, target, topicId,
                    observation.Service, observation.Environment, observation.Cluster, observation.Topic,
                    observation.ConsumerGroup, observation.Partition, observation.Offset, observation.Error,
                    observation.IsBatch, observation.OccurredAt, now, observation.TraceId, observation.SpanId)
                { Parent = observation.Parent, DsmAttributes = observation.DsmAttributes };
                Append(evt);
                causal.Add(observation, now);
                TouchNode(serviceId, "service", observation.Service, observation.ServiceNamespace, observation.Environment, "", observation, now);
                TouchNode(topicId, "topic", observation.Topic, "", observation.Environment, observation.Cluster, observation, now);
                if (!edges.TryGetValue(edgeId, out var edge))
                {
                    edge = new EdgeState(edgeId, source, target, observation.Operation, observation.Environment,
                        observation.Cluster, observation.Topic, observation.ConsumerGroup);
                    edges.Add(edgeId, edge);
                    topologyVersion++;
                }
                edge.LastSeen = now; edge.UpdateSequence = sequence; edge.Count++;
                if (observation.Error) edge.Errors++;

                if (!observation.Error && !observation.IsBatch) Correlate(observation, evt, now);
            }
            foreach (var span in batch.CausalSpans)
            {
                if (!causal.Add(span, now)) continue;
                // Causal enrichment has its own update revision, but never a synthetic receive event.
                sequence++;
                if (span.Operation == "process") TouchProcessing(span, now);
            }
            SyncMultiTopicTopology();
            // A high-volume export must not temporarily exceed the configured event ring.
            while (events.Count > options.MaxEvents) events.Dequeue();
            return new IngestResult(batchAccepted, batchDuplicates, batch.Ignored, batchRejected);
        }
    }

    public StreamFrame Snapshot(int? recentEvents = null)
    {
        lock (gate)
        {
            Prune(Now);
            return Frame("snapshot", true, true, null, nodes.Values, edges.Values,
                events.TakeLast(Math.Clamp(recentEvents ?? options.MaxEvents, 0, options.MaxEvents)).ToArray(), 0);
        }
    }

    public StreamFrame Since(string? instanceId, long cursor, long knownTopologyVersion)
    {
        lock (gate)
        {
            Prune(Now);
            var reason = instanceId != InstanceId ? "instance_changed"
                : cursor < 0 || cursor > sequence ? "invalid_cursor"
                : events.Count > 0 && cursor < events.Peek().Sequence - 1 ? "cursor_expired"
                : events.Count == 0 && cursor < sequence ? "cursor_expired" : null;
            if (reason is not null)
                return Frame("snapshot", true, true, reason, nodes.Values, edges.Values, events.ToArray(), 0);

            var available = events.Where(e => e.Sequence > cursor).ToArray();
            var skipped = Math.Max(0, available.Length - options.MaxVisualEventsPerFrame);
            var replace = knownTopologyVersion != topologyVersion;
            return Frame("update", replace, false, null,
                nodes.Values.Where(n => replace || n.UpdateSequence > cursor),
                edges.Values.Where(e => replace || e.UpdateSequence > cursor),
                available.TakeLast(options.MaxVisualEventsPerFrame).ToArray(), skipped);
        }
    }

    public FlowStats Stats { get { lock (gate) return GetStats(); } }
    public Observation? CausalSpan(string traceId, string spanId) { lock (gate) { Prune(Now); return causal.Find($"{traceId}:{spanId}"); } }
    public void Sweep() { lock (gate) Prune(Now); }
    public bool TryAddViewer() { lock (gate) { if (viewers >= options.MaxViewers) return false; viewers++; return true; } }
    public void RemoveViewer() { lock (gate) viewers = Math.Max(0, viewers - 1); }
    public void SlowViewer() { lock (gate) slowViewers++; }
    private long Now => clock.GetUtcNow().ToUnixTimeMilliseconds();

    private StreamFrame Frame(string type, bool replace, bool resetEvents, string? reason,
        IEnumerable<NodeState> selectedNodes, IEnumerable<EdgeState> selectedEdges, FlowEvent[] selectedEvents, int skipped)
    {
        var frame = new StreamFrame(type, InstanceId, sequence, topologyVersion, replace, resetEvents, reason, Now,
            selectedNodes.Select(n => n.View()).OrderBy(n => n.Id).ToArray(),
            selectedEdges.Select(e => e.View()).OrderBy(e => e.Id).ToArray(), selectedEvents, GetStats(), skipped)
        {
            Pathways = causal.Pathways().Select(p => p with { EdgeIds = p.EdgeIds.Where(edges.ContainsKey).ToArray() }).ToArray(),
            Causality = causal.Stats
        };
        return FrameBudget.Fit(frame, options.MaxFrameBytes, events.Count,
            events.TryPeek(out var oldest) ? oldest.ObservedAt : null);
    }

    private FlowStats GetStats() => new(accepted, published, received, errors, matched, duplicates, ignored, rejected,
        correlationEvictions, expiredReceives, waiting.Count, publications.Count, seen.Count, events.Count,
        nodes.Count, edges.Count, viewers, slowViewers) { Processed = causal.Stats.ProcessedSpans };

    private void TouchProcessing(Observation span, long now)
    {
        var serviceId = Id("service", span.Environment, span.ServiceNamespace, span.Service);
        var topicId = span.Topic.Length > 0 ? Id("topic", span.Environment, span.Cluster, span.Topic) : null;
        var newNodes = (nodes.ContainsKey(serviceId) ? 0 : 1) + (topicId is not null && !nodes.ContainsKey(topicId) ? 1 : 0);
        var edgeId = topicId is null ? null : Id("edge", topicId, serviceId, span.ConsumerGroup ?? "");
        if (nodes.Count + newNodes > options.MaxNodes || edgeId is not null && !edges.ContainsKey(edgeId) && edges.Count >= options.MaxEdges) return;
        ProcessingNode(serviceId, "service", span.Service, span.ServiceNamespace, "");
        if (topicId is not null)
        {
            ProcessingNode(topicId, "topic", span.Topic, "", span.Cluster);
            if (!edges.TryGetValue(edgeId!, out var edge))
            {
                edge = new EdgeState(edgeId!, topicId, serviceId, "receive", span.Environment, span.Cluster, span.Topic, span.ConsumerGroup);
                edges.Add(edgeId!, edge); topologyVersion++;
            }
            edge.LastSeen = now; edge.UpdateSequence = sequence; edge.Processed++;
        }
        void ProcessingNode(string id, string kind, string name, string ns, string cluster)
        {
            if (!nodes.TryGetValue(id, out var node))
            { node = new NodeState(id, kind, name, ns, span.Environment, cluster); nodes.Add(id, node); topologyVersion++; }
            node.LastSeen = now; node.UpdateSequence = sequence; node.Processed++;
        }
    }

    private void SyncMultiTopicTopology()
    {
        foreach (var (process, input, at) in causal.MultiTopicInputs())
        {
            var serviceId = Id("service", process.Environment, process.ServiceNamespace, process.Service);
            if (!nodes.ContainsKey(serviceId)) continue;
            var cluster = CausalStore.InputCluster(process, input);
            var topicId = Id("topic", process.Environment, cluster, input.Topic);
            var edgeId = Id("edge", topicId, serviceId, process.ConsumerGroup ?? "");
            if ((!nodes.ContainsKey(topicId) && nodes.Count >= options.MaxNodes) || (!edges.ContainsKey(edgeId) && edges.Count >= options.MaxEdges)) continue;
            if (!nodes.TryGetValue(topicId, out var topic))
            { topic = new NodeState(topicId, "topic", input.Topic, "", process.Environment, cluster); nodes.Add(topicId, topic); topologyVersion++; }
            if (topic.LastSeen < at) { topic.LastSeen = at; topic.UpdateSequence = sequence; }
            if (!edges.TryGetValue(edgeId, out var edge))
            {
                edge = new EdgeState(edgeId, topicId, serviceId, "receive", process.Environment, cluster, input.Topic, process.ConsumerGroup);
                edges.Add(edgeId, edge); topologyVersion++;
            }
            if (edge.LastSeen < at) { edge.LastSeen = at; edge.UpdateSequence = sequence; }
            // This is a known dependency of a multi-topic processing span, not an
            // attributable per-topic receive/message count. Leave wire/processed edge counts unchanged.
        }
    }

    private void TouchNode(string id, string kind, string name, string ns, string env, string cluster, Observation o, long now)
    {
        if (!nodes.TryGetValue(id, out var node))
        { node = new NodeState(id, kind, name, ns, env, cluster); nodes.Add(id, node); topologyVersion++; }
        node.LastSeen = now; node.UpdateSequence = sequence;
        if (o.Operation == "publish") node.Published++; else node.Received++;
        if (o.Error) node.Errors++;
    }

    private void Remember(string id, long now)
    {
        while (seen.Count >= options.MaxSeenSpans && seenOrder.TryDequeue(out var old)) seen.Remove(old.Id);
        seen[id] = now;
        seenOrder.Enqueue((id, now));
    }

    private static string? RecordKey(Observation o) =>
        o.Cluster is "unknown" or "" || o.Partition is null || o.Offset is null ? null
            : Id("record", o.Environment, o.Cluster, o.Topic, o.Partition, o.Offset);
    private static string SpanKey(string traceId, string spanId) => $"span:{traceId}:{spanId}";
    private static bool SameScope(Observation a, Observation b) => a.Environment == b.Environment && a.Topic == b.Topic
        && a.Cluster == b.Cluster && (RecordKey(a) is not { } ak || RecordKey(b) is not { } bk || ak == bk);

    private string[] ConsumerKeys(Observation o)
    {
        var keys = new List<string>();
        if (RecordKey(o) is { } record) keys.Add(record);
        // A batch or multi-link observation cannot be presented as one identifiable message.
        if (o.Links.Count == 1) keys.Add(SpanKey(o.Links[0].TraceId, o.Links[0].SpanId));
        return keys.ToArray();
    }

    private void Correlate(Observation o, FlowEvent evt, long now)
    {
        // Full parent/link vectors live only in the globally bounded causal store.
        // Legacy wire matching needs at most one link; retaining arbitrary vectors here
        // would multiply their memory cost across the event/correlation retention windows.
        var wireOnly = o with { Parent = null, Links = o.Links.Count == 1 ? o.Links : [], DsmAttributes = new Dictionary<string, string>() };
        if (o.Operation == "publish")
        {
            var keys = new List<string> { SpanKey(o.TraceId, o.SpanId) };
            if (RecordKey(o) is { } record) keys.Add(record);
            while (publications.Count >= options.MaxCorrelationEntries) EvictOldestPublication();
            var cached = new CachedObservation(wireOnly, evt, now, keys.ToArray());
            publications[o.Id] = cached;
            foreach (var key in keys) producerIndex[key] = o.Id;
            var candidates = keys.Where(waitingIndex.ContainsKey).SelectMany(k => waitingIndex[k]).Distinct().ToArray();
            foreach (var id in candidates)
                if (waiting.TryGetValue(id, out var consumer) && SameScope(o, consumer.Observation))
                { Match(cached, consumer); RemoveWaiting(id); }
        }
        else
        {
            var keys = ConsumerKeys(o);
            // Prefer immutable record coordinates, then an explicit producer span link.
            foreach (var key in keys)
                if (producerIndex.TryGetValue(key, out var producerId) && publications.TryGetValue(producerId, out var producer)
                    && SameScope(producer.Observation, o))
                { Match(producer, new CachedObservation(wireOnly, evt, now, keys)); return; }
            if (keys.Length == 0) return;
            while (waiting.Count >= options.MaxCorrelationEntries)
            { RemoveWaiting(waiting.First().Key); correlationEvictions++; }
            waiting[o.Id] = new CachedObservation(wireOnly, evt, now, keys);
            foreach (var key in keys)
            {
                if (!waitingIndex.TryGetValue(key, out var set)) waitingIndex[key] = set = [];
                set.Add(o.Id);
            }
        }
    }

    private void Match(CachedObservation producer, CachedObservation consumer)
    {
        matched++;
        var o = consumer.Observation;
        var method = RecordKey(o) is { } record && record == RecordKey(producer.Observation) ? "record" : "span_link";
        Append(consumer.Event with { Sequence = ++sequence, Kind = "matched", ProducerObservationId = producer.Observation.Id,
            ProducerService = producer.Observation.Service, ProducerTraceId = producer.Observation.TraceId,
            CorrelationMethod = method, ObservedAt = Now });
    }

    private void RemoveWaiting(string id)
    {
        if (!waiting.Remove(id, out var item)) return;
        foreach (var key in item.Keys)
            if (waitingIndex.TryGetValue(key, out var set))
            { set.Remove(id); if (set.Count == 0) waitingIndex.Remove(key); }
    }

    private void RemovePublication(string id)
    {
        if (!publications.Remove(id, out var item)) return;
        foreach (var key in item.Keys)
            if (producerIndex.GetValueOrDefault(key) == id) producerIndex.Remove(key);
    }
    private void EvictOldestPublication() { RemovePublication(publications.First().Key); correlationEvictions++; }
    private void Append(FlowEvent evt)
    { events.Enqueue(evt); while (events.Count > options.MaxEvents) events.Dequeue(); }

    private void Prune(long now)
    {
        if (now < nextPruneAt) return;
        nextPruneAt = now + 1000;
        if (causal.Prune(now)) sequence++;
        var eventCutoff = now - options.EventRetentionMinutes * 60_000L;
        while (events.TryPeek(out var evt) && evt.ObservedAt < eventCutoff) events.Dequeue();
        while (seenOrder.TryPeek(out var item) && item.At < eventCutoff)
        { seenOrder.Dequeue(); seen.Remove(item.Id); }
        var correlationCutoff = now - options.CorrelationRetentionMinutes * 60_000L;
        foreach (var id in publications.Where(p => p.Value.At < correlationCutoff).Select(p => p.Key).ToArray()) RemovePublication(id);
        foreach (var id in waiting.Where(p => p.Value.At < correlationCutoff).Select(p => p.Key).ToArray())
        { RemoveWaiting(id); expiredReceives++; }
        var topologyCutoff = now - options.TopologyRetentionMinutes * 60_000L;
        foreach (var id in edges.Where(e => e.Value.LastSeen < topologyCutoff).Select(e => e.Key).ToArray())
        { edges.Remove(id); topologyVersion++; }
        foreach (var id in nodes.Where(n => n.Value.LastSeen < topologyCutoff).Select(n => n.Key).ToArray())
        { nodes.Remove(id); topologyVersion++; }
    }

    public static string Id(string kind, params string[] parts) => kind + "_" +
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(parts))))[..24];

    private sealed record CachedObservation(Observation Observation, FlowEvent Event, long At, string[] Keys);
    private sealed class NodeState(string id, string kind, string name, string ns, string env, string cluster)
    {
        public long LastSeen, UpdateSequence, Published, Received, Errors, Processed;
        public FlowNode View() => new(id, kind, name, ns, env, cluster, LastSeen, Published, Received, Errors) { Processed = Processed };
    }
    private sealed class EdgeState(string id, string source, string target, string operation, string env, string cluster, string topic, string? group)
    {
        public long LastSeen, UpdateSequence, Count, Errors, Processed;
        public FlowEdge View() => new(id, source, target, operation, env, cluster, topic, group, LastSeen, Count, Errors) { Processed = Processed };
    }
}
