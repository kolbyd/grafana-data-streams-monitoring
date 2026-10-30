using System.Text.Json;

namespace LiveFlow.Flow;

/// <summary>Bounded optional OTel ancestry processor. Caller holds the FlowEngine lock.</summary>
internal sealed class CausalStore(FlowOptions options)
{
    private readonly Dictionary<string, Entry> spans = [];
    private readonly Queue<string> order = new();
    private readonly Dictionary<string, HashSet<string>> recordProducers = [];
    private readonly HashSet<string> multiTopicProcesses = [];
    private IReadOnlyList<FlowPathway> pathways = [];
    private int relations, incomplete, skewed, truncatedPaths;
    private long evictions, truncatedRelations, duplicates, processed;
    private bool dirty;
    public Observation? Find(string id) => spans.GetValueOrDefault(id)?.Span;

    public bool Add(Observation span, long now)
    {
        if (spans.ContainsKey(span.Id)) { duplicates++; return false; }
        var count = span.Links.Count + (span.Parent is null ? 0 : 1);
        if (count > options.MaxCausalRelations)
        {
            // A single over-budget span is omitted as a whole; its links are never silently truncated.
            truncatedRelations += count; return false;
        }
        while (spans.Count >= options.MaxCausalSpans || relations + count > options.MaxCausalRelations)
        {
            if (!order.TryDequeue(out var old)) break;
            if (Remove(old)) evictions++;
        }
        spans.Add(span.Id, new(span, now, count)); order.Enqueue(span.Id); relations += count;
        truncatedRelations += span.InvalidRelationships;
        if (span.Operation == "process") processed++;
        if (span.Operation == "process" && span.Topic.Length == 0) multiTopicProcesses.Add(span.Id);
        if (span.Operation == "publish" && RecordKey(span) is { } key)
        {
            if (!recordProducers.TryGetValue(key, out var set)) recordProducers[key] = set = [];
            set.Add(span.Id);
        }
        dirty = true;
        return true;
    }

    public bool Prune(long now)
    {
        var changed = false;
        var cutoff = now - options.CorrelationRetentionMinutes * 60_000L;
        while (order.TryPeek(out var id))
        {
            if (!spans.TryGetValue(id, out var span)) { order.Dequeue(); continue; }
            if (span.At >= cutoff) break;
            order.Dequeue(); changed |= Remove(id);
        }
        return changed;
    }

    private bool Remove(string id)
    {
        if (!spans.Remove(id, out var entry)) return false;
        relations -= entry.Relations;
        multiTopicProcesses.Remove(id);
        if (entry.Span.Operation == "publish" && RecordKey(entry.Span) is { } key && recordProducers.TryGetValue(key, out var set))
        { set.Remove(id); if (set.Count == 0) recordProducers.Remove(key); }
        dirty = true; return true;
    }

    public IReadOnlyList<FlowPathway> Pathways()
    {
        if (!dirty) return pathways;
        var result = new Dictionary<string, FlowPathway>();
        var work = 0;
        var outputBytes = 2;
        var outputLimited = false;
        truncatedPaths = 0;
        var terminals = spans.Values.Where(e => e.Span.Operation == "process")
            .OrderByDescending(e => e.Span.OccurredAt).ThenBy(e => e.Span.Id).ToArray();
        foreach (var terminal in terminals)
        {
            if (outputLimited || result.Count >= options.MaxPathways || work >= options.MaxCausalDerivationWork) { truncatedPaths++; continue; }
            Visit(terminal.Span, [], new HashSet<string>(), [], false, null);
        }
        pathways = result.Values.OrderByDescending(p => p.CompletedAt).ThenBy(p => p.Id).ToArray();
        incomplete = pathways.Count(p => p.Status == "incomplete");
        skewed = pathways.Count(p => p.Status == "clock_skew");
        dirty = false;
        return pathways;

        void Visit(Observation current, List<Observation> reverse, HashSet<string> visited,
            List<string> missing, bool assisted, string? failure)
        {
            if (outputLimited || ++work > options.MaxCausalDerivationWork || result.Count >= options.MaxPathways) { truncatedPaths++; return; }
            if (!visited.Add(current.Id)) { Finish(reverse, missing, assisted, "cycle"); return; }
            reverse.Add(current);
            if (reverse.Count >= options.MaxPathwayDepth && Upstream(current).Any())
            { Finish(reverse, missing, assisted, "depth_limit"); truncatedPaths++; }
            else
            {
                var found = false;
                foreach (var relation in Upstream(current))
                {
                    found = true;
                    if (outputLimited || result.Count >= options.MaxPathways || work >= options.MaxCausalDerivationWork) { truncatedPaths++; break; }
                    if (spans.TryGetValue(relation.Id, out var parent))
                    {
                        if (!Compatible(current, parent.Span))
                            Finish(reverse, missing, assisted, "incompatible_message_scope");
                        else Visit(parent.Span, reverse, visited, missing, assisted || relation.RecordAssisted, failure);
                    }
                    else
                    {
                        missing.Add(relation.Id);
                        Finish(reverse, missing, assisted, "upstream_span_missing");
                        missing.RemoveAt(missing.Count - 1);
                    }
                }
                if (!found) Finish(reverse, missing, assisted, failure);
            }
            reverse.RemoveAt(reverse.Count - 1); visited.Remove(current.Id);
        }

        void Finish(List<Observation> reverse, List<string> missing, bool assisted, string? failure)
        {
            if (reverse.Count == 0 || result.Count >= options.MaxPathways) return;
            var ordered = reverse.AsEnumerable().Reverse().ToArray();
            var messaging = ordered.Where(s => s.Operation is "publish" or "receive" or "process").ToArray();
            var terminal = reverse[0];
            var first = messaging.FirstOrDefault();
            var origin = first?.Operation == "publish" ? first : null;
            // An unknown non-messaging ancestor before the first observed send is a boundary,
            // not proof that an earlier Kafka message was dropped. The measured range starts at that send.
            if (origin is not null && failure == "upstream_span_missing") { missing = []; failure = null; }
            if (origin is null) failure ??= "missing_send";
            var processSinceSend = false;
            var sendSeen = false;
            foreach (var step in messaging)
            {
                if (step.Operation == "process") processSinceSend = true;
                if (step.Operation != "publish") continue;
                if (sendSeen && !processSinceSend) failure ??= "missing_processing_span";
                sendSeen = true; processSinceSend = false;
            }
            if (ordered.Any(s => s.InvalidRelationships > 0)) failure ??= "invalid_relationship";
            var clockSkew = false;
            for (var i = 1; i < ordered.Length; i++)
            {
                var before = ordered[i - 1]; var after = ordered[i];
                var beforeBoundary = before.Operation == "receive" ? before.OccurredAt : before.StartedAt;
                var afterBoundary = after.Operation == "receive" ? after.OccurredAt : after.StartedAt;
                if (afterBoundary < beforeBoundary) clockSkew = true;
            }
            if (origin is not null && terminal.OccurredAt < origin.StartedAt) clockSkew = true;
            var status = failure is not null ? "incomplete" : clockSkew ? "clock_skew" : "complete";
            var nodeIds = new List<string>(); var edgeIds = new List<string>();
            Observation? lastInput = null;
            foreach (var step in messaging)
            {
                var service = ServiceId(step);
                var topicName = step.Topic;
                var cluster = step.Cluster;
                if (topicName.Length == 0 && step.Operation == "process" && lastInput is not null)
                { topicName = lastInput.Topic; cluster = InputCluster(step, lastInput); }
                if (topicName.Length == 0) { AddNode(service); continue; }
                var topic = FlowEngine.Id("topic", step.Environment, cluster, topicName);
                if (step.Operation == "publish") { AddNode(service); AddNode(topic); edgeIds.Add(FlowEngine.Id("edge", service, topic, "")); }
                else { AddNode(topic); AddNode(service); edgeIds.Add(FlowEngine.Id("edge", topic, service, step.ConsumerGroup ?? "")); }
                if (step.Operation is "publish" or "receive") lastInput = step;
            }
            var dsm = new Dictionary<string, string>();
            foreach (var step in ordered)
                foreach (var pair in step.DsmAttributes)
                    if (dsm.ContainsKey(pair.Key) || dsm.Count < 16) dsm[pair.Key] = pair.Value;
            var ids = ordered.Select(s => s.Id).Concat(missing).ToArray();
            var id = FlowEngine.Id("pathway", ids);
            var pathway = new FlowPathway(id, origin?.Id, terminal.Id, origin?.Service ?? "unknown", terminal.Service,
                terminal.Environment, origin is null ? null : ServiceId(origin), ServiceId(terminal), nodeIds, edgeIds,
                origin?.StartedAt, terminal.OccurredAt, status == "complete" ? terminal.OccurredAt - origin!.StartedAt : null,
                status, failure, assisted ? "record_assisted" : "otel_parent_links",
                ordered.Any(s => s.IsBatch || s.Operation is "process" or "receive" && MessageLinks(s) > 1), ordered.Any(s => s.Error),
                ordered.Select(s => new PathwayStep(s.Id, s.TraceId, s.SpanId, s.Operation, s.Service,
                    s.Operation == "internal" ? null : ServiceId(s), s.Topic, s.Cluster, s.ConsumerGroup, s.StartedAt, s.OccurredAt, s.Error)).ToArray(),
                missing.Distinct().ToArray(), dsm);
            if (!result.ContainsKey(id))
            {
                var bytes = JsonSerializer.SerializeToUtf8Bytes(pathway).Length + 1;
                if (outputBytes + bytes > options.MaxPathwayOutputBytes) { outputLimited = true; truncatedPaths++; return; }
                outputBytes += bytes;
            }
            result[id] = pathway;
            void AddNode(string value) { if (nodeIds.Count == 0 || nodeIds[^1] != value) nodeIds.Add(value); }
        }
    }

    public CausalityStats Stats => new(spans.Count, relations, evictions, truncatedRelations, incomplete, skewed, duplicates, truncatedPaths, processed);

    public IEnumerable<(Observation Process, Observation Input, long At)> MultiTopicInputs()
    {
        foreach (var id in multiTopicProcesses)
        {
            if (!spans.TryGetValue(id, out var process)) continue;
            var unique = new HashSet<string>();
            var references = process.Span.Parent is { } parent ? process.Span.Links.Prepend(parent) : process.Span.Links;
            foreach (var reference in references)
            {
                if (!spans.TryGetValue(reference.Id, out var input) || input.Span.Topic.Length == 0
                    || input.Span.Operation is not ("publish" or "receive" or "create") || !Compatible(process.Span, input.Span)) continue;
                var key = FlowEngine.Id("topic", process.Span.Environment, InputCluster(process.Span, input.Span), input.Span.Topic);
                if (unique.Add(key)) yield return (process.Span, input.Span, process.At);
            }
        }
    }
    public static string InputCluster(Observation process, Observation input) => input.Cluster is "" or "unknown" ? process.Cluster : input.Cluster;

    private IEnumerable<Relation> Upstream(Observation span)
    {
        var incoming = span.Operation is "process" or "receive";
        var hasMessageLink = incoming && span.Links.Any(l => spans.TryGetValue(l.Id, out var linked)
            && linked.Span.Operation is "publish" or "receive" or "create" && Compatible(span, linked.Span));
        var hasMessageParent = incoming && span.Parent is { } messageParent && spans.TryGetValue(messageParent.Id, out var parentEntry)
            && parentEntry.Span.Operation is "publish" or "receive" or "process" or "create" && Compatible(span, parentEntry.Span);
        var yielded = new HashSet<string>();
        if (span.Parent is { } parent)
        {
            // Standard processing can have an ambient HTTP parent and a separate producer link.
            if (!(hasMessageLink && spans.TryGetValue(parent.Id, out var entry) && entry.Span.Operation == "internal"))
                if (yielded.Add(parent.Id)) yield return new(parent.Id, false);
        }
        foreach (var link in span.Links)
        {
            if (hasMessageLink && spans.TryGetValue(link.Id, out var entry) && entry.Span.Operation == "internal") continue;
            if (yielded.Add(link.Id)) yield return new(link.Id, false);
        }
        // Immutable Kafka coordinates can assist a missing trace relationship; ambiguous records never guess.
        if (incoming && span.Links.Count == 0 && !hasMessageLink && !hasMessageParent && RecordKey(span) is { } key
            && recordProducers.TryGetValue(key, out var producers) && producers.Count == 1)
        {
            var id = producers.First();
            if (yielded.Add(id)) yield return new(id, true);
        }
    }

    private int MessageLinks(Observation span) => span.Links.Count(l => spans.TryGetValue(l.Id, out var entry)
        && entry.Span.Operation is "publish" or "receive" or "create" && Compatible(span, entry.Span));
    private static bool Compatible(Observation child, Observation parent)
    {
        if (child.Environment != parent.Environment) return false;
        if (child.Operation is not ("receive" or "process") || parent.Operation is not ("publish" or "create" or "receive")) return true;
        if (child.Topic.Length > 0 && parent.Topic.Length > 0 && child.Topic != parent.Topic) return false;
        if (child.Cluster is not ("" or "unknown") && parent.Cluster is not ("" or "unknown") && child.Cluster != parent.Cluster) return false;
        return RecordKey(child) is not { } left || RecordKey(parent) is not { } right || left == right;
    }
    private static string? RecordKey(Observation span) => span.Cluster is "" or "unknown" || span.Topic.Length == 0 || span.Partition is null || span.Offset is null
        ? null : FlowEngine.Id("record", span.Environment, span.Cluster, span.Topic, span.Partition, span.Offset);
    private static string ServiceId(Observation span) => FlowEngine.Id("service", span.Environment, span.ServiceNamespace, span.Service);
    private sealed record Entry(Observation Span, long At, int Relations);
    private sealed record Relation(string Id, bool RecordAssisted);
}
