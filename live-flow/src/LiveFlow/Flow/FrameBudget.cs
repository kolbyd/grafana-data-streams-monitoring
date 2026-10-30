using System.Text.Json;

namespace LiveFlow.Flow;

/// <summary>Bounds the final JSON, without allocating an oversized serialization buffer.</summary>
internal static class FrameBudget
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static StreamFrame Fit(StreamFrame original, int maxBytes, int retainedEvents, long? oldestRetained)
    {
        var frame = original;
        while (true)
        {
            var omittedEvents = (original.ResetEvents ? retainedEvents : original.Events.Count + original.VisualEventsSkipped) - frame.Events.Count;
            var omittedNodes = original.Nodes.Count - frame.Nodes.Count;
            var omittedEdges = original.Edges.Count - frame.Edges.Count;
            var omittedPaths = original.Pathways.Count - frame.Pathways.Count;
            frame = frame with
            {
                VisualEventsSkipped = original.VisualEventsSkipped + original.Events.Count - frame.Events.Count,
                Coverage = new(retainedEvents, frame.Events.Count, oldestRetained,
                    frame.Events.Count > 0 ? frame.Events.Min(e => e.ObservedAt) : null,
                    omittedEvents, omittedNodes, omittedEdges, omittedPaths,
                    omittedEvents > 0 || omittedNodes > 0 || omittedEdges > 0 || omittedPaths > 0)
            };
            if (Fits(frame, maxBytes)) return frame;

            // Keep a contiguous recent tail. Do not keep a matched enrichment after
            // dropping its publication from this snapshot: it cannot supply hop timing.
            if (frame.Events.Count > 0)
            {
                var tail = frame.Events.TakeLast(frame.Events.Count / 2).ToArray();
                if (frame.ResetEvents)
                {
                    var publications = tail.Where(e => e.Kind == "publish").Select(e => e.ObservationId).ToHashSet();
                    tail = tail.Where(e => e.Kind != "matched" || e.ProducerObservationId is not null && publications.Contains(e.ProducerObservationId)).ToArray();
                }
                frame = frame with { Events = tail };
            }
            else if (frame.Pathways.Count > 0)
                frame = frame with { Pathways = frame.Pathways.Take(frame.Pathways.Count / 2).ToArray() };
            else if (frame.Edges.Count > 0)
                frame = frame with { Edges = frame.Edges.Take(frame.Edges.Count / 2).ToArray() };
            else if (frame.Nodes.Count > 0)
                frame = frame with { Nodes = frame.Nodes.Take(frame.Nodes.Count / 2).ToArray() };
            else
                throw new InvalidOperationException("The minimum stream frame exceeds MaxFrameBytes.");
        }
    }

    private static bool Fits(StreamFrame frame, int maxBytes)
    {
        try
        {
            using var counter = new LimitedCounter(maxBytes);
            JsonSerializer.Serialize(counter, frame, Json);
            return true;
        }
        catch (BudgetExceededException) { return false; }
    }

    private sealed class BudgetExceededException : Exception;

    private sealed class LimitedCounter(int maxBytes) : Stream
    {
        private long count;
        public override void Write(byte[] buffer, int offset, int size) => Add(size);
        public override void Write(ReadOnlySpan<byte> buffer) => Add(buffer.Length);
        private void Add(int size) { count += size; if (count > maxBytes) throw new BudgetExceededException(); }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => count;
        public override long Position { get => count; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
