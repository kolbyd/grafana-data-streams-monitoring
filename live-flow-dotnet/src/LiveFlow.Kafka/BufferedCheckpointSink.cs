using System.Threading.Channels;

namespace LiveFlow.Kafka;

/// <summary>A bounded, nonblocking handoff to an application-owned exporter. It does not send telemetry itself.</summary>
public sealed class BufferedCheckpointSink : ICheckpointSink
{
    private readonly Channel<FlowCheckpoint> _channel;
    public BufferedCheckpointSink(int capacity = 10_000)
    {
        if (capacity is < 1 or > 100_000) throw new ArgumentOutOfRangeException(nameof(capacity));
        _channel = Channel.CreateBounded<FlowCheckpoint>(new BoundedChannelOptions(capacity)
        { FullMode = BoundedChannelFullMode.Wait, AllowSynchronousContinuations = false, SingleReader = false, SingleWriter = false });
    }
    public bool TryWrite(FlowCheckpoint checkpoint) => _channel.Writer.TryWrite(checkpoint);
    public IAsyncEnumerable<FlowCheckpoint> ReadAllAsync(CancellationToken cancellationToken = default)
        => _channel.Reader.ReadAllAsync(cancellationToken);
    public void Complete() => _channel.Writer.TryComplete();
}
