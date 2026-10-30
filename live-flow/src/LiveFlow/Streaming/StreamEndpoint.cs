using System.Net.WebSockets;
using System.Text.Json;
using LiveFlow.Flow;

namespace LiveFlow.Streaming;

public static class StreamEndpoint
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static async Task HandleAsync(HttpContext context)
    {
        var engine = context.RequestServices.GetRequiredService<FlowEngine>();
        var options = context.RequestServices.GetRequiredService<FlowOptions>();
        if (!context.WebSockets.IsWebSocketRequest) { context.Response.StatusCode = 400; return; }
        if (!engine.TryAddViewer()) { context.Response.StatusCode = 429; return; }
        try
        {
            var protocol = context.WebSockets.WebSocketRequestedProtocols.Contains("liveflow.v1") ? "liveflow.v1" : null;
            using var socket = await context.WebSockets.AcceptWebSocketAsync(protocol);
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
            var receiver = ReceiveUntilCloseAsync(socket, lifetime);
            try
            {
                var instance = context.Request.Query["instanceId"].ToString();
                var hasCursor = long.TryParse(context.Request.Query["after"], out var cursor);
                var first = hasCursor ? engine.Since(instance, cursor, -1) : engine.Snapshot();
                await SendAsync(socket, first, lifetime.Token);
                cursor = first.Cursor;
                var version = first.TopologyVersion;
                var heartbeat = DateTimeOffset.UtcNow;
                using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.StreamIntervalMs));
                while (socket.State == WebSocketState.Open && await timer.WaitForNextTickAsync(lifetime.Token))
                {
                    var frame = engine.Since(engine.InstanceId, cursor, version);
                    if (frame.Cursor != cursor || frame.TopologyVersion != version || DateTimeOffset.UtcNow - heartbeat >= TimeSpan.FromSeconds(2))
                    {
                        await SendAsync(socket, frame, lifetime.Token);
                        cursor = frame.Cursor; version = frame.TopologyVersion; heartbeat = DateTimeOffset.UtcNow;
                    }
                }
            }
            catch (OperationCanceledException) when (!context.RequestAborted.IsCancellationRequested && !lifetime.IsCancellationRequested)
            { engine.SlowViewer(); }
            finally
            {
                await lifetime.CancelAsync();
                await receiver;
                if (socket.State == WebSocketState.CloseReceived)
                    try { using var close = new CancellationTokenSource(TimeSpan.FromSeconds(1)); await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Closed", close.Token); }
                    catch (Exception ex) when (ex is WebSocketException or OperationCanceledException) { }
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or IOException) { }
        finally { engine.RemoveViewer(); }
    }

    private static async Task SendAsync(WebSocket socket, StreamFrame frame, CancellationToken cancellation)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        await socket.SendAsync(JsonSerializer.SerializeToUtf8Bytes(frame, Json).AsMemory(), WebSocketMessageType.Text, true, deadline.Token);
    }

    private static async Task ReceiveUntilCloseAsync(WebSocket socket, CancellationTokenSource lifetime)
    {
        var buffer = new byte[1024];
        try
        {
            while (!lifetime.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                var result = await socket.ReceiveAsync(buffer.AsMemory(), lifetime.Token);
                if (result.MessageType == WebSocketMessageType.Close || !result.EndOfMessage) break;
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException) { }
        finally { await lifetime.CancelAsync(); }
    }
}
