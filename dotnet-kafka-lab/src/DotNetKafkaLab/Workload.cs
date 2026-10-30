using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using LiveFlow.Kafka;

namespace DotNetKafkaLab;

public static class Workload
{
    public static readonly string[] ConsumerRoles = ["router", "payment", "inventory", "retry", "joiner", "fulfillment", "notifications", "analytics", "deadletter"];
    public static readonly string[] TopicSuffixes = ["orders", "payment.requests", "inventory.requests", "payment.retry", "payment.dlq", "branch.results", "ready"];

    public static async Task InitializeAsync(Options options, CancellationToken token)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        { BootstrapServers = Options.BootstrapServers, ClientId = "dotnet-lab-admin", SocketTimeoutMs = 10000 }).Build();
        var cluster = await admin.DescribeClusterAsync().WaitAsync(TimeSpan.FromSeconds(15), token);
        if (cluster.ClusterId != Options.ClusterId)
            throw new InvalidOperationException("Port 19092 does not contain this lab's expected cluster; no topics were changed.");
        try
        {
            await admin.CreateTopicsAsync(TopicSuffixes.Select(suffix => new TopicSpecification
            {
                Name = options.Topic(suffix), NumPartitions = 1, ReplicationFactor = 1,
                Configs = new Dictionary<string, string> { ["retention.ms"] = "7200000" }
            })).WaitAsync(TimeSpan.FromSeconds(20), token);
        }
        catch (CreateTopicsException error) when (error.Results.All(result => result.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists)) { }
        Console.WriteLine("Isolated lab cluster verified; seven demo topics ready.");
    }

    public static async Task RunWorkerAsync(Options options, CancellationToken token)
    {
        var stats = new Statistics { Role = options.Role, RunId = options.RunId };
        Directory.CreateDirectory(options.RunDirectory);
        using var telemetry = new WorkerTelemetry(options, stats);
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = Options.BootstrapServers, ClientId = $"dotnet-lab-{options.Role}",
            Acks = Acks.All, EnableIdempotence = true, MessageTimeoutMs = 10000
        }).Build();
        try
        {
            if (options.Role == "source") await SourceAsync(options, stats, telemetry, producer, token);
            else await ConsumerAsync(options, stats, telemetry, producer, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception error) { stats.Error = error.GetType().Name; throw; }
        finally
        {
            producer.Flush(TimeSpan.FromSeconds(5));
            stats.Finished = true;
            stats.Write(options, force: true);
            Console.WriteLine($"{options.Role}: observed={stats.Processed}, unique={stats.UniqueOrders}, published={stats.Published}, joins={stats.JoinSpans}");
        }
    }

    private static async Task PublishAsync(Options options, Statistics stats, WorkerTelemetry telemetry,
        IProducer<string, string> producer, string suffix, DemoEvent value, CancellationToken token)
    {
        await telemetry.Flow.ProduceAsync(producer, options.Topic(suffix), new Message<string, string>
            { Key = value.OrderId, Value = value.Serialize() }, cancellationToken: token);
        lock (stats) stats.Published++;
    }

    private static async Task SourceAsync(Options options, Statistics stats, WorkerTelemetry telemetry,
        IProducer<string, string> producer, CancellationToken token)
    {
        File.WriteAllText(options.ReadyFile(options.Role), "ready");
        for (var sequence = 1; options.Count == 0 || sequence <= options.Count; sequence++)
        {
            token.ThrowIfCancellationRequested();
            var order = DemoEvent.Order(options.RunId, sequence);
            using (var submission = telemetry.Work.StartActivity("submit order", ActivityKind.Internal))
            {
                submission?.SetTag("messaging.message.conversation_id", order.OrderId);
                await PublishAsync(options, stats, telemetry, producer, "orders", order, token);
            }
            stats.ObserveOrder(order.OrderId); stats.Write(options);
            await Task.Delay(TimeSpan.FromSeconds(1 / options.Rate), token);
        }
    }

    private static string[] Inputs(Options options) => options.Role switch
    {
        "router" => [options.Topic("orders")], "payment" => [options.Topic("payment.requests")],
        "inventory" => [options.Topic("inventory.requests")], "retry" => [options.Topic("payment.retry")],
        "joiner" => [options.Topic("branch.results")], "deadletter" => [options.Topic("payment.dlq")],
        "fulfillment" or "notifications" or "analytics" => [options.Topic("ready")],
        _ => throw new ArgumentException("Unknown worker role")
    };

    private static async Task ConsumerAsync(Options options, Statistics stats, WorkerTelemetry telemetry,
        IProducer<string, string> producer, CancellationToken token)
    {
        var group = options.Group(options.Role);
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = Options.BootstrapServers, GroupId = group, ClientId = $"dotnet-lab-{options.Role}",
            AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false, EnableAutoOffsetStore = false,
            SessionTimeoutMs = 10000, SocketTimeoutMs = 10000
        }).SetPartitionsAssignedHandler((_, partitions) =>
        {
            if (partitions.Count > 0) File.WriteAllText(options.ReadyFile(options.Role), "ready");
        }).Build();
        consumer.Subscribe(Inputs(options));
        var pending = new FanInBuffer();
        var emittedJoins = new HashSet<string>();
        try
        {
            while (!token.IsCancellationRequested)
            {
                var polled = Poll(options, telemetry, consumer, group);
                var record = polled.Record;
                if (record?.Message is null) { stats.PendingJoins = pending.Count; pending.Expire(DateTimeOffset.UtcNow); stats.Write(options); continue; }
                var value = polled.Event!;
                if (value.RunId != options.RunId) { consumer.Commit(record); continue; }
                stats.ObserveOrder(value.OrderId);
                if (record.Message.Headers?.Any(header => header.Key == FlowInstrumentation.PropagationHeader) == true) stats.PrivateHeaders++;
                if (record.Message.Headers?.Any(header => header.Key == "traceparent") == true) stats.StandardHeaders++;
                var received = polled.Received!;
                if (options.Role == "joiner")
                {
                    if (emittedJoins.Contains(value.OrderId)) continue;
                    var joined = pending.Add(new JoinInput(value, received.TraceContext, record.Partition.Value, record.Offset.Value, record.Topic), DateTimeOffset.UtcNow);
                    stats.PendingJoins = pending.Count;
                    if (joined is not null)
                    {
                        await JoinAsync(options, stats, telemetry, producer, joined, token);
                        emittedJoins.Add(value.OrderId);
                    }
                    // Volatile lab join: input offsets remain uncommitted. Restart/replay can duplicate ready events.
                    // A production join needs durable state and coordinated offsets/transactions.
                }
                else
                {
                    // This models an independent background-job boundary in one branch. Its process span
                    // links the producer context; it does not falsely force both branches into one trace.
                    using var inventoryJob = options.Role == "inventory"
                        ? telemetry.Work.StartActivity("inventory background job", ActivityKind.Internal) : null;
                    await telemetry.Flow.ProcessAsync(received, async cancellation =>
                    {
                        Activity.Current?.SetTag("messaging.message.conversation_id", value.OrderId);
                        await HandleAsync(options, stats, telemetry, producer, value, cancellation);
                    }, token);
                    consumer.Commit(record);
                }
                stats.Write(options);
            }
        }
        finally { consumer.Close(); }
    }

    private static (ConsumeResult<string, string>? Record, DemoEvent? Event, ReceivedMessage<string, string>? Received)
        Poll(Options options, WorkerTelemetry telemetry, IConsumer<string, string> consumer, string group)
    {
        // Measure the actual blocking poll. Null/old-run polls have no Kafka observation attributes.
        using var receive = telemetry.Work.StartActivity("poll Kafka", ActivityKind.Client);
        var record = consumer.Consume(TimeSpan.FromMilliseconds(150));
        if (record?.Message is null) return (record, null, null);
        var value = DemoEvent.Deserialize(record.Message.Value);
        if (value.RunId != options.RunId) return (record, value, null);
        var observed = telemetry.Flow.ObserveReceived(record, group);
        if (receive is not null) receive.DisplayName = $"poll {record.Topic}";
        receive?.SetTag("messaging.system", "kafka");
        receive?.SetTag("messaging.operation.name", "poll");
        receive?.SetTag("messaging.operation.type", "receive");
        receive?.SetTag("messaging.destination.name", record.Topic);
        receive?.SetTag("messaging.consumer.group.name", group);
        receive?.SetTag("messaging.kafka.cluster.id", Options.ClusterId);
        receive?.SetTag("messaging.destination.partition.id", record.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        receive?.SetTag("messaging.kafka.offset", record.Offset.Value);
        if (observed.TraceContext.TraceId != default) receive?.AddLink(new ActivityLink(observed.TraceContext));
        return (record, value, observed); // Dispose ends the receive span before any handler/join work begins.
    }

    private static async Task HandleAsync(Options options, Statistics stats, WorkerTelemetry telemetry,
        IProducer<string, string> producer, DemoEvent value, CancellationToken token)
    {
        switch (options.Role)
        {
            case "router":
                await Task.Delay(8, token);
                await Task.WhenAll(
                    PublishAsync(options, stats, telemetry, producer, "payment.requests", value with { Stage = "payment-request" }, token),
                    PublishAsync(options, stats, telemetry, producer, "inventory.requests", value with { Stage = "inventory-request" }, token));
                break;
            case "payment":
                await Task.Delay(20, token);
                var route = Routing.Payment(value.Sequence);
                if (route == PaymentRoute.DeadLetter)
                { stats.DeadLetter++; await PublishAsync(options, stats, telemetry, producer, "payment.dlq", value with { Stage = "payment-dlq" }, token); }
                else if (route == PaymentRoute.Retry)
                { stats.Retry++; await PublishAsync(options, stats, telemetry, producer, "payment.retry", value with { Stage = "payment-retry" }, token); }
                else await PublishAsync(options, stats, telemetry, producer, "branch.results", value with { Stage = "payment" }, token);
                break;
            case "retry":
                await Task.Delay(120, token);
                await PublishAsync(options, stats, telemetry, producer, "branch.results", value with { Stage = "payment", Attempt = 2 }, token);
                break;
            case "inventory":
                await Task.Delay(30 + value.Sequence % 3 * 20, token);
                await PublishAsync(options, stats, telemetry, producer, "branch.results", value with { Stage = "inventory" }, token);
                break;
            default:
                await Task.Delay(options.Role == "fulfillment" ? 45 : 15, token);
                break;
        }
    }

    private static async Task JoinAsync(Options options, Statistics stats, WorkerTelemetry telemetry,
        IProducer<string, string> producer, JoinedOrder joined, CancellationToken token)
    {
        var tags = new ActivityTagsCollection
        {
            { "messaging.system", "kafka" }, { "messaging.operation.name", "join" }, { "messaging.operation.type", "process" },
            { "messaging.destination.name", options.Topic("branch.results") }, { "messaging.consumer.group.name", options.Group("joiner") },
            { "messaging.kafka.cluster.id", Options.ClusterId }, { "messaging.batch.message_count", 2 },
            { "messaging.message.conversation_id", joined.Payment.Event.OrderId }
        };
        // No ambient Activity in the consume loop: this is a new trace with TWO real causal links.
        using var process = telemetry.Work.StartActivity($"join {options.Topic("branch.results")}", ActivityKind.Consumer,
            default(ActivityContext), tags, joined.Links);
        await Task.Delay(15, token);
        await PublishAsync(options, stats, telemetry, producer, "ready", joined.Payment.Event with { Stage = "ready" }, token);
    }
}
