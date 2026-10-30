using Confluent.Kafka;
using LiveFlow.Kafka;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

if (args.Contains("--help"))
{
    Console.WriteLine("Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, OTEL_SERVICE_NAME and OTEL_EXPORTER_OTLP_ENDPOINT.");
    Console.WriteLine("Set OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf for a Collector on port 4318 (or grpc for port 4317).");
    Console.WriteLine("Run with send (default) or consume. Optional KAFKA_OUTPUT_TOPIC enables consume->send processing.");
    Console.WriteLine("This sample requires Kafka when running; --help does not start a client or exporter.");
    return;
}

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS")
    ?? throw new InvalidOperationException("Set KAFKA_BOOTSTRAP_SERVERS to an existing Kafka broker; see --help.");
var service = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "liveflow-dotnet-sample";
var topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "orders";
var output = Environment.GetEnvironmentVariable("KAFKA_OUTPUT_TOPIC");
var group = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "liveflow-sample";
var resource = ResourceBuilder.CreateDefault().AddService(service);

// Endpoint, protocol, timeout and authentication are configured by normal OTEL_* environment variables.
// The application owns these providers. No custom exporter, checkpoint receiver or Grafana plugin is required.
using var traces = Sdk.CreateTracerProviderBuilder()
    .SetResourceBuilder(resource)
    .AddSource(FlowInstrumentation.ActivitySourceName)
    .AddOtlpExporter()
    .Build();
using var metrics = Sdk.CreateMeterProviderBuilder()
    .SetResourceBuilder(resource)
    .AddMeter(FlowInstrumentation.MeterName)
    .AddOtlpExporter()
    .Build();
using var flow = new FlowInstrumentation(new FlowOptions
{
    ServiceName = service,
    Environment = Environment.GetEnvironmentVariable("DEPLOYMENT_ENVIRONMENT") ?? "dev",
    Cluster = Environment.GetEnvironmentVariable("KAFKA_CLUSTER_ID") ?? "unknown"
});
using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, args) => { args.Cancel = true; cancellation.Cancel(); };
using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

try
{
    if (!args.Contains("consume"))
    {
        await flow.ProduceAsync(producer, topic, new Message<string, string> { Key = "sample", Value = "example" },
            cancellationToken: cancellation.Token);
    }
    else
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap, GroupId = group, EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(topic);
        try
        {
            while (!cancellation.IsCancellationRequested)
            {
                var record = consumer.Consume(cancellation.Token);
                var received = flow.ObserveReceived(record, group);
                await flow.ProcessAsync(received, async token =>
                {
                    // Replace this example with the application's own business handling.
                    Console.WriteLine($"Processing {record.Topic}/{record.Partition}/{record.Offset}");
                    if (!string.IsNullOrWhiteSpace(output))
                        await flow.ProduceAsync(producer, output, new Message<string, string>
                        { Key = record.Message.Key, Value = record.Message.Value }, cancellationToken: token);
                }, cancellation.Token);
                consumer.Commit(record); // Application policy; handler completion alone does not mean an offset was committed.
            }
        }
        finally { consumer.Close(); }
    }
}
catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
finally
{
    // Deterministic shutdown for the short-lived sample; long-running hosts normally manage provider disposal.
    traces.ForceFlush();
    metrics.ForceFlush();
}
