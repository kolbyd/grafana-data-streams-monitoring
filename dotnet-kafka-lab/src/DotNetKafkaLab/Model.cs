using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace DotNetKafkaLab;

public sealed record Options
{
    public const string ClusterId = "ZHNtbGFiRG90bmV0VGVzdA";
    public const string BootstrapServers = "127.0.0.1:19092";
    public string Command { get; init; } = "run";
    public string Role { get; init; } = "";
    public string RunId { get; init; } = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
    public string Prefix { get; init; } = "dotnet-lab";
    public double Rate { get; init; } = 1;
    public int Count { get; init; } = 0;
    public int DurationSeconds { get; init; } = 90;
    public bool Export { get; init; } = true;
    public bool ExportMetrics { get; init; }
    public string StateDirectory { get; init; } = Path.GetFullPath(".lab-state");
    public string StopFile { get; init; } = "";
    public string Topic(string suffix) => $"{Prefix}.{suffix}";
    public string Group(string role) => $"{Prefix}-{role}-{RunId}";
    public string RunDirectory => Path.Combine(StateDirectory, RunId);
    public string ProgressFile(string role) => Path.Combine(RunDirectory, $"{role}.json");
    public string ReadyFile(string role) => Path.Combine(RunDirectory, $"{role}.ready");

    public static Options Parse(string[] args)
    {
        var result = new Options { Command = args.FirstOrDefault() ?? "help" };
        for (var i = 1; i < args.Length; i++)
        {
            var option = args[i];
            if (option == "--no-export") { result = result with { Export = false }; continue; }
            if (option == "--export-metrics") { result = result with { ExportMetrics = true }; continue; }
            if (++i == args.Length) throw new ArgumentException($"Missing value for {option}");
            var value = args[i];
            result = option switch
            {
                "--role" => result with { Role = value }, "--run-id" => result with { RunId = value },
                "--prefix" => result with { Prefix = value }, "--rate" => result with { Rate = double.Parse(value, System.Globalization.CultureInfo.InvariantCulture) },
                "--count" => result with { Count = int.Parse(value) }, "--duration" => result with { DurationSeconds = int.Parse(value) },
                "--state-dir" => result with { StateDirectory = Path.GetFullPath(value) },
                "--stop-file" => result with { StopFile = Path.GetFullPath(value) },
                _ => throw new ArgumentException($"Unknown option {option}")
            };
        }
        if (!Regex.IsMatch(result.RunId, "^[a-zA-Z0-9-]{1,64}$") || !Regex.IsMatch(result.Prefix, "^[a-zA-Z0-9-]{1,64}$"))
            throw new ArgumentException("Run ID and topic prefix must be 1-64 ASCII letters, digits or hyphens.");
        if (result.Rate is < 0.1 or > 20 || !double.IsFinite(result.Rate) || result.Count is < 0 or > 100000 || result.DurationSeconds is < 10 or > 3600)
            throw new ArgumentException("Rate must be 0.1-20/sec; count 0-100000; duration 10-3600 seconds.");
        return result;
    }
}

public sealed record DemoEvent(string RunId, int Sequence, string OrderId, string Stage, int Attempt = 1)
{
    public static DemoEvent Order(string runId, int sequence) => new(runId, sequence, $"{runId}-order-{sequence:D6}", "order");
    public string Serialize() => JsonSerializer.Serialize(this);
    public static DemoEvent Deserialize(string text) => JsonSerializer.Deserialize<DemoEvent>(text) ?? throw new InvalidOperationException("Invalid lab event");
}

public enum PaymentRoute { Result, Retry, DeadLetter }
public static class Routing
{
    public static PaymentRoute Payment(int sequence) => sequence % 11 == 0 ? PaymentRoute.DeadLetter
        : sequence % 7 == 0 ? PaymentRoute.Retry : PaymentRoute.Result;
    public static int DeadLetters(int count) => Enumerable.Range(1, count).Count(sequence => Payment(sequence) == PaymentRoute.DeadLetter);
    public static int Retries(int count) => Enumerable.Range(1, count).Count(sequence => Payment(sequence) == PaymentRoute.Retry);
}

public sealed record JoinInput(DemoEvent Event, ActivityContext CreationContext, int Partition, long Offset, string Topic);
public sealed record JoinedOrder(JoinInput Payment, JoinInput Inventory)
{
    public ActivityLink[] Links => [new(Payment.CreationContext), new(Inventory.CreationContext)];
}

/// <summary>Bounded, volatile demo join. It is deliberately not a durable stream-processing engine.</summary>
public sealed class FanInBuffer(int capacity = 10000, TimeSpan? ttl = null)
{
    private sealed class Pending(DateTimeOffset at) { public DateTimeOffset At = at; public JoinInput? Payment; public JoinInput? Inventory; }
    private readonly Dictionary<string, Pending> _pending = new();
    private readonly TimeSpan _ttl = ttl ?? TimeSpan.FromSeconds(60);
    public int Count => _pending.Count;
    public int Expire(DateTimeOffset now)
    {
        var keys = _pending.Where(entry => now - entry.Value.At > _ttl).Select(entry => entry.Key).ToArray();
        foreach (var key in keys) _pending.Remove(key);
        return keys.Length;
    }
    public JoinedOrder? Add(JoinInput input, DateTimeOffset now)
    {
        Expire(now);
        if (!_pending.TryGetValue(input.Event.OrderId, out var item))
        {
            if (_pending.Count >= capacity) throw new InvalidOperationException("Demo join capacity exceeded");
            _pending.Add(input.Event.OrderId, item = new Pending(now));
        }
        switch (input.Event.Stage)
        {
            case "payment": item.Payment ??= input; break;
            case "inventory": item.Inventory ??= input; break;
            default: throw new ArgumentException("Join accepts payment/inventory results only");
        }
        if (item.Payment is null || item.Inventory is null) return null;
        _pending.Remove(input.Event.OrderId);
        return new JoinedOrder(item.Payment, item.Inventory);
    }
}
