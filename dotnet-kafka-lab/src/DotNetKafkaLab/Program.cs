using System.Diagnostics;
using System.Reflection;

namespace DotNetKafkaLab;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            var options = Options.Parse(args);
            if (options.Command is "help" or "--help")
            {
                Console.WriteLine("DotNetKafkaLab: init | run | smoke | worker --role NAME");
                Console.WriteLine("Options: --rate 1 --count 0 --duration 90 --run-id ID --prefix dotnet-lab --no-export --export-metrics");
                Console.WriteLine("Kafka is locked to 127.0.0.1:19092 and verified against this lab's cluster ID before topic creation.");
                Console.WriteLine("Export uses standard OTEL_* configuration inherited in memory; no credentials are logged or saved.");
                return 0;
            }
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(options.DurationSeconds + (options.Command == "worker" ? 0 : 45)));
            Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; cancellation.Cancel(); };
            var monitor = MonitorStopAsync(options.StopFile, cancellation);
            try
            {
                switch (options.Command)
                {
                    case "init": await Workload.InitializeAsync(options, cancellation.Token); break;
                    case "worker": await Workload.RunWorkerAsync(options, cancellation.Token); break;
                    case "run": await RunAllAsync(options, smoke: false, cancellation.Token); break;
                    case "smoke":
                        options = options with { Export = false, Count = options.Count == 0 ? 24 : options.Count };
                        await RunAllAsync(options, smoke: true, cancellation.Token); break;
                    default: throw new ArgumentException("Unknown command; use --help");
                }
            }
            finally { cancellation.Cancel(); await monitor; }
            return 0;
        }
        catch (OperationCanceledException) { Console.WriteLine("Lab stopped; workers were asked to drain and dispose."); return 0; }
        catch (Exception error) { Console.Error.WriteLine($"Lab failed ({error.GetType().Name}). See the local worker summary/error type; no configuration values were printed."); return 1; }
    }

    private static async Task MonitorStopAsync(string stopFile, CancellationTokenSource cancellation)
    {
        try
        {
            while (!cancellation.IsCancellationRequested)
            {
                if (stopFile.Length > 0 && File.Exists(stopFile)) { cancellation.Cancel(); break; }
                await Task.Delay(200, cancellation.Token);
            }
        }
        catch (OperationCanceledException) { }
    }

    private static Process StartWorker(Options options, string role, int duration)
    {
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true,
            WorkingDirectory = Directory.GetCurrentDirectory()
        };
        foreach (var argument in new[]
        {
            Assembly.GetExecutingAssembly().Location, "worker", "--role", role, "--run-id", options.RunId,
            "--prefix", options.Prefix, "--rate", options.Rate.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--count", options.Count.ToString(), "--duration", duration.ToString(), "--state-dir", options.StateDirectory,
            "--stop-file", Path.Combine(options.RunDirectory, "stop.request")
        }) start.ArgumentList.Add(argument);
        if (!options.Export) start.ArgumentList.Add("--no-export");
        if (options.ExportMetrics) start.ArgumentList.Add("--export-metrics");
        var process = new Process { StartInfo = start };
        process.OutputDataReceived += (_, output) => { if (output.Data is not null) Console.WriteLine($"[{role}] {output.Data}"); };
        process.ErrorDataReceived += (_, output) => { if (output.Data is not null) Console.Error.WriteLine($"[{role}] {output.Data}"); };
        process.Start(); process.BeginOutputReadLine(); process.BeginErrorReadLine();
        return process;
    }

    public static async Task RunAllAsync(Options options, bool smoke, CancellationToken token)
    {
        if (Directory.Exists(options.RunDirectory) && Directory.EnumerateFileSystemEntries(options.RunDirectory).Any())
            throw new InvalidOperationException("This run ID already has state. Choose a fresh --run-id; no files were replaced.");
        Directory.CreateDirectory(options.RunDirectory);
        await Workload.InitializeAsync(options, token);
        var processes = new List<(string Role, Process Process)>();
        var expected = options.Count - Routing.DeadLetters(options.Count);
        var completed = false;
        try
        {
            foreach (var role in Workload.ConsumerRoles) processes.Add((role, StartWorker(options, role, Math.Min(3600, options.DurationSeconds + 30))));
            var readiness = Stopwatch.StartNew();
            while (Workload.ConsumerRoles.Any(role => !File.Exists(options.ReadyFile(role))))
            {
                token.ThrowIfCancellationRequested(); CheckFailures(processes);
                if (readiness.Elapsed > TimeSpan.FromSeconds(30)) throw new TimeoutException("Consumers did not receive partition assignments");
                await Task.Delay(200, token);
            }
            Console.WriteLine($"Nine consumer groups assigned. Starting {options.Rate:0.##} orders/sec for run {options.RunId}.");
            var source = StartWorker(options, "source", options.DurationSeconds);
            processes.Add(("source", source));
            var active = Stopwatch.StartNew();
            while (active.Elapsed < TimeSpan.FromSeconds(options.DurationSeconds))
            {
                token.ThrowIfCancellationRequested(); CheckFailures(processes);
                if (options.Count > 0 && source.HasExited && new[] { "fulfillment", "notifications", "analytics" }
                    .All(role => Statistics.Read(options.ProgressFile(role))?.UniqueOrders == expected)
                    && Statistics.Read(options.ProgressFile("deadletter"))?.UniqueOrders == Routing.DeadLetters(options.Count))
                { completed = true; break; }
                await Task.Delay(500, token);
            }
            if (smoke && !completed) throw new TimeoutException("Finite workload did not reach all expected destinations");
        }
        finally
        {
            File.WriteAllText(Path.Combine(options.RunDirectory, "stop.request"), "stop");
            await Task.WhenAll(processes.Select(async entry =>
            {
                try { await entry.Process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20)); }
                catch (TimeoutException) { if (!entry.Process.HasExited) entry.Process.Kill(entireProcessTree: true); }
            }));
            foreach (var entry in processes) entry.Process.Dispose();
        }
        if (smoke) VerifySmoke(options);
        Console.WriteLine($"Lab finished. Nonsecret worker counters: {options.RunDirectory}");
    }

    private static void CheckFailures(IEnumerable<(string Role, Process Process)> processes)
    {
        foreach (var entry in processes)
            if (entry.Process.HasExited && entry.Process.ExitCode != 0) throw new InvalidOperationException($"Worker {entry.Role} failed");
    }

    private static void VerifySmoke(Options options)
    {
        Statistics Read(string role) => Statistics.Read(options.ProgressFile(role)) ?? throw new InvalidOperationException($"Missing {role} summary");
        void Require(bool condition, string message) { if (!condition) throw new InvalidOperationException(message); }
        var good = options.Count - Routing.DeadLetters(options.Count);
        foreach (var role in new[] { "fulfillment", "notifications", "analytics" })
            Require(Read(role).UniqueOrders == good, $"Incorrect {role} fan-out count");
        Require(Read("source").Published == options.Count && Read("router").Published == options.Count * 2, "Source/router fan-out lost messages");
        Require(Read("retry").UniqueOrders == Routing.Retries(options.Count), "Retry route count incorrect");
        Require(Read("deadletter").UniqueOrders == Routing.DeadLetters(options.Count), "DLQ route count incorrect");
        var join = Read("joiner");
        Require(join.JoinSpans == good && join.TwoInputLinks == good && join.CrossTraceJoins == good && join.RootJoins == good,
            "Fan-in failed to preserve BOTH cross-trace causal links on a new root process span");
        foreach (var role in Workload.ConsumerRoles)
        {
            var stats = Read(role);
            Require(stats.Error is null && stats.PrivateHeaders == 0 && stats.StandardHeaders == stats.Processed,
                "An error, private header, or missing W3C propagation was observed");
            Require(stats.ReceiveSpans == stats.Processed && stats.TimedReceiveSpans == stats.Processed,
                "Actual receive polls were not timed separately from processing");
        }
        Console.WriteLine($"SMOKE PASS: {options.Count} orders; {good} two-input cross-trace joins; {good} messages in EACH ready consumer group; {Routing.Retries(options.Count)} retries; {Routing.DeadLetters(options.Count)} DLQ records; no private headers.");
    }
}
