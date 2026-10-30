# Live Flow

A .NET 10 service that turns standard OpenTelemetry Kafka spans into an automatically discovered producer → topic → consumer map, correlated observations, and a low-latency WebSocket stream. Includes a local animated viewer for review. No Kafka connection or Kafka credentials are required.

**This delivery is a working local service, not an installed Grafana plugin or a deployment to your Kafka environment.** Its Grafana integration boundary is documented below.

## Review it

Prerequisite: the .NET 10 SDK. From this directory:

```powershell
.\start-review.ps1
```

Or, on Windows, macOS, or Linux:

```sh
dotnet run --project src/LiveFlow --no-launch-profile
```

Open [http://127.0.0.1:4318](http://127.0.0.1:4318) and choose **Start demo**. The demo is off by default. It sends synthetic OTLP JSON through the same ingestion endpoint used by real clients, in environment `demo` and cluster `demo-kafka`. It does not connect to Kafka. Stop demo stops new generation; existing demo observations remain labeled until retention expires or the service restarts.

The viewer supports automatic directed layout (including cycles), consumer-group edge labels, moving observation particles, pause motion, a short replay, service/topic filtering, environment/cluster filtering, node and observation details, and reconnect recovery. A displayed particle represents an observed operation; travel time is illustrative. The feed displays the operation completion timestamp. It does not simulate where a message physically is inside Kafka.

A command-line demo is also included (Python 3, standard library only):

```sh
python tools/send_demo.py --duration 30 --rate 3
```

## Connect your .NET applications

Install OpenTelemetry .NET Automatic Instrumentation using its official platform-specific instructions. Its supported Confluent.Kafka versions must match your applications; this project does not install an agent into your processes.

See [examples/dotnet-auto.env](examples/dotnet-auto.env) for the exporter settings. The direct local destination is:

```text
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
OTEL_BSP_SCHEDULE_DELAY=250
```

Set stable resource identities, for example:

```text
OTEL_SERVICE_NAME=payments
OTEL_RESOURCE_ATTRIBUTES=service.namespace=shop,deployment.environment.name=prod,kafka.cluster.id=cluster-a
```

Do not send a containerized or remote application's traffic to its own `127.0.0.1`; use the reachable collector/service address. Restart the application after changing startup instrumentation settings.

If your apps already export through Alloy or another collector, **preserve that route** and add a fan-out branch to Live Flow. [examples/alloy.alloy](examples/alloy.alloy) and [examples/collector.yaml](examples/collector.yaml) provide local bridge examples. Their receivers use HTTP port 4319 to avoid the Live Flow service's port 4318. An OTLP/HTTP exporter appends `/v1/traces` to its base endpoint.

The service accepts **OTLP/HTTP binary protobuf and OTLP JSON**, including gzip. It does not expose native OTLP/gRPC; the included collector bridge handles gRPC clients. The protocol schemas are vendored from OpenTelemetry Proto v1.11.1 and compiled during build.

## What is collected

Only Kafka `publish`/`send`/`produce` and `receive`/`consume`/`poll` boundaries become wire observations. Producer/consumer span kinds provide a fallback when an operation attribute is missing. `process` spans are retained as distinct processing phases; they update an additive `processed` count and incoming topology but never synthesize a receive event or increment receive/publish counts. `create`, settle-like and non-Kafka spans can be retained as bounded ancestry bridges without becoming wire observations. The legacy `ignored` count still means “not a wire observation”; it does not imply that useful parent/link metadata was discarded.

Old .NET auto-instrumentation attributes and newer messaging attributes are normalized:

| Meaning | Accepted attributes |
| --- | --- |
| Service | Resource `service.name`; resource `service.namespace` is preserved |
| Environment | `deployment.environment.name`, `deployment.environment`, `env` |
| Cluster | `messaging.kafka.cluster.id`, `kafka.cluster.id`, `kafka.cluster.name`, `liveflow.cluster`; otherwise configured default |
| Topic | `messaging.destination.name`, `messaging.destination` |
| Operation | `messaging.operation.type`, `messaging.operation.name`, `messaging.operation` |
| Consumer group | `messaging.consumer.group.name`, `messaging.kafka.consumer.group`, `messaging.kafka.consumer_group` |
| Partition | `messaging.destination.partition.id`, `messaging.kafka.destination.partition` |
| Offset | `messaging.kafka.offset`, `messaging.kafka.message.offset` |

Required wire-observation fields are a resource service name, topic, valid nonzero trace/span IDs, and valid completion timestamps. Rejected eligible wire spans produce an OTLP partial-success response. Other valid spans can enrich ancestry without affecting wire counters. Offsets remain decimal **strings** throughout the API, including values above JavaScript's safe integer range. No payloads, message keys or arbitrary resource/span attributes are retained. A small allowlist of bounded optional `dsm.*` scalar metadata can enrich pathway details; no private header or checkpoint intake is required.

Set a correct cluster identity. `unknown` deliberately disables offset-only correlation. For one process talking to multiple Kafka clusters, a single resource-wide default is insufficient: add span-level cluster metadata or a collector/client mapping first. Identity labels longer than 256 characters are rejected rather than truncated, preventing accidental identity collisions.

## Correlation and honest limits

- Kafka record coordinates `(environment, cluster, topic, partition, offset)` are preferred. A single explicit producer span link is the fallback. Conflicting record coordinates cannot be overridden by a link.
- Receive-before-produce arrivals are supported. Separate consumer groups can match the same publication. Replayed consumption is another receive observation; it is not treated as another publication.
- Retransmitted exports are deduplicated by `(traceId, spanId)` within the bounded deduplication retention/capacity window. Dedupe is not permanent or durable.
- Failed operations and declared batches are displayed but excluded from individual-message matching. Multi-link receives without record coordinates are left uncorrelated.
- A match proves an observed Kafka hop. It does **not** prove that an output message was causally produced from an input message through business processing. The base instrumentation does not capture business completion after `Consume` returns.
- Cached publications and waiting receives are correlation state, **not broker backlog**. Broker lag is not measured by this service.
- The live branch should precede collector-side tail sampling. It cannot restore spans discarded by application-side sampling. Missing observations, expired entries, dropped exports, or an uninstrumented service reduce coverage.
- The live service sees completed operations after SDK/collector export delays. This is near-real-time observation, not a wire tap. Clock differences affect event timestamps and displayed export delay.

## Optional standard-OTel causal pathways

The processor also retains parent IDs and every valid span link, including cross-trace links, within global span/relation limits. A retained send → process → send → process chain can produce an observed pathway range. Its duration is **the earliest retained send span's start to the terminal processing span's completion**, never a sum of unrelated hop durations or percentiles. A receive span alone does not establish business processing. Linked batches remain explicitly marked as batch dependencies and do not multiply wire counts.

Each frame includes bounded, replace-on-frame `pathways` and `causality` fields. Only `status: complete` carries `durationMs`; missing processing/ancestors, conflicting scope or clock skew suppress a value. An unknown non-messaging parent before the first observed send is an observation boundary, not proof that an earlier Kafka stage is missing. A shorter retained range can expand when late spans arrive, so this is not a claim that uninstrumented work before/after the displayed range is absent. Sampling, retention and derivation/output budgets limit coverage.

See [docs/otel-causality.md](docs/otel-causality.md) for the exact contract, bounds, fixtures and extension allowlist. This is an optional advanced processor: native [Tempo service graphs](https://grafana.com/docs/tempo/latest/metrics-from-traces/service_graphs/) already support producer/consumer service edges and an opt-in messaging-system latency histogram. Standard Grafana/Tempo/Prometheus queries can be used without this service. This processor adds its own topic-oriented live protocol and retained multi-hop evidence; it does not replace Tempo or assert that standard Grafana cannot show messaging relationships.

## API

| Endpoint | Behavior |
| --- | --- |
| `POST /v1/traces` | OTLP/HTTP protobuf or JSON, optional gzip |
| `GET /api/topology` | Full topology, recent events, cursor, and counters |
| `GET /api/causality/spans/{traceId}/{spanId}` | One retained normalized span with its parent and all valid links; 404 after eviction/expiry |
| `GET /api/events?instanceId=...&after=...` | Cursor-based event retrieval plus full topology |
| `GET /api/stream` | WebSocket snapshot followed by incremental frames |
| `GET /api/status` | Instance identity, limits relevant to the viewer, and counters |
| `GET /metrics` | Prometheus service counters and state gauges |
| `GET /healthz` | Process liveness; does not prove telemetry is arriving |

See [docs/stream-protocol.md](docs/stream-protocol.md) for reconnect, reset, and authentication behavior, and [docs/grafana-integration.md](docs/grafana-integration.md) for the Grafana boundary.

## Authentication and hosting

The default listener is loopback-only and anonymous for local review. Binding beyond loopback requires `Flow__ApiKey`; startup fails if the key is missing. For example, after choosing a secret in your shell:

```powershell
$env:Flow__ApiKey = $env:LIVEFLOW_API_KEY
dotnet run --project src/LiveFlow --no-launch-profile -- --urls http://0.0.0.0:4318
```

All ingestion, data API, streaming, and metrics endpoints then require `Authorization: Bearer ...`. The static viewer remains readable and its connection settings accept the key in tab memory. Browser WebSocket credentials use a subprotocol, not a URL query parameter. Do not log credential-bearing WebSocket protocol headers at a proxy.

Use HTTPS through your ingress/reverse proxy when connecting across a network. This service does not configure certificates. Browser origins are restricted to the service's own origin unless explicitly added with `Flow__AllowedOrigins__0`, etc. For Grafana-originated browser WebSockets, add the exact Grafana origin. For TLS-terminating proxies, explicitly configure the public allowed origin; arbitrary forwarded headers are not trusted. A shared API key is a simple internal-service boundary, not per-user authorization or multi-tenancy.

## Bounded state and scaling

| Setting (`Flow__...` for environment variables) | Default |
| --- | --- |
| `MaxRequestBytes` | 4 MiB, enforced on compressed and decompressed bodies |
| `MaxSpansPerRequest` | 10,000 |
| `MaxEvents` | 5,000 |
| `MaxSeenSpans` | 50,000 |
| `MaxNodes` / `MaxEdges` | 2,000 / 4,000 |
| `MaxCorrelationEntries` | 10,000 publications and 10,000 waiting receives |
| `EventRetentionMinutes` / `CorrelationRetentionMinutes` | 10 / 10 |
| `TopologyRetentionMinutes` | 60 |
| `StaleAfterSeconds` | 30 |
| `StreamIntervalMs` | 200 |
| `MaxVisualEventsPerFrame` | 100 |
| `MaxFrameBytes` | 3 MiB (also the maximum) |
| `MaxViewers` | 32 |

Initial/reset snapshots include the retained event ring (up to `MaxEvents`), allowing retained-window rates immediately on first load/reconnect. Incremental animation still has the separate 100-event limit. Every JSON frame stays below `MaxFrameBytes`; `coverage` reports missing history/topology/pathways if a byte limit requires a reduction. Partial history must be labeled partial when calculating a rate. Excess animation events are omitted while aggregate accepted-operation counts remain intact. A slow socket has a five-second send deadline. A reconnect obtains a current snapshot if its cursor has expired. State expiration is swept about once per second. New relationships are rejected at topology capacity; existing relationships continue updating. Cache evictions and rejections are observable.

State is **in memory**, including replay and dedupe. A process restart starts a new instance ID and resets counters. This delivery is a single instance with bounded diagnostic state. Multiple uncoordinated replicas would produce separate partial graphs; horizontal scaling needs routing/partitioning or shared state. No production throughput, latency SLO, persistent replay, or high availability has been established. Test overhead and capacities with representative traffic before wider rollout.

## Build, tests, and publish

```sh
dotnet restore LiveFlow.slnx --locked-mode
dotnet build LiveFlow.slnx -c Release --no-restore
dotnet run --project tests/LiveFlow.Tests -c Release --no-restore
dotnet publish src/LiveFlow -c Release --no-restore -o ./publish
```

The tests are a dependency-free console runner with a nonzero exit on failure. They cover normalization, protobuf/JSON, exact offsets, fan-out, out-of-order matching, false-correlation boundaries, bounded state, TTLs, replay resets, concurrent ingestion, real HTTP/gzip/body limits, real WebSockets, and authentication. They do not require Kafka. Review browser evidence is in `review/`. Representative production Kafka traces have not been supplied or tested.

Runtime dependency: Google.Protobuf. Build-only dependency: Grpc.Tools. Both are pinned and a NuGet lock file is included. Vendored OTLP definitions retain their upstream license; see [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

## Next integration checkpoint

Before connecting to your environment, verify the exact Confluent.Kafka/auto-instrumentation versions and inspect a few real producer and consumer spans. Confirm service, environment, cluster, topic, group, offsets, and links, then measure export delay and application overhead on a small pipeline. No Grafana, Kafka, or application deployments are performed by this project.
