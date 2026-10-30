# Local validation — 2026-10-30

Verified on Windows with .NET SDK 10.0.403, .NET 8 runtime, Docker Engine 29.8.2 and Compose 5.6.0.

- Release solution build passed with **0 warnings and 0 errors**.
- Formal xUnit project: **10 passed, 0 failed, 0 skipped**. The sandbox initially blocked the testhost connection; the successful test run used the authorized local execution context.
- Compose configuration validation passed. Only `dsm-dotnet-lab` was started, using the already-present `confluentinc/cp-kafka:8.4.0` image. Broker became healthy and its cluster ID was verified before topic creation.
- Actual Kafka smoke run `smoke-20261030-133619` passed: 24 source orders, 48 router publications, 24 payment requests, 24 inventory requests, 46 result inputs, 22 joined ready publications, and 22 observations in **each** of fulfillment/notifications/analytics.
- Real retry/DLQ consumers observed **3 retries and 2 DLQ records**.
- All **22 join process spans** were new roots with exactly two different producer links, across two distinct trace IDs. All observed input messages had W3C propagation; none had the private pathway header.
- Actual consume calls produced separately timed receive spans for every current-run input observation. Processing was recorded separately.
- Hidden background Start/Stop lifecycle passed with OTLP export disabled; the orchestrator and all child workers exited on the stop-file signal.

Smoke/lifecycle checks made no OTLP exporter, Cloud or existing-cluster calls. Kafka is left running for the parent task's live UI traffic run; only loopback port 19092 is published. The normal start script can reuse it with `-SkipKafka -SkipBuild`. Stop/cleanup commands are in the README.

This validates the local broker/workload/trace-link behavior. It does not claim durable join/outbox semantics, exactly-once business processing, production performance or authoritative broker lag. The credentialed Collector/Grafana traffic run is managed separately by the parent task.

## Live app integration

The parent task started a separate 30-minute background run at two source orders per second with standard OTLP HTTP/protobuf export to the authenticated local collector. The credential was inherited through process memory and not saved to this project.

The collector received actual send, receive and process spans and exposed 10 services, 7 topics and 18 relationships. A browser verification observed 17 topology nodes and live observation particles, with no page overflow, example-data banner or JavaScript errors. Selecting `dotnet-lab-source` → `dotnet-lab-fulfillment` in Measure produced complete pathway samples and a measured p95 value. The saved pipeline rendered all four native Grafana time-series charts. The timing values vary as the workload runs; these are observed retained pathway samples, including batch joins, rather than individual-message latency guarantees.

The current run is recorded in `.lab-state/active.json`. Stop it with `Stop-Lab.ps1`; that file and runtime logs are excluded from distributable source archives. No Grafana Cloud deployment or customer cluster was used.
