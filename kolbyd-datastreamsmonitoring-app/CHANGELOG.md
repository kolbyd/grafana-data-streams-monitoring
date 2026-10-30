# Changelog

## Unreleased

- Add inline app-owned Map/Measure screenshots and links to the four-project contribution guide.
- Document the root CI/browser matrix and preserve local Edge behavior alongside the CI Chromium override.
- Keep v1.0.0 tag/submission/artifacts frozen; main-branch documentation and CI work does not replace the release.

## 1.0.0 — 2026-10-30

Initial public release and unsigned Linux AMD64 package for first Grafana catalog review.

- Native Grafana dashboard-style canvases, sidebar navigation, variable selectors and relative/absolute/timezone controls.
- Interactive producer/topic/consumer topology with pointer/touch pan, cursor-anchored zoom, Fit, observed-operation tooltips/particles and automatic reduced motion.
- Native Services/Queues List, Kafka inventories, Setup and browser-local pipeline catalog/detail/Measure workflows.
- Standard OTel causal parent/all-link/cross-trace processing evidence, displayed as complete retained operation ranges where supported.
- Go Grafana Live bridge and nested snapshot data source, with secure backend connection settings and bounded/partial-coverage reporting.
- Separate ASP.NET Core collector, OTel-first .NET instrumentation preview and actual isolated ten-service Kafka fan-out/fan-in lab.

Rates reflect sampled/retained operations, not authoritative message counts. Batch ranges are not per-record latency. Broker lag/health, durable history, monitor management and a CLR profiler are not supplied. Saved Views is dormant; no prebuilt dashboards are bundled. The collector must be separately reachable; catalog approval, public signing and Cloud installation are not implied.

The [repository changelog](https://github.com/kolbyd/grafana-data-streams-monitoring/blob/main/CHANGELOG.md) details all four projects and the release's measurement/deployment limits.
