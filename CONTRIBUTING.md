# Contributing

This repository contains four sibling projects. Keep that layout: the Grafana review Compose build references `../live-flow`, and the Kafka lab references `../live-flow-dotnet`.

| Directory | Primary checks |
| --- | --- |
| `kolbyd-datastreamsmonitoring-app` | TypeScript, ESLint, Jest, Go tests/vet, Webpack/Mage build and Grafana browser tests. |
| `live-flow` | .NET Release build and the executable collector test runner. |
| `live-flow-dotnet` | .NET Release build, executable instrumentation tests and sample build. |
| `dotnet-kafka-lab` | .NET Release build, xUnit tests and optional real-Kafka smoke. |

Read the [plugin's agent/build instructions](kolbyd-datastreamsmonitoring-app/AGENTS.md) when working in that directory. Do not modify its generated `.config/` files.

## Prerequisites

- Node.js 24.15.0 or newer. The plugin lockfile uses npm 11.12.1.
- Go compatible with the plugin's `go.mod` (minimum 1.26.5); the new CI target is Go 1.27.3.
- Mage 1.17.2 on `PATH` for Go SDK build targets.
- .NET SDK 10, selected by each .NET project's `global.json` with feature-band roll-forward. The library and lab target .NET 8 and need the .NET 8 runtime to execute their tests/samples.
- Docker with Linux containers for the integrated Grafana review and real Kafka smoke.
- PowerShell for the checked-in local launcher scripts. Microsoft Edge is the existing local browser-test default.
- The local image `confluentinc/cp-kafka:8.4.0` for the Kafka lab. Its test script uses `--pull never`.

Install Mage using the version already used by the local build:

```powershell
go install github.com/magefile/mage@v1.17.2
```

Ensure Go's binary directory is on `PATH`. Prepared workstation caches are not part of this repository.

## Grafana app: fast checks

Run from `kolbyd-datastreamsmonitoring-app`:

```powershell
npm ci --ignore-scripts --no-audit --no-fund
npm run typecheck
npm run lint
npm run test:ci
go test ./pkg/...
go vet ./pkg/...
```

Keep package-lock changes intentional. Dependency lifecycle scripts stay disabled in the checked-in review build.

The production frontend uses the managed Webpack configuration; the backend uses Grafana SDK Mage targets. On Linux:

```sh
npm run build
mage -v build:linux
chmod 0755 dist/gpx_*
```

On the established Windows review path, use:

```powershell
./build-review.ps1
```

That script installs/builds the frontend in `node:24.15.0-bookworm-slim`, runs TypeScript/Jest checks, runs Go tests, invokes `build:linux` and preserves backend executable permissions. Its `-SkipTests` option is for a repeated build whose unchanged checks already passed. It does not run every command in the fast-check block; run lint/vet separately.

After a complete frontend/backend build, return to the repository root and create local packages with:

```sh
python scripts/package-plugin.py
```

The packager verifies the backend source manifest, Linux ELF and executable permissions, then creates local archives/checksums under ignored `artifacts/`. It does not publish a GitHub release or replace the frozen v1.0.0 submission.

## Grafana app: integrated browser review

From the plugin directory, after building:

```powershell
./start-review.ps1
./start-review.ps1 -RunTests
./start-review.ps1 -Stop
```

The launcher reuses the isolated review credentials and runs `npm run e2e`. Install host Node dependencies with the earlier `npm ci` command before using this path; the Linux build volume is separate from host `node_modules`. Browser tests use `@grafana/plugin-e2e`; local Windows configuration defaults to installed Microsoft Edge.

The review publishes only loopback 3001/4318. Anonymous access is Viewer; configuration needs administrator access. If explicit credentials are needed, provide `GRAFANA_ADMIN_PASSWORD` and `FLOW_SERVICE_API_KEY` through process environment before the first start. Never commit an env file, cookie, auth-state file or credential value.

Change a plugin manifest only with a corresponding rebuild and Grafana restart. Preserve the published app/data-source IDs. Do not recreate the removed bundled dashboards or add a duplicate app-navigation row.

## Flow collector

Run from `live-flow`:

```powershell
dotnet restore LiveFlow.slnx --locked-mode
dotnet build LiveFlow.slnx -c Release --no-restore
dotnet run --project tests/LiveFlow.Tests -c Release --no-restore
```

The collector tests are an executable test runner, not a `dotnet test` project. They require neither Kafka nor a Grafana instance. Use the [collector README](live-flow/README.md) for standalone startup, OTLP fixtures, authentication and collector fan-out examples.

Changes to normalization/correlation should preserve exact offset strings, out-of-order handling, scope isolation, bounded state and honest incomplete/clock-skew results. Test parent IDs, all links and cross-trace joins rather than inferring causality from graph connectivity.

## .NET instrumentation library

Run from `live-flow-dotnet`:

```powershell
dotnet restore LiveFlow.DotNet.slnx --locked-mode
dotnet build LiveFlow.DotNet.slnx -c Release --no-restore
dotnet run --project tests/LiveFlow.Kafka.Tests -c Release --no-build
dotnet run --project samples/OtlpKafka -c Release --no-build -- --help
```

These tests use fake producers and collecting listeners/sinks; they contact neither Kafka nor an exporter. The sample's `--help` also starts no Kafka client/exporter. Packaging, when needed, uses the existing command:

```powershell
dotnet pack src/LiveFlow.Kafka -c Release --no-build -o artifacts
```

Keep default telemetry standard OTel/W3C. Private pathway/checkpoint extensions remain opt-in. Application payloads/keys must not become telemetry or metric dimensions, and helper failures must not replace application results/exceptions.

## Kafka lab

Run from `dotnet-kafka-lab` for the tests that need no broker:

```powershell
dotnet restore DotNetKafkaLab.slnx --locked-mode
dotnet build DotNetKafkaLab.slnx -c Release --no-restore
dotnet test DotNetKafkaLab.slnx -c Release --no-restore
```

For the actual isolated Kafka smoke, with the documented image already present:

```powershell
./Test-Lab.ps1
```

This starts only Compose project `dsm-dotnet-lab`, runs the unit suite and sends a finite 24-order smoke workload with OTLP export disabled. Its expected results include distinct fan-out groups, retry/DLQ records and two-input cross-trace joins. It does not test a credentialed Collector/Grafana connection.

Use the [lab README](dotnet-kafka-lab/README.md) for foreground/background demo configuration. Cleanup is scoped explicitly:

```powershell
./Stop-Lab.ps1
./Stop-Lab.ps1 -StopKafka
```

The first command signals workers; the second also stops only the lab's Kafka project and retains its data volume. The separate `-ResetData` option is destructive and should only be used when intentionally discarding that lab data.

## CI and review expectations

The root [CI workflow](.github/workflows/ci.yml) is the canonical monorepo definition, triggered on main pushes, pull requests, manual dispatch and reusable calls. It targets pinned Node 24.15.0/Go 1.27.3, Linux Mage packaging, plugin validation, .NET checks and Grafana 13.2.4/13.3.0 browser jobs. .NET SDK/runtime versions are selected by the workflow and project global.json files; .NET 8 execution support is installed alongside the .NET 10 SDK. Check actual workflow runs for status; this document and the recorded 1.0.0 local evidence do not claim the new CI has passed.

CI sets `PLAYWRIGHT_CHANNEL=chromium` to use Playwright's bundled Chromium; an unset variable retains the local Microsoft Edge default. For a Linux browser run with its dependencies, Grafana URL and credentials already configured:

```sh
PLAYWRIGHT_CHANNEL=chromium npm run e2e
```

Compose accepts `GRAFANA_VERSION`, `GRAFANA_HTTP_PORT` and `FLOW_SERVICE_PORT`. The PowerShell launcher supports optional `-ProjectName` while preserving the default local review project. Follow the root workflow's values for isolated CI jobs; do not redirect a test job into an existing unrelated project.

For a change, run the checks for the affected project and any integration boundary it changes. Explain the trigger, resulting behavior, commands run and remaining limitations in the pull request. Attach fresh screenshots for visible UI changes; do not replace evidence with a passing badge. Existing [Map](kolbyd-datastreamsmonitoring-app/src/img/screenshots/map.png) and [Measure](kolbyd-datastreamsmonitoring-app/src/img/screenshots/measure.png) images are local-lab captures, not arbitrary design references.

Do not commit `node_modules`, `dist`, `bin`, `obj`, local runtime state, auth cookies, logs, credentials or test reports. Preserve runtime third-party notices and managed build configuration.

## Release boundary

The `v1.0.0` tag, first catalog submission and its archives are frozen. These main-branch changes do not rebuild or replace those artifacts. Record work in [Unreleased](CHANGELOG.md) and leave version/tag/release/signing/catalog actions to an explicitly authorized release task. The [release workflow](.github/workflows/release.yml) gates future tags on full CI, requires a matching package version and refuses an existing release. It creates a **draft** with three ZIPs, SHA-1/SHA-256 checksums and the validator report. Optional signing uses an existing `GRAFANA_ACCESS_POLICY_TOKEN` secret when configured; it does not create a token or automatically approve a catalog submission.

Only for an authorized future release, use a clean committed checkout and replace the example tag with the matching package version:

```sh
python scripts/package-plugin.py --tag vX.Y.Z --source-ref HEAD
```

This selects source for packaging; it does not itself create a Git tag. A source commit or passing CI is not catalog approval or a Cloud installation.
