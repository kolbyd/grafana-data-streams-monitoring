# Contributing

This repository contains four sibling projects. Keep that layout: the Grafana review Compose build references `../live-flow`, and the Kafka lab references `../live-flow-dotnet`.

| Directory | Primary checks |
| --- | --- |
| `kolbyd-datastreamsmonitoring-app` | TypeScript, ESLint, Jest, Go tests/vet, Webpack/Mage build and Grafana browser tests. |
| `live-flow` | .NET Release build and the executable collector test runner. |
| `live-flow-dotnet` | .NET Release build, executable instrumentation tests and sample build. |
| `dotnet-kafka-lab` | .NET Release build, xUnit tests and optional real-Kafka smoke. |

Read the [plugin's agent/build instructions](kolbyd-datastreamsmonitoring-app/AGENTS.md) when working in that directory. Do not modify its generated `.config/` files.

## Set up your tools

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

Add Go's binary directory to `PATH` so the build can find Mage. You'll need your own installed tools; workstation caches aren't included in the repository.

## Check the Grafana app

Run from `kolbyd-datastreamsmonitoring-app`:

```powershell
npm ci --ignore-scripts --no-audit --no-fund
npm run typecheck
npm run lint
npm run test:ci
go test ./pkg/...
go vet ./pkg/...
```

These commands check frontend types, style and behavior, then test the Go backend. Keep lockfile changes intentional. The review build disables dependency lifecycle scripts.

## Build and package the plugin

The production frontend uses the managed Webpack configuration; the backend uses Grafana SDK Mage targets. On Linux:

```sh
npm run build
mage -v build:linux
chmod 0755 dist/gpx_*
```

On Windows, run:

```powershell
./build-review.ps1
```

The script builds the frontend in `node:24.15.0-bookworm-slim`, checks TypeScript and Jest, tests Go, and runs the Mage `build:linux` target. It leaves an executable backend in `dist/`. Run lint and vet separately. Use `-SkipTests` only when repeating a build whose unchanged tests already passed.

After a complete frontend/backend build, return to the repository root and create local packages with:

```sh
python scripts/package-plugin.py
```

The packager verifies the backend source manifest, Linux ELF and executable permissions, then creates local archives/checksums under ignored `artifacts/`. It does not publish a GitHub release or replace the frozen v1.0.0 submission.

## Test the app in Grafana

From the plugin directory, after building:

```powershell
./start-review.ps1
./start-review.ps1 -RunTests
./start-review.ps1 -Stop
```

The launcher reuses the review credentials and runs `npm run e2e` with `@grafana/plugin-e2e`. Install host Node dependencies with the earlier `npm ci` command first: the Linux build volume doesn't populate host `node_modules`. Local Windows tests use installed Microsoft Edge by default.

The review publishes only loopback 3001/4318. Anonymous access is Viewer; configuration needs administrator access. If explicit credentials are needed, provide `GRAFANA_ADMIN_PASSWORD` and `FLOW_SERVICE_API_KEY` through process environment before the first start. Never commit an env file, cookie, auth-state file or credential value.

Change a plugin manifest only with a corresponding rebuild and Grafana restart. Preserve the published app/data-source IDs. Do not recreate the removed bundled dashboards or add a duplicate app-navigation row.

## Test the flow collector

Run from `live-flow`:

```powershell
dotnet restore LiveFlow.slnx --locked-mode
dotnet build LiveFlow.slnx -c Release --no-restore
dotnet run --project tests/LiveFlow.Tests -c Release --no-restore
```

The collector tests are an executable test runner, not a `dotnet test` project. They require neither Kafka nor a Grafana instance. Use the [collector README](live-flow/README.md) for standalone startup, OTLP fixtures, authentication and collector fan-out examples.

Changes to normalization/correlation should preserve exact offset strings, out-of-order handling, scope isolation, bounded state and honest incomplete/clock-skew results. Test parent IDs, all links and cross-trace joins rather than inferring causality from graph connectivity.

## Test the .NET library

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

## Test the Kafka lab

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

Use the [lab README](dotnet-kafka-lab/README.md) to configure a foreground or background demo. To stop the lab:

```powershell
./Stop-Lab.ps1
./Stop-Lab.ps1 -StopKafka
```

The first command signals workers; the second also stops only the lab's Kafka project and retains its data volume. The separate `-ResetData` option is destructive and should only be used when intentionally discarding that lab data.

## Prepare a pull request

The root [CI workflow](.github/workflows/ci.yml) checks all four projects on main pushes, pull requests, manual runs and reusable calls. It uses Node 24.15.0 and Go 1.27.3 for Linux Mage packaging, plugin validation, .NET checks and Grafana 13.2.4/13.3.0 browser jobs. The workflow and each project's `global.json` select .NET versions; .NET 8 execution support is installed alongside the .NET 10 SDK. Check [workflow results](https://github.com/kolbyd/grafana-data-streams-monitoring/actions/workflows/ci.yml) for status. The recorded 1.0.0 local evidence doesn't establish that current CI passes.

CI sets `PLAYWRIGHT_CHANNEL=chromium` to use Playwright's bundled Chromium; an unset variable retains the local Microsoft Edge default. For a Linux browser run with its dependencies, Grafana URL and credentials already configured:

```sh
PLAYWRIGHT_CHANNEL=chromium npm run e2e
```

Compose accepts `GRAFANA_VERSION`, `GRAFANA_HTTP_PORT` and `FLOW_SERVICE_PORT`. The PowerShell launcher supports optional `-ProjectName` while preserving the default local review project. Follow the root workflow's values for isolated CI jobs; do not redirect a test job into an existing unrelated project.

Before opening a pull request:

1. Run checks for the affected project and any integration it changes.
2. Describe the problem, resulting behavior, commands you ran and remaining limitations.
3. Attach current screenshots for visible UI changes. A passing badge doesn't show whether the interface works as intended.

The existing [Map](kolbyd-datastreamsmonitoring-app/src/img/screenshots/map.png) and [Measure](kolbyd-datastreamsmonitoring-app/src/img/screenshots/measure.png) images show the local lab.

Do not commit `node_modules`, `dist`, `bin`, `obj`, local runtime state, auth cookies, logs, credentials or test reports. Preserve runtime third-party notices and managed build configuration.

## Write documentation

Follow [Grafana's voice and tone guidelines](https://grafana.com/docs/writers-toolkit/write/style-guide/voice-tone-guidelines/) for README files, task guides and supporting documentation.

- Address the reader directly, using plain language and a friendly, confident tone.
- Give task headings an action verb. Keep steps short and state what the reader should see after completing them.
- Explain why an action matters when that helps the reader choose the next step.
- Separate verified results from expected behavior. Link to current CI results instead of implying that historical checks prove a new run passed.
- Keep useful product facts and limitations; leave internal work chronology out of task instructions.
- Preserve the absolute, versioned image URLs in the shipping plugin README. Catalog validation requires those images to resolve outside the repository.

For UI work, follow [Grafana's design principles](https://developers.grafana.com/ui/latest/index.html?path=/docs/docs-overview-design-principles--docs): Task-at-Hand, Tasteful Friction, Simple Solutions for Users and Default to Reusability.

- Keep the current task clear and avoid extra controls or content that compete with it.
- Use native Grafana components, theme roles and default interactions consistently.
- Let the system handle complexity where possible. Reuse an existing interaction before adding a new one.
- Keep ordinary filtering direct. Ask for confirmation when a destructive change warrants it, rather than adding dialogs to routine filter changes.

## Prepare a future release

Keep the `v1.0.0` tag, first catalog submission and its archives unchanged. Record new work in [Unreleased](CHANGELOG.md). Maintainers handle version changes, tags, signing, releases and catalog submissions.

The [release workflow](.github/workflows/release.yml) requires full CI and a matching package version, and refuses to replace an existing release. It creates a **draft** with three ZIPs, SHA-1/SHA-256 checksums and a validator report. If an existing `GRAFANA_ACCESS_POLICY_TOKEN` secret is configured, the workflow can sign the package. It doesn't create credentials or approve a catalog submission.

Only for an authorized future release, use a clean committed checkout and replace the example tag with the matching package version:

```sh
python scripts/package-plugin.py --tag vX.Y.Z --source-ref HEAD
```

This selects source for packaging; it does not itself create a Git tag. A source commit or passing CI is not catalog approval or a Cloud installation.
