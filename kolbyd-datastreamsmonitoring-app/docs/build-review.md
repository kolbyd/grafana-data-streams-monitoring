# Build and review

The source starts from the official `@grafana/create-plugin@7.17.0` App + Go backend scaffold. The CLI does not run directly on Windows, so it was invoked in `node:24.15.0-bookworm-slim` through Docker with the workspace mounted at `/workspace`.

## Build

Use `./build-review.ps1` from PowerShell. It runs:

1. `npm ci --ignore-scripts --no-audit --no-fund` in the Linux Node container.
2. TypeScript checks, Jest tests, and `npm run build` using the generated Webpack configuration.
3. Go backend tests and the SDK Mage Linux build.
4. Linux executable permissions for `dist/gpx_*`.

The Linux Node installation uses the `liveflow-plugin-node-modules` Docker volume and the workspace's `work/npm-cache` directory. This keeps Linux native dependencies separate from host `node_modules`. The review script's `-SkipTests` option omits tests when a previously verified build is being repeated.

The local Go helper lives under `work/go-toolchain`, with Mage under `work/go-cache/bin`. On another workstation, install a supported Go version and Mage on `PATH`:

```powershell
go install github.com/magefile/mage@v1.17.2
```

The generated `.config/` build configuration is not modified. Frontend package installation keeps lifecycle scripts disabled. Native controls use `@grafana/ui`; `@grafana/scenes` 9.2.0 time-series panels are lazy-loaded for Pipeline and Kafka charts. Custom Dagre topology and node sparklines remain SVG. Theme aliases come from active GrafanaTheme2, not a fixed app palette.

## Start and test

```powershell
./start-review.ps1
./start-review.ps1 -RunTests
```

The second invocation reuses credentials already supplied to the local containers. It runs `@grafana/plugin-e2e` tests in installed Microsoft Edge by default, including native configuration, Setup-only example activation, Map/List, Pipelines/Measure, Kafka Console, Setup, sidebar navigation, native variable/time controls and narrow layouts. Saved Views source is dormant and not a rendered workflow. Direct `npm run e2e` also works when `GRAFANA_ADMIN_PASSWORD` and optionally `GRAFANA_URL` are supplied in the process environment. `PLAYWRIGHT_CHANNEL=chromium` selects bundled Chromium for the Linux CI path. Final check counts, focused reruns and fresh review verdicts belong in `review-results.md`.

The application is available at `http://127.0.0.1:3001/a/eageraspen2909-datastreamsmonitoring-app/explore`, labeled **Map** in Grafana's sidebar. Use the sidebar for all top-level destinations. Each app page uses a dashboard-style canvas; the package no longer includes separate prebuilt dashboards or dashboard menu links. The nested Data Streams data source remains provisioned.

The specific former dashboard files (`overview.json`, `consumers.json`) and `provisioning/dashboards/flow.yaml` are removed. Their exact local Grafana copies, UIDs `liveflow-overview` and `liveflow-consumers`, were separately deleted. No other dashboards or Cloud resources were mutated. Rebuild/restart after app manifest changes so sidebar navigation updates.

## Local isolation

Compose project: `liveflow-plugin-review`. Only loopback ports 3001 and 4318 are published. Grafana grants anonymous users the Viewer role. The normal administrator login is required to change the app's service connection. Random administrator and service credentials are supplied as Docker secrets from process environment values; no env file or credential literal is stored in the source.

The companion flow service builds the sibling `../live-flow` project with .NET SDK 10.0.403 and runs on ASP.NET Core 10.0.14. The configured service key is required for OTLP ingestion, API reads, and the upstream WebSocket. Grafana's provisioning reads that key into `secureJsonData` at startup.

To configure a collector with a known key, supply your chosen `FLOW_SERVICE_API_KEY` before starting a new local review project, then use that value in its OTLP authorization header. Preview example mode needs no collector or Kafka broker.

Standard OTLP traces are the required input. The separate collector retains causal parent/link evidence and derives complete pathway ranges when available; the plugin Go backend still bridges its stream and snapshots into Grafana. There is no OTLP metrics receiver or collected Kafka broker health/lag. Preserve an application's existing OTel pipeline and use a fan-out branch where appropriate.

The sibling `../dotnet-kafka-lab` provides bounded real-Kafka .NET fan-out/fan-in runs with standard W3C propagation, cross-trace joins and retry/DLQ routes. Its README documents isolated Compose startup, tests, foreground/background execution and stop behavior. Set its normal OTLP endpoint and authorization environment for this review service without saving credentials. Run metrics export only against a compatible metrics destination. Actual smoke-test and background live-connection results must be validated separately from browser example tests; do not infer them from lab source or build success.

## Stop and rollback

```powershell
./start-review.ps1 -Stop
```

This stops/removes only the named review project's containers and network. Source, build outputs, and the Node dependency volume remain. It does not operate on unrelated Docker projects. Start again from the provisioned definitions to restore the review setup.

App or nested data-source `plugin.json` changes require a backend/frontend rebuild and Grafana restart. If a running review needs the updated manifest, run `docker compose restart grafana` in this directory with the existing launcher environment, or use the launcher to restart the isolated review project.

## Deployment boundary

This setup is for local plugin development. It is not a Cloud installation or a real Kafka load test. Before distribution, follow Grafana's plugin packaging, validation, signing and publishing guidance, and provide a reachable flow-service deployment. Grafana Cloud's restrictions on manually uploaded private plugins still apply.

The repository-level [contribution guide](../../CONTRIBUTING.md) covers all four projects. Root CI targets Grafana 13.2.4 and 13.3.0 with pinned Node/Go tooling; actual runs, not the older local evidence, establish its result. The v1.0.0 tag, submission and archives are frozen. New main-branch documentation/CI work does not rebuild or replace them; future release automation is scoped to subsequent authorized releases.
