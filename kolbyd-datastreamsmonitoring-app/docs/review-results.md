# Validation evidence

Recorded 2026-10-30 against local Grafana 13.2.4 and an isolated Kafka cluster. This is local validation, not Grafana Cloud installation or catalog approval.

## Current behavior

- Native dashboard-style canvas on every page, with Grafana sidebar navigation. No duplicate app heading/tabs, saved Views UI, facet drawer, Pause button or Map Preview/Updated shortcut.
- Native Filter, Environment, Cluster, Services and Queues controls, with Grafana's relative/absolute/timezone picker. Choice options refresh as telemetry changes; controlled subscriptions reconnect after returning from Setup.
- Pointer/touch pan, cursor-anchored wheel zoom, Fit and organized directional arrows. Hover/focus shows observed ops/s. New observations animate as dots; reduced motion follows the browser preference.
- Selected time ranges remove inactive edges and orphan nodes, including worker/consumer-group relationships. Retained event/process evidence supports historical filtering; missing history cannot be recovered.
- The former Overview and Consumer Groups dashboards and their sidebar/provisioning definitions are removed. Only the exact two former local dashboard UIDs were deleted; no Cloud dashboards were changed.
- Catalog publisher organization is eageraspen2909. App ID is eageraspen2909-datastreamsmonitoring-app; nested datasource ID is eageraspen2909-datastreamsmonitoring-datasource. The physical source directory retains its scaffold name.

## Automated checks

- Official create-plugin Webpack production build passed; TypeScript and ESLint passed. Frontend Jest: **56 tests in 12 suites passed**.
- Go backend tests and vet passed. SDK Mage produced the Linux AMD64 binary after the catalog-ID change. Official govulncheck reports **no vulnerabilities found** after dependency updates.
- Collector Release build passed. Its 34-check suite passed, with three additional focused frame-budget checks. Standard OTLP SDK fixtures established a complete 45 ms causal range across parent/link evidence.
- .NET instrumentation Release build and **33 tests passed**. A compiled standard SDK/OTLP sample was exercised.
- Kafka lab Release build and **10 tests passed**. Background lifecycle/start/stop checks passed.
- Browser checks use **@grafana/plugin-e2e** in Microsoft Edge. Native sidebar/Back navigation, Kafka/Setup, relative and absolute time ranges, timezone controls, Pipeline CRUD and native throughput canvases passed. Final control regression results are recorded below.

## Real Kafka evidence

The isolated Confluent Kafka lab runs ten .NET workers with standard W3C propagation and ordinary OTLP trace export. A 24-order smoke run produced 22 completed joins, 22 observations in each of the three independent downstream groups, three retries and two dead-letter operations. Each join had two links to distinct upstream trace IDs; no private pathway headers were required.

A bounded background run was connected to the actual collector and Grafana Live. The map displayed 17 nodes (ten services and seven topics), 18 edges and moving observations. Pipeline timing/native charts rendered from complete retained causal evidence; an observed p95 was approximately 279 ms over 22 samples at one inspection. This is test-workload evidence, not a performance guarantee.

Motion verification confirmed changing particle positions on multiple actual routes, an arrow tooltip matching its observed operation rate, drag pan, cursor-anchored zoom and no browser page errors. A 3 MiB collector frame budget prevents snapshots from exceeding Grafana Live's 4 MiB outbound queue. Same-instance recovery snapshots preserve the animation cutoff; a new service instance resets it. Direct collector continuity produced one initial snapshot followed by fourteen updates without resets.

## Visual evidence

The native-canvas capture matrix contains all ten surfaces at both 1600x1000 and 390x844: Map, List, Measure, pipeline Definition/Summary/catalog, Kafka clusters/topics, Setup and an empty transport fixture. The empty fixture modified only browser-delivered frames, leaving the real collector unchanged. No horizontal app overflow or browser page errors were found.

An independent fresh reviewer requested two material corrections: operation-derived rates must use operations/ops/s wording, and desktop controls must keep the time picker on the compact controls row. Both corrections were applied as one batch; final scoring is recorded below. No QUALITY BAR card was supplied, so the review is limited to the user-pinned native Grafana brief and documented contract. A one-time detector had no primary findings, only documented type-scale advisories.

## Distribution boundaries

Rates are observed operations, affected by sampling and batching. Complete parent/link chains establish observed causal operation ranges; graph reachability or a receive span alone does not prove business completion. Batch timing is not per-record latency. Broker health, lag, partitions and rebalances remain uncollected; there is no OTLP metrics receiver, durable history or CLR auto-instrumentation profiler. Pipeline definitions remain browser-local.

The app requires the separate collector. A Grafana Cloud backend needs network access to it; workstation loopback is not reachable from Cloud. The first catalog archive is unsigned for review, as Grafana permits; catalog approval and later public signing are separate.

The package includes runtime dependency notices and preserves backend executable permissions. Source staging excludes cookies, environment files, local credentials, caches, dependency trees, test reports and build outputs. Credential-pattern audit is performed before GitHub upload. Datadog reference screenshots/source caches are excluded; shipping logos are original SVGs and catalog screenshots capture this app's own local Kafka lab.

## Final control and release checks

Final focused browser runs passed: native Services/Queues selection and All restoration; Environment/Cluster selection; topic filtering after Setup-to-Map reactivation; Map/details/List/Measure; sidebar and Back navigation; Kafka/Setup; mobile overflow; and the final 1600px toolbar geometry. The final geometry-only run passed 2/2 including auth, with controls at native 32px height and the time picker inline. Earlier focused runs passed full relative/absolute/timezone selection and Pipeline CRUD/native canvases. No further production edits were required by these checks.

The independent reviewer scored both listed material fixes resolved, found no material regressions in affected recaptures, and returned **ship** at the scope of those scored fixes. The final Go SDK is v0.298.0; tests, vet, Mage build and govulncheck passed. The signing tool is v4.0.0, removing the vulnerable basic-ftp development dependency. Final catalog validation is recorded with the release evidence.

Catalog validation: the official grafana/plugin-validator-cli completed with exit 0 against the exact install archive and plugin-root source. No errors remained. Warnings: unsigned first submission (expected) and a possible @floating-ui/react React 19 false positive. The managed externals and built sourcemaps verify no bundled React runtime or legacy __SECRET_INTERNALS code. Styles now use scoped Emotion classes, including the portaled arrow tooltip. Source layout for catalog validation is the dedicated plugin-source ZIP; the GitHub repository/complete-source ZIP retain all four projects. The post-migration Webpack build and all 56 Jest tests passed, and one engineering capture batch checked all twenty desktop/mobile surfaces without material style regressions or browser errors.

Post-scoping focused @grafana/plugin-e2e regression run: **4/4 passed** including authentication, sidebar/Back/Kafka navigation, compact desktop geometry, and Pipeline CRUD with native chart canvases. No JavaScript or native panel errors.
