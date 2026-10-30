# Explorer surface

Mode: Operate. Audience: engineers inspecting active Kafka topology and causal evidence.

## Direction contract

THESIS: Put the active observed graph directly in a full remaining dashboard canvas, controlled by a compact native Grafana row.

OWN-WORLD: Grafana's native sidebar is the only top-level navigation. No in-page app title or primary page tabs. Native theme, Scenes variables, TimeRangePicker, controls, charts and tables own ordinary interaction. Domain topology retains Dagre SVG shapes, node sparklines and observed moving dots.

STORY: Choose Filter/Environment/Cluster/Services/Queues and relative or absolute time/timezone, switch Map/List, pan/zoom the active graph and inspect native tooltips/details. Follow sidebar destinations for Pipelines, Measure, Kafka Console and Setup. Only Setup activates example data. Saved Views is dormant source, not a rendered workflow.

FIRST VIEWPORT: One compact row with a 160px native Filter, Scenes selectors (Services/Queues multiselect pills default All), a text-only 32px Map/List toggle without icons and Grafana TimeRangePicker. Selector options/callbacks refresh after Setup navigation. The map fills all remaining space. There is no Services/Queues drawer, Pause button, Updated label or Map Preview shortcut. Pointer/touch drag pans, wheel zoom anchors at the cursor, native zoom/Fit controls stay reachable, and scale is bounded to 0.25–3.

FORM: Organized arrow ports/curves show producer/topic/consumer direction. Native hover/focus tooltips show observed ops/s and error statistics. Blue/error particles traverse accepted observations over 1.8 seconds, automatically respecting reduced motion. Do not add repeated Kafka/Topic captions. Native Scenes time-series panels remain in Pipelines/Kafka; List uses native tables.

FINISH: DESIGN.md/sidecar describe this final compact canvas and live theme bindings. No screenshot or detector run belongs to this documenter pass. The parent records final source/build/browser/Kafka results in docs/review-results.md.

## Evidence and boundaries

src/flow/activity.ts prunes edges without activity in the chosen interval and then removes orphan nodes. Relative live ranges can use last-arrival edge timestamps as well as retained events/process completions. Historical ranges use retained event/process completion evidence. Workers and consumer-group relationships are included in pruning; selector options derive from active entities.

The bridge limits frames to 3 MiB for Grafana Live's 4 MiB outbound queue and reports partial coverage. Sampled traces are not full traffic; retained history is not durable. Complete standard OTel send/process chains can provide causal operation ranges, including cross-trace joins. Batch ranges are not individual-message latency. Broker health/lag remain uncollected; no OTLP metrics receiver exists.

The app retains browser-local pipeline definitions, not saved-view UI or bundled dashboards. The collector and Go Grafana Live bridge remain separate backend processes. The .NET library is OTel/W3C-first with opt-in custom extensions; the actual local Kafka lab has supplied ten services and linked joins. No Cloud installation or production coverage is claimed.

