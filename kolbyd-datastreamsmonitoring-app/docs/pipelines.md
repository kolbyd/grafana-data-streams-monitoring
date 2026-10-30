# Saved pipelines

The Pipelines page recreates the supplied DSM catalog/detail workflow inside Grafana. The catalog opens a pipeline detail with Definition, Summary, Components, and Monitors navigation. Details own their scrolling pane; they do not create a second full-page canvas.

## Personal definitions

Create a pipeline with a name, start service, end service, and optional environment. The start/end services must currently be observed and connected by a directed path. Editing and removing a definition changes only the browser's saved definition; it does not change Kafka or the flow service.

Definitions use versioned browser local storage, scoped by the parent app's storage key and separate `live`/`example` suffixes. The first version is personal to this browser, not a shared server-side catalog. Storage failures are reported and failed writes do not silently change the visible catalog. No credentials are stored with pipeline definitions.

Endpoint identities include the stable node ID and a fallback of service name, namespace and environment. A fallback binds only when it is unambiguous; a missing or ambiguous endpoint shows the saved definition as unavailable rather than selecting a different service.

## Connectivity and metrics

The definition contains the directed dependency corridor between the chosen endpoints: nodes reachable from the start that can reach the end. Internal cycles are represented once, and unrelated fan-out branches are excluded. Reachability traversal terminates on cycles and stops at the opposite endpoint. This is a connectivity definition, not proof of causal message lineage across a processing service.

Catalog and detail throughput use actual retained publish observations at the start and receive observations at the end, divided by the selected window duration. The Components table uses the same scoped observations. Empty windows display zero observed operations; missing endpoint definitions display unavailable.

The Summary shows correlated producer-to-consumer hop latency when both observations are retained and both relationships belong to the definition. It does not add hop latencies together or claim full end-to-end pathway latency. Endpoint rates cover operations on the dependency corridor; shared queues may carry records from other producers. Full pathway timing, business-processing duration, and Kafka consumer lag remain explicitly unavailable until the required telemetry is integrated. The Monitors section links to Grafana Alerting and states that this plugin has no configured pipeline monitor assignments.

## Component API

`src/components/Pipelines/PipelinesPage.tsx` exports `PipelinesPageProps` and a default React component. It accepts the shared flow state, scoped/windowed events, current server-adjusted time, window duration, environment, example/live mode, a personal storage namespace, and an `onExplore(nodeIds)` callback. Navigation from a definition or selected component passes stable node IDs back to the app map.

The optional `initialDraft` prop preselects start/end service IDs in an unsaved create form on mount. The parent mounts the page with a key combining personal storage namespace and live/example mode, so switching identity or source remounts its personal UI state. Merely opening a draft does not save it.

The page inherits `--dsm-*` theme variables from the app, including its light and dark variants. It imports the existing automatic Topology renderer and Sparkline renderer; it does not introduce a new chart or layout dependency.
