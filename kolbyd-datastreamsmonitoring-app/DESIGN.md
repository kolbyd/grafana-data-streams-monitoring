---
name: Data Streams Monitoring
description: A native Grafana controls row over a live, time-scoped Kafka canvas.
colors:
  action: "var(--dsm-blue, #3871dc)"
  focus: "var(--dsm-focus, #1f62e0)"
  ink: "var(--dsm-ink, rgba(36, 41, 46, 1))"
  muted: "var(--dsm-muted, rgba(36, 41, 46, 0.75))"
  ground: "var(--dsm-ground, #ffffff)"
  surface: "var(--dsm-surface, #f4f5f5)"
  canvas: "var(--dsm-surface-alt, #fbfbfb)"
  rule: "var(--dsm-rule, rgba(36, 41, 46, 0.12))"
  border: "var(--dsm-border, rgba(36, 41, 46, 0.3))"
  hover: "var(--dsm-hover)"
  node-fill: "var(--dsm-node-fill, #f4f5f5)"
  topic-end: "var(--dsm-topic-end, rgba(36, 41, 46, 0.4))"
  chart-area: "var(--dsm-chart-area, #3871dc26)"
  error: "var(--dsm-error)"
  error-area: "var(--dsm-error-bg)"
  warning-area: "var(--dsm-warning-bg)"
  warning-ink: "var(--dsm-warning-ink)"
typography:
  body:
    fontFamily: "'Inter', 'Helvetica', 'Arial', sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.5714285714
  small:
    fontSize: "12px"
  node-name:
    fontSize: "11px"
    fontWeight: 500
  node-rate:
    fontSize: "10px"
rounded:
  native: "var(--dsm-radius)"
spacing:
  control: "8px"
  group: "12px"
  panel: "16px"
  section: "24px"
components:
  native-filter:
    width: "152px"
  map-list-display:
    height: "32px"
  topology-node:
    backgroundColor: "{colors.node-fill}"
    textColor: "{colors.ink}"
    typography: "{typography.node-name}"
    width: "176px"
    height: "90px"
  inventory-region:
    backgroundColor: "{colors.ground}"
    textColor: "{colors.ink}"
  native-timeseries:
    height: "220px"
---

# Design System: Data Streams Monitoring

## Overview

**Creative North Star: "The Grafana Stream Canvas"**

Grafana owns navigation, theme and ordinary interaction. A single compact native controls row sits above the operating canvas. The app has no in-page app title or primary page-tab bar; top-level destinations live only in Grafana's sidebar.

The map receives all remaining canvas space. Its organized custom topology, readable names, arrowed curves and short observation particles carry the domain information. Native variables, TimeRangePicker, tables and charts retain Grafana's interaction language. Saved Views and facet-drawer concepts are not part of the rendered surface.

**Key Characteristics:**

- Sidebar-only top-level navigation and dashboard-style canvases.
- One compact native selector, display and time-range row.
- Direct pointer/touch map pan and cursor-anchored wheel zoom.
- Organized directional topology with automatic reduced-motion behavior.

## Colors

Live CSS aliases come from the active GrafanaTheme2 in Explorer; the sidecar maps each role to its source. Installed default-theme fallbacks permit documentation previews outside the app and never override the runtime theme.

### Primary

Action/Focus follow native primary roles. Chart Area is the native transparent primary role. Observed traffic uses primary/blue particles and errors use the semantic error role. Native charts use Grafana's series palette.

### Neutral

Ink/Muted follow text primary/secondary; Ground/Surface/Canvas follow native background roles. Rule/Border use weak/medium borders, Hover uses action hover, Node Fill uses secondary background and Topic End the strong border. Semantic error/warning roles follow Grafana rather than an app-specific palette.

**The Native Theme Rule.** Consume active Grafana roles and native component states; do not add independent light/dark palettes or a theme switch.

## Typography

The app inherits GrafanaTheme2 font family/body size. Body records the installed default rather than an override. Native controls/pills retain their own typography. Small covers compact metadata. Node names and rates use the observed SVG coordinate sizes; tabular numerals stabilize changing values. There is no app-title/display type role in the rendered canvas.

**The Readable Map Rule.** Start at readable graph scale, keep names clear and make Fit an explicit action. Pan/zoom adapts the view rather than squeezing every graph into tiny text.

## Layout

Every app page uses a full Grafana dashboard-style canvas. Top-level navigation stays in Grafana's sidebar. The compact native row contains Filter, Environment, Cluster, Services, Queues, Map/List and the standard Grafana TimeRangePicker. Scenes multiselect variable pills default to All. Relative/absolute ranges and timezone remain native picker behavior.

The map occupies remaining width/height without a Services/Queues drawer, saved-view drawer, title block or duplicated primary tabs. Selected-node details can open as a responsive content region. Native controls wrap/overflow responsively without creating a second app shell. Lists, Pipeline details, Kafka charts and Setup keep native panels/forms and their local scrolling.

**The Canvas Rule.** Preserve the compact controls-to-canvas relationship; give graph content remaining space instead of adding app chrome.

## Elevation & Depth

Grafana components own hover, focus, dropdown, tooltip and popup elevation. The map is a flat themed canvas with directed strokes and selection emphasis. Do not suppress native focus/elevation or add decorative shadows to domain geometry.

## Shapes

Native controls inherit Grafana's radius and component forms. Custom topology uses rectangular services, horizontal topic cylinders and distinct arrow ports joined by organized curves. Topic names are sufficient; do not repeat Kafka/Topic captions beneath every queue.

## Components

### Native dashboard controls

Scenes variables implement Filter plus Environment/Cluster/Services/Queues selectors. Filter uses the observed compact width in the frontmatter. Services/Queues are actual multiselect pills with All as the default, not substitute drawer buttons. The normal Grafana TimeRangePicker handles relative/absolute/timezone. The native Map/List display toggle is text-only, without icons, at the frontmatter height to preserve a compact row. No Updated label or example shortcut belongs in that row.

### Tables, forms and charts

Use native Button/Field/Input/Combobox/Checkbox/RadioButtonGroup/InteractiveTable and in-page detail controls where implemented. List labels rates as Operations in/out (ops/s); Setup uses Observed ops/s. Pipeline definitions remain browser-local; there is no rendered Saved Views CRUD. Lazy Scenes TimeSeries panels render observed rates and causal/hop timing in Pipeline/Kafka surfaces. Setup owns collection guidance and example-data activation. Selector option refresh and event subscriptions remain live after navigation through Setup.

### Map interaction

Pointer/touch drag pans; wheel zoom anchors at the cursor. Scale is clamped to 0.25–3. Native zoom buttons and Fit remain reachable. Hover/focus native tooltips expose observed ops/s and error statistics. Curves/arrows retain distinct entry/exit ports to make fan-out and joins readable.

### Observation motion

Accepted observations appear as primary/blue or error particles over a 1.8-second transit. They remain visible against the themed canvas. Reduced motion is automatic; there is no Pause button. Particle motion is an illustration of observations, not physical broker position or measured transit speed.

### Time-scoped content

The chosen range removes inactive edges and orphan nodes—including workers/groups—and limits variable options to active entities. Live-relative ranges can use last-arrival activity alongside retained events/process completions. Historical ranges depend on retained completion evidence. Keep partial coverage explicit without recurring implementation footers.

## Do's and Don'ts

### Do:

- **Do** use Grafana's sidebar as the only top-level navigation.
- **Do** keep one compact native selector/display/time row above the remaining canvas.
- **Do** use genuine Scenes multiselect pills and Grafana TimeRangePicker behavior.
- **Do** preserve direct map pan, cursor-anchored zoom, explicit Fit and automatic reduced motion.
- **Do** scope graph/options to active time evidence and report partial coverage.

### Don't:

- **Don't** render an in-page app title, duplicate primary tabs or saved-view UI.
- **Don't** add a Services/Queues drawer, Pause button or Map Preview/Updated shortcut.
- **Don't** repeat Kafka/Topic captions under queue names.
- **Don't** replace native controls with app-specific styling or palettes.
- **Don't** imply sampled observations, batch ranges or retained history are complete message traffic.

