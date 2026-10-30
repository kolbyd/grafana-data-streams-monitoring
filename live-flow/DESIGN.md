---
name: Live Flow
description: A local diagnostic viewer for observed Kafka activity.
colors:
  ground: "#101719"
  surface: "#172124"
  raised: "#1d2a2e"
  line: "#354448"
  muted: "#a9bbbf"
  ink: "#eef5f3"
  publish: "#69d7c4"
  receive: "#efbd76"
  error: "#ff9691"
  focus: "#b6f3e5"
typography:
  headline:
    fontFamily: '"Segoe UI", system-ui, sans-serif'
    fontSize: "32px"
    fontWeight: 600
    letterSpacing: "-0.03em"
  title:
    fontFamily: '"Segoe UI", system-ui, sans-serif'
    fontSize: "16px"
    fontWeight: 600
    letterSpacing: "-0.015em"
  body:
    fontFamily: '"Segoe UI", system-ui, sans-serif'
    fontSize: "14px"
    lineHeight: 1.6
  label:
    fontFamily: '"Segoe UI", system-ui, sans-serif'
    fontSize: "12px"
  control:
    fontFamily: '"Segoe UI", system-ui, sans-serif'
    fontSize: "13px"
    fontWeight: 600
  graph-title:
    fontFamily: '"Segoe UI", system-ui, sans-serif'
    fontSize: "13px"
    fontWeight: 600
  graph-label:
    fontFamily: '"Segoe UI", system-ui, sans-serif'
    fontSize: "12px"
rounded:
  field: "5px"
  control: "6px"
  service: "7px"
  workspace: "9px"
  topic: "28px"
spacing:
  compact: "8px"
  field: "12px"
  standard: "16px"
  panel: "20px"
  section: "30px"
  page: "36px"
components:
  button-primary:
    backgroundColor: "{colors.publish}"
    textColor: "#112c26"
    typography: "{typography.control}"
    rounded: "{rounded.control}"
    padding: "8px 18px"
  button-primary-hover:
    backgroundColor: "#9ee8d8"
  button-quiet:
    backgroundColor: "transparent"
    textColor: "{colors.ink}"
    typography: "{typography.control}"
    rounded: "{rounded.control}"
    padding: "8px 13px"
  button-icon:
    backgroundColor: "{colors.raised}"
    textColor: "{colors.ink}"
    rounded: "{rounded.control}"
    padding: "8px"
  search-field:
    backgroundColor: "{colors.ground}"
    textColor: "{colors.ink}"
    rounded: "{rounded.field}"
    padding: "8px 12px 8px 36px"
  filter-select:
    backgroundColor: "{colors.ground}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    rounded: "{rounded.field}"
    padding: "7px 26px 7px 10px"
  workspace:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.workspace}"
  service-node:
    backgroundColor: "#1b2a2e"
    textColor: "{colors.ink}"
    typography: "{typography.graph-title}"
    rounded: "{rounded.service}"
    width: "195px"
    height: "58px"
  topic-node:
    backgroundColor: "#162e2d"
    textColor: "{colors.ink}"
    typography: "{typography.graph-title}"
    rounded: "{rounded.topic}"
    width: "195px"
    height: "58px"
---

# Design System: Live Flow

## Overview

**Creative North Star: "Observed flow, stable map"** — descriptive of the shipped implementation.

Live Flow uses a dark, flat operational graph for inspecting Kafka observations. A restrained shell leaves the topology, event feed, and session counts in the foreground. Native system typography and compact controls keep service names and timing readable.

This document records the local diagnostic companion as implemented. The descriptive north star below is an implementation summary, not a user-selected brand direction. The viewer is not a deployed Grafana plugin.

**Key Characteristics:**

- Stable directed topology with activity moving along relationships.
- Mint publication, amber receipt, and coral error semantics.
- Flat surfaces, thin borders, and compact system typography.
- Explicit freshness, synthetic traffic, replay, and coverage language.

## Colors

The palette combines dark blue-green neutrals with three operation colors. The frontmatter records the normative CSS custom properties; component-specific colors remain in component definitions. The sidecar's generated tonal ramps are preview aids, not additional colors used by the viewer.

### Primary

- **Publication mint** (`publish`): publish particles, topic marks, matched correlation text, links, and the explicit demo action.
- **Pale mint focus** (`focus`): keyboard focus outlines and focused node strokes.

### Secondary

- **Receipt amber** (`receive`): receive particles and labels, connecting status, synthetic-traffic notices, and replay status.

### Tertiary

- **Error coral** (`error`): failed-operation particles and labels and unavailable connection status.

### Neutral

- **Deep ground** (`ground`): page canvas, fields, and sticky table headings.
- **Panel surface** (`surface`): topology workspace, settings strip, and hovered event rows.
- **Raised control** (`raised`): default button fill.
- **Structural line** (`line`): borders, dividers, and scrollbar thumbs.
- **Muted text** (`muted`): supporting labels, metadata, and freshness.
- **Primary ink** (`ink`): headings, service names, field text, and main values.

**The Operation Color Rule.** Pair publish, receive, and error color with operation text or the visible legend; color alone does not explain an observation.

## Typography

All interface and graph text uses the native **Segoe UI, system-ui, sans-serif** stack. There are no downloaded font assets. Code in the initial empty-state markup uses the browser's monospace default.

The headline is the only large type treatment. Dense operational labels remain compact; tabular numerals align timestamps, detail values, and session totals.

- **Headline:** page title; reduces to (27px) at the narrow breakpoint.
- **Title:** section headings; detail titles use (19px), and the empty-state title uses (22px).
- **Body:** introductory paragraph; becomes (12px) on narrow screens.
- **Label:** metadata, table text, legend, status, and notes.
- **Control:** action buttons. Search text uses (13px); selects use the label role.
- **Graph title / graph label:** node names use the title role specific to graphs; node subtitles and consumer-group edge labels use the graph label role.

**The Readable Staleness Rule.** Stale nodes retain text opacity. Dashed outlines and a muted node mark communicate age.

## Layout

The desktop shell has a (73px) header and a centered main region with a maximum width of (1800px), horizontal padding of (36px), and top padding of (34px). The heading precedes one bordered topology workspace. The feed and session summary sit below in a flexible column plus a (285px) summary column, separated by a (30px) gap.

The workspace stacks its wrapping toolbar, legend and freshness row, map and optional details, and a footer. Search takes available toolbar space; environment and cluster remain native selects. The graph is horizontally scrollable by default, preserving compact SVG text rather than shrinking the entire topology. The optional detail pane occupies (270px) beside the map.

Graph columns advance by (260 SVG units); nodes are (195 × 58 SVG units), rows advance by (105), and shorter columns are centered. Directed ranks condense cycles before placement. Nodes sort by name and identity. Layout rebuilds when the visible topology changes, not for every activity observation. The generated viewBox is at least (1000 × 400); the source sets matching minimum width and height to preserve the normal reading scale. Fit graph explicitly toggles the minimum width to zero; a second activation restores the current viewBox width.

At (1000px) and below, search takes its own toolbar row, the summary narrows to (250px), the detail pane narrows to (230px), and the header descriptor hides. At (700px) and below, the header becomes (63px), page gutters reduce to (16px), filters share a row, motion controls receive a full row, details move below the graph, and feed and summary stack. The graph and table retain independent horizontal scrolling; the page shell fits the viewport. The map footer and page footer stack. A source rule at (1600px) increases the initial graph height; once topology is rendered, its generated geometry sets the inline height.

**The Stable Map Rule.** Keep activity separate from placement: moving observations must not continuously rearrange a known topology.

## Elevation & Depth

The interface is flat. It uses no box shadows, blur, or gradients. Tonal changes distinguish the page, workspace, fields, and controls; thin borders establish grouping. Hover uses fill changes. Selection and keyboard focus use stronger strokes or outlines, with no simulated lift.

## Shapes

The workspace has softly rounded outer corners; fields are slightly tighter than buttons. Services use modest rounded rectangles. Topics use capsule-shaped nodes, giving them a different silhouette before color is considered. Circular marks identify node and connection states. Edges are thin cubic curves with directional arrowheads. Stale node outlines use a (4 4) dash pattern; selected nodes use a (2px) mint stroke; focused nodes use a (3px) pale mint stroke.

## Components

### Buttons and home link

Buttons have a (38px) minimum height, a (1px) structural border, and compact semibold text. The primary action uses publication mint with dark text. Quiet motion controls use a transparent fill. Icon buttons use centered (18px) inline SVGs. Hover changes the fill; disabled buttons use (0.45) opacity. Focus-visible buttons, links, inputs, and selects use a (2px) pale mint outline offset by (4px).

The header's home link combines a mint inline flow glyph and the Live Flow wordmark. Connection state is a small colored dot paired with status text. The settings icon reveals the API-key form.

### Inputs and filters

Search uses a ground-colored field, leading search glyph, and muted placeholder. Native environment and cluster selects preserve native keyboard behavior. The API-key field follows the same dark field and thin border treatment. Its form is hidden until requested.

### Topology workspace

The bordered container groups controls and operational evidence without separate floating cards. Its empty state centers a small source/topic diagram, a short heading, and ingest/demo guidance. A filtered empty result supplies an alternate heading and recovery instruction.

### Service and topic nodes

Nodes expose a title, namespace or cluster subtitle, and mark. Topic capsules use a greener fill and border; services use a cooler fill and mark. Hover changes the fill. Selection highlights the outline. Nodes are keyboard focusable and activate with Enter or Space. Selecting a node opens details; selecting an observation also supplies trace, span, and record context.

### Activity table and summary

The feed has sticky muted headings, ruled rows, operation labels, secondary metadata lines, and tabular timestamps. Rows support pointer and keyboard selection. A selected row uses a subdued green fill. While a row holds keyboard focus, incoming updates do not replace the feed; its status changes to "Feed held for keyboard focus." It refreshes after focus leaves.

The summary is a flat ruled column of observed counts and a coverage note. On narrow screens it becomes a stacked section with a top border. Aggregate counts remain visible when the stream omits visual events.

### Motion and operational states

Particles are created from observed publish or receive events and follow the corresponding edge. Their travel takes (850–1200ms) and is illustrative; event timing is measured separately. Errors use a larger coral particle. The viewer caps simultaneously animated particles at (180). Pause clears current particles while observations and totals continue updating. Reduced-motion preference starts motion paused and suppresses particle spawning, including replay.

Replay selects the recent (15s) observation window, shows an amber replay label, and replays up to (180) observations with compressed timing where needed. Synthetic demo traffic starts only through the explicit action and remains labeled while running or retained in the topology. Freshness, export delay, reconnect notices, resets, and stream omissions are visible operational states.

## Do's and Don'ts

### Do:

- **Do** keep topology positions stable while observations arrive.
- **Do** pair operation colors with readable labels and preserve the legend.
- **Do** keep stale-node text at full opacity and use dashed borders to signal staleness.
- **Do** preserve graph label size through scrolling at the default scale; keep Fit graph an explicit action.
- **Do** keep the event feed stable while a row has keyboard focus.
- **Do** label synthetic observations and retain measured-timing and coverage explanations.

### Don't:

- **Don't** equate particle travel time with measured message latency.
- **Don't** present a receive observation as completed business processing.
- **Don't** use opacity to dim stale-node text.
- **Don't** add ambient particle motion when there is no observation.
- **Don't** replace the flat operational surfaces with ornamental shadows or gradients.

