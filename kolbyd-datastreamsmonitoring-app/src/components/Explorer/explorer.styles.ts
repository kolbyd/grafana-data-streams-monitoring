import { css } from '@emotion/css';

export const explorerStyles = css`
  /* Layout and domain visualization only. Grafana components own control styling.
   Theme variables are populated from the active GrafanaTheme2 in Explorer. */
  & {
    position: relative;
    display: flex;
    flex-direction: column;
    width: 100%;
    min-width: 0;
    min-height: 0;
    overflow: hidden;
    color: var(--dsm-ink);
    background: var(--dsm-ground);
    font-variant-numeric: tabular-nums;
  }

  .dsm-status-banner {
    flex-shrink: 0;
    padding: 8px 16px 0;
  }
  .dsm-status-banner > div {
    margin-bottom: 8px;
  }
  .dsm-banner-content {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px 24px;
  }
  .dsm-banner-content > button {
    margin-left: auto;
  }
  .dsm-view-row {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--dsm-rule);
    flex-shrink: 0;
  }
  .dsm-current-view {
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 190px;
    white-space: nowrap;
  }

  .dsm-filter-row {
    display: flex;
    align-items: flex-end;
    flex-wrap: wrap;
    gap: 12px;
    padding: 12px 16px;
    border-bottom: 1px solid var(--dsm-rule);
    flex-shrink: 0;
  }
  .dsm-search {
    flex: 1;
    min-width: 220px;
  }
  .dsm-select {
    width: 180px;
    min-width: 120px;
  }
  .dsm-explore-body {
    display: flex;
    align-items: stretch;
    position: relative;
    flex: 1;
    min-height: 0;
    overflow: hidden;
  }
  .dsm-facets {
    position: absolute;
    inset: 0 auto 0 0;
    z-index: 4;
    width: 280px;
    max-width: 100%;
    padding: 16px;
    border-right: 1px solid var(--dsm-rule);
    background: var(--dsm-surface);
    overflow: auto;
  }
  .dsm-facets h2 {
    font-size: 14px;
    margin: 0 0 16px;
  }
  .dsm-quick-filter {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
    margin-bottom: 20px;
  }
  .dsm-quick-filter > span {
    font-weight: 500;
  }
  .dsm-quick-filter small {
    color: var(--dsm-muted);
    line-height: 1.5;
  }
  .dsm-facet-search {
    margin: 24px 0 12px;
  }
  .dsm-facet-group {
    padding: 16px 0;
    border-top: 1px solid var(--dsm-rule);
  }
  .dsm-facet-group h3 {
    display: flex;
    justify-content: space-between;
    font-size: 14px;
    margin: 0 0 12px;
  }
  .dsm-facet-group h3 span,
  .dsm-facet-group p {
    color: var(--dsm-muted);
    font-weight: 400;
  }
  .dsm-facet-value {
    padding-block: 5px;
    overflow-wrap: anywhere;
  }
  .dsm-map-panel {
    flex: 1;
    min-width: 0;
    min-height: 0;
    position: relative;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .dsm-map-toolbar {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
    padding: 12px 16px;
    flex-shrink: 0;
  }
  .dsm-map-toolbar strong {
    font-size: 12px;
    font-weight: 500;
  }
  .dsm-toolbar-spacer {
    flex: 1;
  }
  .dsm-map-scroll {
    flex: 1;
    min-height: 0;
    overflow: hidden;
  }
  .dsm-topology {
    display: block;
    max-width: none;
  }
  .dsm-zoom {
    position: absolute;
    z-index: 2;
    top: 12px;
    right: 12px;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .dsm-zoom button {
    width: 32px;
  }
  .dsm-empty {
    flex: 1;
    min-height: 0;
    display: flex;
    justify-content: center;
    align-items: center;
    flex-direction: column;
    text-align: center;
    padding: 32px;
  }
  .dsm-empty > svg {
    color: var(--dsm-muted);
    margin-bottom: 16px;
  }
  .dsm-empty h2 {
    font-size: 24px;
    margin: 0 0 12px;
  }
  .dsm-empty p {
    max-width: 48ch;
    color: var(--dsm-muted);
    line-height: 1.6;
    margin-bottom: 24px;
  }
  .dsm-empty > div {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    justify-content: center;
  }
  .dsm-details {
    position: absolute;
    inset: 0 0 0 auto;
    width: 400px;
    max-width: 100%;
    z-index: 5;
    border-left: 1px solid var(--dsm-rule);
    min-width: 0;
    overflow: auto;
    background: var(--dsm-surface);
  }
  .dsm-details > header {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 16px;
  }
  .dsm-details h2 {
    font-size: 18px;
    margin: 0;
    overflow-wrap: anywhere;
  }
  .dsm-details > header > svg {
    flex-shrink: 0;
    margin-top: 4px;
    color: var(--dsm-muted);
  }
  .dsm-details > header > button {
    margin-left: auto;
    flex-shrink: 0;
  }
  .dsm-detail-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 0 16px 16px;
    color: var(--dsm-muted);
    font-size: 12px;
  }
  .dsm-details nav {
    padding-inline: 16px;
    overflow-x: auto;
  }
  .dsm-details nav > div {
    min-width: max-content;
  }
  .dsm-detail-content {
    padding: 16px;
  }
  .dsm-detail-content h3 {
    font-size: 16px;
    margin-bottom: 16px;
  }
  .dsm-detail-chart {
    padding-block: 8px;
    margin-bottom: 24px;
  }
  .dsm-detail-chart svg {
    width: 100%;
    height: auto;
  }
  .dsm-detail-content dl {
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 16px 12px;
    margin: 0;
    font-size: 12px;
  }
  .dsm-detail-content dt {
    color: var(--dsm-muted);
  }
  .dsm-detail-content dd {
    margin: 0;
    text-align: right;
    overflow-wrap: anywhere;
    max-width: 180px;
  }
  .dsm-detail-content > p {
    color: var(--dsm-muted);
    font-size: 12px;
    line-height: 1.6;
    margin-top: 24px;
  }
  .dsm-dependency {
    display: flex;
    align-items: flex-start;
    flex-direction: column;
    padding: 12px 0;
    border-bottom: 1px solid var(--dsm-rule);
    gap: 4px;
  }
  .dsm-dependency small {
    color: var(--dsm-muted);
  }
  .dsm-related-events small {
    color: var(--dsm-muted);
    font-size: 11px;
    margin-left: 8px;
  }
  .dsm-related-events dl {
    display: block;
    padding-block: 12px;
  }
  .dsm-related-events dd {
    text-align: left;
    margin: 4px 0 12px;
    max-width: none;
  }
  .dsm-error-text {
    color: var(--dsm-error);
  }
  .dsm-table-scroll {
    min-width: 0;
    overflow: auto;
  }
  .dsm-table-empty {
    padding: 24px;
    color: var(--dsm-muted);
  }
  .dsm-list-view {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 0 16px 24px;
  }
  .dsm-segmented {
    margin: 4px 0 12px;
  }
  .dsm-list-title {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding-block: 8px;
  }
  .dsm-column-choices {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    padding: 16px 0;
    border-block: 1px solid var(--dsm-rule);
  }
  .dsm-cell-metric {
    position: relative;
    display: inline-block;
    padding-bottom: 5px;
    min-width: 80px;
    font-variant-numeric: tabular-nums;
  }
  .dsm-cell-metric i {
    position: absolute;
    bottom: 0;
    left: 0;
    height: 3px;
    background: var(--dsm-blue);
    opacity: 0.6;
  }
  .dsm-measure-builder {
    display: flex;
    flex: 1;
    min-height: 0;
    overflow: hidden;
  }
  .dsm-pathway-controls {
    flex: 0 0 300px;
    width: 300px;
    padding: 20px 16px;
    border-right: 1px solid var(--dsm-rule);
    overflow: auto;
  }
  .dsm-pathway-controls h2 {
    font-size: 18px;
    margin-bottom: 20px;
  }
  .dsm-pathway-actions {
    display: flex;
    gap: 8px;
  }
  .dsm-pathway-node {
    border-top: 1px solid var(--dsm-rule);
    margin-top: 24px;
    padding-top: 16px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    align-items: flex-start;
  }
  .dsm-pathway-node h3 {
    font-size: 14px;
    overflow-wrap: anywhere;
  }
  .dsm-pathway-controls dl {
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 12px;
    margin: 24px 0;
    font-size: 12px;
  }
  .dsm-pathway-controls dt {
    color: var(--dsm-muted);
  }
  .dsm-pathway-controls dd {
    margin: 0;
  }
  .dsm-pathway-message {
    color: var(--dsm-muted);
    margin-top: 24px;
  }
  .dsm-measure-map {
    position: relative;
    flex: 1;
    min-width: 0;
    min-height: 0;
    display: flex;
  }
  .dsm-measure-map .dsm-zoom {
    top: 16px;
  }
  .dsm-setup {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 24px;
  }
  .dsm-setup-heading {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    flex-wrap: wrap;
    gap: 16px;
  }
  .dsm-setup-heading h2 {
    font-size: 24px;
    margin-bottom: 8px;
  }
  .dsm-setup-heading p,
  .dsm-setup-heading > span {
    color: var(--dsm-muted);
  }
  .dsm-enable-collection {
    margin-block: 24px;
  }
  .dsm-setup-tabs {
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px;
  }
  .dsm-setup-filters {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    margin-block: 24px;
  }
  .dsm-setup-filters > div {
    width: 220px;
    max-width: 100%;
  }
  .dsm-setup-count {
    font-size: 14px;
    margin-bottom: 16px;
  }
  .dsm-setup-guide {
    margin-top: 32px;
  }
  .dsm-setup-guide pre {
    overflow: auto;
    padding: 16px;
    background: var(--dsm-surface);
    color: var(--dsm-ink);
    font-size: 12px;
  }
  .dsm-setup-guide p {
    max-width: 75ch;
    color: var(--dsm-muted);
    line-height: 1.6;
  }
  .dsm-setup-guide h3 {
    font-size: 16px;
    margin-block: 24px 16px;
  }
  .dsm-coverage {
    display: grid;
    grid-template-columns: minmax(120px, 200px) 1fr;
    gap: 16px;
    max-width: 900px;
  }
  .dsm-coverage dd {
    margin: 0;
    color: var(--dsm-muted);
  }
  .dsm-spark path {
    fill: var(--dsm-chart-area);
  }
  @media (max-width: 1200px) {
  }
  @media (max-width: 850px) {
    .dsm-details {
      position: absolute;
      inset: 0;
      width: 100%;
      z-index: 3;
    }
  }
  @media (max-width: 700px) {
    .dsm-view-row,
    .dsm-filter-row {
      padding: 10px 12px;
      gap: 8px;
    }
    .dsm-status-banner {
      padding-inline: 12px;
    }

    .dsm-current-view {
      max-width: 125px;
    }
    .dsm-search {
      flex-basis: 100%;
      min-width: 0;
    }
    .dsm-select {
      flex: 1;
      width: auto;
      min-width: 125px;
    }
    .dsm-options {
      margin-top: 4px;
    }
    .dsm-facets {
      position: absolute;
      top: 0;
      bottom: 0;
      left: 0;
      z-index: 2;
      width: 260px;
      background: var(--dsm-ground);
    }
    .dsm-map-toolbar {
      gap: 8px;
      padding: 10px 12px;
    }
    .dsm-map-toolbar strong {
      order: 4;
      flex-basis: 100%;
    }
    .dsm-map-panel .dsm-zoom {
      top: 12px;
      right: 12px;
    }
    .dsm-empty {
      padding: 24px 16px;
    }
    .dsm-empty h2 {
      font-size: 20px;
    }
    .dsm-list-view {
      padding-inline: 12px;
    }
    .dsm-measure-builder {
      flex-direction: column;
    }
    .dsm-pathway-controls {
      flex: 0 0 auto;
      width: 100%;
      max-height: 280px;
      border-right: 0;
      border-bottom: 1px solid var(--dsm-rule);
    }
    .dsm-setup {
      padding: 16px 12px;
    }
    .dsm-setup-filters > div {
      width: 100%;
    }
    .dsm-coverage {
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .dsm-coverage dd {
      margin-bottom: 8px;
    }
  }

  /* Domain-specific SVG topology. */

  .dsm-node {
    cursor: pointer;
    outline: none;
  }
  .dsm-service-outline {
    fill: var(--dsm-node-fill);
    stroke: var(--dsm-border);
    stroke-width: 1.2;
  }
  .dsm-node:hover .dsm-service-outline {
    stroke: var(--dsm-blue);
  }
  .dsm-node.is-selected .dsm-service-outline,
  .dsm-node:focus-visible .dsm-service-outline {
    stroke: var(--dsm-blue);
    stroke-width: 2.5;
  }
  .dsm-node.has-error .dsm-service-outline {
    stroke: var(--dsm-error);
  }
  .dsm-error-bar {
    fill: var(--dsm-error-bg);
  }
  .dsm-error-caption {
    font-size: 9px;
    fill: var(--dsm-error);
  }
  .dsm-node.is-stale .dsm-service-outline {
    stroke-dasharray: 4 3;
  }
  .dsm-node-name {
    fill: var(--dsm-ink);
    font-size: 11px;
    font-weight: 500;
  }
  .dsm-node-rate {
    fill: var(--dsm-muted);
    font-size: 10px;
  }
  .dsm-service-glyph {
    fill: none;
    stroke: var(--dsm-muted);
    stroke-width: 0.8;
  }
  .dsm-service-glyph rect {
    stroke: var(--dsm-border);
  }
  .dsm-topic-body {
    fill: var(--dsm-surface);
    stroke: var(--dsm-border);
    stroke-width: 1.5;
  }
  .dsm-topic-end {
    fill: var(--dsm-topic-end);
    stroke: var(--dsm-ground);
    stroke-width: 1.5;
  }
  .dsm-node.is-selected .dsm-topic-body,
  .dsm-node:focus-visible .dsm-topic-body {
    stroke: var(--dsm-blue);
    stroke-width: 2.5;
  }
  .dsm-node.is-stale .dsm-topic-body {
    stroke-dasharray: 4 3;
  }
  .dsm-topic-name {
    fill: var(--dsm-ink);
    font-size: 11px;
  }

  .dsm-facet-heading {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 16px;
  }
  .dsm-facet-heading h2 {
    margin: 0;
  }
  .dsm-setup-preview {
    margin-top: 24px;
  }
`;
