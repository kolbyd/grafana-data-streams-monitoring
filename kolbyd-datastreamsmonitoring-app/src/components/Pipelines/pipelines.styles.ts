import { css, cx } from '@emotion/css';
import { GrafanaTheme2 } from '@grafana/data';

const layout = css`
  & {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;
    background: var(--dsm-ground);
    color: var(--dsm-ink);
  }
  & .dsm-pipeline-catalog-heading {
    display: flex;
    flex: none;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    min-height: 66px;
    padding: 14px 18px;
    border-bottom: 1px solid var(--dsm-rule);
  }
  & .dsm-pipeline-catalog-heading > div:last-child {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  & .dsm-pipeline-catalog-heading h2 {
    font-size: 21px;
    font-weight: 600;
  }
  & .dsm-pipeline-breadcrumb {
    display: flex;
    align-items: center;
    gap: 12px;
    min-width: 0;
  }
  & .dsm-pipeline-breadcrumb > svg {
    transform: rotate(180deg);
    color: var(--dsm-muted);
    flex: none;
  }
  & .dsm-pipeline-breadcrumb h2 {
    font-size: 18px;
    overflow-wrap: anywhere;
  }
  & .dsm-pipeline-env-tag {
    display: inline-block;
    color: var(--dsm-blue);
    background: var(--dsm-surface);
    border-radius: 3px;
    font-size: 11px;
    font-weight: 400;
    padding: 2px 5px;
    margin-left: 6px;
    white-space: nowrap;
  }
  & .dsm-pipeline-catalog-tabs {
    display: flex;
    flex: none;
    align-items: center;
    gap: 22px;
    padding: 0 18px;
    border-bottom: 1px solid var(--dsm-rule);
    min-height: 40px;
  }
  & .dsm-pipeline-catalog-tabs > span {
    color: var(--dsm-muted);
    font-size: 12px;
  }
  & .dsm-pipeline-catalog-tabs .active {
    align-self: stretch;
    display: flex;
    align-items: center;
    gap: 7px;
    color: var(--dsm-ink);
    border-bottom: 2px solid var(--dsm-blue);
  }
  & .dsm-pipeline-catalog-tabs b,
  & .dsm-pipeline-count {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 20px;
    height: 20px;
    padding: 0 5px;
    border-radius: 10px;
    background: var(--dsm-surface);
    color: var(--dsm-muted);
    font-size: 11px;
  }
  & .dsm-pipeline-coverage-strip {
    display: flex;
    flex: none;
    flex-wrap: wrap;
    gap: 16px 30px;
    padding: 15px 20px;
    background: var(--dsm-surface);
    border-bottom: 1px solid var(--dsm-rule);
    color: var(--dsm-muted);
    font-size: 12px;
  }
  & .dsm-pipeline-coverage-strip > span {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
  & .dsm-pipeline-coverage-strip strong {
    color: var(--dsm-ink);
    font-weight: 500;
  }
  & .dsm-pipeline-catalog-filters {
    display: flex;
    flex: none;
    flex-wrap: wrap;
    align-items: end;
    gap: 12px;
    padding: 16px 18px;
  }

  & .dsm-pipeline-muted {
    color: var(--dsm-muted);
  }
  & .dsm-pipeline-catalog-footer {
    flex: none;
    padding: 12px 18px;
    color: var(--dsm-muted);
    font-size: 11px;
    border-top: 1px solid var(--dsm-rule);
  }
  & .dsm-pipeline-empty {
    display: flex;
    align-items: center;
    flex-direction: column;
    justify-content: center;
    gap: 12px;
    min-height: 230px;
    padding: 30px;
    text-align: center;
  }
  & .dsm-pipeline-empty p {
    color: var(--dsm-muted);
    max-width: 65ch;
  }
  & .dsm-pipeline-error,
  & .dsm-pipeline-storage-error {
    color: var(--dsm-error);
  }
  & .dsm-pipeline-storage-error {
    flex: none;
    margin: 0;
    padding: 10px 18px;
    border-bottom: 1px solid var(--dsm-rule);
  }
  & .dsm-pipeline-remove {
    display: flex;
    flex: none;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
    padding: 12px 18px;
    background: var(--dsm-surface);
    border-bottom: 1px solid var(--dsm-rule);
  }
  & .dsm-pipeline-remove span {
    flex: 1;
    min-width: 200px;
  }
  & .dsm-pipeline-form {
    flex: none;
    max-height: 60vh;
    overflow: auto;
    margin: 12px 18px;
    padding: 16px;
    border: 1px solid var(--dsm-rule);
    border-radius: 4px;
    background: var(--dsm-surface);
  }
  & .dsm-pipeline-form-heading {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 14px;
  }
  & .dsm-pipeline-form-fields {
    display: grid;
    grid-template-columns: minmax(180px, 1.4fr) minmax(120px, 0.7fr) minmax(180px, 1fr) minmax(180px, 1fr);
    gap: 14px;
  }
  & .dsm-pipeline-form > p {
    margin-top: 12px;
    color: var(--dsm-muted);
    font-size: 12px;
  }
  & .dsm-pipeline-form .dsm-pipeline-error {
    color: var(--dsm-error);
  }
  & .dsm-pipeline-form-actions {
    display: flex;
    gap: 8px;
    margin-top: 14px;
  }
  & .dsm-pipeline-detail {
    display: flex;
    flex: 1;
    min-height: 0;
    gap: 10px;
    padding: 16px 10px 0;
    overflow: hidden;
  }
  & .dsm-pipeline-detail-rail {
    display: flex;
    flex: none;
    flex-direction: column;
    align-self: flex-start;
    gap: 3px;
    width: 172px;
    padding: 4px;
    border: 1px solid var(--dsm-rule);
    border-radius: 5px;
  }

  & .dsm-pipeline-detail.rail-collapsed .dsm-pipeline-detail-rail {
    width: 43px;
  }
  & .dsm-pipeline-detail-content {
    display: flex;
    flex: 1;
    min-width: 0;
    min-height: 0;
    flex-direction: column;
    gap: 16px;
    overflow: auto;
    padding: 0 8px 18px 0;
    scroll-behavior: smooth;
  }
  & .dsm-pipeline-section {
    flex: none;
    min-width: 0;
    border: 1px solid var(--dsm-rule);
    border-radius: 5px;
    background: var(--dsm-ground);
    scroll-margin-top: 0;
  }
  & .dsm-pipeline-section > header {
    display: flex;
    align-items: center;
    gap: 12px;
    min-height: 52px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--dsm-rule);
  }
  & .dsm-pipeline-section > header > span:empty {
    flex: 1;
  }
  & .dsm-pipeline-section > header h3 {
    font-size: 16px;
    font-weight: 500;
  }
  & .dsm-pipeline-definition-map {
    min-height: 280px;
    height: 300px;
    overflow: auto;
  }
  & .dsm-pipeline-definition-map .dsm-map-scroll {
    min-height: 280px;
    height: 100%;
  }
  & .dsm-pipeline-definition-map .dsm-topology {
    margin-top: -45px;
  }
  & .dsm-pipeline-picked {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 10px 16px;
    padding: 10px 16px;
    border-top: 1px solid var(--dsm-rule);
    color: var(--dsm-muted);
    font-size: 12px;
  }
  & .dsm-pipeline-picked strong {
    color: var(--dsm-ink);
  }
  & .dsm-pipeline-summary-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 24px 20px;
    padding: 20px 16px;
  }
  & .dsm-pipeline-summary-grid article {
    min-width: 0;
  }
  & .dsm-pipeline-plot-heading {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--dsm-rule);
  }
  & .dsm-pipeline-plot-heading h4 {
    margin: 0;
    font-size: 13px;
    font-weight: 500;
  }
  & .dsm-pipeline-plot-heading strong {
    font-size: 12px;
    font-weight: 400;
    color: var(--dsm-muted);
  }
  & .dsm-pipeline-metric-unavailable {
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 8px;
    min-height: 160px;
    padding: 20px;
    color: var(--dsm-muted);
    text-align: center;
  }
  & .dsm-pipeline-metric-unavailable small {
    font-size: 12px;
    max-width: 55ch;
    align-self: center;
  }
  & .dsm-pipeline-summary-grid .dsm-spark {
    display: block;
    width: 100%;
    height: 150px;
    padding-top: 18px;
  }
  & .dsm-pipeline-latency-plot {
    display: block;
    width: 100%;
    height: 160px;
  }
  & .dsm-pipeline-latency-plot text {
    fill: var(--dsm-muted);
    font-size: 10px;
  }
  & .dsm-pipeline-chart-grid {
    stroke: var(--dsm-rule);
    stroke-width: 1;
    fill: none;
  }
  & .dsm-pipeline-chart-line {
    stroke: var(--dsm-blue);
    stroke-width: 1.6;
    fill: none;
  }
  & .dsm-pipeline-plot-note {
    margin: 8px 0 0;
    color: var(--dsm-muted);
    font-size: 11px;
  }
  & .dsm-pipeline-summary-note {
    padding: 0 16px 16px;
    color: var(--dsm-muted);
    font-size: 12px;
  }
  & .dsm-pipeline-monitor-empty {
    padding: 24px;
  }
  & .dsm-pipeline-monitor-empty h4 {
    margin: 0 0 8px;
    font-size: 14px;
    font-weight: 500;
  }
  & .dsm-pipeline-monitor-empty p {
    color: var(--dsm-muted);
    max-width: 65ch;
    margin-bottom: 16px;
  }
  & .dsm-pipeline-unavailable {
    padding: 32px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }
  & .dsm-pipeline-unavailable p {
    color: var(--dsm-muted);
  }
  & .dsm-pipeline-detail-footer {
    padding: 0 8px 10px;
    color: var(--dsm-muted);
    font-size: 11px;
  }
  @media (max-width: 1100px) {
    & .dsm-pipeline-form-fields {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    & .dsm-pipeline-catalog-heading h2 {
      font-size: 18px;
    }
    & .dsm-pipeline-summary-grid {
      gap: 16px;
    }
  }
  @media (max-width: 720px) {
    & .dsm-pipeline-catalog-heading {
      padding: 12px;
      align-items: flex-start;
    }
    & .dsm-pipeline-catalog-heading > div:last-child {
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    & .dsm-pipeline-breadcrumb {
      flex-wrap: wrap;
      gap: 6px;
    }
    & .dsm-pipeline-breadcrumb h2 {
      flex-basis: 100%;
    }
    & .dsm-pipeline-breadcrumb > svg {
      display: none;
    }
    & .dsm-pipeline-catalog-tabs {
      padding-inline: 12px;
      gap: 12px;
    }
    & .dsm-pipeline-catalog-tabs > span:last-child {
      font-size: 11px;
    }
    & .dsm-pipeline-coverage-strip {
      gap: 10px;
      padding: 12px;
    }
    & .dsm-pipeline-coverage-strip > span {
      width: 100%;
    }
    & .dsm-pipeline-catalog-filters {
      padding: 12px;
      gap: 10px;
    }

    & .dsm-pipeline-catalog-footer {
      padding: 10px 12px;
    }
    & .dsm-pipeline-detail {
      flex-direction: column;
      padding: 10px 8px 0;
      gap: 10px;
    }
    & .dsm-pipeline-detail-rail {
      flex-direction: row;
      flex-wrap: wrap;
      align-self: stretch;
      width: auto;
      gap: 2px;
    }

    & .dsm-pipeline-detail.rail-collapsed .dsm-pipeline-detail-rail {
      width: auto;
    }
    & .dsm-pipeline-detail-content {
      padding-right: 0;
    }
    & .dsm-pipeline-summary-grid {
      grid-template-columns: minmax(0, 1fr);
      padding: 16px 12px;
    }
    & .dsm-pipeline-form {
      margin: 10px 12px;
      padding: 12px;
    }
    & .dsm-pipeline-form-fields {
      grid-template-columns: minmax(0, 1fr);
    }
    & .dsm-pipeline-section > header {
      padding-inline: 12px;
    }
  }
  @media (prefers-reduced-motion: reduce) {
    & .dsm-pipeline-detail-content {
      scroll-behavior: auto;
    }
  }
`;

export const getPipelineStyles = (theme: GrafanaTheme2) => ({
  root: cx(
    layout,
    css({
      background: theme.colors.background.canvas,
      color: theme.colors.text.primary,
      '& .dsm-pipeline-form': { background: theme.colors.background.secondary, borderColor: theme.colors.border.weak },
      '& .dsm-pipeline-catalog-filters > div': { minWidth: 160, marginBottom: 0 },
      '& .dsm-pipeline-catalog-filters .dsm-pipeline-search': { flex: '1 1 260px' },
      '& .dsm-pipeline-form-fields > div': { minWidth: 0 },
      '& .dsm-pipeline-catalog-table': { flex: 1, minHeight: 0, overflow: 'auto', padding: theme.spacing(0, 2, 2) },
      '& .dsm-pipeline-components-table': { overflowX: 'auto', padding: theme.spacing(1, 2) },
      '& .dsm-pipeline-detail-rail': { gap: theme.spacing(1), padding: theme.spacing(1) },
      '& .dsm-pipeline-detail-rail > button': { justifyContent: 'flex-start' },
    })
  ),
});
