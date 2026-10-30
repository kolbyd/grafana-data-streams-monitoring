import { css, cx } from '@emotion/css';
import { GrafanaTheme2 } from '@grafana/data';

const layout = css`
  & {
    background: var(--dsm-ground, Canvas);
    color: var(--dsm-ink, CanvasText);
    min-width: 0;
  }
  & .dsm-kafka-notice {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    padding: 10px 18px;
    background: var(--dsm-surface, Canvas);
    border-bottom: 1px solid var(--dsm-rule);
    color: var(--dsm-muted);
    font-size: 12px;
  }
  & .dsm-kafka-filters {
    display: flex;
    gap: 10px;
    padding: 16px 18px;
    border-bottom: 1px solid var(--dsm-rule);
    align-items: end;
  }
  & .dsm-kafka-coverage {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px 18px;
    padding: 14px 18px;
    border-bottom: 1px solid var(--dsm-rule);
    font-size: 12px;
    color: var(--dsm-muted);
  }
  & .dsm-kafka-coverage strong {
    color: var(--dsm-ink);
  }
  & .dsm-kafka-tabs-row {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    justify-content: space-between;
    gap: 10px;
    padding: 10px 18px 0;
    border-bottom: 1px solid var(--dsm-rule);
  }
  & .dsm-kafka-tabs {
    display: flex;
    align-items: stretch;
    gap: 14px;
    overflow-x: auto;
    min-width: 0;
  }
  & .dsm-kafka-tab-actions {
    display: flex;
    align-items: center;
    gap: 8px;
    padding-bottom: 8px;
  }
  & .dsm-kafka-charts {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 18px;
    padding: 22px 18px;
  }
  & .dsm-kafka-chart {
    min-width: 0;
  }
  & .dsm-kafka-chart h3 {
    font-size: 13px;
    font-weight: 500;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--dsm-rule);
    margin: 0;
  }
  & .dsm-kafka-chart > p {
    font-size: 11px;
    color: var(--dsm-muted);
    line-height: 1.45;
    padding-top: 8px;
  }
  & .dsm-kafka-rate-plot {
    width: 100%;
    padding-top: 12px;
  }
  & .dsm-kafka-rate-plot svg {
    display: block;
    width: 100%;
    height: auto;
    min-height: 150px;
  }
  & .dsm-kafka-rate-plot text {
    fill: var(--dsm-muted);
    font-family: inherit;
    font-size: 10px;
  }
  & .dsm-kafka-gridline {
    stroke: var(--dsm-rule);
    stroke-width: 1px;
  }
  & .dsm-kafka-chart-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 5px 12px;
    padding: 5px 0;
    font-size: 10px;
    color: var(--dsm-muted);
  }
  & .dsm-kafka-chart-legend > span {
    display: inline-flex;
    gap: 5px;
    align-items: center;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  & .dsm-kafka-chart-legend i {
    width: 8px;
    height: 8px;
    flex-shrink: 0;
    border-radius: 2px;
  }
  & .dsm-kafka-chart-empty {
    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: column;
    gap: 12px;
    min-height: 180px;
    padding: 18px;
    text-align: center;
    color: var(--dsm-muted);
    font-size: 12px;
  }
  & .dsm-kafka-unavailable-value {
    color: var(--dsm-muted);
    cursor: help;
  }
  & .dsm-kafka-empty-row {
    height: 80px;
    text-align: center;
    color: var(--dsm-muted);
  }
  & .dsm-kafka-no-inventory {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 14px;
    padding: 34px 24px;
    min-height: 280px;
  }
  & .dsm-kafka-no-inventory h2 {
    font-size: 18px;
  }
  & .dsm-kafka-no-inventory p {
    max-width: 70ch;
    color: var(--dsm-muted);
    line-height: 1.6;
  }
  & .dsm-kafka-pagination {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 8px;
    padding: 12px 18px;
    font-size: 12px;
    color: var(--dsm-muted);
  }
  & .dsm-kafka-pagination > span {
    margin-right: 4px;
  }
  & .dsm-kafka-footer {
    color: var(--dsm-muted);
    font-size: 11px;
    line-height: 1.5;
    padding: 18px;
  }
  @media (max-width: 1000px) {
    & .dsm-kafka-charts {
      gap: 14px;
    }
    & .dsm-kafka-tabs-row {
      align-items: stretch;
    }
  }
  @media (max-width: 700px) {
    & .dsm-kafka-notice {
      align-items: flex-start;
      flex-direction: column;
      gap: 8px;
    }
    & .dsm-kafka-filters {
      flex-wrap: wrap;
      padding: 12px;
    }
    & .dsm-kafka-tabs {
      gap: 12px;
      width: 100%;
    }
    & .dsm-kafka-charts {
      grid-template-columns: minmax(0, 1fr);
      padding: 18px 12px;
    }
    & .dsm-kafka-chart-empty {
      min-height: 120px;
    }
    & .dsm-kafka-coverage {
      padding: 12px;
    }
  }
`;

export const getKafkaConsoleStyles = (theme: GrafanaTheme2) => ({
  root: cx(
    layout,
    css({
      background: theme.colors.background.canvas,
      color: theme.colors.text.primary,
      '& .dsm-kafka-filters > div': { minWidth: 160, marginBottom: 0 },
      '& .dsm-kafka-filters .dsm-kafka-search': { flex: '1 1 280px' },
      '& .dsm-kafka-tabs-row': { alignItems: 'flex-end' },
      '& .dsm-kafka-table-scroll': { overflowX: 'auto', margin: theme.spacing(0, 2, 2) },
      '& .dsm-kafka-tabs-row > div:first-child': { minWidth: 0, overflowX: 'auto' },
      '& .dsm-kafka-chart': {
        background: theme.colors.background.primary,
        border: '1px solid ' + theme.colors.border.weak,
        borderRadius: theme.shape.radius.default,
        padding: theme.spacing(2),
      },
    })
  ),
});
