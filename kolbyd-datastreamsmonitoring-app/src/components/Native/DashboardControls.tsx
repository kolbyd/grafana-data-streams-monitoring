import React, { useEffect, useRef, useState } from 'react';
import { css } from '@emotion/css';
import { dateTime, getTimeZone, rangeUtil, type GrafanaTheme2, type TimeRange } from '@grafana/data';
import { TimeRangePicker, useStyles2 } from '@grafana/ui';
import type { TimeZone } from '@grafana/schema';
import * as scenes from '@grafana/scenes';

// Scenes 9.2 exports these constructors at runtime, but its declaration bundle
// mistakenly marks their exports type-only. Keep the adapter typed by public state APIs.
const { EmbeddedScene, SceneFlexLayout, SceneVariableSet, VariableValueSelectors } = scenes;
const { CustomVariable, AdHocFiltersVariable } = scenes as unknown as {
  CustomVariable: new (state: Parameters<scenes.CustomVariable['setState']>[0]) => scenes.CustomVariable;
  AdHocFiltersVariable: new (
    state: Parameters<scenes.AdHocFiltersVariable['setState']>[0]
  ) => scenes.AdHocFiltersVariable;
};

class FlowFilterController extends scenes.AdHocFiltersVariableController {
  getOperators() {
    return super.getOperators().filter((option) => ['=', '!=', '=~', '!~'].includes(String(option.value)));
  }
}
class FlowFiltersVariable extends AdHocFiltersVariable {
  static Component = FlowFiltersVariableRenderer;
}
function FlowFiltersVariableRenderer({ model }: { model: FlowFiltersVariable }) {
  const [controller] = useState(() => new FlowFilterController(model));
  const styles = useStyles2(getStyles);
  return (
    <div className={styles.filter}>
      <scenes.AdHocFiltersComboboxRenderer controller={controller} />
    </div>
  );
}

export type DashboardFilter = { key: string; operator: string; value: string; condition?: string };
export interface DashboardControlsProps {
  showFilters: boolean;
  environment: string;
  cluster: string;
  environments: string[];
  clusters: string[];
  services: string[];
  queues: string[];
  allServices: string[];
  allQueues: string[];
  onServicesChange: (values: string[]) => void;
  onQueuesChange: (values: string[]) => void;
  actions?: React.ReactNode;
  filters: DashboardFilter[];
  filterValues: { services: string[]; topics: string[]; consumerGroups: string[] };
  onEnvironmentChange: (value: string) => void;
  onClusterChange: (value: string) => void;
  onFiltersChange: (filters: DashboardFilter[]) => void;
  timeRange: TimeRange;
  onTimeRangeChange: (range: TimeRange) => void;
  timeZone?: TimeZone;
  onTimeZoneChange?: (zone: TimeZone) => void;
}

const tagKeys = [
  { text: 'service', value: 'service' },
  { text: 'topic', value: 'topic' },
  { text: 'consumer_group', value: 'consumer_group' },
  { text: 'search', value: 'search' },
  { text: 'error', value: 'error' },
];
const optionsQuery = (values: string[], selected: string) =>
  JSON.stringify([
    { text: 'All', value: '' },
    ...[...new Set([...values, selected].filter(Boolean))].sort().map((value) => ({ text: value, value })),
  ]);
const cleanFilters = (filters: DashboardFilter[]): DashboardFilter[] =>
  filters.map(({ key, operator, value, condition }) => ({ key, operator, value, ...(condition ? { condition } : {}) }));

/** Grafana's own dashboard variable pills and complete time range picker. */
export function DashboardControls(props: DashboardControlsProps) {
  const styles = useStyles2(getStyles);
  const latest = useRef(props);
  const syncing = useRef(false);
  const [localTimeZone, setLocalTimeZone] = useState<TimeZone>(() => getTimeZone());
  const [model] = useState(() => {
    const environment = new CustomVariable({
      name: 'environment',
      label: 'Environment',
      value: props.environment,
      text: props.environment || 'All',
      valuesFormat: 'json',
      query: optionsQuery(props.environments, props.environment),
      skipUrlSync: true,
    });
    const cluster = new CustomVariable({
      name: 'cluster',
      label: 'Cluster',
      value: props.cluster,
      text: props.cluster || 'All',
      valuesFormat: 'json',
      query: optionsQuery(props.clusters, props.cluster),
      skipUrlSync: true,
    });
    const services = new CustomVariable({
      name: 'services',
      label: 'Services',
      isMulti: true,
      includeAll: true,
      defaultToAll: true,
      skipUrlSync: true,
      valuesFormat: 'json',
      query: multiOptionsQuery(props.allServices, props.services),
      value: props.services.length ? props.services : ['$__all'],
      text: props.services.length ? props.services : ['All'],
    });
    const queues = new CustomVariable({
      name: 'queues',
      label: 'Queues',
      isMulti: true,
      includeAll: true,
      defaultToAll: true,
      skipUrlSync: true,
      valuesFormat: 'json',
      query: multiOptionsQuery(props.allQueues, props.queues),
      value: props.queues.length ? props.queues : ['$__all'],
      text: props.queues.length ? props.queues : ['All'],
    });
    const filters = new FlowFiltersVariable({
      name: 'filters',
      label: 'Filter',
      filters: cleanFilters(props.filters),
      applyMode: 'manual',
      datasource: null,
      skipUrlSync: true,
      allowCustomValue: true,
      supportsMultiValueOperators: false,
      enableGroupBy: false,
      addFilterButtonText: 'Filter',
      inputPlaceholder: 'Add filter',
      getTagKeysProvider: async () => ({ replace: true, values: tagKeys }),
      getTagValuesProvider: tagValuesProvider(props.filterValues),
    });
    const selectors = new VariableValueSelectors({});
    const scene = new EmbeddedScene({
      $variables: new SceneVariableSet({ variables: [filters, environment, cluster, services, queues] }),
      controls: [selectors],
      body: new SceneFlexLayout({ children: [] }),
    });
    return { environment, cluster, services, queues, filters, selectors, scene };
  });
  useEffect(() => {
    latest.current = props;
  });
  useEffect(() => model.scene.activate(), [model]);
  useEffect(() => {
    // Scenes clears variable listeners when their pill components deactivate.
    // Reconnect controlled callbacks whenever the variables become visible again.
    if (!props.showFilters) {
      return;
    }
    const env = model.environment.subscribeToState((next, previous) => {
      if (!syncing.current && next.value !== previous.value && String(next.value) !== latest.current.environment) {
        latest.current.onEnvironmentChange(String(next.value));
      }
    });
    const cluster = model.cluster.subscribeToState((next, previous) => {
      if (!syncing.current && next.value !== previous.value && String(next.value) !== latest.current.cluster) {
        latest.current.onClusterChange(String(next.value));
      }
    });
    const filters = model.filters.subscribeToState((next, previous) => {
      if (!syncing.current && next.filters !== previous.filters) {
        const clean = cleanFilters(next.filters);
        if (JSON.stringify(clean) !== JSON.stringify(cleanFilters(latest.current.filters))) {
          latest.current.onFiltersChange(clean);
        }
      }
    });
    const services = model.services.subscribeToState((next, previous) => {
      if (!syncing.current && next.value !== previous.value) {
        const values = selection(next.value);
        if (!sameSelection(values, latest.current.services)) {
          latest.current.onServicesChange(values);
        }
      }
    });
    const queues = model.queues.subscribeToState((next, previous) => {
      if (!syncing.current && next.value !== previous.value) {
        const values = selection(next.value);
        if (!sameSelection(values, latest.current.queues)) {
          latest.current.onQueuesChange(values);
        }
      }
    });
    return () => {
      services.unsubscribe();
      queues.unsubscribe();
      env.unsubscribe();
      cluster.unsubscribe();
      filters.unsubscribe();
    };
  }, [model, props.showFilters]);
  const environmentQuery = optionsQuery(props.environments, props.environment);
  const clusterQuery = optionsQuery(props.clusters, props.cluster);
  const servicesQuery = multiOptionsQuery(props.allServices, props.services);
  const queuesQuery = multiOptionsQuery(props.allQueues, props.queues);
  const servicesKey = JSON.stringify(props.services);
  const queuesKey = JSON.stringify(props.queues);
  const filtersKey = JSON.stringify(cleanFilters(props.filters));
  const filterValuesKey = JSON.stringify(props.filterValues);
  useEffect(() => {
    model.filters.setState({ getTagValuesProvider: tagValuesProvider(latest.current.filterValues) });
  }, [model, filterValuesKey]);
  useEffect(() => {
    syncing.current = true;
    if (model.services.state.query !== servicesQuery) {
      model.services.setState({ query: servicesQuery });
      model.services.refreshOptions();
    }
    if (model.queues.state.query !== queuesQuery) {
      model.queues.setState({ query: queuesQuery });
      model.queues.refreshOptions();
    }
    if (!sameSelection(selection(model.services.state.value), latest.current.services)) {
      model.services.changeValueTo(
        latest.current.services.length ? latest.current.services : ['$__all'],
        latest.current.services.length ? latest.current.services : ['All']
      );
    }
    if (!sameSelection(selection(model.queues.state.value), latest.current.queues)) {
      model.queues.changeValueTo(
        latest.current.queues.length ? latest.current.queues : ['$__all'],
        latest.current.queues.length ? latest.current.queues : ['All']
      );
    }
    if (model.environment.state.query !== environmentQuery) {
      model.environment.setState({ query: environmentQuery });
      model.environment.refreshOptions();
    }
    if (model.cluster.state.query !== clusterQuery) {
      model.cluster.setState({ query: clusterQuery });
      model.cluster.refreshOptions();
    }
    if (model.environment.state.value !== props.environment) {
      model.environment.changeValueTo(props.environment, props.environment || 'All');
    }
    if (model.cluster.state.value !== props.cluster) {
      model.cluster.changeValueTo(props.cluster, props.cluster || 'All');
    }
    if (JSON.stringify(cleanFilters(model.filters.state.filters)) !== filtersKey) {
      model.filters.updateFilters(cleanFilters(latest.current.filters), { skipPublish: true });
    }
    syncing.current = false;
  }, [
    model,
    environmentQuery,
    clusterQuery,
    filtersKey,
    props.environment,
    props.cluster,
    servicesQuery,
    queuesQuery,
    servicesKey,
    queuesKey,
  ]);

  function move(direction: number) {
    const range = rangeUtil.convertRawToRange(props.timeRange.raw, props.timeZone ?? localTimeZone);
    const duration = range.to.valueOf() - range.from.valueOf();
    const from = dateTime(range.from.valueOf() + duration * direction);
    const to = dateTime(range.to.valueOf() + duration * direction);
    props.onTimeRangeChange({ from, to, raw: { from, to } });
  }
  function zoomOut() {
    const range = rangeUtil.convertRawToRange(props.timeRange.raw, props.timeZone ?? localTimeZone);
    const duration = Math.max(1000, range.to.valueOf() - range.from.valueOf());
    const from = dateTime(range.from.valueOf() - duration / 2);
    const to = dateTime(range.to.valueOf() + duration / 2);
    props.onTimeRangeChange({ from, to, raw: { from, to } });
  }
  return (
    <div className={styles.row} data-testid="dashboard-controls">
      {props.showFilters && (
        <div className={styles.variables} data-testid="dashboard-variable-pills">
          <model.selectors.Component model={model.selectors} />
        </div>
      )}
      {props.actions && <div className={styles.actions}>{props.actions}</div>}
      <div className={styles.time} data-testid="dashboard-time-picker">
        <TimeRangePicker
          value={props.timeRange}
          onChange={props.onTimeRangeChange}
          timeZone={props.timeZone ?? localTimeZone}
          onChangeTimeZone={(zone) => {
            setLocalTimeZone(zone);
            props.onTimeZoneChange?.(zone);
          }}
          onMoveBackward={() => move(-1)}
          onMoveForward={() => move(1)}
          onZoom={zoomOut}
          isOnCanvas
        />
      </div>
    </div>
  );
}

const getStyles = (theme: GrafanaTheme2) => ({
  filter: css({ width: theme.spacing(19), maxWidth: '100%', minWidth: 0 }),
  row: css({
    display: 'flex',
    flexWrap: 'wrap',
    alignItems: 'flex-start',
    gap: theme.spacing(1),
    minWidth: 0,
    flex: 'none',
  }),
  variables: css({
    display: 'flex',
    // Allocate the remaining row width before wrapping; max-content flex basis
    // otherwise pushes the time picker down even when the pills can shrink to fit.
    flex: '1 1 0',
    flexWrap: 'wrap',
    alignItems: 'flex-start',
    gap: theme.spacing(1),
    minWidth: 0,
    '@media (max-width: 1100px)': { flexBasis: '100%' },
  }),
  actions: css({ display: 'flex', alignItems: 'center', gap: theme.spacing(1) }),
  time: css({ marginLeft: 'auto', flex: '0 1 auto', minWidth: 0 }),
});

function tagValuesProvider(values: DashboardControlsProps['filterValues']) {
  return async (_variable: scenes.AdHocFiltersVariable, filter: scenes.AdHocFilterWithLabels) => {
    const suggestions =
      filter.key === 'error'
        ? ['true', 'false']
        : filter.key === 'service'
          ? values.services
          : filter.key === 'topic'
            ? values.topics
            : filter.key === 'consumer_group'
              ? values.consumerGroups
              : [...values.services, ...values.topics, ...values.consumerGroups];
    return { replace: true, values: [...new Set(suggestions)].sort().map((value) => ({ text: value, value })) };
  };
}

export default DashboardControls;

function selection(value: scenes.VariableValue): string[] {
  const values = (Array.isArray(value) ? value : [value]).map(String);
  return values.includes('$__all') ? [] : values.filter(Boolean);
}
function sameSelection(a: string[], b: string[]) {
  return JSON.stringify([...a].sort()) === JSON.stringify([...b].sort());
}
function multiOptionsQuery(values: string[], selected: string[]) {
  return JSON.stringify([...new Set([...values, ...selected])].sort().map((value) => ({ text: value, value })));
}
