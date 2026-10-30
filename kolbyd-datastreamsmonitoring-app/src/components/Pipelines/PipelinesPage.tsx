import React, { SubmitEvent, useId, useMemo, useRef, useState } from 'react';
import {
  FlowEvent,
  FlowNode,
  FlowState,
  formatRate,
  matchedHops,
  measuredPathways,
  nodeEvents,
  percentile,
  rate,
} from '../../flow/model';
import { Topology } from '../Explorer/Topology';
import {
  endpointOf,
  loadPipelines,
  MAX_PIPELINES,
  PipelineDefinition,
  pipelineGraph,
  PipelineGraph,
  savePipelines,
} from './pipelines';
import { getPipelineStyles } from './pipelines.styles';
import { Button, Input, Field, Icon, TabsBar, Tab, LinkButton, LoadingPlaceholder, useStyles2 } from '@grafana/ui';
import { IconName } from '@grafana/data';
import { NativeSelect } from '../Native/NativeSelect';
import { NativeInventoryTable } from './NativeInventoryTable';

export interface PipelinesPageProps {
  state: FlowState;
  events: FlowEvent[];
  now: number;
  seconds: number;
  environment: string;
  example: boolean;
  onExplore: (nodeIds: string[]) => void;
  storageKey: string;
  initialDraft?: { startId: string; endId: string };
}

const NativeTimeSeries = React.lazy(() => import('../Native/NativeTimeSeries'));

type FormState = { id: string; name: string; start: string; end: string; environment: string };
const sections = ['Definition', 'Summary', 'Components', 'Monitors'] as const;
const milliseconds = (value: number | null) => (value === null ? 'Unavailable' : `${value.toFixed(1)} ms`);
const lastSeen = (now: number, value: number) => `${Math.max(0, Math.floor((now - value) / 1000))}s ago`;

function PipelineIcon({ kind, size = 16 }: { kind: string; size?: number }) {
  const icons: Record<string, IconName> = {
    plus: 'plus',
    back: 'angle-left',
    edit: 'pen',
    Definition: 'share-alt',
    Summary: 'chart-line',
    Components: 'list-ul',
    Monitors: 'bell',
    search: 'search',
    close: 'times',
    collapse: 'angle-left',
  };
  return <Icon name={icons[kind] || 'share-alt'} size={size > 16 ? 'lg' : 'md'} />;
}

function PipelineForm({
  initial,
  nodes,
  environment,
  existingNames,
  onSave,
  onCancel,
}: {
  initial: FormState;
  nodes: FlowNode[];
  environment: string;
  existingNames: string[];
  onSave: (form: FormState) => string;
  onCancel: () => void;
}) {
  const [form, setForm] = useState(initial),
    [error, setError] = useState('');
  const services = nodes
    .filter((node) => node.kind === 'service' && (!form.environment || node.environment === form.environment))
    .sort((a, b) => a.name.localeCompare(b.name));
  const environments = [...new Set(nodes.map((node) => node.environment))].sort();
  function submit(event: SubmitEvent<HTMLFormElement>) {
    event.preventDefault();
    const name = form.name.trim();
    if (!name) {
      setError('Enter a pipeline name.');
      return;
    }
    if (existingNames.some((value) => value.toLocaleLowerCase() === name.toLocaleLowerCase())) {
      setError('Choose a different name. A saved pipeline already uses this name.');
      return;
    }
    if (!form.start || !form.end) {
      setError('Select a start service and an end service.');
      return;
    }
    setError(onSave({ ...form, name }));
  }
  return (
    <form
      className="dsm-pipeline-form"
      onSubmit={submit}
      aria-label={form.id ? 'Edit pipeline definition' : 'Create pipeline definition'}
    >
      <div className="dsm-pipeline-form-heading">
        <h3>{form.id ? 'Edit pipeline' : 'Create pipeline'}</h3>
        <Button
          variant="secondary"
          type="button"
          icon="times"
          aria-label="Close pipeline form"
          onClick={onCancel}
        ></Button>
      </div>
      <div className="dsm-pipeline-form-fields">
        <Field label="Name">
          <Input
            autoFocus
            aria-label="Pipeline name"
            value={form.name}
            maxLength={120}
            placeholder="e.g. Order processing"
            onChange={(event) => setForm({ ...form, name: event.currentTarget.value })}
          />
        </Field>
        <Field label="Environment">
          <NativeSelect
            aria-label="Pipeline environment"
            value={form.environment}
            options={[
              { value: '', label: 'All environments' },
              ...Array.from(new Set([...environments, environment].filter(Boolean))).map((value) => ({
                value,
                label: value,
              })),
            ]}
            onChange={(value) => setForm({ ...form, environment: value, start: '', end: '' })}
          />
        </Field>
        <Field label="Start service">
          <NativeSelect
            aria-label="Pipeline start service"
            value={form.start}
            options={[
              { value: '', label: 'Select a service' },
              ...services.map((node) => ({
                value: node.id,
                label:
                  node.name +
                  (!form.environment ? ' · ' + node.environment : '') +
                  (node.serviceNamespace ? ' · ' + node.serviceNamespace : ''),
              })),
            ]}
            onChange={(value) => setForm({ ...form, start: value })}
          />
        </Field>
        <Field label="End service">
          <NativeSelect
            aria-label="Pipeline end service"
            value={form.end}
            options={[
              { value: '', label: 'Select a service' },
              ...services.map((node) => ({
                value: node.id,
                label:
                  node.name +
                  (!form.environment ? ' · ' + node.environment : '') +
                  (node.serviceNamespace ? ' · ' + node.serviceNamespace : ''),
              })),
            ]}
            onChange={(value) => setForm({ ...form, end: value })}
          />
        </Field>
      </div>
      <p>
        Includes the services and queues on directed dependencies between the endpoints. Internal cycles are shown once.
        This definition does not establish per-message lineage.
      </p>
      {error && (
        <p className="dsm-pipeline-error" role="alert">
          {error}
        </p>
      )}
      <div className="dsm-pipeline-form-actions">
        <Button type="submit" variant="primary">
          {form.id ? 'Save changes' : 'Create pipeline'}
        </Button>
        <Button variant="secondary" type="button" onClick={onCancel}>
          Cancel
        </Button>
      </div>
    </form>
  );
}

function HopLatencyPlot({
  hops,
  now,
  seconds,
}: {
  hops: ReturnType<typeof matchedHops>;
  now: number;
  seconds: number;
}) {
  const bins: Array<{ count: number; total: number }> = Array.from({ length: 24 }, () => ({ count: 0, total: 0 }));
  for (const hop of hops) {
    const index = Math.floor((hop.receive.occurredAt - (now - seconds * 1000)) / ((seconds * 1000) / bins.length));
    if (index >= 0 && index < bins.length) {
      bins[index].count++;
      bins[index].total += hop.ms;
    }
  }
  const values = bins.map((bin) => (bin.count ? bin.total / bin.count : null));
  const samples = values.map((value, index) => ({
    at: now - seconds * 1000 + ((index + 0.5) * seconds * 1000) / bins.length,
    value,
  }));
  return (
    <React.Suspense fallback={<LoadingPlaceholder text="Loading chart" />}>
      <NativeTimeSeries samples={samples} title="Correlated hop latency" now={now} seconds={seconds} />
    </React.Suspense>
  );
}

function RatePlot({
  events,
  now,
  seconds,
  title,
}: {
  events: FlowEvent[];
  now: number;
  seconds: number;
  title: string;
}) {
  const buckets = Array.from({ length: 24 }, () => 0);
  for (const event of events) {
    const bucket = Math.floor((event.occurredAt - (now - seconds * 1000)) / ((seconds * 1000) / 24));
    if (bucket >= 0 && bucket < 24) {
      buckets[bucket] += 24 / seconds;
    }
  }
  return (
    <React.Suspense fallback={<LoadingPlaceholder text="Loading chart" />}>
      <NativeTimeSeries
        title={title}
        now={now}
        seconds={seconds}
        unit="ops"
        samples={buckets.map((value, index) => ({
          at: now - seconds * 1000 + ((index + 0.5) * seconds * 1000) / 24,
          value,
        }))}
      />
    </React.Suspense>
  );
}

export default function PipelinesPage({
  state,
  events,
  now,
  seconds,
  environment,
  example,
  onExplore,
  storageKey,
  initialDraft,
}: PipelinesPageProps) {
  const styles = useStyles2(getPipelineStyles);
  const key = `${storageKey}:pipelines:${example ? 'example' : 'live'}:v1`;
  const [loaded] = useState(() => {
    try {
      return loadPipelines(window.localStorage, key);
    } catch {
      return { pipelines: [], error: 'Browser storage is unavailable. Pipeline definitions cannot be loaded.' };
    }
  });
  const [definitions, setDefinitions] = useState<PipelineDefinition[]>(loaded.pipelines),
    [storageError, setStorageError] = useState(loaded.error);
  const [selectedId, setSelectedId] = useState(''),
    [form, setForm] = useState<FormState | null>(() =>
      initialDraft
        ? {
            id: '',
            name: '',
            start: initialDraft.startId,
            end: initialDraft.endId,
            environment: environment || state.nodes.get(initialDraft.startId)?.environment || '',
          }
        : null
    );
  const [search, setSearch] = useState(''),
    [catalogEnv, setCatalogEnv] = useState(environment),
    [serviceFilter, setServiceFilter] = useState(''),
    [topicFilter, setTopicFilter] = useState('');
  const [removeId, setRemoveId] = useState(''),
    [activeSection, setActiveSection] = useState<(typeof sections)[number]>('Definition');
  const [railCollapsed, setRailCollapsed] = useState(false),
    [selectedNode, setSelectedNode] = useState('');
  const prefix = useId().replaceAll(':', ''),
    detailScroll = useRef<HTMLDivElement>(null);
  const allNodes = [...state.nodes.values()],
    allEdges = [...state.edges.values()];
  const graphs = useMemo(
    () =>
      new Map(
        definitions.map((definition) => [
          definition.id,
          pipelineGraph(definition, [...state.nodes.values()], [...state.edges.values()]),
        ])
      ),
    [definitions, state.nodes, state.edges]
  );
  const selected = definitions.find((definition) => definition.id === selectedId),
    graph = selected && graphs.get(selected.id);
  const rows = definitions
    .filter((definition) => {
      const scope = graphs.get(definition.id)!;
      return (
        (!search || definition.name.toLowerCase().includes(search.toLowerCase())) &&
        (!catalogEnv ||
          definition.environment === catalogEnv ||
          scope.nodes.some((node) => node.environment === catalogEnv)) &&
        (!serviceFilter || scope.nodes.some((node) => node.kind === 'service' && node.id === serviceFilter)) &&
        (!topicFilter || scope.nodes.some((node) => node.kind === 'topic' && node.id === topicFilter))
      );
    })
    .sort((a, b) => a.name.localeCompare(b.name));
  function persist(next: PipelineDefinition[]): string {
    let error = '';
    try {
      error = savePipelines(window.localStorage, key, next);
    } catch {
      error = 'Browser storage is unavailable. Pipeline changes could not be saved.';
    }
    if (error) {
      setStorageError(error);
      return error;
    }
    setDefinitions(next);
    setStorageError('');
    return '';
  }
  function save(formValue: FormState): string {
    const start = state.nodes.get(formValue.start),
      end = state.nodes.get(formValue.end);
    if (!start || !end || start.kind !== 'service' || end.kind !== 'service') {
      return 'Select endpoints that are currently observed.';
    }
    const existing = definitions.find((definition) => definition.id === formValue.id);
    const definition: PipelineDefinition = {
      id: existing?.id || crypto.randomUUID(),
      name: formValue.name,
      start: endpointOf(start),
      end: endpointOf(end),
      environment: formValue.environment,
      createdAt: existing?.createdAt || now,
      updatedAt: now,
    };
    const corridor = pipelineGraph(definition, allNodes, allEdges);
    if (corridor.error) {
      return corridor.error;
    }
    const next = existing
      ? definitions.map((value) => (value.id === definition.id ? definition : value))
      : [...definitions, definition];
    const error = persist(next);
    if (!error) {
      setForm(null);
      setSelectedId(definition.id);
      setActiveSection('Definition');
      setSelectedNode('');
    }
    return error;
  }
  function create() {
    setForm({ id: '', name: '', start: '', end: '', environment: catalogEnv });
  }
  function edit(definition: PipelineDefinition) {
    const current = graphs.get(definition.id);
    setForm({
      id: definition.id,
      name: definition.name,
      start: current?.start?.id || definition.start.id,
      end: current?.end?.id || definition.end.id,
      environment: definition.environment,
    });
  }
  function goSection(section: (typeof sections)[number]) {
    setActiveSection(section);
    const element = detailScroll.current?.querySelector<HTMLElement>(`[data-pipeline-section="${section}"]`);
    element?.scrollIntoView({
      behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth',
      block: 'start',
    });
  }
  function open(definition: PipelineDefinition) {
    setSelectedId(definition.id);
    setActiveSection('Definition');
    setSelectedNode('');
    setRemoveId('');
  }
  function remove() {
    if (persist(definitions.filter((definition) => definition.id !== removeId))) {
      return;
    }
    if (selectedId === removeId) {
      setSelectedId('');
    }
    setRemoveId('');
  }
  const sortedServices = allNodes
    .filter((node) => node.kind === 'service')
    .sort((a, b) => a.name.localeCompare(b.name));
  const sortedTopics = allNodes.filter((node) => node.kind === 'topic').sort((a, b) => a.name.localeCompare(b.name));
  const environments = [
    ...new Set(
      allNodes
        .map((node) => node.environment)
        .concat(definitions.map((definition) => definition.environment))
        .filter(Boolean)
    ),
  ].sort();
  const errorPipelineCount = definitions.filter((definition) => {
    const scope = graphs.get(definition.id)!;
    const edgeIds = new Set(scope.edges.map((edge) => JSON.stringify([edge.source, edge.target, edge.consumerGroup])));
    return events.some(
      (event) => event.error && edgeIds.has(JSON.stringify([event.source, event.target, event.consumerGroup]))
    );
  }).length;
  const scopeEvents = (scope: PipelineGraph) => {
    const connections = new Set(
      scope.edges.map((edge) => JSON.stringify([edge.source, edge.target, edge.consumerGroup]))
    );
    return events.filter((event) => connections.has(JSON.stringify([event.source, event.target, event.consumerGroup])));
  };
  const selectedEvents = graph ? scopeEvents(graph) : [];
  const publicationEdges = new Set(
    (graph?.edges || [])
      .filter((edge) => edge.operation === 'publish')
      .map((edge) => JSON.stringify([edge.source, edge.target, edge.consumerGroup]))
  );
  const hops = matchedHops(state, selectedEvents).filter((hop) =>
    publicationEdges.has(JSON.stringify([hop.producer.source, hop.producer.target, hop.producer.consumerGroup]))
  );
  const pathways =
    graph?.start && graph?.end
      ? measuredPathways(state, {
          startId: graph.start.id,
          endId: graph.end.id,
          now,
          seconds,
          environment: selected?.environment,
        })
      : [];
  const endToEndP95 = percentile(
    pathways.map((pathway) => Number(pathway.durationMs)),
    0.95
  );
  const startEvents = graph?.start
    ? nodeEvents(graph.start, selectedEvents).filter((event) => event.kind === 'publish')
    : [];
  const endEvents = graph?.end ? nodeEvents(graph.end, selectedEvents).filter((event) => event.kind === 'receive') : [];
  const picked = graph?.nodes.find((node) => node.id === selectedNode);
  const formNode = form && definitions.find((definition) => definition.id === form.id);
  const shownComponents = graph?.nodes || [];
  return (
    <div className={`dsm-pipelines ${styles.root}`} data-testid="pipelines-page">
      <header className="dsm-pipeline-catalog-heading">
        {selected ? (
          <div className="dsm-pipeline-breadcrumb">
            <Button
              variant="secondary"
              fill="text"

              onClick={() => {
                setSelectedId('');
                setForm(null);
              }}
            >
              Pipelines
            </Button>
            <PipelineIcon kind="back" />
            <h2>{selected.name}</h2>
            {selected.environment && <span className="dsm-pipeline-env-tag">env:{selected.environment}</span>}
          </div>
        ) : (
          <h2>Pipeline Catalog</h2>
        )}
        <div>
          {selected ? (
            <>
              <Button variant="secondary" icon="pen" onClick={() => edit(selected)}>
                Edit
              </Button>
              <Button variant="secondary" onClick={() => setRemoveId(selected.id)}>
                Remove
              </Button>
            </>
          ) : (
            <Button variant="primary" icon="plus" onClick={create} disabled={definitions.length >= MAX_PIPELINES}>
              Create Pipeline
            </Button>
          )}
        </div>
      </header>
      {storageError && (
        <p className="dsm-pipeline-storage-error" role="alert">
          {storageError}
        </p>
      )}
      {removeId && (
        <div className="dsm-pipeline-remove" role="status">
          <span>
            Remove “{definitions.find((definition) => definition.id === removeId)?.name}” from saved pipelines? This
            only removes its personal definition.
          </span>
          <Button variant="destructive" onClick={remove}>
            Remove pipeline
          </Button>
          <Button variant="secondary" onClick={() => setRemoveId('')}>
            Cancel
          </Button>
        </div>
      )}
      {form && (
        <PipelineForm
          key={`${key}:${form.id || 'new'}`}
          initial={form}
          nodes={allNodes}
          environment={environment}
          existingNames={definitions
            .filter((definition) => definition.id !== formNode?.id)
            .map((definition) => definition.name)}
          onSave={save}
          onCancel={() => setForm(null)}
        />
      )}
      {!selected || !graph ? (
        <>
          <TabsBar>
            <Tab label="Saved" counter={definitions.length} active />
          </TabsBar>
          <div className="dsm-pipeline-coverage-strip">
            <span>
              <PipelineIcon kind="Monitors" />
              <strong>Monitors</strong> Not configured in this plugin
            </span>
            <span>
              <PipelineIcon kind="Summary" />
              <strong>Kafka lag</strong> Not collected
            </span>
            <span>
              <PipelineIcon kind="Components" />
              <strong>Observed errors</strong> {errorPipelineCount} pipelines
            </span>
          </div>
          <div className="dsm-pipeline-catalog-filters">
            <Field label="Search" className="dsm-pipeline-search">
              <Input
                prefix={<Icon name="search" />}
                aria-label="Search saved pipelines"
                placeholder="Search pipelines"
                value={search}
                onChange={(event) => setSearch(event.currentTarget.value)}
              />
            </Field>
            <Field label="Environment">
              <NativeSelect
                aria-label="Catalog environment"
                value={catalogEnv}
                onChange={setCatalogEnv}
                options={[{ value: '', label: 'All' }, ...environments.map((value) => ({ value, label: value }))]}
              />
            </Field>
            <Field label="Service">
              <NativeSelect
                aria-label="Catalog service"
                value={serviceFilter}
                onChange={setServiceFilter}
                options={[
                  { value: '', label: 'All services' },
                  ...sortedServices.map((node) => ({ value: node.id, label: node.name + ' · ' + node.environment })),
                ]}
              />
            </Field>
            <Field label="Queue">
              <NativeSelect
                aria-label="Catalog queue"
                value={topicFilter}
                onChange={setTopicFilter}
                options={[
                  { value: '', label: 'All queues' },
                  ...sortedTopics.map((node) => ({ value: node.id, label: node.name })),
                ]}
              />
            </Field>
          </div>
          <div className="dsm-pipeline-catalog-table">
            <NativeInventoryTable
              columns={catalogColumns}
              rows={rows.map((definition) => {
                const scope = graphs.get(definition.id)!,
                  activity = scopeEvents(scope),
                  errors = activity.filter((event) => event.error).length;
                const start = scope.start
                  ? nodeEvents(scope.start, activity).filter((event) => event.kind === 'publish')
                  : [];
                const end = scope.end
                  ? nodeEvents(scope.end, activity).filter((event) => event.kind === 'receive')
                  : [];
                return {
                  id: definition.id,
                  name: definition.name,
                  components: scope.error ? 'Unavailable' : scope.nodes.length,
                  monitors: 'Not configured',
                  status: scope.error
                    ? 'Definition unavailable'
                    : errors
                      ? errors + ' errors observed'
                      : activity.length
                        ? 'No errors observed'
                        : 'No activity observed',
                  latency:
                    scope.start && scope.end
                      ? percentile(
                          measuredPathways(state, {
                            startId: scope.start.id,
                            endId: scope.end.id,
                            now,
                            seconds,
                            environment: definition.environment,
                          }).map((pathway) => Number(pathway.durationMs)),
                          0.95
                        )
                      : null,
                  start: scope.error ? null : rate(start, seconds),
                  end: scope.error ? null : rate(end, seconds),
                  cells: {
                    start: scope.error ? 'Unavailable' : formatRate(rate(start, seconds)) + ' ops/s',
                    end: scope.error ? 'Unavailable' : formatRate(rate(end, seconds)) + ' ops/s',
                    latency: milliseconds(
                      scope.start && scope.end
                        ? percentile(
                            measuredPathways(state, {
                              startId: scope.start.id,
                              endId: scope.end.id,
                              now,
                              seconds,
                              environment: definition.environment,
                            }).map((pathway) => Number(pathway.durationMs)),
                            0.95
                          )
                        : null
                    ),
                    name: (
                      <Button variant="secondary" fill="text" onClick={() => open(definition)}>
                        {definition.name}
                      </Button>
                    ),
                  },
                };
              })}
            />
            {!rows.length && (
              <div className="dsm-pipeline-empty">
                <h3>{definitions.length ? 'No pipelines match these filters' : 'Save the pipelines you follow'}</h3>
                <p>
                  {definitions.length
                    ? 'Change the search or filters to show your saved definitions.'
                    : 'Define a start service and end service. The pipeline will include their observed services and Kafka queues.'}
                </p>
                {!definitions.length && (
                  <Button variant="primary" onClick={create}>
                    Create your first pipeline
                  </Button>
                )}
              </div>
            )}
          </div>
        </>
      ) : (
        <div className={`dsm-pipeline-detail ${railCollapsed ? 'rail-collapsed' : ''}`}>
          <nav className="dsm-pipeline-detail-rail" aria-label="Pipeline sections">
            {sections.map((section) => (
              <Button
                variant="secondary"
                icon={
                  (
                    { Definition: 'share-alt', Summary: 'chart-line', Components: 'list-ul', Monitors: 'bell' } as const
                  )[section]
                }
                key={section}
                title={section}
                aria-current={activeSection === section ? 'location' : undefined}
                className={activeSection === section ? 'active' : ''}
                onClick={() => goSection(section)}
              >
                {!railCollapsed && section}
              </Button>
            ))}
            <Button
              variant="secondary"
              icon="angle-left"
              className="dsm-pipeline-rail-collapse"
              aria-label={railCollapsed ? 'Expand pipeline navigation' : 'Collapse pipeline navigation'}
              onClick={() => setRailCollapsed(!railCollapsed)}
            >
              {!railCollapsed && 'Collapse'}
            </Button>
          </nav>
          <div className="dsm-pipeline-detail-content" ref={detailScroll}>
            <section className="dsm-pipeline-section" data-pipeline-section="Definition" id={`${prefix}-definition`}>
              <header>
                <PipelineIcon kind="Definition" />
                <h3>Definition</h3>
                <span />
                {!graph.error && (
                  <Button variant="secondary" fill="text" onClick={() => onExplore(graph.nodes.map((node) => node.id))}>
                    View in Map
                  </Button>
                )}
              </header>
              {graph.error ? (
                <div className="dsm-pipeline-unavailable">
                  <h3>Definition unavailable</h3>
                  <p>{graph.error}</p>
                  <Button variant="secondary" icon="pen" onClick={() => edit(selected)}>
                    Edit endpoints
                  </Button>
                </div>
              ) : (
                <>
                  <div className="dsm-pipeline-definition-map">
                    <Topology
                      nodes={graph.nodes}
                      edges={graph.edges}
                      events={selectedEvents}
                      now={now}
                      seconds={seconds}
                      selected={selectedNode}
                      onSelect={setSelectedNode}
                      paused={window.matchMedia('(prefers-reduced-motion: reduce)').matches}
                      zoom={1}
                      fit={false}
                      animationAfter={state.animationAfter}
                    />
                  </div>
                  {picked && (
                    <div className="dsm-pipeline-picked">
                      <strong>{picked.name}</strong>
                      <span>{picked.kind === 'topic' ? 'Kafka topic' : 'Service'}</span>
                      <span>env:{picked.environment}</span>
                      <span>{nodeEvents(picked, selectedEvents).length} observations</span>
                      <Button variant="secondary" fill="text" onClick={() => onExplore([picked.id])}>
                        Open component in Map
                      </Button>
                      <Button
                        variant="secondary"
                        aria-label="Close selected component"
                        onClick={() => setSelectedNode('')}
                      ></Button>
                    </div>
                  )}
                </>
              )}
            </section>
            <section className="dsm-pipeline-section" data-pipeline-section="Summary" id={`${prefix}-summary`}>
              <header>
                <PipelineIcon kind="Summary" />
                <h3>Summary</h3>
              </header>
              <div className="dsm-pipeline-summary-grid">
                <article>
                  <div className="dsm-pipeline-plot-heading">
                    <h4>End-to-end latency</h4>
                    <strong>p95 {milliseconds(endToEndP95)}</strong>
                  </div>
                  {pathways.length ? (
                    <React.Suspense fallback={<LoadingPlaceholder text="Loading chart" />}>
                      <NativeTimeSeries
                        samples={pathways.map((pathway) => ({ at: pathway.completedAt, value: pathway.durationMs }))}
                        title="End-to-end latency"
                        now={now}
                        seconds={seconds}
                      />
                    </React.Suspense>
                  ) : (
                    <div className="dsm-pipeline-metric-unavailable">
                      <p>No complete pathways retained</p>
                      <small>Collect causally connected send and process spans.</small>
                    </div>
                  )}
                  <p className="dsm-pipeline-plot-note">
                    {pathways.length} completed pathways · first send to processing completion
                  </p>
                </article>
                <article>
                  <div className="dsm-pipeline-plot-heading">
                    <h4>Correlated hop latency</h4>
                    <strong>
                      p95{' '}
                      {milliseconds(
                        percentile(
                          hops.map((hop) => hop.ms),
                          0.95
                        )
                      )}
                    </strong>
                  </div>
                  {hops.length ? (
                    <HopLatencyPlot hops={hops} now={now} seconds={seconds} />
                  ) : (
                    <div className="dsm-pipeline-metric-unavailable">
                      <p>No matched hops retained</p>
                      <small>Both producer and consumer observations are required.</small>
                    </div>
                  )}
                  <p className="dsm-pipeline-plot-note">
                    {hops.length} matched hops · bucketed means · host clock differences can affect timing
                  </p>
                </article>
                <article>
                  <div className="dsm-pipeline-plot-heading">
                    <h4>Throughput start</h4>
                    <strong>{formatRate(rate(startEvents, seconds))} ops/s</strong>
                  </div>
                  <RatePlot events={startEvents} now={now} seconds={seconds} title="Throughput start" />
                  <p className="dsm-pipeline-plot-note">
                    {graph.start?.name || selected.start.name} · observed publish operations
                  </p>
                </article>
                <article>
                  <div className="dsm-pipeline-plot-heading">
                    <h4>Throughput end</h4>
                    <strong>{formatRate(rate(endEvents, seconds))} ops/s</strong>
                  </div>
                  <RatePlot events={endEvents} now={now} seconds={seconds} title="Throughput end" />
                  <p className="dsm-pipeline-plot-note">
                    {graph.end?.name || selected.end.name} · observed receive operations
                  </p>
                </article>
              </div>
            </section>
            <section className="dsm-pipeline-section" data-pipeline-section="Components" id={`${prefix}-components`}>
              <header>
                <PipelineIcon kind="Components" />
                <h3>Components</h3>
                <span className="dsm-pipeline-count">{shownComponents.length}</span>
              </header>
              <div className="dsm-pipeline-components-table">
                <NativeInventoryTable
                  columns={componentColumns}
                  rows={shownComponents.map((node) => {
                    const activity = nodeEvents(node, selectedEvents);
                    return {
                      id: node.id,
                      name: node.name,
                      type: node.kind === 'topic' ? 'Kafka topic' : 'Service',
                      environment: node.environment,
                      incoming: formatRate(rate(activity, seconds, 'receive')),
                      outgoing: formatRate(rate(activity, seconds, 'publish')),
                      seen: lastSeen(now, node.lastSeen),
                      cells: {
                        name: (
                          <Button
                            variant="secondary"
                            fill="text"
                            onClick={() => {
                              setSelectedNode(node.id);
                              goSection('Definition');
                            }}
                          >
                            {node.name}
                          </Button>
                        ),
                      },
                    };
                  })}
                />
              </div>
            </section>
            <section className="dsm-pipeline-section" data-pipeline-section="Monitors" id={`${prefix}-monitors`}>
              <header>
                <PipelineIcon kind="Monitors" />
                <h3>Monitors</h3>
              </header>
              <div className="dsm-pipeline-monitor-empty">
                <h4>No pipeline monitors configured</h4>
                <p>
                  This plugin does not store monitor assignments or query Grafana alert states for saved definitions.
                </p>
                <LinkButton href="/alerting/list" variant="secondary">
                  Open Grafana Alerting
                </LinkButton>
              </div>
            </section>
          </div>
        </div>
      )}
    </div>
  );
}

const catalogColumns = [
  { id: 'name', label: 'Name' },
  { id: 'components', label: 'Components' },
  { id: 'monitors', label: 'Monitors' },
  { id: 'status', label: 'Observation status' },
  { id: 'latency', label: 'End-to-end latency' },
  { id: 'start', label: 'Throughput start' },
  { id: 'end', label: 'Throughput end' },
];
const componentColumns = [
  { id: 'name', label: 'Name' },
  { id: 'type', label: 'Type' },
  { id: 'environment', label: 'Environment' },
  { id: 'incoming', label: 'Incoming ops/s' },
  { id: 'outgoing', label: 'Outgoing ops/s' },
  { id: 'seen', label: 'Last observed' },
];
