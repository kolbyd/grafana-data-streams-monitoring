import React, { useEffect, useRef, useState } from 'react';
import { AppRootProps, PageLayoutType, rangeUtil, type TimeRange } from '@grafana/data';
import { PluginPage, config, locationService } from '@grafana/runtime';
import { useLocation } from 'react-router-dom';
import { Alert, Button, Collapse, Icon, RadioButtonGroup, Tab, TabsBar, useTheme2 } from '@grafana/ui';
import { DashboardControls, type DashboardFilter } from '../Native/DashboardControls';
import { PLUGIN_BASE_URL } from '../../constants';
import { connected, formatRate, matchedHops, nodeEvents, percentile, rate } from '../../flow/model';
import { useFlow } from '../../flow/useFlow';
import { activeTopology } from '../../flow/activity';
import { Sparkline, Topology } from './Topology';
import { StreamList } from './StreamList';
import { Measure } from './Measure';
import { Setup } from './Setup';
import PipelinesPage from '../Pipelines/PipelinesPage';
import KafkaConsole from '../KafkaConsole';
import { explorerStyles } from './explorer.styles';

function Glyph({ kind, size = 18 }: { kind: string; size?: number }) {
  const names = {
    stream: 'sitemap',
    map: 'sitemap',
    list: 'list-ul',
    search: 'search',
    clock: 'clock-nine',
    settings: 'sliders-v-alt',
    close: 'times',
    plus: 'plus',
    minus: 'minus',
    fit: 'expand-arrows',
    service: 'cube',
    chevron: 'angle-right',
    alert: 'exclamation-triangle',
  } as const;
  return <Icon name={names[kind as keyof typeof names] || 'sitemap'} size={size >= 24 ? 'xl' : 'md'} />;
}

const ms = (value: number | null) => (value === null ? 'Unavailable' : `${value.toFixed(1)} ms`);
const age = (now: number, at: number) =>
  !at ? 'No observations' : `${Math.max(0, Math.floor((now - at) / 1000))}s ago`;

export default function Explorer(_props: AppRootProps) {
  const theme = useTheme2();
  const themeVariables = {
    '--dsm-ink': theme.colors.text.primary,
    '--dsm-muted': theme.colors.text.secondary,
    '--dsm-rule': theme.colors.border.weak,
    '--dsm-border': theme.colors.border.medium,
    '--dsm-blue': theme.colors.primary.main,
    '--dsm-focus': theme.colors.primary.border,
    '--dsm-ground': theme.colors.background.canvas,
    '--dsm-surface': theme.colors.background.secondary,
    '--dsm-surface-alt': theme.colors.background.canvas,
    '--dsm-hover': theme.colors.action.hover,
    '--dsm-node-fill': theme.colors.background.secondary,
    '--dsm-topic-end': theme.colors.border.strong,
    '--dsm-chart-area': theme.colors.primary.transparent,
    '--dsm-error': theme.colors.error.text,
    '--dsm-error-bg': theme.colors.error.transparent,
    '--dsm-warning-bg': theme.colors.warning.transparent,
    '--dsm-warning-ink': theme.colors.warning.text,
    '--dsm-radius': theme.shape.radius.default,
    fontFamily: theme.typography.fontFamily,
    fontSize: theme.typography.body.fontSize,
    colorScheme: theme.isDark ? 'dark' : 'light',
  } as React.CSSProperties;
  const location = useLocation();
  const routes: Record<string, string> = {
    Explore: 'explore',
    Pipelines: 'pipelines',
    Measure: 'measure',
    'Kafka Console': 'kafka',
    Setup: 'setup',
  };
  const currentRoute = location.pathname.split('/').filter(Boolean).at(-1);
  const tab = Object.keys(routes).find((name) => routes[name] === currentRoute) || 'Explore';
  function setTab(name: string) {
    locationService.push(`${PLUGIN_BASE_URL}/${routes[name] || 'explore'}`);
  }
  const storageKey = `liveflow:${config.bootData.user.orgId}:${config.bootData.user.id || 'anonymous'}`;
  const [listKind, setListKind] = useState<'services' | 'queues'>('services');
  const routeParameters = new URLSearchParams(location.search);
  const draftStart = routeParameters.get('from');
  const draftEnd = routeParameters.get('to');
  const pipelineDraft = draftStart && draftEnd ? { startId: draftStart, endId: draftEnd } : undefined;
  const [example, setExample] = useState(false),
    [retry, setRetry] = useState(0);
  const { state, message } = useFlow(example, retry);
  const [clock, setClock] = useState(() => Date.now()),
    [view, setView] = useState('Map');
  const [timeRange, setTimeRange] = useState<TimeRange>(() =>
    rangeUtil.convertRawToRange({ from: 'now-1m', to: 'now' })
  );
  const [timeZone, setTimeZone] = useState('browser');
  const [adHocFilters, setAdHocFilters] = useState<DashboardFilter[]>([]);
  const [env, setEnv] = useState('');
  const [cluster, setCluster] = useState('');
  const [selectedServices, setSelectedServices] = useState<string[]>([]);
  const [selectedQueues, setSelectedQueues] = useState<string[]>([]);
  const [hidden, setHidden] = useState(new Set<string>());
  const [selected, setSelected] = useState(''),
    [detailTab, setDetailTab] = useState('Throughput');
  const [expandedObservations, setExpandedObservations] = useState(new Set<string>());
  const paused = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const [zoom, setZoom] = useState(1),
    [fit, setFit] = useState(false);
  const area = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const element = area.current;
    if (!element) {
      return;
    }
    const resize = () => {
      element.style.height = `${Math.max(320, window.innerHeight - element.getBoundingClientRect().top)}px`;
    };
    const media = window.matchMedia('(min-width: 701px)');
    const adapt = () => {
      setFit(false);
      resize();
    };
    resize();
    const observer = new ResizeObserver(resize);
    if (element.parentElement) {
      observer.observe(element.parentElement);
    }
    window.addEventListener('resize', resize);
    media.addEventListener('change', adapt);
    return () => {
      observer.disconnect();
      window.removeEventListener('resize', resize);
      media.removeEventListener('change', adapt);
    };
  }, []);
  useEffect(() => {
    const timer = setInterval(() => setClock(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  function changeSource(value: boolean) {
    setExample(value);
    setHidden(new Set());
    setSelected('');
    setEnv('');
    setCluster('');
    setAdHocFilters([]);
    setSelectedServices([]);
    setSelectedQueues([]);
  }
  const effectiveRange = rangeUtil.convertRawToRange(timeRange.raw, timeZone);
  const seconds = Math.max(1, Math.ceil((effectiveRange.to.valueOf() - effectiveRange.from.valueOf()) / 1000));
  function exploreNodes(nodeIds: string[]) {
    const visible = nodeIds.length === 1 ? connected(nodeIds[0], [...state.edges.values()]) : new Set(nodeIds);
    setHidden(new Set([...state.nodes.keys()].filter((id) => !visible.has(id))));
    if (nodeIds.length === 1) {
      setSelected(nodeIds[0]);
      setDetailTab('Throughput');
    }
    setAdHocFilters([]);
    setTab('Explore');
    setView('Map');
  }
  const now =
    timeRange.raw.to === 'now'
      ? Math.max(clock + state.clockOffset, state.updatedAt + state.clockOffset)
      : effectiveRange.to.valueOf();
  const scopedState = activeTopology(state, now - seconds * 1000, now, timeRange.raw.to === 'now');
  const allNodes = [...scopedState.nodes.values()];
  const events = state.events.filter((e) => e.occurredAt >= now - seconds * 1000 && e.occurredAt <= now);
  const errorConnections = new Set(
    events
      .filter((event) => event.error)
      .map((event) => JSON.stringify([event.source, event.target, event.consumerGroup]))
  );
  let edges = [...scopedState.edges.values()].filter(
    (e) =>
      (!env || e.environment === env) &&
      (!cluster || e.cluster === cluster) &&
      (!selectedServices.length ||
        [state.nodes.get(e.source)?.name, state.nodes.get(e.target)?.name].some(
          (name) => !!name && selectedServices.includes(name)
        )) &&
      (!selectedQueues.length || selectedQueues.includes(e.topic)) &&
      !hidden.has(e.source) &&
      !hidden.has(e.target)
  );
  if (adHocFilters.length) {
    edges = edges.filter((edge) =>
      adHocFilters.every((filter) => {
        const values =
          filter.key === 'service'
            ? ([state.nodes.get(edge.source)?.name, state.nodes.get(edge.target)?.name].filter(Boolean) as string[])
            : filter.key === 'topic'
              ? [edge.topic]
              : filter.key === 'consumer_group'
                ? [edge.consumerGroup ?? '']
                : filter.key === 'error'
                  ? [String(errorConnections.has(JSON.stringify([edge.source, edge.target, edge.consumerGroup])))]
                  : filter.key === 'search'
                    ? [
                        edge.topic,
                        edge.consumerGroup ?? '',
                        state.nodes.get(edge.source)?.name ?? '',
                        state.nodes.get(edge.target)?.name ?? '',
                      ]
                    : [];
        if (!values.length) {
          return false;
        }
        let matches: boolean;
        if (filter.operator === '=~' || filter.operator === '!~') {
          try {
            const pattern = new RegExp(filter.value);
            matches = values.some((value) => pattern.test(value));
          } catch {
            return false;
          }
        } else {
          matches = values.some((value) =>
            filter.key === 'search' ? value.toLowerCase().includes(filter.value.toLowerCase()) : value === filter.value
          );
        }
        return filter.operator === '!=' || filter.operator === '!~' ? !matches : matches;
      })
    );
  }
  const ids = new Set(edges.flatMap((e) => [e.source, e.target]));
  const nodes = allNodes.filter((n) => ids.has(n.id));
  const chosen = scopedState.nodes.get(selected);
  const selectedEvents = chosen ? nodeEvents(chosen, events) : [];
  const hops = matchedHops(
    state,
    events.filter((e) => ids.has(e.source) && ids.has(e.target))
  );
  const selectedHops = chosen
    ? hops.filter(
        (h) => h.receive.source === chosen.id || h.receive.target === chosen.id || h.producer.source === chosen.id
      )
    : [];
  function select(id: string) {
    setSelected(id);
    setDetailTab('Throughput');
  }
  const environments = [...new Set(allNodes.map((n) => n.environment))].sort();
  const clusters = [...new Set([...scopedState.edges.values()].map((e) => e.cluster))].sort();
  function changeTimeRange(value: TimeRange) {
    setTimeRange(value);
  }

  return (
    <PluginPage layout={PageLayoutType.Canvas}>
      <div
        className={`${explorerStyles} dsm-app ${theme.isDark ? 'dsm-dark' : ''}`}
        data-testid="dsm-app"
        ref={area}
        style={themeVariables}
      >
        <DashboardControls
          showFilters={tab === 'Explore'}
          environment={env}
          cluster={cluster}
          environments={environments}
          clusters={clusters}
          filters={adHocFilters}
          filterValues={{
            services: allNodes.filter((node) => node.kind === 'service').map((node) => node.name),
            topics: allNodes.filter((node) => node.kind === 'topic').map((node) => node.name),
            consumerGroups: [
              ...new Set(
                [...scopedState.edges.values()].flatMap((edge) => (edge.consumerGroup ? [edge.consumerGroup] : []))
              ),
            ],
          }}
          onEnvironmentChange={setEnv}
          onClusterChange={setCluster}
          onFiltersChange={(filters) => {
            setAdHocFilters(filters);
          }}
          services={selectedServices}
          queues={selectedQueues}
          allServices={allNodes.filter((node) => node.kind === 'service').map((node) => node.name)}
          allQueues={allNodes.filter((node) => node.kind === 'topic').map((node) => node.name)}
          onServicesChange={(values) => {
            setSelectedServices(values);
            setHidden(new Set());
          }}
          onQueuesChange={(values) => {
            setSelectedQueues(values);
            setHidden(new Set());
          }}
          actions={
            tab === 'Explore' ? (
              <RadioButtonGroup
                aria-label="Display mode"
                value={view}
                size="sm"
                options={[
                  { label: 'Map', value: 'Map' },
                  { label: 'List', value: 'List' },
                ]}
                onChange={setView}
              />
            ) : undefined
          }
          timeRange={timeRange}
          onTimeRangeChange={changeTimeRange}
          timeZone={timeZone}
          onTimeZoneChange={setTimeZone}
        />
        {example && (
          <div className="dsm-status-banner">
            <Alert severity="warning" title="Example data">
              <div className="dsm-banner-content">
                <span>Synthetic topology and traffic. No Kafka broker is connected to this preview.</span>
                <Button variant="secondary" size="sm" onClick={() => changeSource(false)}>
                  Use live data
                </Button>
              </div>
            </Alert>
          </div>
        )}
        {!example && (message || state.reset) && (
          <div className="dsm-status-banner">
            <Alert severity="warning" title="Connection needs attention">
              <div className="dsm-banner-content">
                <span>{message || state.reset}</span>
                <Button variant="secondary" size="sm" onClick={() => setRetry((v) => v + 1)}>
                  Retry connection
                </Button>
              </div>
            </Alert>
          </div>
        )}
        {tab === 'Setup' ? (
          <Setup
            nodes={allNodes}
            events={events}
            seconds={seconds}
            onExplore={exploreNodes}
            onPreviewExample={() => {
              changeSource(true);
              setTab('Explore');
            }}
          />
        ) : tab === 'Pipelines' ? (
          <PipelinesPage
            key={storageKey + (example ? ':example' : ':live') + ':' + location.key}
            state={scopedState}
            events={events}
            now={now}
            seconds={seconds}
            environment={env}
            example={example}
            storageKey={storageKey}
            onExplore={exploreNodes}
            initialDraft={pipelineDraft}
          />
        ) : tab === 'Measure' ? (
          <Measure
            nodes={nodes}
            edges={edges}
            events={events}
            state={scopedState}
            now={now}
            seconds={seconds}
            paused={paused}
            onSave={(startId, endId) => {
              locationService.push(
                `${PLUGIN_BASE_URL}/pipelines?from=${encodeURIComponent(startId)}&to=${encodeURIComponent(endId)}`
              );
            }}
            onTraces={(id) => {
              setSelected(id);
              setDetailTab('Traces');
              setTab('Explore');
            }}
          />
        ) : tab === 'Kafka Console' ? (
          <KafkaConsole
            key={storageKey + ':' + example + ':' + env}
            state={scopedState}
            events={events}
            now={now}
            seconds={seconds}
            environment={env}
            onExplore={exploreNodes}
            onSetup={() => setTab('Setup')}
          />
        ) : (
          <div className="dsm-explore-body">
            <section className="dsm-map-panel" aria-label="Pipeline explorer">
              {view === 'List' ? (
                <StreamList
                  nodes={nodes}
                  events={events}
                  state={scopedState}
                  seconds={seconds}
                  kind={listKind}
                  onKind={setListKind}
                  onSelect={select}
                />
              ) : nodes.length ? (
                <>
                  <div className="dsm-zoom">
                    <Button
                      variant="secondary"
                      size="sm"
                      aria-label="Zoom in"
                      onClick={() => {
                        setFit(false);
                        setZoom((v) => Math.min(3, v + 0.15));
                      }}
                    >
                      <Glyph kind="plus" />
                    </Button>
                    <Button
                      variant="secondary"
                      size="sm"
                      aria-label="Zoom out"
                      onClick={() => {
                        setFit(false);
                        setZoom((v) => Math.max(0.25, v - 0.15));
                      }}
                    >
                      <Glyph kind="minus" />
                    </Button>
                    <Button
                      variant="secondary"
                      size="sm"
                      aria-label={fit ? 'Actual size' : 'Fit map'}
                      onClick={() => {
                        setFit(!fit);
                        setZoom(1);
                      }}
                    >
                      <Glyph kind="fit" />
                    </Button>
                  </div>
                  <Topology
                    nodes={nodes}
                    edges={edges}
                    events={events}
                    now={now}
                    seconds={seconds}
                    selected={selected}
                    onSelect={select}
                    paused={paused}
                    zoom={zoom}
                    fit={fit}
                    onZoomChange={setZoom}
                    onFitChange={setFit}
                    animationAfter={state.animationAfter}
                  />
                </>
              ) : (
                <div className="dsm-empty">
                  <Glyph kind="stream" size={40} />
                  <h2>{allNodes.length ? 'No matching pipelines' : 'Discover your data streams'}</h2>
                  <p>
                    {allNodes.length
                      ? 'Adjust the filters to bring services and queues back into view.'
                      : 'Connect the flow service to automatically map producers, Kafka topics, and consumers.'}
                  </p>
                  <div>
                    {allNodes.length ? (
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => {
                          setHidden(new Set());
                          setEnv('');
                          setCluster('');
                          setAdHocFilters([]);
                          setSelectedServices([]);
                          setSelectedQueues([]);
                        }}
                      >
                        Clear filters
                      </Button>
                    ) : (
                      <>
                        <Button variant="primary" size="sm" onClick={() => setTab('Setup')}>
                          Set up monitoring
                        </Button>
                      </>
                    )}
                  </div>
                </div>
              )}
            </section>
            {chosen && (
              <aside className="dsm-details" aria-label="Selected node details">
                <header>
                  <Glyph kind={chosen.kind === 'service' ? 'service' : 'stream'} />
                  <h2>{chosen.name}</h2>
                  <Button variant="secondary" size="sm" aria-label="Close details" onClick={() => setSelected('')}>
                    <Glyph kind="close" />
                  </Button>
                </header>
                <div className="dsm-detail-meta">
                  <span>{chosen.kind === 'service' ? 'SERVICE' : 'KAFKA TOPIC'}</span>
                  <span>env:{chosen.environment}</span>
                </div>
                <nav aria-label="Node detail sections">
                  <TabsBar>
                    {['Throughput', 'Dependencies', 'Traces', 'Kafka'].map((t) => (
                      <Tab key={t} label={t} active={detailTab === t} onChangeTab={() => setDetailTab(t)} />
                    ))}
                  </TabsBar>
                </nav>
                {detailTab === 'Throughput' ? (
                  <div className="dsm-detail-content">
                    <h3>Observed throughput</h3>
                    <div className="dsm-detail-chart">
                      <Sparkline events={selectedEvents} now={now} seconds={seconds} width={330} height={110} />
                    </div>
                    <dl>
                      <dt>Incoming operations/s</dt>
                      <dd>{formatRate(rate(selectedEvents, seconds, 'receive'))}</dd>
                      <dt>Outgoing operations/s</dt>
                      <dd>{formatRate(rate(selectedEvents, seconds, 'publish'))}</dd>
                      <dt>Operation errors</dt>
                      <dd>{selectedEvents.filter((e) => e.error).length}</dd>
                      <dt>p95 correlated hop latency</dt>
                      <dd>
                        {ms(
                          percentile(
                            selectedHops.map((h) => h.ms),
                            0.95
                          )
                        )}
                      </dd>
                      <dt>Last observed</dt>
                      <dd>{age(now, chosen.lastSeen)}</dd>
                    </dl>
                    <p>
                      Rates are computed from retained observations in the selected window. Sampling, batching and
                      stream limits affect coverage.
                    </p>
                  </div>
                ) : detailTab === 'Dependencies' ? (
                  <div className="dsm-detail-content">
                    <h3>Connected services and queues</h3>
                    {[...state.edges.values()]
                      .filter((e) => e.source === chosen.id || e.target === chosen.id)
                      .map((edge) => (
                        <div className="dsm-dependency" key={edge.id}>
                          <small>{edge.source === chosen.id ? 'Downstream' : 'Upstream'}</small>
                          <Button
                            variant="secondary"
                            fill="text"
                            size="sm"
                            onClick={() => select(edge.source === chosen.id ? edge.target : edge.source)}
                          >
                            {state.nodes.get(edge.source === chosen.id ? edge.target : edge.source)?.name}
                          </Button>
                          <small>{edge.consumerGroup || 'Producer relationship'}</small>
                        </div>
                      ))}
                    <p>
                      Dependencies describe observed connectivity. They do not prove causal message lineage through a
                      processing service.
                    </p>
                  </div>
                ) : detailTab === 'Traces' ? (
                  <div className="dsm-detail-content">
                    <h3>Related observations</h3>
                    <div className="dsm-related-events">
                      {selectedEvents
                        .slice(-35)
                        .reverse()
                        .map((event) => (
                          <Collapse
                            key={event.observationId}
                            isOpen={expandedObservations.has(event.observationId)}
                            onToggle={() =>
                              setExpandedObservations((current) => {
                                const next = new Set(current);
                                if (next.has(event.observationId)) {
                                  next.delete(event.observationId);
                                } else {
                                  next.add(event.observationId);
                                }
                                return next;
                              })
                            }
                            label={
                              <>
                                <span className={event.error ? 'dsm-error-text' : ''}>
                                  {event.error ? 'Failed ' : ''}
                                  {event.kind === 'publish' ? 'Publish' : 'Receive'} {event.topic}
                                </span>
                                <small>{new Date(event.occurredAt).toLocaleTimeString()}</small>
                              </>
                            }
                          >
                            <dl>
                              <dt>Trace ID</dt>
                              <dd>
                                <code>{event.traceId}</code>
                              </dd>
                              <dt>Span ID</dt>
                              <dd>
                                <code>{event.spanId}</code>
                              </dd>
                              <dt>Partition / offset</dt>
                              <dd>
                                {event.partition ?? 'Unknown'} / {event.offset ?? 'Unknown'}
                              </dd>
                              <dt>Correlation</dt>
                              <dd>
                                {state.matches.has(event.observationId)
                                  ? 'Matched producer observation'
                                  : 'No match retained'}
                              </dd>
                            </dl>
                          </Collapse>
                        ))}
                    </div>
                    {!selectedEvents.length && <p>No observations retained in this window.</p>}
                  </div>
                ) : (
                  <div className="dsm-detail-content">
                    <h3>Kafka context</h3>
                    <dl>
                      <dt>Cluster</dt>
                      <dd>
                        {chosen.cluster ||
                          [...state.edges.values()].find((e) => e.source === chosen.id || e.target === chosen.id)
                            ?.cluster ||
                          'Unknown'}
                      </dd>
                      <dt>Consumer groups</dt>
                      <dd>
                        {[
                          ...new Set(
                            [...state.edges.values()]
                              .filter((e) => e.source === chosen.id || e.target === chosen.id)
                              .map((e) => e.consumerGroup)
                              .filter(Boolean)
                          ),
                        ].join(', ') || 'No consumer group observed'}
                      </dd>
                      <dt>Consumer lag</dt>
                      <dd>Not collected</dd>
                      <dt>Processing latency</dt>
                      <dd>Not collected</dd>
                    </dl>
                    <p>
                      Consumer lag requires broker/group offsets. A receive observation does not prove business
                      processing has finished.
                    </p>
                  </div>
                )}
              </aside>
            )}
          </div>
        )}
      </div>
    </PluginPage>
  );
}
