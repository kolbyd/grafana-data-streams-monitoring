import React, { useId, useRef, useState } from 'react';
import { FlowEvent, FlowState, formatRate } from '../../flow/model';
import { ClusterInventory, GroupInventory, observedInventory, TopicInventory } from './inventory';
import { getKafkaConsoleStyles } from './kafka-console.styles';
import { Button, Input, Field, Icon as GrafanaIcon, TabsBar, Tab, useStyles2, LoadingPlaceholder } from '@grafana/ui';
import { NativeSelect } from '../Native/NativeSelect';
import { NativeInventoryTable, InventoryTableRow } from '../Pipelines/NativeInventoryTable';

export type KafkaConsoleProps = {
  state: FlowState;
  events: FlowEvent[];
  now: number;
  seconds: number;
  environment: string;
  onExplore: (nodeIds: string[]) => void;
  onSetup: () => void;
};
type ConsoleTab = 'Clusters' | 'Topics' | 'Brokers' | 'Consumer Groups' | 'Connectors';
const TABS: ConsoleTab[] = ['Clusters', 'Topics', 'Brokers', 'Consumer Groups', 'Connectors'];
type InventoryRow = ClusterInventory | TopicInventory | GroupInventory;
const NativeTimeSeries = React.lazy(() => import('../Native/NativeTimeSeries'));

function Unavailable({ reason }: { reason: string }) {
  return (
    <span className="dsm-kafka-unavailable-value" title={reason} aria-label={`Unavailable: ${reason}`}>
      —
    </span>
  );
}
const observedRate = (events: FlowEvent[], seconds: number, receive = false) =>
  events.filter((event) => event.kind === (receive ? 'receive' : 'publish')).length / Math.max(1, seconds);
const relativeTime = (now: number, at: number) => `${Math.max(0, Math.floor((now - at) / 1000))}s ago`;

function IncomingChart({
  rows,
  now,
  seconds,
  receive,
  kind,
}: {
  rows: InventoryRow[];
  now: number;
  seconds: number;
  receive: boolean;
  kind: string;
}) {
  const series = rows.slice(0, 6).map((row) => {
    const buckets = Array.from({ length: 24 }, () => 0);
    for (const event of row.events) {
      if (event.kind !== (receive ? 'receive' : 'publish')) {
        continue;
      }
      const index = Math.floor((event.occurredAt - (now - seconds * 1000)) / ((seconds * 1000) / 24));
      if (index >= 0 && index < 24) {
        buckets[index] += 24 / seconds;
      }
    }
    return {
      name: row.name + ' · ' + row.environment,
      samples: buckets.map((value, index) => ({
        at: now - seconds * 1000 + (index + 0.5) * ((seconds * 1000) / 24),
        value,
      })),
    };
  });
  return (
    <section className="dsm-kafka-chart">
      <h3>
        {receive ? 'Receive' : 'Incoming publish'} operations by {kind}
      </h3>
      {rows.length ? (
        <React.Suspense fallback={<LoadingPlaceholder text="Loading Grafana chart…" />}>
          <NativeTimeSeries
            title={`Observed ${receive ? 'receive' : 'publish'} operations per second by ${kind}`}
            series={series}
            now={now}
            seconds={seconds}
            unit="ops"
          />
        </React.Suspense>
      ) : (
        <div className="dsm-kafka-chart-empty">No client observations match the filters.</div>
      )}
    </section>
  );
}

function MissingChart({ title, onSetup }: { title: string; onSetup: () => void }) {
  return (
    <section className="dsm-kafka-chart">
      <h3>{title}</h3>
      <div className="dsm-kafka-chart-empty">
        <span>Broker metrics not connected</span>
        <Button variant="secondary" type="button" onClick={onSetup}>
          Set up monitoring
        </Button>
      </div>
    </section>
  );
}

export function KafkaConsole({ state, events, now, seconds, environment, onExplore, onSetup }: KafkaConsoleProps) {
  const styles = useStyles2(getKafkaConsoleStyles);
  const [tab, setTab] = useState<ConsoleTab>('Clusters'),
    [query, setQuery] = useState(''),
    [env, setEnv] = useState(environment),
    [cluster, setCluster] = useState('');
  const [charts, setCharts] = useState(true);
  const tabsId = useId(),
    tabRefs = useRef<Array<HTMLElement | null>>([]);
  const retained = events.filter((event) => event.occurredAt >= now - seconds * 1000 && event.occurredAt <= now);
  const inventory = observedInventory(state, retained, env, cluster);
  const environments = [...new Set([...state.edges.values()].map((edge) => edge.environment))].sort();
  const clusters = [
    ...new Set(
      [...state.edges.values()].filter((edge) => !env || edge.environment === env).map((edge) => edge.cluster)
    ),
  ].sort();
  const q = query.trim().toLowerCase();
  const found = <T extends InventoryRow>(rows: T[]) =>
    rows.filter(
      (row) =>
        !q ||
        [
          row.name,
          row.environment,
          'cluster' in row ? row.cluster : '',
          ...('topicNames' in row ? row.topicNames : []),
          ...('consumerNames' in row ? row.consumerNames : []),
          ...('groups' in row ? row.groups : []),
        ].some((value) => value.toLowerCase().includes(q))
    );
  const clusterRows = found(inventory.clusters),
    topicRows = found(inventory.topics),
    groupRows = found(inventory.groups);
  const receive = tab === 'Consumer Groups';
  const rows: InventoryRow[] = tab === 'Clusters' ? clusterRows : tab === 'Topics' ? topicRows : groupRows;
  const unavailableInventory = tab === 'Brokers' || tab === 'Connectors';
  const countFor = (value: ConsoleTab) =>
    value === 'Clusters'
      ? clusterRows.length
      : value === 'Topics'
        ? topicRows.length
        : value === 'Consumer Groups'
          ? groupRows.length
          : '—';
  const kind = tab === 'Clusters' ? 'cluster' : tab === 'Topics' ? 'topic' : 'consumer group';
  return (
    <section className={`dsm-kafka-console ${styles.root}`} aria-label="Kafka Console" data-testid="kafka-console">
      <div className="dsm-kafka-notice">
        <span>Kafka broker monitoring is not configured.</span>
        <Button variant="secondary" type="button" onClick={onSetup}>
          Set up monitoring
        </Button>
      </div>
      <div className="dsm-kafka-filters">
        <Field label="Search" className="dsm-kafka-search">
          <Input
            prefix={<GrafanaIcon name="search" />}
            aria-label="Filter Kafka inventory"
            value={query}
            maxLength={256}
            onChange={(event) => setQuery(event.currentTarget.value)}
            placeholder="Filter Kafka inventory…"
          />
        </Field>
        <Field label="Cluster">
          <NativeSelect
            aria-label="Kafka Console cluster"
            value={cluster}
            onChange={setCluster}
            options={[{ value: '', label: 'All' }, ...clusters.map((value) => ({ value, label: value }))]}
          />
        </Field>
        <Field label="Environment">
          <NativeSelect
            aria-label="Kafka Console environment"
            value={env}
            onChange={(value) => {
              setEnv(value);
              setCluster('');
            }}
            options={[
              { value: '', label: 'All environments' },
              ...environments.map((value) => ({ value, label: value })),
            ]}
          />
        </Field>
      </div>
      <div className="dsm-kafka-tabs-row">
        <TabsBar aria-label="Kafka inventory types">
          {TABS.map((value, index) => (
            <Tab
              key={value}
              id={tabsId + '-' + index}
              ref={(element) => {
                tabRefs.current[index] = element;
              }}
              label={value}
              active={tab === value}
              counter={typeof countFor(value) === 'number' ? Number(countFor(value)) : undefined}
              suffix={
                typeof countFor(value) === 'number'
                  ? undefined
                  : () => <span title="Inventory requires additional collection">—</span>
              }
              aria-controls={tabsId + '-panel'}
              onChangeTab={() => setTab(value)}
              onKeyDown={(event) => {
                let next: number | null = null;
                if (event.key === 'ArrowRight') {
                  next = (index + 1) % TABS.length;
                } else if (event.key === 'ArrowLeft') {
                  next = (index + TABS.length - 1) % TABS.length;
                } else if (event.key === 'Home') {
                  next = 0;
                } else if (event.key === 'End') {
                  next = TABS.length - 1;
                }
                if (next !== null) {
                  event.preventDefault();
                  setTab(TABS[next]);
                  tabRefs.current[next]?.focus();
                }
              }}
            />
          ))}
        </TabsBar>
        <div className="dsm-kafka-tab-actions">
          <Button variant="secondary" type="button" icon="plus" onClick={onSetup}>
            Set up Kafka metrics
          </Button>
          <Button
            variant="secondary"
            type="button"
            icon="sliders-v-alt"
            onClick={() => setCharts(!charts)}
            aria-pressed={charts}
          >
            Charts
          </Button>
        </div>
      </div>
      <div id={`${tabsId}-panel`} role="tabpanel" aria-labelledby={`${tabsId}-${TABS.indexOf(tab)}`}>
        {unavailableInventory ? (
          <div className="dsm-kafka-no-inventory">
            <h2>{tab === 'Brokers' ? 'Broker' : 'Connector'} monitoring is not connected</h2>
            <p>
              {tab === 'Brokers'
                ? 'Connect Kafka monitoring to view brokers, partition health, and replication status.'
                : 'Connect Kafka Connect monitoring to view workers and connector status.'}
            </p>
            <Button variant="secondary" type="button" onClick={onSetup}>
              Set up monitoring
            </Button>
          </div>
        ) : (
          <>
            {charts && (
              <div className="dsm-kafka-charts">
                <IncomingChart rows={rows} now={now} seconds={seconds} receive={receive} kind={kind} />
                <MissingChart
                  title={tab === 'Clusters' ? 'Under-replicated partitions by cluster' : `Lag by ${kind}`}
                  onSetup={onSetup}
                />
                <MissingChart
                  title={tab === 'Consumer Groups' ? 'Consumer group rebalances' : `Offline partitions by ${kind}`}
                  onSetup={onSetup}
                />
              </div>
            )}
            <div className="dsm-kafka-table-heading">
              <span title="Inventory is discovered from Kafka client observations and may not include every Kafka resource.">
                {rows.length} {tab.toLowerCase()}
              </span>
            </div>
            <div className="dsm-kafka-table-scroll">
              <NativeInventoryTable
                key={tab + query + env + cluster}
                columns={inventoryColumns[tab]}
                rows={rows.map((row) => inventoryTableRow(row, tab, seconds, now, onExplore))}
              />
            </div>
            {!rows.length && (
              <p className="dsm-kafka-chart-empty">No observed {tab.toLowerCase()} match these filters.</p>
            )}
          </>
        )}
      </div>
    </section>
  );
}

export default KafkaConsole;

const inventoryColumns: Record<ConsoleTab, Array<{ id: string; label: string }>> = {
  Clusters: [
    { id: 'name', label: 'Cluster' },
    { id: 'brokers', label: 'Brokers' },
    { id: 'topics', label: 'Topics' },
    { id: 'environment', label: 'Environment' },
    { id: 'health', label: 'Health' },
    { id: 'under', label: 'Under-replicated' },
    { id: 'offline', label: 'Offline' },
    { id: 'rate', label: 'Incoming ops/s' },
  ],
  Topics: [
    { id: 'name', label: 'Topic' },
    { id: 'cluster', label: 'Cluster' },
    { id: 'environment', label: 'Environment' },
    { id: 'health', label: 'Health' },
    { id: 'groups', label: 'Consumer groups' },
    { id: 'partitions', label: 'Partitions' },
    { id: 'lag', label: 'Lag' },
    { id: 'rate', label: 'Incoming ops/s' },
  ],
  'Consumer Groups': [
    { id: 'name', label: 'Consumer group' },
    { id: 'cluster', label: 'Cluster' },
    { id: 'environment', label: 'Environment' },
    { id: 'topics', label: 'Topics' },
    { id: 'consumers', label: 'Consumer services' },
    { id: 'lag', label: 'Lag' },
    { id: 'lastSeen', label: 'Last observed' },
    { id: 'rate', label: 'Receive ops/s' },
  ],
  Brokers: [],
  Connectors: [],
};
function inventoryTableRow(
  row: InventoryRow,
  tab: ConsoleTab,
  seconds: number,
  now: number,
  onExplore: (ids: string[]) => void
): InventoryTableRow {
  const cells: InventoryTableRow['cells'] = {
    name: (
      <Button variant="secondary" fill="text" onClick={() => onExplore([...row.nodeIds])}>
        {row.name || 'Unknown cluster'}
      </Button>
    ),
    brokers: <Unavailable reason="Broker inventory requires broker metadata collection." />,
    health: <Unavailable reason="Broker health metrics are not connected." />,
    under: <Unavailable reason="Under-replication requires broker partition metrics." />,
    offline: <Unavailable reason="Offline partition metrics are not connected." />,
    partitions: <Unavailable reason="Partition inventory requires broker metadata collection." />,
    lag: <Unavailable reason="Lag requires broker end offsets and consumer-group committed offsets." />,
  };
  return {
    id: row.id,
    name: row.name,
    environment: row.environment,
    cluster: 'cluster' in row ? row.cluster : '',
    topics: 'topicNames' in row ? row.topicNames.length : 0,
    groups: 'groups' in row ? row.groups.length : 0,
    consumers: 'consumerNames' in row ? row.consumerNames.length : 0,
    rate: observedRate(row.events, seconds, tab === 'Consumer Groups'),
    lastSeen: relativeTime(now, row.lastSeen),
    cells: { ...cells, rate: formatRate(observedRate(row.events, seconds, tab === 'Consumer Groups')) },
  };
}
