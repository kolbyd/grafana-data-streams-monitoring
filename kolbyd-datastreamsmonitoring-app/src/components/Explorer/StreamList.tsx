import React, { useMemo, useState } from 'react';
import { Button, Checkbox, InteractiveTable, RadioButtonGroup, type Column } from '@grafana/ui';
import { FlowEvent, FlowNode, FlowState, formatRate, matchedHops, nodeEvents, rate } from '../../flow/model';

interface StreamRow {
  id: string;
  node: FlowNode;
  name: string;
  incoming: number;
  outgoing: number;
  errors: number;
  latency: number | null;
  lag: null;
}
const definitions = [
  ['incoming', 'Operations in'],
  ['outgoing', 'Operations out'],
  ['lag', 'Max Kafka lag'],
  ['latency', 'Max hop time'],
  ['errors', 'Errors'],
];
const initialSort = [{ id: 'name', desc: false }];

export function StreamList({
  nodes,
  events,
  state,
  seconds,
  kind,
  onKind,
  onSelect,
}: {
  nodes: FlowNode[];
  events: FlowEvent[];
  state: FlowState;
  seconds: number;
  kind: 'services' | 'queues';
  onKind: (kind: 'services' | 'queues') => void;
  onSelect: (id: string) => void;
}) {
  const [visibleColumns, setVisibleColumns] = useState(['incoming', 'outgoing', 'lag', 'latency', 'errors']);
  const [columnsOpen, setColumnsOpen] = useState(false);
  const rows = useMemo(() => {
    const hops = matchedHops(state, events);
    return nodes
      .filter((n) => n.kind === (kind === 'services' ? 'service' : 'topic'))
      .map((node): StreamRow => {
        const activity = nodeEvents(node, events);
        const messages = activity.filter((e) => !e.error && !e.isBatch);
        const latencies = hops
          .filter((h) => h.receive.target === node.id || h.receive.topicNodeId === node.id)
          .map((h) => h.ms);
        return {
          id: node.id,
          node,
          name: node.name,
          incoming: rate(messages, seconds, node.kind === 'service' ? 'receive' : 'publish'),
          outgoing: rate(messages, seconds, node.kind === 'service' ? 'publish' : 'receive'),
          errors: activity.filter((e) => e.error).length,
          latency: latencies.length ? Math.max(...latencies) : null,
          lag: null,
        };
      });
  }, [nodes, events, state, seconds, kind]);
  const columns = useMemo<Array<Column<StreamRow>>>(
    () => [
      {
        id: 'name',
        header: 'Name',
        sortType: 'string',
        minWidth: 200,
        cell: ({ row }) => (
          <Button
            variant="secondary"
            fill="text"
            size="sm"
            icon={row.original.node.kind === 'topic' ? 'database' : 'cube'}
            onClick={() => onSelect(row.original.id)}
          >
            {row.original.name}
          </Button>
        ),
      },
      ...definitions
        .filter(([key]) => visibleColumns.includes(key))
        .map(([key, header]): Column<StreamRow> => ({
          id: key,
          header,
          sortType: key === 'lag' ? undefined : 'number',
          minWidth: 140,
          cell: ({ row }) => {
            const value = row.original[key as keyof StreamRow];
            if (key === 'lag') {
              return <span title="Requires broker end offsets and committed group offsets">—</span>;
            }
            if (key === 'latency') {
              return value === null ? '—' : `${Number(value).toFixed(1)} ms`;
            }
            if (key === 'errors') {
              return <span className={value ? 'dsm-error-text' : ''}>{String(value)}</span>;
            }
            const maximum = Math.max(1, ...rows.map((r) => Number(r[key as keyof StreamRow])));
            return (
              <span className="dsm-cell-metric">
                {formatRate(Number(value))} ops/s
                <i style={{ width: `${Math.max(0, (Number(value) / maximum) * 100)}%` }} />
              </span>
            );
          },
        })),
    ],
    [visibleColumns, onSelect, rows]
  );
  return (
    <div className="dsm-list-view">
      <RadioButtonGroup
        aria-label="List entity type"
        value={kind}
        size="sm"
        className="dsm-segmented"
        options={[
          { label: 'Services', value: 'services' },
          { label: 'Queues', value: 'queues' },
        ]}
        onChange={onKind}
      />
      <div className="dsm-list-title">
        <span>
          {rows.length} {kind}
        </span>
        <Button
          size="sm"
          variant="secondary"
          icon="columns"
          aria-expanded={columnsOpen}
          onClick={() => setColumnsOpen(!columnsOpen)}
        >
          Columns
        </Button>
      </div>
      {columnsOpen && (
        <div className="dsm-column-choices" aria-label="Visible columns">
          {definitions.map(([key, label]) => (
            <Checkbox
              key={key}
              label={label}
              checked={visibleColumns.includes(key)}
              onChange={() =>
                setVisibleColumns((current) =>
                  current.includes(key) ? current.filter((v) => v !== key) : [...current, key]
                )
              }
            />
          ))}
        </div>
      )}
      {rows.length ? (
        <div className="dsm-table-scroll">
          <InteractiveTable
            columns={columns}
            data={rows}
            getRowId={(row) => row.id}
            initialSortBy={initialSort}
            pageSize={20}
          />
        </div>
      ) : (
        <p className="dsm-table-empty">No {kind} match the current filters.</p>
      )}
    </div>
  );
}
