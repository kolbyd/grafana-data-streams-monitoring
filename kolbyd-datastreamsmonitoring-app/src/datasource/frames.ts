import { createDataFrame, DataFrame, FieldType } from '@grafana/data';
import { FlowEdge, FlowFrame, FlowNode } from '../flow/model';
import { FlowQuery } from './types';

type Cell = string | number | boolean | null;
type Column = { name: string; type: FieldType; unit?: string };

function table(name: string, refId: string, columns: Column[], rows: Cell[][]): DataFrame {
  return createDataFrame({
    name,
    refId,
    fields: columns.map((column, i) => ({
      name: column.name,
      type: column.type,
      config: column.unit ? { unit: column.unit } : {},
      values: rows.map((row) => row[i]),
    })),
  });
}

const s = (name: string): Column => ({ name, type: FieldType.string });
const n = (name: string, unit?: string): Column => ({ name, type: FieldType.number, unit });
const time = (name: string): Column => ({ name, type: FieldType.time });

// Filters are exact, case-sensitive labels. Empty or * means all. No filter is evaluated as code or a regexp.
export function matchesFilter(value: string | null | undefined, filter: string | undefined): boolean {
  return !filter || filter === '*' || value === filter;
}

export function filterSnapshot(snapshot: FlowFrame, query: FlowQuery) {
  const nodesById = new Map(snapshot.nodes.map((node) => [node.id, node]));
  const edges = snapshot.edges.filter(
    (edge) =>
      matchesFilter(edge.environment, query.environment) &&
      matchesFilter(edge.cluster, query.cluster) &&
      matchesFilter(edge.topic, query.topic) &&
      (!query.operation || edge.operation === query.operation) &&
      matchesFilter(edge.consumerGroup, query.consumerGroup) &&
      (!query.service ||
        query.service === '*' ||
        (nodesById.get(edge.source)?.kind === 'service' && nodesById.get(edge.source)?.name === query.service) ||
        (nodesById.get(edge.target)?.kind === 'service' && nodesById.get(edge.target)?.name === query.service))
  );
  const connectedIds = new Set(edges.flatMap((edge) => [edge.source, edge.target]));
  const filtered = [
    query.environment,
    query.cluster,
    query.service,
    query.topic,
    query.consumerGroup,
    query.operation,
  ].some((value) => value && value !== '*');
  const nodes = filtered ? snapshot.nodes.filter((node) => connectedIds.has(node.id)) : snapshot.nodes;
  return { nodes, edges, nodesById };
}

function serviceTable(nodes: FlowNode[], edges: FlowEdge[], query: FlowQuery): DataFrame {
  return table(
    'services',
    query.refId,
    [
      s('Service'),
      s('Namespace'),
      s('Environment'),
      n('Publish observations'),
      n('Receive observations'),
      n('Errors'),
      n('Connected topics'),
      time('Last observed'),
    ],
    nodes
      .filter((node) => node.kind === 'service')
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((node) => {
        const related = edges.filter((edge) => edge.source === node.id || edge.target === node.id);
        return [
          node.name,
          node.serviceNamespace,
          node.environment,
          related.filter((edge) => edge.operation === 'publish').reduce((total, edge) => total + edge.observations, 0),
          related.filter((edge) => edge.operation === 'receive').reduce((total, edge) => total + edge.observations, 0),
          related.reduce((total, edge) => total + edge.errors, 0),
          new Set(related.map((edge) => edge.topic)).size,
          node.lastSeen,
        ];
      })
  );
}

function topicTable(nodes: FlowNode[], edges: FlowEdge[], query: FlowQuery): DataFrame {
  return table(
    'topics',
    query.refId,
    [
      s('Topic'),
      s('Cluster'),
      s('Environment'),
      n('Publish observations'),
      n('Receive observations'),
      n('Producer services'),
      n('Consumer groups'),
      n('Errors'),
      time('Last observed'),
    ],
    nodes
      .filter((node) => node.kind === 'topic')
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((node) => {
        const related = edges.filter((edge) => edge.source === node.id || edge.target === node.id);
        return [
          node.name,
          node.cluster,
          node.environment,
          related.filter((edge) => edge.operation === 'publish').reduce((total, edge) => total + edge.observations, 0),
          related.filter((edge) => edge.operation === 'receive').reduce((total, edge) => total + edge.observations, 0),
          new Set(related.filter((edge) => edge.operation === 'publish').map((edge) => edge.source)).size,
          new Set(
            related.filter((edge) => edge.operation === 'receive').map((edge) => edge.consumerGroup || '(unknown)')
          ).size,
          related.reduce((total, edge) => total + edge.errors, 0),
          node.lastSeen,
        ];
      })
  );
}

export function buildFrames(snapshot: FlowFrame, query: FlowQuery): DataFrame[] {
  const { nodes, edges, nodesById } = filterSnapshot(snapshot, query);
  switch (query.view || 'summary') {
    case 'summary': {
      const publishes = edges.filter((edge) => edge.operation === 'publish');
      const receives = edges.filter((edge) => edge.operation === 'receive');
      const groups = new Set(
        receives
          .filter((edge) => edge.consumerGroup)
          .map((edge) => JSON.stringify([edge.environment, edge.cluster, edge.consumerGroup]))
      );
      return [
        table(
          'session summary',
          query.refId,
          [
            time('Snapshot time'),
            n('Services'),
            n('Topics'),
            n('Relationships'),
            n('Consumer groups'),
            n('Publish observations'),
            n('Receive observations'),
            n('Operation errors'),
          ],
          [
            [
              snapshot.serverTime,
              nodes.filter((node) => node.kind === 'service').length,
              nodes.filter((node) => node.kind === 'topic').length,
              edges.length,
              groups.size,
              publishes.reduce((total, edge) => total + edge.observations, 0),
              receives.reduce((total, edge) => total + edge.observations, 0),
              edges.reduce((total, edge) => total + edge.errors, 0),
            ],
          ]
        ),
      ];
    }
    case 'services':
      return [serviceTable(nodes, edges, query)];
    case 'topics':
      return [topicTable(nodes, edges, query)];
    case 'consumerGroups':
      return [
        table(
          'consumer group relationships',
          query.refId,
          [
            s('Consumer group'),
            s('Consumer service'),
            s('Topic'),
            s('Cluster'),
            s('Environment'),
            n('Receive observations'),
            n('Operation errors'),
            time('Last observed'),
            s('Consumer lag'),
          ],
          edges
            .filter((edge) => edge.operation === 'receive')
            .sort(
              (a, b) => (a.consumerGroup || '').localeCompare(b.consumerGroup || '') || a.topic.localeCompare(b.topic)
            )
            .map((edge) => [
              edge.consumerGroup || '(not reported)',
              nodesById.get(edge.target)?.name || edge.target,
              edge.topic,
              edge.cluster,
              edge.environment,
              edge.observations,
              edge.errors,
              edge.lastSeen,
              'Not collected',
            ])
        ),
      ];
    case 'topology': {
      const nodeFrame = table(
        'nodes',
        query.refId,
        [
          s('id'),
          s('title'),
          s('subTitle'),
          n('mainStat'),
          s('secondaryStat'),
          n('arc__publish'),
          n('arc__receive'),
          s('detail__environment'),
          s('detail__cluster'),
          n('detail__errors'),
        ],
        nodes.map((node) => {
          const related = edges.filter((edge) => edge.source === node.id || edge.target === node.id);
          const publish = related
            .filter((edge) => edge.operation === 'publish')
            .reduce((total, edge) => total + edge.observations, 0);
          const receive = related
            .filter((edge) => edge.operation === 'receive')
            .reduce((total, edge) => total + edge.observations, 0);
          const total = publish + receive;
          return [
            node.id,
            node.name,
            node.kind === 'topic' ? 'Kafka topic' : node.serviceNamespace || 'Service',
            total,
            'observed ops',
            total ? publish / total : 0,
            total ? receive / total : 0,
            node.environment,
            node.cluster,
            related.reduce((count, edge) => count + edge.errors, 0),
          ];
        })
      );
      nodeFrame.meta = { preferredVisualisationType: 'nodeGraph' };
      nodeFrame.fields.find((field) => field.name === 'arc__publish')!.config.color = {
        mode: 'fixed',
        fixedColor: 'blue',
      };
      nodeFrame.fields.find((field) => field.name === 'arc__receive')!.config.color = {
        mode: 'fixed',
        fixedColor: 'purple',
      };
      const edgeFrame = table(
        'edges',
        query.refId,
        [
          s('id'),
          s('source'),
          s('target'),
          n('mainStat'),
          s('secondaryStat'),
          s('detail__topic'),
          s('detail__consumer_group'),
          n('detail__errors'),
        ],
        edges.map((edge) => [
          edge.id,
          edge.source,
          edge.target,
          edge.observations,
          edge.operation === 'publish' ? 'Publish' : `Receive · ${edge.consumerGroup || 'unknown group'}`,
          edge.topic,
          edge.consumerGroup || '',
          edge.errors,
        ])
      );
      edgeFrame.meta = { preferredVisualisationType: 'nodeGraph' };
      return [nodeFrame, edgeFrame];
    }
    case 'observations': {
      const edgeKeys = new Set(edges.map((edge) => JSON.stringify([edge.source, edge.target, edge.consumerGroup])));
      return [
        table(
          'recent observations',
          query.refId,
          [
            time('Observed at'),
            s('Operation'),
            s('Service'),
            s('Topic'),
            s('Consumer group'),
            s('Partition'),
            s('Offset'),
            s('Trace ID'),
            s('Span ID'),
            s('Result'),
          ],
          snapshot.events
            .filter(
              (event) =>
                event.kind !== 'matched' &&
                edgeKeys.has(JSON.stringify([event.source, event.target, event.consumerGroup]))
            )
            .slice(-150)
            .reverse()
            .map((event) => [
              event.occurredAt,
              event.kind,
              event.service,
              event.topic,
              event.consumerGroup || '',
              event.partition ?? '',
              event.offset ?? '',
              event.traceId,
              event.spanId,
              event.error ? 'Error' : 'Observed',
            ])
        ),
      ];
    }
    default:
      throw new Error(`Unsupported flow query view: ${String(query.view)}`);
  }
}
