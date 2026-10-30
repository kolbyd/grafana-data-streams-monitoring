import { FlowEdge, FlowEvent, FlowFrame, FlowNode } from './model';

export function exampleSource() {
  const names = [
    'orders-api',
    'mobile-checkout',
    'payments',
    'fraud-detector',
    'fulfilment',
    'notifications',
    'analytics',
  ];
  const topics = ['orders.created', 'payments.authorized', 'orders.review', 'shipments.created', 'analytics.events'];
  let sequence = 0,
    offset = 0;
  const nodes: FlowNode[] = [
    ...names.map((name) => ({ id: name, kind: 'service' as const, name })),
    ...topics.map((name) => ({ id: name, kind: 'topic' as const, name })),
  ].map((n) => ({
    ...n,
    serviceNamespace: 'commerce',
    environment: 'example',
    cluster: n.kind === 'topic' ? 'example-kafka' : '',
    lastSeen: Date.now(),
    published: 0,
    received: 0,
    errors: 0,
  }));
  const routes = [
    ['orders-api', 'orders.created', 'publish'],
    ['mobile-checkout', 'orders.created', 'publish'],
    ['orders.created', 'payments', 'receive'],
    ['orders.created', 'analytics', 'receive'],
    ['payments', 'payments.authorized', 'publish'],
    ['payments', 'orders.review', 'publish'],
    ['payments.authorized', 'fulfilment', 'receive'],
    ['orders.review', 'fraud-detector', 'receive'],
    ['fulfilment', 'shipments.created', 'publish'],
    ['shipments.created', 'notifications', 'receive'],
    ['fraud-detector', 'analytics.events', 'publish'],
    ['analytics.events', 'analytics', 'receive'],
  ];
  const edges: FlowEdge[] = routes.map(([source, target, operation]) => ({
    id: `${source}:${target}`,
    source,
    target,
    operation,
    environment: 'example',
    cluster: 'example-kafka',
    topic: operation === 'publish' ? target : source,
    consumerGroup: operation === 'receive' ? `${target}-v1` : null,
    lastSeen: Date.now(),
    observations: 0,
    errors: 0,
  }));
  const totals = { published: 0, received: 0, errors: 0, matched: 0 };
  function cycle(at: number): FlowEvent[] {
    offset++;
    const events: FlowEvent[] = [],
      producers = new Map<string, FlowEvent>();
    for (const edge of edges) {
      // The extra producer and review branch intentionally have lower example throughput.
      if ((edge.source === 'mobile-checkout' || edge.topic === 'orders.review') && offset % 3 !== 0) {
        continue;
      }
      const service = edge.operation === 'publish' ? edge.source : edge.target;
      const error = edge.target === 'fraud-detector' && offset % 17 === 0;
      const event: FlowEvent = {
        sequence: ++sequence,
        kind: edge.operation,
        observationId: `example-${sequence}`,
        source: edge.source,
        target: edge.target,
        topicNodeId: edge.topic,
        service,
        environment: 'example',
        cluster: 'example-kafka',
        topic: edge.topic,
        consumerGroup: edge.consumerGroup,
        partition: String(offset % 3),
        offset: String(offset),
        error,
        isBatch: false,
        occurredAt: at + (edge.operation === 'receive' ? 60 + (offset % 80) : 0),
        observedAt: at + 150,
        traceId: offset.toString(16).padStart(32, '0'),
        spanId: sequence.toString(16).padStart(16, '0'),
      };
      events.push(event);
      edge.observations++;
      edge.errors += Number(error);
      edge.lastSeen = at;
      totals[edge.operation === 'publish' ? 'published' : 'received']++;
      totals.errors += Number(error);
      for (const node of nodes.filter((n) => n.id === edge.source || n.id === edge.target)) {
        node[edge.operation === 'publish' ? 'published' : 'received']++;
        node.lastSeen = at;
        node.errors += Number(error);
      }
      if (edge.operation === 'publish') {
        producers.set(edge.topic, event);
      }
      const producer = producers.get(edge.topic);
      if (edge.operation === 'receive' && producer && !error) {
        events.push({
          ...event,
          sequence: ++sequence,
          kind: 'matched',
          producerObservationId: producer.observationId,
          producerService: producer.service,
          producerTraceId: producer.traceId,
          correlationMethod: 'record',
        });
        totals.matched++;
      }
    }
    return events;
  }
  const history: FlowEvent[] = [];
  for (let i = 59; i >= 1; i--) {
    if (i % 7 !== 0) {
      history.push(...cycle(Date.now() - i * 1000));
    }
  }
  function frame(initial = false): FlowFrame {
    const events = initial ? history : cycle(Date.now() - 200);
    return {
      type: initial ? 'snapshot' : 'update',
      instanceId: 'synthetic-example',
      cursor: sequence,
      serverTime: Date.now(),
      replaceTopology: true,
      resetEvents: initial,
      nodes: nodes.map((n) => ({ ...n })),
      edges: edges.map((e) => ({ ...e })),
      events,
      stats: { ...totals },
      visualEventsSkipped: 0,
    };
  }
  return { frame };
}
