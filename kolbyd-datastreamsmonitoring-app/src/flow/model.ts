export interface FlowNode {
  id: string;
  kind: 'service' | 'topic';
  name: string;
  serviceNamespace: string;
  environment: string;
  cluster: string;
  lastSeen: number;
  published: number;
  received: number;
  errors: number;
}
export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  operation: string;
  environment: string;
  cluster: string;
  topic: string;
  consumerGroup: string | null;
  lastSeen: number;
  observations: number;
  errors: number;
}
export interface FlowEvent {
  sequence: number;
  kind: string;
  observationId: string;
  source: string;
  target: string;
  topicNodeId: string;
  service: string;
  environment: string;
  cluster: string;
  topic: string;
  consumerGroup: string | null;
  partition: string | null;
  offset: string | null;
  error: boolean;
  isBatch: boolean;
  occurredAt: number;
  observedAt: number;
  traceId: string;
  spanId: string;
  producerObservationId?: string;
  producerService?: string;
  correlationMethod?: string;
  producerTraceId?: string;
}
export interface FlowFrame {
  type: string;
  instanceId: string;
  cursor: number;
  serverTime: number;
  replaceTopology: boolean;
  resetEvents: boolean;
  reason?: string;
  nodes: FlowNode[];
  edges: FlowEdge[];
  events: FlowEvent[];
  stats: Record<string, number>;
  visualEventsSkipped: number;
  pathways?: FlowPathway[];
  causality?: Record<string, number>;
}
export interface PathwayStep {
  observationId: string;
  traceId: string;
  spanId: string;
  operation: string;
  service: string;
  nodeId: string | null;
  topic: string;
  cluster: string;
  consumerGroup: string | null;
  startedAt: number;
  completedAt: number;
  error: boolean;
}
export interface FlowPathway {
  id: string;
  originObservationId: string | null;
  terminalObservationId: string;
  originService: string;
  terminalService: string;
  environment: string;
  startNodeId: string | null;
  endNodeId: string;
  nodeIds: string[];
  edgeIds: string[];
  startedAt: number | null;
  completedAt: number;
  durationMs: number | null;
  status: 'complete' | 'incomplete' | 'clock_skew';
  incompleteReason: string | null;
  evidence: 'otel_parent_links' | 'record_assisted';
  isBatch: boolean;
  error: boolean;
  steps: PathwayStep[];
  missingSpanIds: string[];
  dsmAttributes: Record<string, string>;
}
export interface FlowState {
  instance: string;
  cursor: number;
  nodes: Map<string, FlowNode>;
  edges: Map<string, FlowEdge>;
  events: FlowEvent[];
  matches: Map<string, FlowEvent>;
  stats: Record<string, number>;
  skipped: number;
  reset: string;
  updatedAt: number;
  clockOffset: number;
  animationAfter: number;
  pathways: FlowPathway[];
  causality: Record<string, number>;
}
export const emptyState = (): FlowState => ({
  instance: '',
  cursor: 0,
  nodes: new Map(),
  edges: new Map(),
  events: [],
  matches: new Map(),
  stats: {},
  skipped: 0,
  reset: '',
  updatedAt: 0,
  clockOffset: 0,
  animationAfter: 0,
  pathways: [],
  causality: {},
});

export function applyFrame(state: FlowState, frame: FlowFrame): FlowState {
  if (state.instance === frame.instanceId && frame.cursor < state.cursor) {
    return state;
  }
  const changedInstance = !!state.instance && state.instance !== frame.instanceId;
  const replace = changedInstance || frame.replaceTopology;
  const reset = changedInstance || frame.resetEvents;
  const nodes = replace ? new Map<string, FlowNode>() : new Map(state.nodes);
  const edges = replace ? new Map<string, FlowEdge>() : new Map(state.edges);
  frame.nodes.forEach((n) => nodes.set(n.id, n));
  frame.edges.forEach((e) => edges.set(e.id, e));
  const matches = reset ? new Map<string, FlowEvent>() : new Map(state.matches);
  const events = reset ? [] : [...state.events];
  const known = new Set(events.map((e) => e.observationId));
  for (const event of frame.events) {
    if (event.kind === 'matched') {
      matches.set(event.observationId, event);
    } else if (!known.has(event.observationId)) {
      events.push(event);
      known.add(event.observationId);
    }
  }
  const retained = events.filter((e) => e.observedAt >= frame.serverTime - 600000).slice(-5000);
  const ids = new Set(retained.map((e) => e.observationId));
  for (const id of matches.keys()) {
    if (!ids.has(id)) {
      matches.delete(id);
    }
  }
  return {
    instance: frame.instanceId,
    cursor: frame.cursor,
    nodes,
    edges,
    events: retained,
    matches,
    stats: frame.stats,
    skipped: (reset ? 0 : state.skipped) + frame.visualEventsSkipped,
    reset: changedInstance
      ? 'Flow service restarted. The current in-memory session has been restored.'
      : frame.reason === 'cursor_expired'
        ? 'The observation cursor expired. Recent observations have been restored.'
        : state.reset,
    updatedAt: Date.now(),
    clockOffset: frame.serverTime - Date.now(),
    // A same-instance recovery snapshot can contain fresh observations. Keeping
    // this initial cutoff lets their stable keys animate once instead of hiding
    // every new event whenever the transport replaces its retained history.
    animationAfter: !state.instance || changedInstance ? frame.cursor : state.animationAfter,
    // The collector sends a bounded replacement set, including updates to previously incomplete chains.
    pathways: frame.pathways ?? (reset ? [] : state.pathways),
    causality: frame.causality ?? (reset ? {} : state.causality),
  };
}

export const nodeEvents = (node: FlowNode, events: FlowEvent[]) =>
  events.filter((e) =>
    node.kind === 'topic' ? e.topicNodeId === node.id : e.source === node.id || e.target === node.id
  );
export const rate = (events: FlowEvent[], seconds: number, direction?: string) =>
  events.filter((e) => !direction || e.kind === direction).length / seconds;
export const formatRate = (value: number) =>
  value >= 1000 ? `${(value / 1000).toFixed(1)}k` : value.toFixed(value >= 10 ? 1 : 2);
export const percentile = (values: number[], p: number) => {
  if (!values.length) {
    return null;
  }
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)];
};

/** Successful complete causal samples. Batch samples measure the batch's observed pathway. */
export function measuredPathways(
  state: FlowState,
  {
    startId,
    endId,
    now,
    seconds,
    environment,
  }: {
    startId?: string;
    endId?: string;
    now: number;
    seconds: number;
    environment?: string;
  }
) {
  return state.pathways.flatMap((pathway) => {
    if (
      pathway.status !== 'complete' ||
      pathway.error ||
      pathway.durationMs === null ||
      !Number.isFinite(pathway.durationMs) ||
      pathway.durationMs < 0 ||
      (endId && pathway.endNodeId !== endId) ||
      (environment && pathway.environment !== environment) ||
      pathway.completedAt < now - seconds * 1000 ||
      pathway.completedAt > now
    ) {
      return [];
    }
    if (!startId || pathway.startNodeId === startId) {
      return [pathway];
    }
    // A saved pipeline may start at a known producer inside a longer retained causal route.
    const boundary = pathway.steps.findIndex(
      (step) => step.nodeId === startId && ['publish', 'send'].includes(step.operation)
    );
    if (boundary < 0) {
      return [];
    }
    const origin = pathway.steps[boundary];
    const durationMs = pathway.completedAt - origin.startedAt;
    return durationMs < 0
      ? []
      : [
          {
            ...pathway,
            id: `${pathway.id}:${origin.observationId}`,
            originObservationId: origin.observationId,
            originService: origin.service,
            startNodeId: startId,
            startedAt: origin.startedAt,
            durationMs,
            steps: pathway.steps.slice(boundary),
          },
        ];
  });
}
export function matchedHops(state: FlowState, events: FlowEvent[]) {
  const publications = new Map(
    state.events.filter((e) => e.kind === 'publish' && !e.error && !e.isBatch).map((e) => [e.observationId, e])
  );
  return events
    .filter((e) => e.kind === 'receive' && !e.error && !e.isBatch)
    .flatMap((receive) => {
      const match = state.matches.get(receive.observationId);
      const producer = match?.producerObservationId && publications.get(match.producerObservationId);
      if (!producer) {
        return [];
      }
      const ms = receive.occurredAt - producer.occurredAt;
      return ms >= 0 ? [{ receive, producer, ms }] : [];
    });
}

// Dependency reachability highlights paths; it does not assert message lineage across services.
export function connected(id: string, edges: FlowEdge[]) {
  const ids = new Set([id]);
  for (const upstream of [true, false]) {
    const seen = new Set([id]),
      queue = [id];
    while (queue.length) {
      const current = queue.shift();
      for (const edge of edges) {
        const from = upstream ? edge.target : edge.source,
          to = upstream ? edge.source : edge.target;
        if (from === current && !seen.has(to)) {
          seen.add(to);
          ids.add(to);
          queue.push(to);
        }
      }
    }
  }
  return ids;
}
