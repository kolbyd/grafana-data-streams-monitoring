import { type FlowState } from './model';

/** Scope topology, including consumer-group edges, to observed activity in the selected range. */
export function activeTopology(state: FlowState, from: number, to: number, live: boolean): FlowState {
  const active = new Set<string>();
  const inRange = (at: number) => at >= from && at <= to;
  const byConnection = new Map<string, string[]>();
  const byOperation = new Map<string, string[]>();
  const add = (index: Map<string, string[]>, key: string, id: string) =>
    index.set(key, [...(index.get(key) ?? []), id]);
  for (const edge of state.edges.values()) {
    add(byConnection, JSON.stringify([edge.source, edge.target, edge.consumerGroup]), edge.id);
    add(
      byOperation,
      JSON.stringify([
        edge.operation === 'publish' ? edge.source : edge.target,
        edge.topic,
        edge.operation,
        edge.consumerGroup,
      ]),
      edge.id
    );
    // Live lastSeen records arrival activity even when the bounded event ring has rolled over.
    if (live && inRange(edge.lastSeen)) {
      active.add(edge.id);
    }
  }
  for (const event of state.events) {
    if (inRange(event.occurredAt) && ['publish', 'receive'].includes(event.kind)) {
      for (const id of byConnection.get(JSON.stringify([event.source, event.target, event.consumerGroup])) ?? []) {
        active.add(id);
      }
    }
  }
  for (const pathway of state.pathways) {
    for (const step of pathway.steps) {
      if (!step.nodeId || !inRange(step.completedAt)) {
        continue;
      }
      const operation = ['publish', 'send'].includes(step.operation)
        ? 'publish'
        : ['receive', 'process'].includes(step.operation)
          ? 'receive'
          : '';
      if (!operation) {
        continue;
      }
      for (const id of byOperation.get(
        JSON.stringify([step.nodeId, step.topic, operation, operation === 'publish' ? null : step.consumerGroup])
      ) ?? []) {
        active.add(id);
      }
    }
  }
  const edges = new Map([...state.edges].filter(([id]) => active.has(id)));
  const ids = new Set([...edges.values()].flatMap((edge) => [edge.source, edge.target]));
  return { ...state, edges, nodes: new Map([...state.nodes].filter(([id]) => ids.has(id))) };
}
