import { activeTopology } from './activity';
import { applyFrame, emptyState } from './model';
import { exampleSource } from './example';

test('live range removes inactive consumer-group edges and their orphaned services/topics', () => {
  const state = applyFrame(emptyState(), exampleSource().frame(true));
  const selected = [...state.edges.values()][0];
  state.edges = new Map(
    [...state.edges].map(([id, edge]) => [id, { ...edge, lastSeen: id === selected.id ? 900 : 100 }])
  );
  state.events = [];
  state.pathways = [];
  const scoped = activeTopology(state, 500, 1000, true);
  expect([...scoped.edges.keys()]).toEqual([selected.id]);
  expect([...scoped.nodes.keys()].sort()).toEqual([selected.source, selected.target].sort());
  expect(activeTopology(state, 950, 1000, true).nodes.size).toBe(0);
});

test('absolute range uses historical observations even when newer traffic updated lastSeen', () => {
  const state = applyFrame(emptyState(), exampleSource().frame(true));
  const event = state.events.find((event) => event.kind === 'receive')!;
  const now = event.occurredAt;
  state.events = [event];
  state.pathways = [];
  state.edges = new Map([...state.edges].map(([id, edge]) => [id, { ...edge, lastSeen: now + 10000 }]));
  const scoped = activeTopology(state, now - 10, now + 10, false);
  expect(scoped.edges.size).toBeGreaterThan(0);
  expect(
    [...scoped.edges.values()].every(
      (edge) =>
        edge.source === event.source && edge.target === event.target && edge.consumerGroup === event.consumerGroup
    )
  ).toBe(true);
  expect(activeTopology(state, now - 100, now - 50, false).edges.size).toBe(0);
});
