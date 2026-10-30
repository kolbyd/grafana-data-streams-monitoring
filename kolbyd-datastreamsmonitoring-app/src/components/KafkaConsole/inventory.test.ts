import { applyFrame, emptyState } from '../../flow/model';
import { exampleSource } from '../../flow/example';
import { observedInventory } from './inventory';

describe('Kafka client inventory', () => {
  test('builds known clusters, topics and groups without treating fanout as extra publishes', () => {
    const state = applyFrame(emptyState(), exampleSource().frame(true));
    const inventory = observedInventory(state, state.events);
    expect(inventory.clusters).toHaveLength(1);
    expect(inventory.topics).toHaveLength(5);
    expect(inventory.groups.length).toBeGreaterThan(0);
    expect(inventory.clusters[0].events.filter((event) => event.kind === 'publish')).toHaveLength(
      state.events.filter((event) => event.kind === 'publish').length
    );
    expect(inventory.groups.every((group) => group.events.every((event) => event.kind === 'receive'))).toBe(true);
    expect(inventory.topics.find((topic) => topic.name === 'orders.created')!.observedPartitions).toBeGreaterThan(0);
  });
  test('keeps equal cluster and group names in different environments separate', () => {
    const frame = exampleSource().frame(true);
    const changed = {
      ...frame,
      cursor: frame.cursor + 1,
      nodes: frame.nodes.map((node) => ({ ...node, id: `prod:${node.id}`, environment: 'prod' })),
      edges: frame.edges.map((edge) => ({
        ...edge,
        id: `prod:${edge.id}`,
        source: `prod:${edge.source}`,
        target: `prod:${edge.target}`,
        environment: 'prod',
      })),
      events: [],
    };
    const state = applyFrame(applyFrame(emptyState(), frame), {
      ...changed,
      replaceTopology: false,
      resetEvents: false,
    });
    expect(observedInventory(state, state.events).clusters).toHaveLength(2);
    const prod = observedInventory(state, state.events, 'prod');
    expect(prod.clusters).toHaveLength(1);
    expect(prod.topics.every((topic) => topic.environment === 'prod')).toBe(true);
    expect(prod.groups.every((group) => group.environment === 'prod')).toBe(true);
  });
  test('does not invent a consumer group when the client span lacks its identity', () => {
    const frame = exampleSource().frame(true);
    const state = applyFrame(emptyState(), {
      ...frame,
      edges: frame.edges.map((edge) => ({ ...edge, consumerGroup: null })),
    });
    const inventory = observedInventory(state, state.events);
    expect(inventory.groups).toHaveLength(0);
    expect(inventory.missingGroupRelationships).toBe(frame.edges.filter((edge) => edge.operation === 'receive').length);
  });
});
