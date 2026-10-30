import { FlowEdge, FlowFrame, FlowNode } from '../flow/model';
import { buildFrames, matchesFilter } from './frames';

const node = (id: string, kind: 'service' | 'topic', environment = 'test'): FlowNode => ({
  id,
  kind,
  name: id,
  environment,
  cluster: kind === 'topic' ? 'cluster-a' : '',
  serviceNamespace: 'shop',
  lastSeen: 1000,
  published: 999,
  received: 999,
  errors: 999,
});
const edge = (
  id: string,
  source: string,
  target: string,
  operation: string,
  consumerGroup: string | null,
  observations: number
): FlowEdge => ({
  id,
  source,
  target,
  operation,
  consumerGroup,
  observations,
  errors: 0,
  environment: 'test',
  cluster: 'cluster-a',
  topic: 'orders',
  lastSeen: 1000,
});
const snapshot = (): FlowFrame => ({
  type: 'snapshot',
  instanceId: 'instance-a',
  cursor: 1,
  serverTime: 1000,
  replaceTopology: true,
  resetEvents: true,
  visualEventsSkipped: 0,
  stats: {},
  events: [],
  nodes: [node('producer', 'service'), node('orders', 'topic'), node('consumer', 'service')],
  edges: [
    edge('send', 'producer', 'orders', 'publish', null, 10),
    edge('group-a', 'orders', 'consumer', 'receive', 'a', 9),
    edge('group-b', 'orders', 'consumer', 'receive', 'b', 4),
  ],
});
const cell = (frames: ReturnType<typeof buildFrames>, frame: number, field: string, row = 0) =>
  frames[frame].fields.find((value) => value.name === field)!.values[row];

describe('data-source snapshot data frames', () => {
  test('preserves separate consumer groups for fan-out to the same service', () => {
    const frames = buildFrames(snapshot(), { refId: 'A', view: 'consumerGroups' });
    expect(frames[0].length).toBe(2);
    expect(frames[0].fields.find((field) => field.name === 'Consumer group')!.values).toEqual(['a', 'b']);
    expect(cell(frames, 0, 'Consumer lag')).toBe('Not collected');
  });

  test('filtering a consumer group never leaks the other groups into totals', () => {
    const frames = buildFrames(snapshot(), { refId: 'A', view: 'summary', consumerGroup: 'a' });
    expect(cell(frames, 0, 'Receive observations')).toBe(9);
    expect(cell(frames, 0, 'Publish observations')).toBe(0);
    expect(cell(frames, 0, 'Consumer groups')).toBe(1);
  });

  test('counts edges once and does not double-count aggregate node counters', () => {
    const frames = buildFrames(snapshot(), { refId: 'A', view: 'summary' });
    expect(cell(frames, 0, 'Publish observations')).toBe(10);
    expect(cell(frames, 0, 'Receive observations')).toBe(13);
    expect(cell(frames, 0, 'Operation errors')).toBe(0);
  });

  test('consumer summary totals exclude producer edges even when all groups are selected', () => {
    const frames = buildFrames(snapshot(), { refId: 'A', view: 'summary', operation: 'receive', consumerGroup: '*' });
    expect(cell(frames, 0, 'Relationships')).toBe(2);
    expect(cell(frames, 0, 'Publish observations')).toBe(0);
    expect(cell(frames, 0, 'Receive observations')).toBe(13);
  });

  test('node graph preserves stable distinct edge IDs and uses only connected endpoints', () => {
    const frames = buildFrames(snapshot(), { refId: 'A', view: 'topology', consumerGroup: 'a' });
    expect(frames.map((frame) => frame.name)).toEqual(['nodes', 'edges']);
    expect(frames.every((frame) => frame.meta?.preferredVisualisationType === 'nodeGraph')).toBe(true);
    expect(frames[0].fields.find((field) => field.name === 'id')!.values).toEqual(['orders', 'consumer']);
    expect(frames[1].fields.find((field) => field.name === 'id')!.values).toEqual(['group-a']);
  });

  test('empty topology has finite zero-safe arc proportions and no invented nodes', () => {
    const input = snapshot();
    input.edges = [];
    input.nodes = [node('idle', 'service')];
    const frames = buildFrames(input, { refId: 'A', view: 'topology' });
    expect(cell(frames, 0, 'arc__publish')).toBe(0);
    expect(cell(frames, 0, 'arc__receive')).toBe(0);
    expect(frames[1].length).toBe(0);
  });

  test('filters are literal rather than untrusted regular expressions', () => {
    expect(matchesFilter('production', 'prod.*')).toBe(false);
    expect(matchesFilter('production', '*')).toBe(true);
    const frames = buildFrames(snapshot(), { refId: 'A', view: 'summary', environment: 'prod.*' });
    expect(cell(frames, 0, 'Services')).toBe(0);
  });
});
