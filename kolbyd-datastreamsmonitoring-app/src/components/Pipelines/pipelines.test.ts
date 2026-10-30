import { FlowEdge, FlowNode } from '../../flow/model';
import { endpointOf, loadPipelines, PipelineDefinition, pipelineGraph, savePipelines } from './pipelines';

const node = (id: string, kind: 'service' | 'topic' = 'service', env = 'prod'): FlowNode => ({
  id,
  name: id,
  kind,
  environment: env,
  serviceNamespace: 'commerce',
  cluster: 'kafka-a',
  lastSeen: 100,
  published: 0,
  received: 0,
  errors: 0,
});
const edge = (source: string, target: string, env = 'prod'): FlowEdge => ({
  id: `${source}-${target}`,
  source,
  target,
  operation: 'publish',
  environment: env,
  cluster: 'kafka-a',
  topic: 'orders',
  consumerGroup: null,
  lastSeen: 100,
  observations: 0,
  errors: 0,
});
const definition = (nodes: FlowNode[]): PipelineDefinition => ({
  id: 'pipeline-one',
  name: 'Order processing',
  start: endpointOf(nodes[0]),
  end: endpointOf(nodes[nodes.length - 1]),
  environment: 'prod',
  createdAt: 1,
  updatedAt: 1,
});

describe('saved pipeline definition', () => {
  test('keeps all branches that reconnect to the endpoint and excludes unrelated branches', () => {
    const nodes = [node('start'), node('orders', 'topic'), node('a'), node('b'), node('unrelated'), node('end')];
    const edges = [
      edge('start', 'orders'),
      edge('orders', 'a'),
      edge('orders', 'b'),
      edge('a', 'end'),
      edge('b', 'end'),
      edge('orders', 'unrelated'),
    ];
    const graph = pipelineGraph(definition(nodes), nodes, edges);
    expect(graph.error).toBe('');
    expect(graph.nodes.map((n) => n.id)).toEqual(['start', 'orders', 'a', 'b', 'end']);
    expect(graph.edges).toHaveLength(5);
  });

  test('handles internal cycles without unbounded traversal or duplicated nodes', () => {
    const nodes = [node('start'), node('a'), node('b'), node('end')];
    const graph = pipelineGraph(definition(nodes), nodes, [
      edge('start', 'a'),
      edge('a', 'b'),
      edge('b', 'a'),
      edge('b', 'end'),
    ]);
    expect(graph.nodes).toHaveLength(4);
    expect(graph.edges).toHaveLength(4);
  });

  test('rejects undirected connectivity when no directed pipeline path exists', () => {
    const nodes = [node('start'), node('queue', 'topic'), node('end')];
    expect(pipelineGraph(definition(nodes), nodes, [edge('start', 'queue'), edge('end', 'queue')]).error).toMatch(
      /No directed/
    );
  });

  test('does not leak another environment into the pipeline', () => {
    const nodes = [node('start'), node('end')];
    expect(pipelineGraph(definition(nodes), nodes, [edge('start', 'end', 'staging')]).error).toMatch(/No directed/);
  });

  test('recovers an endpoint identity only when its full fallback identity is unambiguous', () => {
    const original = [node('start'), node('end')],
      saved = definition(original);
    const renamed = { ...original[0], id: 'new-start' };
    const graph = pipelineGraph(saved, [renamed, original[1]], [edge('new-start', 'end')]);
    expect(graph.start?.id).toBe('new-start');
    const ambiguous = pipelineGraph(saved, [renamed, { ...renamed, id: 'another-start' }, original[1]], []);
    expect(ambiguous.error).toMatch(/not currently observed/);
  });

  test('round-trips personal definitions without changing endpoint identities', () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) || null,
      setItem: (key: string, value: string) => {
        values.set(key, value);
      },
    };
    const saved = definition([node('start'), node('end')]);
    expect(savePipelines(storage, 'personal', [saved])).toBe('');
    expect(loadPipelines(storage, 'personal')).toEqual({ pipelines: [saved], error: '' });
  });

  test('invalid or blocked browser storage is reported rather than throwing', () => {
    expect(loadPipelines({ getItem: () => '{broken' }, 'key').error).toBeTruthy();
    const saved = definition([node('start'), node('end')]);
    expect(
      savePipelines(
        {
          setItem: () => {
            throw new Error('quota');
          },
        },
        'key',
        [saved]
      )
    ).toMatch(/could not be saved/);
    expect(
      loadPipelines({ getItem: () => JSON.stringify({ version: 1, pipelines: [{ ...saved, name: '' }] }) }, 'key')
        .pipelines
    ).toEqual([]);
  });
});
