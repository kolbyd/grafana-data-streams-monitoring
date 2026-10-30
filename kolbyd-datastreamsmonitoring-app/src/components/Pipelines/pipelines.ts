import { FlowEdge, FlowNode } from '../../flow/model';

export interface PipelineEndpoint {
  id: string;
  name: string;
  namespace: string;
  environment: string;
}

export interface PipelineDefinition {
  id: string;
  name: string;
  start: PipelineEndpoint;
  end: PipelineEndpoint;
  environment: string;
  createdAt: number;
  updatedAt: number;
}

export interface PipelineGraph {
  nodes: FlowNode[];
  edges: FlowEdge[];
  start?: FlowNode;
  end?: FlowNode;
  error: string;
}

interface StorageReader {
  getItem(key: string): string | null;
}
interface StorageWriter {
  setItem(key: string, value: string): void;
}

export const MAX_PIPELINES = 50;
export const endpointOf = (node: FlowNode): PipelineEndpoint => ({
  id: node.id,
  name: node.name,
  namespace: node.serviceNamespace,
  environment: node.environment,
});

function resolveEndpoint(endpoint: PipelineEndpoint, nodes: FlowNode[], environment: string): FlowNode | undefined {
  const candidates = nodes.filter(
    (node) => node.kind === 'service' && (!environment || node.environment === environment)
  );
  const exact = candidates.find((node) => node.id === endpoint.id);
  if (exact) {
    return exact;
  }
  // An identity change must never silently bind to an unrelated service with the same display name.
  const matches = candidates.filter(
    (node) =>
      node.name === endpoint.name &&
      node.serviceNamespace === endpoint.namespace &&
      node.environment === endpoint.environment
  );
  return matches.length === 1 ? matches[0] : undefined;
}

function reachable(start: string, adjacency: Map<string, string[]>, stopAt: string): Set<string> {
  const visited = new Set([start]),
    queue = [start];
  for (let index = 0; index < queue.length; index++) {
    const id = queue[index];
    if (id === stopAt) {
      continue;
    }
    for (const target of adjacency.get(id) || []) {
      if (!visited.has(target)) {
        visited.add(target);
        queue.push(target);
      }
    }
  }
  return visited;
}

// Includes the dependency corridor between endpoints, preserving internal Kafka cycles once.
// This is graph connectivity, not proof of causal message lineage through a service.
export function pipelineGraph(
  definition: PipelineDefinition,
  allNodes: FlowNode[],
  allEdges: FlowEdge[]
): PipelineGraph {
  const start = resolveEndpoint(definition.start, allNodes, definition.environment);
  const end = resolveEndpoint(definition.end, allNodes, definition.environment);
  const empty = { nodes: [], edges: [], start, end };
  if (!start || !end) {
    return { ...empty, error: 'A saved endpoint is not currently observed in this environment.' };
  }
  if (start.id === end.id) {
    return { ...empty, error: 'Choose different start and end services.' };
  }
  const nodes = allNodes.filter((node) => !definition.environment || node.environment === definition.environment);
  const known = new Set(nodes.map((node) => node.id));
  const edges = allEdges.filter(
    (edge) =>
      known.has(edge.source) &&
      known.has(edge.target) &&
      (!definition.environment || edge.environment === definition.environment)
  );
  const forward = new Map<string, string[]>(),
    reverse = new Map<string, string[]>();
  for (const edge of edges) {
    const downstream = forward.get(edge.source) || [],
      upstream = reverse.get(edge.target) || [];
    downstream.push(edge.target);
    upstream.push(edge.source);
    forward.set(edge.source, downstream);
    reverse.set(edge.target, upstream);
  }
  const fromStart = reachable(start.id, forward, end.id);
  if (!fromStart.has(end.id)) {
    return { ...empty, error: 'No directed producer-to-consumer path connects these endpoints.' };
  }
  const toEnd = reachable(end.id, reverse, start.id);
  const corridor = new Set([...fromStart].filter((id) => toEnd.has(id)));
  return {
    nodes: nodes.filter((node) => corridor.has(node.id)),
    edges: edges.filter(
      (edge) =>
        corridor.has(edge.source) && corridor.has(edge.target) && edge.source !== end.id && edge.target !== start.id
    ),
    start,
    end,
    error: '',
  };
}

const record = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === 'object' && !Array.isArray(value);
const text = (value: unknown, max = 512): value is string => typeof value === 'string' && value.length <= max;
function validEndpoint(value: unknown): value is PipelineEndpoint {
  return (
    record(value) &&
    text(value.id, 4096) &&
    !!value.id &&
    text(value.name, 256) &&
    !!value.name &&
    text(value.namespace, 256) &&
    text(value.environment, 256)
  );
}
export function validDefinition(value: unknown): value is PipelineDefinition {
  return (
    record(value) &&
    text(value.id, 128) &&
    !!value.id &&
    text(value.name, 120) &&
    !!value.name.trim() &&
    validEndpoint(value.start) &&
    validEndpoint(value.end) &&
    text(value.environment, 256) &&
    typeof value.createdAt === 'number' &&
    Number.isFinite(value.createdAt) &&
    value.createdAt > 0 &&
    typeof value.updatedAt === 'number' &&
    Number.isFinite(value.updatedAt) &&
    value.updatedAt > 0
  );
}

export function loadPipelines(storage: StorageReader, key: string): { pipelines: PipelineDefinition[]; error: string } {
  try {
    const raw = storage.getItem(key);
    if (!raw) {
      return { pipelines: [], error: '' };
    }
    if (raw.length > 1500000) {
      return { pipelines: [], error: 'Saved pipeline data is too large to read.' };
    }
    const stored: unknown = JSON.parse(raw);
    if (!record(stored) || stored.version !== 1 || !Array.isArray(stored.pipelines)) {
      return { pipelines: [], error: 'The saved pipeline format could not be read.' };
    }
    const valid = stored.pipelines.filter(validDefinition).slice(0, MAX_PIPELINES);
    const unique = new Map(valid.map((pipeline) => [pipeline.id, pipeline]));
    return {
      pipelines: [...unique.values()],
      error: valid.length !== stored.pipelines.length ? 'Some invalid saved pipeline definitions were skipped.' : '',
    };
  } catch {
    return { pipelines: [], error: 'Browser storage is unavailable or saved pipeline data could not be read.' };
  }
}

export function savePipelines(storage: StorageWriter, key: string, pipelines: PipelineDefinition[]): string {
  if (pipelines.length > MAX_PIPELINES) {
    return `Save up to ${MAX_PIPELINES} pipeline definitions in this browser.`;
  }
  if (pipelines.some((pipeline) => !validDefinition(pipeline))) {
    return 'A pipeline definition is invalid and could not be saved.';
  }
  try {
    storage.setItem(key, JSON.stringify({ version: 1, pipelines }));
    return '';
  } catch {
    return 'Pipeline changes could not be saved. Browser storage may be disabled or full.';
  }
}
