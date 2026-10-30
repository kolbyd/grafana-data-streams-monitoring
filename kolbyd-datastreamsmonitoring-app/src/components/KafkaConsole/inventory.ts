import { FlowEdge, FlowEvent, FlowNode, FlowState } from '../../flow/model';

export type ClusterInventory = {
  id: string;
  name: string;
  environment: string;
  topicIds: string[];
  topicNames: string[];
  nodeIds: string[];
  groups: string[];
  events: FlowEvent[];
  lastSeen: number;
};
export type TopicInventory = {
  id: string;
  name: string;
  environment: string;
  cluster: string;
  nodeIds: string[];
  groups: string[];
  events: FlowEvent[];
  observedPartitions: number;
  lastSeen: number;
};
export type GroupInventory = {
  id: string;
  name: string;
  environment: string;
  cluster: string;
  topicNames: string[];
  nodeIds: string[];
  consumerNames: string[];
  events: FlowEvent[];
  lastSeen: number;
};

export function observedInventory(state: FlowState, events: FlowEvent[], environment = '', cluster = '') {
  const edges = [...state.edges.values()].filter(
    (edge) => (!environment || edge.environment === environment) && (!cluster || edge.cluster === cluster)
  );
  const scopeEvents = events.filter(
    (event) =>
      (!environment || event.environment === environment) &&
      (!cluster || event.cluster === cluster) &&
      (event.kind === 'publish' || event.kind === 'receive')
  );
  const nodes = [...state.nodes.values()].filter(
    (node) =>
      node.kind === 'topic' &&
      (!environment || node.environment === environment) &&
      (!cluster || node.cluster === cluster)
  );
  const unique = (values: string[]) => [...new Set(values)].sort();
  const lastSeen = (related: FlowEdge[], topics: FlowNode[] = []) =>
    Math.max(0, ...related.map((edge) => edge.lastSeen), ...topics.map((node) => node.lastSeen));
  const clusterKeys = new Map<string, { name: string; environment: string }>();
  edges.forEach((edge) =>
    clusterKeys.set(JSON.stringify([edge.environment, edge.cluster]), {
      name: edge.cluster,
      environment: edge.environment,
    })
  );
  nodes.forEach((node) =>
    clusterKeys.set(JSON.stringify([node.environment, node.cluster]), {
      name: node.cluster,
      environment: node.environment,
    })
  );
  const clusters: ClusterInventory[] = [...clusterKeys].map(([id, identity]) => {
    const related = edges.filter((edge) => edge.environment === identity.environment && edge.cluster === identity.name);
    const topics = nodes.filter((node) => node.environment === identity.environment && node.cluster === identity.name);
    return {
      id,
      ...identity,
      topicIds: topics.map((node) => node.id),
      topicNames: unique(topics.map((node) => node.name)),
      nodeIds: unique([...topics.map((node) => node.id), ...related.flatMap((edge) => [edge.source, edge.target])]),
      groups: unique(related.flatMap((edge) => (edge.consumerGroup ? [edge.consumerGroup] : []))),
      events: scopeEvents.filter(
        (event) => event.environment === identity.environment && event.cluster === identity.name
      ),
      lastSeen: lastSeen(related, topics),
    };
  });
  const topics: TopicInventory[] = nodes.map((node) => {
    const related = edges.filter((edge) => edge.source === node.id || edge.target === node.id);
    const activity = scopeEvents.filter((event) => event.topicNodeId === node.id);
    return {
      id: node.id,
      name: node.name,
      environment: node.environment,
      cluster: node.cluster,
      nodeIds: [node.id],
      groups: unique(related.flatMap((edge) => (edge.consumerGroup ? [edge.consumerGroup] : []))),
      events: activity,
      observedPartitions: new Set(activity.flatMap((event) => (event.partition === null ? [] : [event.partition])))
        .size,
      lastSeen: node.lastSeen,
    };
  });
  const groupKeys = new Map<string, { name: string; environment: string; cluster: string }>();
  edges
    .filter((edge) => edge.operation === 'receive' && edge.consumerGroup)
    .forEach((edge) => {
      groupKeys.set(JSON.stringify([edge.environment, edge.cluster, edge.consumerGroup]), {
        name: edge.consumerGroup!,
        environment: edge.environment,
        cluster: edge.cluster,
      });
    });
  const groups: GroupInventory[] = [...groupKeys].map(([id, identity]) => {
    const related = edges.filter(
      (edge) =>
        edge.operation === 'receive' &&
        edge.consumerGroup === identity.name &&
        edge.environment === identity.environment &&
        edge.cluster === identity.cluster
    );
    return {
      id,
      ...identity,
      topicNames: unique(related.map((edge) => edge.topic)),
      nodeIds: unique(related.flatMap((edge) => [edge.source, edge.target])),
      consumerNames: unique(
        related.flatMap((edge) => (state.nodes.get(edge.target)?.name ? [state.nodes.get(edge.target)!.name] : []))
      ),
      events: scopeEvents.filter(
        (event) =>
          event.kind === 'receive' &&
          event.consumerGroup === identity.name &&
          event.environment === identity.environment &&
          event.cluster === identity.cluster
      ),
      lastSeen: lastSeen(related),
    };
  });
  return {
    clusters,
    topics,
    groups,
    missingGroupRelationships: edges.filter((edge) => edge.operation === 'receive' && !edge.consumerGroup).length,
  };
}
