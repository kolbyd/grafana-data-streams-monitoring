import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export type FlowQueryView = 'summary' | 'services' | 'topics' | 'consumerGroups' | 'topology' | 'observations';

export interface FlowQuery extends DataQuery {
  view: FlowQueryView;
  environment?: string;
  cluster?: string;
  service?: string;
  topic?: string;
  consumerGroup?: string;
  operation?: 'publish' | 'receive';
}

export type FlowDataSourceOptions = DataSourceJsonData;

export const defaultQuery: Partial<FlowQuery> = { view: 'summary' };

export const queryViews = [
  { label: 'Session summary', value: 'summary', description: 'Current session observation totals and topology counts' },
  { label: 'Services', value: 'services', description: 'Observed service publication and receive counts' },
  { label: 'Kafka topics', value: 'topics', description: 'Observed topic relationships and counts' },
  {
    label: 'Consumer groups',
    value: 'consumerGroups',
    description: 'Distinct consumer-group relationships, not broker lag',
  },
  { label: 'Topology', value: 'topology', description: 'Nodes and edges for the Grafana Node graph panel' },
  {
    label: 'Recent observations',
    value: 'observations',
    description: 'Bounded recent observations retained by the flow service',
  },
];
