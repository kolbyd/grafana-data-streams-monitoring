import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MetricFindValue,
  ScopedVars,
} from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { lastValueFrom } from 'rxjs';
import { FlowFrame } from '../flow/model';
import { buildFrames } from './frames';
import { defaultQuery, FlowDataSourceOptions, FlowQuery } from './types';

const APP_ID = 'eageraspen2909-datastreamsmonitoring-app';
const RESOURCES = `/api/plugins/${APP_ID}/resources`;

export class DataSource extends DataSourceApi<FlowQuery, FlowDataSourceOptions> {
  constructor(settings: DataSourceInstanceSettings<FlowDataSourceOptions>) {
    super(settings);
  }

  getDefaultQuery(): Partial<FlowQuery> {
    return defaultQuery;
  }

  applyTemplateVariables(query: FlowQuery, scopedVars: ScopedVars): FlowQuery {
    const replace = (value?: string) => (value ? getTemplateSrv().replace(value, scopedVars, 'raw') : '');
    return {
      ...query,
      environment: replace(query.environment),
      cluster: replace(query.cluster),
      service: replace(query.service),
      topic: replace(query.topic),
      consumerGroup: replace(query.consumerGroup),
    };
  }

  async query(request: DataQueryRequest<FlowQuery>): Promise<DataQueryResponse> {
    const targets = request.targets.filter((target) => !target.hide);
    if (!targets.length) {
      return { data: [] };
    }
    const response = await lastValueFrom(
      getBackendSrv().fetch<FlowFrame>({ url: `${RESOURCES}/topology`, method: 'GET' })
    );
    const snapshot = response.data;
    if (
      !snapshot ||
      !Array.isArray(snapshot.nodes) ||
      !Array.isArray(snapshot.edges) ||
      !Array.isArray(snapshot.events)
    ) {
      throw new Error('The flow service returned an invalid topology snapshot. Check the app connection settings.');
    }
    return {
      data: targets.flatMap((target) => buildFrames(snapshot, this.applyTemplateVariables(target, request.scopedVars))),
    };
  }

  async testDatasource() {
    try {
      await lastValueFrom(getBackendSrv().fetch({ url: `${RESOURCES}/status`, method: 'GET' }));
      return { status: 'success', message: 'Connected through the Data Streams Monitoring app backend.' };
    } catch {
      return {
        status: 'error',
        message: 'The flow service could not be reached. Configure its URL and API key in the app settings.',
      };
    }
  }

  async metricFindQuery(query: string): Promise<MetricFindValue[]> {
    const response = await lastValueFrom(
      getBackendSrv().fetch<FlowFrame>({ url: `${RESOURCES}/topology`, method: 'GET' })
    );
    const snapshot = response.data;
    const values =
      query === 'environments'
        ? snapshot.nodes.map((node) => node.environment)
        : query === 'clusters'
          ? snapshot.edges.map((edge) => edge.cluster)
          : query === 'services'
            ? snapshot.nodes.filter((node) => node.kind === 'service').map((node) => node.name)
            : query === 'topics'
              ? snapshot.nodes.filter((node) => node.kind === 'topic').map((node) => node.name)
              : query === 'consumerGroups'
                ? snapshot.edges.map((edge) => edge.consumerGroup || '')
                : [];
    return [...new Set(values.filter(Boolean))].sort().map((text) => ({ text, value: text }));
  }
}
