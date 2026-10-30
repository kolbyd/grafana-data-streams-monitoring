import React, { useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Collapse,
  Field,
  Icon,
  Input,
  InteractiveTable,
  LinkButton,
  Tab,
  TabsBar,
  type Column,
} from '@grafana/ui';
import { FlowEvent, FlowNode, formatRate, nodeEvents, rate } from '../../flow/model';
import { PLUGIN_ID } from '../../constants';
import { NativeSelect } from '../Native/NativeSelect';

const docs = 'https://opentelemetry.io/docs/zero-code/dotnet/';
const agentSettings = `OTEL_SERVICE_NAME=your-service
OTEL_RESOURCE_ATTRIBUTES=service.namespace=commerce,deployment.environment.name=dev,kafka.cluster.id=your-cluster
OTEL_DOTNET_AUTO_TRACES_KAFKA_INSTRUMENTATION_ENABLED=true
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
OTEL_BSP_SCHEDULE_DELAY=250`;
interface ServiceRow {
  id: string;
  name: string;
  messages: number;
  technology: string;
  environment: string;
}
const initialSort = [{ id: 'name', desc: false }];

export function Setup({
  nodes,
  events,
  seconds,
  onExplore,
  onPreviewExample,
}: {
  nodes: FlowNode[];
  events: FlowEvent[];
  seconds: number;
  onExplore: (ids: string[]) => void;
  onPreviewExample?: () => void;
}) {
  const [query, setQuery] = useState('');
  const [environment, setEnvironment] = useState('');
  const [copied, setCopied] = useState('');
  const [guideOpen, setGuideOpen] = useState(false);
  const services = nodes.filter((node) => node.kind === 'service');
  const environments = [...new Set(services.map((node) => node.environment))].sort();
  const visible = useMemo(
    () =>
      nodes
        .filter(
          (node) =>
            node.kind === 'service' &&
            (!environment || node.environment === environment) &&
            node.name.toLowerCase().includes(query.toLowerCase())
        )
        .map((node): ServiceRow => ({
          id: node.id,
          name: node.name,
          environment: node.environment,
          technology: 'Kafka',
          messages: rate(
            nodeEvents(node, events).filter((event) => !event.error && !event.isBatch),
            seconds
          ),
        })),
    [nodes, environment, query, events, seconds]
  );
  const columns = useMemo<Array<Column<ServiceRow>>>(
    () => [
      {
        id: 'name',
        header: 'Service',
        sortType: 'string',
        minWidth: 200,
        cell: ({ row }) => (
          <Button variant="secondary" fill="text" size="sm" onClick={() => onExplore([row.original.id])}>
            {row.original.name}
          </Button>
        ),
      },
      {
        id: 'messages',
        header: 'Observed ops/s',
        sortType: 'number',
        cell: ({ row }) => `${formatRate(row.original.messages)} ops/s`,
      },
      { id: 'technology', header: 'Queue technology', sortType: 'string' },
      { id: 'environment', header: 'Environment', sortType: 'string' },
      {
        id: 'docs',
        header: '',
        disableGrow: true,
        cell: () => (
          <LinkButton
            href={docs}
            target="_blank"
            rel="noreferrer"
            variant="secondary"
            fill="text"
            size="sm"
            icon="external-link-alt"
          >
            View Docs
          </LinkButton>
        ),
      },
    ],
    [onExplore]
  );
  async function copy() {
    try {
      await navigator.clipboard.writeText(agentSettings);
      setCopied('Copied');
    } catch {
      setCopied('Copy unavailable. Select the configuration below.');
      setGuideOpen(true);
    }
  }
  return (
    <section className="dsm-setup dsm-instrumentation-setup">
      <div className="dsm-setup-heading">
        <div>
          <h2>Setup</h2>
          <p>Configure collection for your Kafka producers and consumers.</p>
        </div>
        <span>{services.length} services discovered</span>
      </div>
      <div className="dsm-enable-collection">
        <Alert
          title="Enable Data Streams Monitoring"
          severity="info"
          action={
            <Button variant="secondary" size="sm" icon="copy" onClick={copy}>
              Copy setup instructions
            </Button>
          }
        >
          <p>Attach the OpenTelemetry .NET agent, enable Kafka instrumentation, and send spans to the flow service.</p>
          <LinkButton
            href={docs}
            target="_blank"
            rel="noreferrer"
            variant="secondary"
            fill="text"
            size="sm"
            icon="external-link-alt"
          >
            Docs
          </LinkButton>
          {copied && <p role="status">{copied}</p>}
        </Alert>
      </div>
      <div className="dsm-setup-tabs">
        <TabsBar>
          <Tab label="Instrumented" counter={services.length} active />
        </TabsBar>
        <LinkButton
          href={`/plugins/${PLUGIN_ID}?page=configuration`}
          variant="secondary"
          fill="text"
          size="sm"
          icon="cog"
        >
          Configure connection
        </LinkButton>
      </div>
      <div className="dsm-setup-filters">
        <Field label="Service" noMargin>
          <Input
            aria-label="Filter instrumented services"
            value={query}
            prefix={<Icon name="search" />}
            onChange={(event) => setQuery(event.currentTarget.value)}
            placeholder="All services"
          />
        </Field>
        <Field label="Queue technology" noMargin>
          <NativeSelect
            aria-label="Queue technology"
            value="kafka"
            onChange={() => {}}
            options={[{ label: 'Kafka', value: 'kafka' }]}
          />
        </Field>
        <Field label="Environment" noMargin>
          <NativeSelect
            aria-label="Setup environment"
            value={environment}
            onChange={setEnvironment}
            options={[
              { label: 'All environments', value: '' },
              ...environments.map((value) => ({ label: value, value })),
            ]}
          />
        </Field>
      </div>
      <h3 className="dsm-setup-count">Showing {visible.length} instrumented services</h3>
      {visible.length ? (
        <div className="dsm-table-scroll">
          <InteractiveTable
            columns={columns}
            data={visible}
            getRowId={(row) => row.id}
            initialSortBy={initialSort}
            pageSize={20}
          />
        </div>
      ) : (
        <p className="dsm-table-empty">No services are sending Kafka telemetry for these filters.</p>
      )}
      {onPreviewExample && (
        <div className="dsm-setup-preview">
          <Button variant="secondary" onClick={onPreviewExample}>
            Preview example topology
          </Button>
        </div>
      )}
      <div className="dsm-setup-guide">
        <Collapse label="Collection configuration" isOpen={guideOpen} onToggle={setGuideOpen}>
          <pre>{agentSettings}</pre>
          <p>
            These settings supplement the agent’s profiler and startup-hook setup. Give each service a unique name and
            use a consistent environment and cluster identity. Keep the service API key in the OTLP exporter’s secret
            configuration.
          </p>
          <h3>Additional measurements</h3>
          <dl className="dsm-coverage">
            <dt>Consumer lag</dt>
            <dd>Needs Kafka end offsets and committed group offsets.</dd>
            <dt>Processing and pathway timing</dt>
            <dd>Standard send and process spans preserve handler context through parent relationships and links.</dd>
            <dt>Retries, DLQs and outboxes</dt>
            <dd>Optional .NET extensions add properties and preserve context across durable handoffs.</dd>
            <dt>Retention and sampling</dt>
            <dd>The explorer uses bounded retained observations. Historical metrics need a metrics backend.</dd>
          </dl>
        </Collapse>
      </div>
    </section>
  );
}
