import React from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { deserialize, serialize } from 'node:v8';
import { emptyState, FlowEdge, FlowNode } from '../../flow/model';
import PipelinesPage, { PipelinesPageProps } from './PipelinesPage';
import type { TimeSeriesSample } from '../Native/NativeTimeSeries';

jest.mock('../Native/NativeTimeSeries', () => ({
  __esModule: true,
  default: ({ title, samples, unit }: { title: string; samples?: TimeSeriesSample[]; unit?: string }) => (
    <div
      role="group"
      aria-label={title}
      data-testid="native-chart-boundary"
      data-samples={samples?.length ?? 0}
      data-unit={unit}
    />
  ),
}));

function props(): PipelinesPageProps {
  const state = emptyState();
  const nodes: FlowNode[] = [
    { id: 'producer', name: 'orders-api', kind: 'service' },
    { id: 'topic', name: 'orders.created', kind: 'topic' },
    { id: 'consumer', name: 'fulfilment', kind: 'service' },
  ].map((node) => ({
    ...node,
    kind: node.kind as FlowNode['kind'],
    environment: 'prod',
    cluster: 'kafka-a',
    serviceNamespace: 'shop',
    lastSeen: 1000,
    published: 0,
    received: 0,
    errors: 0,
  }));
  const edges: FlowEdge[] = [
    { id: 'send', source: 'producer', target: 'topic', operation: 'publish', consumerGroup: null },
    { id: 'receive', source: 'topic', target: 'consumer', operation: 'receive', consumerGroup: 'fulfilment-v1' },
  ].map((edge) => ({
    ...edge,
    topic: 'orders.created',
    environment: 'prod',
    cluster: 'kafka-a',
    lastSeen: 1000,
    observations: 0,
    errors: 0,
  }));
  state.nodes = new Map(nodes.map((node) => [node.id, node]));
  state.edges = new Map(edges.map((edge) => [edge.id, edge]));
  return {
    state,
    events: [],
    now: 1000,
    seconds: 60,
    environment: 'prod',
    example: false,
    storageKey: 'test-user',
    onExplore: jest.fn(),
  };
}

describe('pipeline catalog interactions', () => {
  const originalClone = globalThis.structuredClone;
  beforeAll(() => {
    globalThis.structuredClone = <T,>(value: T): T => deserialize(serialize(value)) as T;
  });
  afterAll(() => {
    globalThis.structuredClone = originalClone;
  });
  beforeEach(() => {
    localStorage.clear();
  });

  test('a preselected draft is not saved until named and submitted, then supports edit and removal', async () => {
    const input = props();
    render(<PipelinesPage {...input} initialDraft={{ startId: 'producer', endId: 'consumer' }} />);
    expect(screen.getByLabelText('Pipeline start service')).toHaveValue('orders-api · shop');
    expect(screen.getByLabelText('Pipeline end service')).toHaveValue('fulfilment · shop');
    expect(localStorage.getItem('test-user:pipelines:live:v1')).toBeNull();
    fireEvent.change(screen.getByLabelText('Pipeline name'), { target: { value: 'Order flow' } });
    await act(async () => {
      fireEvent.submit(screen.getByRole('form', { name: 'Create pipeline definition' }));
    });
    expect(screen.getByRole('group', { name: 'Throughput start' })).toHaveAttribute('data-samples', '24');
    expect(screen.getByRole('group', { name: 'Throughput end' })).toHaveAttribute('data-unit', 'ops');
    expect(screen.getByRole('heading', { name: 'Order flow' })).toBeInTheDocument();
    const saved = JSON.parse(localStorage.getItem('test-user:pipelines:live:v1')!);
    expect(saved.pipelines).toHaveLength(1);
    fireEvent.click(screen.getByRole('button', { name: 'View in Map' }));
    expect(input.onExplore).toHaveBeenCalledWith(['producer', 'topic', 'consumer']);
    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    fireEvent.change(screen.getByLabelText('Pipeline name'), { target: { value: 'Fulfilment flow' } });
    fireEvent.submit(screen.getByRole('form', { name: 'Edit pipeline definition' }));
    expect(screen.getByRole('heading', { name: 'Fulfilment flow' })).toBeInTheDocument();
    expect(JSON.parse(localStorage.getItem('test-user:pipelines:live:v1')!).pipelines[0].id).toBe(
      saved.pipelines[0].id
    );
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }));
    fireEvent.click(screen.getByRole('button', { name: 'Remove pipeline' }));
    expect(screen.getByRole('heading', { name: 'Save the pipelines you follow' })).toBeInTheDocument();
    expect(JSON.parse(localStorage.getItem('test-user:pipelines:live:v1')!).pipelines).toHaveLength(0);
  });

  test('an invalid reversed path stays an unsaved draft with a clear error', () => {
    render(<PipelinesPage {...props()} initialDraft={{ startId: 'consumer', endId: 'producer' }} />);
    fireEvent.change(screen.getByLabelText('Pipeline name'), { target: { value: 'Reverse flow' } });
    fireEvent.submit(screen.getByRole('form', { name: 'Create pipeline definition' }));
    expect(screen.getByRole('alert')).toHaveTextContent('No directed producer-to-consumer path');
    expect(localStorage.getItem('test-user:pipelines:live:v1')).toBeNull();
  });
});
