import React from 'react';
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { applyFrame, emptyState } from '../../flow/model';
import { exampleSource } from '../../flow/example';
import KafkaConsole from './KafkaConsole';
import type { TimeSeriesInput } from '../Native/NativeTimeSeries';

jest.mock('../Native/NativeTimeSeries', () => ({
  __esModule: true,
  default: ({ title, series }: { title: string; series: TimeSeriesInput[] }) => (
    <div role="group" aria-label={title} data-testid="native-chart-boundary" data-series-count={series.length} />
  ),
}));

function props() {
  const state = applyFrame(emptyState(), exampleSource().frame(true));
  return {
    state,
    events: state.events,
    now: Date.now(),
    seconds: 60,
    environment: '',
    onExplore: jest.fn(),
    onSetup: jest.fn(),
  };
}

describe('Kafka Console', () => {
  afterEach(async () => {
    await act(async () => {});
  });
  test('shows topic rows and graph navigation from observed inventory', async () => {
    const current = props();
    await act(async () => {
      render(<KafkaConsole {...current} />);
    });
    fireEvent.click(screen.getByRole('tab', { name: /Topics/ }));
    const table = screen.getByRole('table');
    expect(within(table).getByRole('columnheader', { name: 'Lag' })).toBeInTheDocument();
    const topic = [...current.state.nodes.values()].find((node) => node.name === 'orders.created')!;
    fireEvent.click(within(table).getByRole('button', { name: 'orders.created' }));
    expect(current.onExplore).toHaveBeenCalledWith([topic.id]);
    expect(within(table).getAllByLabelText(/Unavailable: Lag requires/)).toHaveLength(5);
  });
  test('filters inventory, toggles charts, and shows truthful missing broker and connector states', async () => {
    const current = props();
    await act(async () => {
      render(<KafkaConsole {...current} />);
    });
    fireEvent.click(screen.getByRole('tab', { name: /Topics/ }));
    fireEvent.change(screen.getByRole('textbox', { name: 'Filter Kafka inventory' }), {
      target: { value: 'orders.created' },
    });
    expect(screen.getByText('1 topics')).toBeInTheDocument();
    expect(
      await screen.findByRole('group', { name: 'Observed publish operations per second by topic' })
    ).toHaveAttribute('data-series-count', '1');
    fireEvent.click(screen.getByRole('button', { name: 'Charts' }));
    expect(
      screen.queryByRole('group', { name: 'Observed publish operations per second by topic' })
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: /Brokers/ }));
    expect(screen.getByRole('heading', { name: 'Broker monitoring is not connected' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /Brokers/ })).toHaveTextContent('—');
    const panel = screen.getByRole('tabpanel');
    fireEvent.click(within(panel).getByRole('button', { name: 'Set up monitoring' }));
    expect(current.onSetup).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('tab', { name: /Connectors/ }));
    expect(screen.getByRole('heading', { name: 'Connector monitoring is not connected' })).toBeInTheDocument();
    expect(screen.queryByText(/3 checks passing|unused topics|All healthy/)).not.toBeInTheDocument();
  });
  test('supports keyboard tab navigation and scoped environment filtering', async () => {
    const current = props();
    await act(async () => {
      render(<KafkaConsole {...current} />);
    });
    const clusters = screen.getByRole('tab', { name: /Clusters/ });
    clusters.focus();
    fireEvent.keyDown(clusters, { key: 'ArrowRight' });
    expect(screen.getByRole('tab', { name: /Topics/ })).toHaveFocus();
    expect(screen.getByRole('tab', { name: /Topics/ })).toHaveAttribute('aria-selected', 'true');
    const environment = screen.getByRole('combobox', { name: 'Kafka Console environment' });
    fireEvent.click(environment);
    fireEvent.change(environment, { target: { value: 'example' } });
    fireEvent.keyDown(environment, { key: 'ArrowDown' });
    fireEvent.keyDown(environment, { key: 'Enter' });
    fireEvent.blur(environment);
    expect(environment).toHaveValue('example');
    expect(screen.getByRole('table')).toHaveTextContent('orders.created');
  });
});
