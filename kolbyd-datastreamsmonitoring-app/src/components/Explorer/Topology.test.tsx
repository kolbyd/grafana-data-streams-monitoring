import React from 'react';
import { deserialize, serialize } from 'node:v8';
import { fireEvent, render, screen } from '@testing-library/react';
import { FlowEdge, FlowEvent, FlowNode } from '../../flow/model';
import { Topology } from './Topology';

const node = (id: string, kind: FlowNode['kind']): FlowNode => ({
  id,
  name: id,
  kind,
  environment: 'test',
  cluster: 'kafka',
  serviceNamespace: '',
  lastSeen: 10000,
  published: 0,
  received: 0,
  errors: 0,
});
const nodes = [node('producer', 'service'), node('orders', 'topic')];
const edge: FlowEdge = {
  id: 'publish',
  source: 'producer',
  target: 'orders',
  consumerGroup: null,
  operation: 'publish',
  environment: 'test',
  cluster: 'kafka',
  topic: 'orders',
  lastSeen: 10000,
  observations: 0,
  errors: 0,
};
const observation = (overrides: Partial<FlowEvent> = {}): FlowEvent => ({
  sequence: 11,
  observationId: 'new',
  kind: 'publish',
  source: 'producer',
  target: 'orders',
  consumerGroup: null,
  topicNodeId: 'orders',
  service: 'producer',
  environment: 'test',
  cluster: 'kafka',
  topic: 'orders',
  partition: null,
  offset: null,
  error: false,
  isBatch: false,
  occurredAt: 9500,
  observedAt: 9900,
  traceId: '',
  spanId: '',
  ...overrides,
});
const props = {
  nodes,
  edges: [edge],
  events: [],
  now: 10000,
  seconds: 2,
  selected: '',
  onSelect: jest.fn(),
  paused: false,
  zoom: 1,
  fit: false,
  animationAfter: 10,
};

beforeAll(() => {
  global.structuredClone = (value) => deserialize(serialize(value));
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
  global.PointerEvent = MouseEvent as typeof PointerEvent;
});

test('connection rate counts unique actual operations in its own window and group', async () => {
  const event = observation();
  render(
    <Topology
      {...props}
      events={[
        event,
        event,
        observation({ observationId: 'match', kind: 'matched' }),
        observation({ observationId: 'other-group', consumerGroup: 'another' }),
        observation({ observationId: 'old', occurredAt: 7000 }),
      ]}
    />
  );
  const connection = screen.getByRole('img', { name: 'producer → orders: 0.50 observed ops/s' });
  fireEvent.focus(connection);
  expect(await screen.findByRole('tooltip')).toHaveTextContent('0.50 observed ops/s');
  expect(screen.queryByText('Kafka topic')).not.toBeInTheDocument();
  expect(screen.getByRole('button', { name: 'Kafka topic orders' })).toBeInTheDocument();
});

test('new observations explicitly start motion; paused and restored history do not animate', () => {
  const begin = jest.fn();
  Object.defineProperty(SVGElement.prototype, 'beginElement', { configurable: true, value: begin });
  const events = [
    observation(),
    observation({ observationId: 'history', sequence: 9 }),
    observation({ observationId: 'matched', kind: 'matched' }),
  ];
  const { container, rerender } = render(<Topology {...props} events={events} />);
  expect(container.querySelectorAll('.dsm-observation-particle')).toHaveLength(1);
  expect(begin).toHaveBeenCalledTimes(2);
  const path = container.querySelector('.dsm-edge')?.getAttribute('d');
  rerender(<Topology {...props} events={events} now={10100} />);
  expect(begin).toHaveBeenCalledTimes(2);
  expect(container.querySelector('.dsm-edge')).toHaveAttribute('d', path);
  rerender(<Topology {...props} events={events} paused />);
  expect(container.querySelectorAll('.dsm-observation-particle')).toHaveLength(0);
  delete (SVGElement.prototype as SVGElement & { beginElement?: () => void }).beginElement;
});

test('wheel zoom keeps the cursor point fixed and synchronizes controls', () => {
  const onZoomChange = jest.fn(),
    onFitChange = jest.fn();
  const { container } = render(<Topology {...props} onZoomChange={onZoomChange} onFitChange={onFitChange} />);
  fireEvent.wheel(screen.getByTestId('topology-canvas'), { clientX: 200, clientY: 100, deltaY: -200 });
  const scale = Math.exp(0.3);
  expect(onZoomChange).toHaveBeenLastCalledWith(scale);
  expect(onFitChange).toHaveBeenLastCalledWith(false);
  expect(container.querySelector('.dsm-topology-camera')).toHaveAttribute(
    'transform',
    `translate(${200 - 200 * scale},${100 - 100 * scale}) scale(${scale})`
  );
  fireEvent.wheel(screen.getByTestId('topology-canvas'), { deltaY: -100000 });
  expect(onZoomChange).toHaveBeenLastCalledWith(3);
});

test('Actual size restores native scale after Fit even when the zoom prop was already one', () => {
  const width = jest.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(400);
  const height = jest.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(300);
  const { container, rerender } = render(<Topology {...props} fit />);
  expect(container.querySelector('.dsm-topology-camera')?.getAttribute('transform')).not.toBe(
    'translate(0,0) scale(1)'
  );
  rerender(<Topology {...props} fit={false} />);
  expect(container.querySelector('.dsm-topology-camera')).toHaveAttribute('transform', 'translate(0,0) scale(1)');
  width.mockRestore();
  height.mockRestore();
});

test('dragging pans the camera and suppresses accidental node selection while keyboard activation remains', () => {
  const onSelect = jest.fn();
  const { container } = render(<Topology {...props} onSelect={onSelect} />);
  const canvas = screen.getByTestId('topology-canvas');
  const producer = screen.getByRole('button', { name: 'Service producer' });
  fireEvent.pointerDown(producer, { button: 0, clientX: 100, clientY: 100 });
  fireEvent.pointerMove(canvas, { clientX: 150, clientY: 130 });
  fireEvent.pointerUp(canvas);
  fireEvent.click(producer, { detail: 1 });
  expect(onSelect).not.toHaveBeenCalled();
  expect(container.querySelector('.dsm-topology-camera')).toHaveAttribute('transform', 'translate(50,30) scale(1)');
  fireEvent.keyDown(producer, { key: 'Enter' });
  expect(onSelect).toHaveBeenCalledWith('producer');
});
