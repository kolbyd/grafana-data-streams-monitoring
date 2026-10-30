import React, { useState } from 'react';
import { Button, Field } from '@grafana/ui';
import { NativeSelect } from '../Native/NativeSelect';
import { FlowEdge, FlowEvent, FlowNode, FlowState, matchedHops, measuredPathways, percentile } from '../../flow/model';
import { Topology } from './Topology';

function pathNodes(start: string, end: string, edges: FlowEdge[]) {
  function walk(id: string, backwards: boolean) {
    const found = new Set([id]),
      queue = [id];
    while (queue.length) {
      const current = queue.shift();
      for (const edge of edges) {
        const source = backwards ? edge.target : edge.source,
          target = backwards ? edge.source : edge.target;
        if (source === current && !found.has(target)) {
          found.add(target);
          queue.push(target);
        }
      }
    }
    return found;
  }
  const down = walk(start, false),
    up = walk(end, true);
  return new Set([...down].filter((id) => up.has(id)));
}

export function Measure({
  nodes,
  edges,
  events,
  state,
  now,
  seconds,
  paused,
  onSave,
  onTraces,
}: {
  nodes: FlowNode[];
  edges: FlowEdge[];
  events: FlowEvent[];
  state: FlowState;
  now: number;
  seconds: number;
  paused: boolean;
  onSave: (startId: string, endId: string) => void;
  onTraces: (id: string) => void;
}) {
  const [start, setStart] = useState(''),
    [end, setEnd] = useState(''),
    [chosen, setChosen] = useState('');
  const [zoom, setZoom] = useState(1),
    [fit, setFit] = useState(false);
  const services = nodes.filter((n) => n.kind === 'service').sort((a, b) => a.name.localeCompare(b.name));
  const ids = start && end ? pathNodes(start, end, edges) : null;
  const visibleNodes = ids ? nodes.filter((n) => ids.has(n.id)) : nodes;
  const visibleEdges = ids ? edges.filter((e) => ids.has(e.source) && ids.has(e.target)) : edges;
  const hops = matchedHops(state, events).filter(
    (h) => !ids || (ids.has(h.receive.source) && ids.has(h.receive.target))
  );
  const p95 = percentile(
    hops.map((h) => h.ms),
    0.95
  );
  const pathways = start && end ? measuredPathways(state, { startId: start, endId: end, now, seconds }) : [];
  const endToEnd = percentile(
    pathways.map((pathway) => Number(pathway.durationMs)),
    0.95
  );
  return (
    <section className="dsm-measure-builder" aria-label="Measure a pathway">
      <aside className="dsm-pathway-controls">
        <h2>Define pathway</h2>
        <Field label="Start service">
          <NativeSelect
            aria-label="Start service"
            value={start}
            onChange={setStart}
            options={[
              { value: '', label: 'Select a service…' },
              ...services.map((node) => ({ value: node.id, label: node.name })),
            ]}
          />
        </Field>
        <Field label="End service">
          <NativeSelect
            aria-label="End service"
            value={end}
            onChange={setEnd}
            options={[
              { value: '', label: 'Select a service…' },
              ...services.map((node) => ({ value: node.id, label: node.name })),
            ]}
          />
        </Field>
        <div className="dsm-pathway-actions">
          <Button
            variant="secondary"
            size="sm"
            onClick={() => {
              setStart(end);
              setEnd(start);
            }}
          >
            Swap
          </Button>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => {
              setStart('');
              setEnd('');
              setChosen('');
            }}
          >
            Clear
          </Button>
        </div>
        {chosen && (
          <div className="dsm-pathway-node">
            <h3>{state.nodes.get(chosen)?.name}</h3>
            <Button variant="secondary" size="sm" onClick={() => setStart(chosen)}>
              Set as Start Service
            </Button>
            <Button variant="secondary" size="sm" onClick={() => setEnd(chosen)}>
              Set as End Service
            </Button>
            <Button variant="secondary" size="sm" onClick={() => onTraces(chosen)}>
              View Related Traces
            </Button>
          </div>
        )}
        {ids && !ids.size && <p className="dsm-pathway-message">No observed pathway connects those services.</p>}
        {ids && ids.size > 0 && (
          <>
            <dl>
              <dt>Components</dt>
              <dd>{ids.size}</dd>
              <dt>p95 end-to-end latency</dt>
              <dd>{endToEnd === null ? '—' : `${endToEnd.toFixed(1)} ms`}</dd>
              <dt>Completed pathways</dt>
              <dd>{pathways.length}</dd>
              <dt>p95 matched hop time</dt>
              <dd>{p95 === null ? '—' : `${p95.toFixed(1)} ms`}</dd>
            </dl>
            <Button variant="primary" size="sm" onClick={() => onSave(start, end)}>
              Save as pipeline
            </Button>
          </>
        )}
      </aside>
      <div className="dsm-measure-map">
        <div className="dsm-zoom">
          <Button
            variant="secondary"
            size="sm"
            icon="plus"
            aria-label="Zoom in"
            onClick={() => {
              setFit(false);
              setZoom((z) => Math.min(3, z + 0.15));
            }}
          />
          <Button
            variant="secondary"
            size="sm"
            icon="minus"
            aria-label="Zoom out"
            onClick={() => {
              setFit(false);
              setZoom((z) => Math.max(0.25, z - 0.15));
            }}
          />
          <Button
            variant="secondary"
            size="sm"
            onClick={() => {
              setFit(!fit);
              setZoom(1);
            }}
          >
            {fit ? '1:1' : 'Fit'}
          </Button>
        </div>
        {visibleNodes.length ? (
          <Topology
            nodes={visibleNodes}
            edges={visibleEdges}
            events={events}
            now={now}
            seconds={seconds}
            selected={chosen || start}
            onSelect={(id) => {
              if (state.nodes.get(id)?.kind === 'service') {
                setChosen(id);
              }
            }}
            paused={paused}
            zoom={zoom}
            fit={fit}
            onZoomChange={setZoom}
            onFitChange={setFit}
            animationAfter={state.animationAfter}
          />
        ) : (
          <div className="dsm-empty">
            <h2>{nodes.length ? 'No matching pathway' : 'No services discovered yet'}</h2>
            <p>
              {nodes.length
                ? 'Choose different start and end services.'
                : 'Connect applications or preview the example from Setup.'}
            </p>
          </div>
        )}
      </div>
    </section>
  );
}
