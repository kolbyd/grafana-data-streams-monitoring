import React, { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { graphlib, layout } from '@dagrejs/dagre';
import { Tooltip } from '@grafana/ui';
import { connected, FlowEdge, FlowEvent, FlowNode, formatRate, nodeEvents, rate } from '../../flow/model';
import { connectionTooltipStyles, topologyStyles } from './Topology.styles';

export function Sparkline({
  events,
  now,
  seconds,
  width = 158,
  height = 26,
}: {
  events: FlowEvent[];
  now: number;
  seconds: number;
  width?: number;
  height?: number;
}) {
  const buckets = Array.from({ length: 24 }, () => 0);
  for (const event of events) {
    const i = Math.floor((event.occurredAt - (now - seconds * 1000)) / ((seconds * 1000) / 24));
    if (i >= 0 && i < buckets.length) {
      buckets[i]++;
    }
  }
  const max = Math.max(1, ...buckets);
  const points = buckets
    .map((value, i) => `${(i * width) / 23},${height - 2 - (value / max) * (height - 4)}`)
    .join(' ');
  return (
    <svg
      className="dsm-spark"
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      height={height}
      aria-label="Observed operation rate over the selected window"
      role="img"
    >
      <path d={`M0 ${height} L${points.replaceAll(' ', ' L')} L${width} ${height} Z`} fill="var(--dsm-chart-area)" />
      <polyline points={points} stroke="var(--dsm-blue)" strokeWidth="1.3" fill="none" />
    </svg>
  );
}

function arranged(nodes: FlowNode[], edges: FlowEdge[]) {
  const graph = new graphlib.Graph({ multigraph: true });
  graph.setGraph({
    rankdir: 'LR',
    ranker: 'network-simplex',
    nodesep: 64,
    ranksep: 96,
    edgesep: 24,
    marginx: 32,
    marginy: 48,
  });
  graph.setDefaultEdgeLabel(() => ({}));
  [...nodes]
    .sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id))
    .forEach((n) => graph.setNode(n.id, { width: n.kind === 'service' ? 176 : 126, height: 90 }));
  [...edges].sort((a, b) => a.id.localeCompare(b.id)).forEach((e) => graph.setEdge(e.source, e.target, {}, e.id));
  layout(graph);
  const positions = new Map(nodes.map((n) => [n.id, graph.node(n.id)]));
  const paths = new Map(
    edges.map((e) => {
      const source = positions.get(e.source)!;
      const target = positions.get(e.target)!;
      // Ordered ports keep fan-out distinct and enter each node horizontally.
      const port = (outgoing: boolean) => {
        const siblings = edges
          .filter((edge) => (outgoing ? edge.source === e.source : edge.target === e.target))
          .sort(
            (a, b) =>
              positions.get(outgoing ? a.target : a.source)!.y - positions.get(outgoing ? b.target : b.source)!.y ||
              a.id.localeCompare(b.id)
          );
        const index = siblings.findIndex((edge) => edge.id === e.id);
        const node = nodes.find((n) => n.id === (outgoing ? e.source : e.target))!;
        return siblings.length < 2 ? 0 : ((index / (siblings.length - 1)) * 2 - 1) * (node.kind === 'topic' ? 13 : 26);
      };
      const start = { x: source.x + source.width / 2, y: source.y + port(true) };
      const end = { x: target.x - target.width / 2 - 9, y: target.y + port(false) };
      const route = graph.edge({ v: e.source, w: e.target, name: e.id }).points;
      const points = route.length <= 3 ? [start, end] : [start, ...route.slice(1, -1), end];
      let path = `M${points[0].x},${points[0].y}`;
      for (let i = 1; i < points.length; i++) {
        const a = points[i - 1],
          b = points[i];
        const mid = (a.x + b.x) / 2;
        path += ` C${mid},${a.y} ${mid},${b.y} ${b.x},${b.y}`;
      }
      return [e.id, path];
    })
  );
  return {
    positions,
    paths,
    width: Math.max(900, graph.graph().width || 0),
    height: Math.max(520, graph.graph().height || 0),
  };
}

const PARTICLE_DURATION = 1800;
const edgeKey = (source: string, target: string, group: string | null) => JSON.stringify([source, target, group]);
const isOperation = (event: FlowEvent) => event.kind === 'publish' || event.kind === 'receive';

function ObservationParticle({ event, path }: { event: FlowEvent; path: string }) {
  const motion = useRef<SVGAnimateMotionElement>(null);
  const fade = useRef<SVGAnimateElement>(null);
  useEffect(() => {
    // SMIL's default begin=0 is the SVG document clock, so late arrivals would
    // otherwise appear already finished. Start each newly mounted observation.
    motion.current?.beginElement?.();
    fade.current?.beginElement?.();
  }, []);
  return (
    <g
      className={`dsm-observation-particle ${event.error ? 'has-error' : ''}`}
      data-observation-id={event.observationId}
    >
      <circle className="dsm-particle-halo" r="7" />
      <circle className="dsm-particle-core" r="4" />
      <animateMotion
        ref={motion}
        begin="indefinite"
        dur={`${PARTICLE_DURATION}ms`}
        path={path}
        fill="freeze"
        calcMode="paced"
      />
      <animate
        ref={fade}
        begin="indefinite"
        attributeName="opacity"
        values="1;1;0"
        keyTimes="0;0.88;1"
        dur={`${PARTICLE_DURATION}ms`}
        fill="freeze"
      />
    </g>
  );
}

export function Topology({
  nodes,
  edges,
  events,
  now,
  seconds,
  selected,
  onSelect,
  paused,
  zoom,
  fit,
  animationAfter,
  onZoomChange,
  onFitChange,
}: {
  nodes: FlowNode[];
  edges: FlowEdge[];
  events: FlowEvent[];
  now: number;
  seconds: number;
  selected: string;
  onSelect: (id: string) => void;
  paused: boolean;
  zoom: number;
  fit: boolean;
  animationAfter: number;
  onZoomChange?: (zoom: number) => void;
  onFitChange?: (fit: boolean) => void;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const [viewportSize, setViewportSize] = useState({ width: 900, height: 520 });
  const [camera, setCamera] = useState({ x: 0, y: 0, scale: zoom });
  const cameraRef = useRef(camera);
  const [dragging, setDragging] = useState(false);
  const [localFit, setLocalFit] = useState(fit);
  const localFitRef = useRef(localFit);
  useLayoutEffect(() => {
    cameraRef.current = camera;
    localFitRef.current = localFit;
  }, [camera, localFit]);
  const gesture = useRef<{
    pointerId: number;
    startX: number;
    startY: number;
    x: number;
    y: number;
    moved: boolean;
  } | null>(null);
  const suppressClick = useRef(false);
  const markerId = `dsm-arrow-${useId().replaceAll(':', '')}`;
  useEffect(() => {
    const element = viewport.current;
    if (!element) {
      return;
    }
    const update = () => setViewportSize({ width: element.clientWidth || 900, height: element.clientHeight || 520 });
    update();
    const observer = new ResizeObserver(update);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  useEffect(() => {
    if (!fit && localFitRef.current) {
      setCamera({ x: 0, y: 0, scale: zoom });
    }
    // Synchronize the external Fit/Actual size control with this imperative viewport.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLocalFit(fit);
    // Zoom changes are applied separately around the viewport center.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fit]);
  const signature =
    nodes
      .map((n) => n.id)
      .sort()
      .join('|') +
    '#' +
    edges
      .map((e) => e.id)
      .sort()
      .join('|');
  // Only topology changes affect placement; rates and incoming observations do not.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const geometry = useMemo(() => arranged(nodes, edges), [signature]);
  useEffect(() => {
    if (localFit) {
      const scale = Math.min(
        3,
        Math.max(0.25, Math.min(viewportSize.width / geometry.width, viewportSize.height / geometry.height))
      );
      // ResizeObserver dimensions and explicit Fit determine the SVG camera.
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setCamera({
        x: (viewportSize.width - geometry.width * scale) / 2,
        y: (viewportSize.height - geometry.height * scale) / 2,
        scale,
      });
    }
  }, [geometry, localFit, viewportSize]);
  const previousZoom = useRef(zoom);
  useEffect(() => {
    if (zoom === previousZoom.current) {
      return;
    }
    previousZoom.current = zoom;
    setLocalFit(false);
    setCamera((current) => {
      const scale = Math.max(0.25, Math.min(3, zoom));
      const ratio = scale / current.scale;
      return {
        x: viewportSize.width / 2 - (viewportSize.width / 2 - current.x) * ratio,
        y: viewportSize.height / 2 - (viewportSize.height / 2 - current.y) * ratio,
        scale,
      };
    });
  }, [zoom, viewportSize]);
  const callbacks = useRef({ onZoomChange, onFitChange });
  useLayoutEffect(() => {
    callbacks.current = { onZoomChange, onFitChange };
  }, [onZoomChange, onFitChange]);
  useEffect(() => {
    const element = viewport.current;
    if (!element) {
      return;
    }
    const wheel = (event: WheelEvent) => {
      event.preventDefault();
      const bounds = element.getBoundingClientRect();
      const x = event.clientX - bounds.left,
        y = event.clientY - bounds.top;
      const delta = event.deltaY * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? element.clientHeight : 1);
      setLocalFit(false);
      callbacks.current.onFitChange?.(false);
      const current = cameraRef.current;
      const scale = Math.max(0.25, Math.min(3, current.scale * Math.exp(-delta * 0.0015)));
      const ratio = scale / current.scale;
      previousZoom.current = scale;
      callbacks.current.onZoomChange?.(scale);
      const next = { x: x - (x - current.x) * ratio, y: y - (y - current.y) * ratio, scale };
      cameraRef.current = next;
      setCamera(next);
    };
    element.addEventListener('wheel', wheel, { passive: false });
    return () => element.removeEventListener('wheel', wheel);
  }, []);
  const related = selected ? connected(selected, edges) : null;
  const edgeActivity = new Map<string, FlowEvent[]>();
  const known = new Set<string>();
  for (const event of events) {
    if (
      !isOperation(event) ||
      known.has(event.observationId) ||
      event.occurredAt < now - seconds * 1000 ||
      event.occurredAt > now
    ) {
      continue;
    }
    known.add(event.observationId);
    const key = edgeKey(event.source, event.target, event.consumerGroup);
    const activity = edgeActivity.get(key) ?? [];
    activity.push(event);
    edgeActivity.set(key, activity);
  }
  const uniqueEvents = Array.from(new Map(events.map((event) => [event.observationId, event])).values());
  const particles = paused
    ? []
    : uniqueEvents
        .filter(
          (e) =>
            isOperation(e) &&
            e.sequence > animationAfter &&
            now - e.observedAt < PARTICLE_DURATION + 300 &&
            now >= e.observedAt
        )
        .slice(-90);
  return (
    <div
      ref={viewport}
      className={`${topologyStyles} dsm-map-scroll dsm-topology-viewport ${dragging ? 'is-dragging' : ''}`}
      data-testid="topology-canvas"
      onPointerDown={(event) => {
        if (event.button !== 0) {
          return;
        }
        suppressClick.current = false;
        gesture.current = {
          pointerId: event.pointerId,
          startX: event.clientX,
          startY: event.clientY,
          x: camera.x,
          y: camera.y,
          moved: false,
        };
      }}
      onPointerMove={(event) => {
        const active = gesture.current;
        if (!active || active.pointerId !== event.pointerId) {
          return;
        }
        const dx = event.clientX - active.startX,
          dy = event.clientY - active.startY;
        if (!active.moved && Math.hypot(dx, dy) < 4) {
          return;
        }
        if (!active.moved) {
          active.moved = true;
          event.currentTarget.setPointerCapture?.(event.pointerId);
          setDragging(true);
          setLocalFit(false);
          onFitChange?.(false);
        }
        setCamera((current) => ({ ...current, x: active.x + dx, y: active.y + dy }));
      }}
      onPointerUp={(event) => {
        suppressClick.current = gesture.current?.moved ?? false;
        gesture.current = null;
        setDragging(false);
        if (event.currentTarget.hasPointerCapture?.(event.pointerId)) {
          event.currentTarget.releasePointerCapture(event.pointerId);
        }
      }}
      onPointerCancel={() => {
        gesture.current = null;
        setDragging(false);
      }}
      onClickCapture={(event) => {
        if (suppressClick.current && event.detail !== 0) {
          event.stopPropagation();
          suppressClick.current = false;
        }
      }}
    >
      <svg
        className="dsm-topology"
        role="group"
        aria-label="Producer topic consumer topology"
        width="100%"
        height="100%"
      >
        <defs>
          <marker
            id={markerId}
            markerWidth="9"
            markerHeight="9"
            refX="8"
            refY="4.5"
            orient="auto"
            markerUnits="userSpaceOnUse"
          >
            <path
              d="M1 1 L8 4.5 L1 8"
              fill="none"
              stroke="context-stroke"
              strokeWidth="1.7"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </marker>
        </defs>
        <g className="dsm-topology-camera" transform={`translate(${camera.x},${camera.y}) scale(${camera.scale})`}>
          {edges.map((edge) => {
            const activity = edgeActivity.get(edgeKey(edge.source, edge.target, edge.consumerGroup)) ?? [];
            const errors = activity.filter((e) => e.error).length;
            const source = nodes.find((node) => node.id === edge.source)?.name ?? edge.source;
            const target = nodes.find((node) => node.id === edge.target)?.name ?? edge.target;
            const label = `${source} → ${target}: ${formatRate(rate(activity, seconds))} observed ops/s${edge.consumerGroup ? ` · Consumer group ${edge.consumerGroup}` : ''}`;
            return (
              <g
                key={edge.id}
                className={`dsm-connection ${errors ? 'has-error' : ''} ${related && related.has(edge.source) && related.has(edge.target) ? 'is-path' : ''}`}
              >
                <path className="dsm-edge-separation" d={geometry.paths.get(edge.id)} />
                <path
                  d={geometry.paths.get(edge.id)}
                  className="dsm-edge"
                  strokeWidth={Math.min(3.2, 1.5 + Math.log1p(rate(activity, seconds)) * 0.45)}
                  markerEnd={`url(#${markerId})`}
                />
                <Tooltip
                  placement="top"
                  content={
                    <div className={`${connectionTooltipStyles} dsm-connection-tooltip`}>
                      <strong>
                        {source} → {target}
                      </strong>
                      <span>{formatRate(rate(activity, seconds))} observed ops/s</span>
                      {edge.consumerGroup && <span>Consumer group: {edge.consumerGroup}</span>}
                      {errors > 0 && (
                        <span>
                          {errors} observed operation {errors === 1 ? 'error' : 'errors'} (
                          {((errors / activity.length) * 100).toFixed(1)}%)
                        </span>
                      )}
                    </div>
                  }
                >
                  <path
                    d={geometry.paths.get(edge.id)}
                    className="dsm-edge-hit"
                    tabIndex={0}
                    role="img"
                    aria-label={label}
                  />
                </Tooltip>
              </g>
            );
          })}
          {nodes.map((node) => {
            const position = geometry.positions.get(node.id)!;
            const activity = nodeEvents(node, events);
            const error = activity.some((e) => e.error),
              stale = now - node.lastSeen > 30000;
            const name = node.name.length > 23 ? node.name.slice(0, 21) + '…' : node.name;
            return (
              <g
                key={node.id}
                transform={`translate(${position.x},${position.y})`}
                role="button"
                tabIndex={0}
                aria-label={`${node.kind === 'topic' ? 'Kafka topic' : 'Service'} ${node.name}`}
                aria-pressed={selected === node.id}
                className={`dsm-node ${node.kind} ${selected === node.id ? 'is-selected' : ''} ${error ? 'has-error' : ''} ${stale ? 'is-stale' : ''}`}
                onClick={() => onSelect(node.id)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    onSelect(node.id);
                  }
                }}
              >
                <title>
                  {node.name} · {node.environment} · {formatRate(rate(activity, seconds))} observed operations/s
                </title>
                {node.kind === 'service' ? (
                  <>
                    <rect className="dsm-service-outline" x="-88" y="-45" width="176" height="90" rx="4" />
                    {error && (
                      <>
                        <path
                          className="dsm-error-bar"
                          d="M-84 -45 H84 Q88 -45 88 -41 V-28 H-88 V-41 Q-88 -45 -84 -45"
                        />
                        <text className="dsm-error-caption" x="-80" y="-33">
                          Observed operation errors
                        </text>
                      </>
                    )}
                    <g transform={`translate(-77,${error ? -16 : -29})`} className="dsm-service-glyph">
                      <rect width="15" height="15" rx="3" />
                      <path d="M4 5 7.5 3 11 5v5l-3.5 2L4 10ZM4 5l3.5 2L11 5M7.5 7v5" />
                    </g>
                    <text className="dsm-node-name" x="-55" y={error ? -5 : -18}>
                      {name}
                    </text>
                    <text className="dsm-node-rate" x="-77" y={error ? 9 : 0}>
                      {formatRate(rate(activity, seconds))} ops/s
                    </text>
                    <g transform="translate(-79,15)">
                      <Sparkline events={activity} now={now} seconds={seconds} />
                    </g>
                  </>
                ) : (
                  <>
                    <path className="dsm-topic-body" d="M-50 -22 H50 A13 22 0 0 1 50 22 H-50 A13 22 0 0 1 -50 -22" />
                    <ellipse className="dsm-topic-end" cx="50" cy="0" rx="13" ry="22" />
                    <text className="dsm-topic-name" y="43" textAnchor="middle">
                      {name}
                    </text>
                  </>
                )}
              </g>
            );
          })}
          <g className="dsm-particles" aria-hidden="true">
            {particles.map((event) => {
              const edge = edges.find(
                (e) => e.source === event.source && e.target === event.target && e.consumerGroup === event.consumerGroup
              );
              const path = edge && geometry.paths.get(edge.id);
              return path ? <ObservationParticle key={event.observationId} event={event} path={path} /> : null;
            })}
          </g>
        </g>
      </svg>
    </div>
  );
}
