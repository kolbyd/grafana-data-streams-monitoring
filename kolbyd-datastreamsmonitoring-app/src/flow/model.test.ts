import { applyFrame, emptyState, matchedHops, measuredPathways, percentile, type FlowPathway } from './model';
import { exampleSource } from './example';

describe('flow state and telemetry coverage', () => {
  test('initial snapshot and stream updates do not double count observations', () => {
    const frame = exampleSource().frame(true);
    const once = applyFrame(emptyState(), frame);
    const repeated = applyFrame(once, { ...frame, replaceTopology: false, resetEvents: false });
    expect(repeated.events).toHaveLength(once.events.length);
    expect(repeated.matches.size).toBe(once.matches.size);
    expect(once.events.every((e) => e.kind !== 'matched')).toBe(true);
  });
  test('late initial data cannot regress a shared stream', () => {
    const source = exampleSource();
    const initial = source.frame(true);
    const update = source.frame();
    const latest = applyFrame(applyFrame(emptyState(), initial), update);
    expect(applyFrame(latest, initial)).toBe(latest);
  });
  test('same-instance recovery snapshots preserve fresh motion while a new service resets the cutoff', () => {
    const source = exampleSource();
    const initial = source.frame(true);
    const current = applyFrame(emptyState(), initial);
    const next = source.frame();
    const recovered = applyFrame(current, { ...next, type: 'snapshot', replaceTopology: true, resetEvents: true });
    expect(recovered.animationAfter).toBe(initial.cursor);
    expect(recovered.events.some((event) => event.sequence > recovered.animationAfter)).toBe(true);
    const restarted = applyFrame(recovered, { ...next, instanceId: 'new-service', resetEvents: true });
    expect(restarted.animationAfter).toBe(next.cursor);
  });
  test('service restart replaces graph and observation cache', () => {
    const frame = exampleSource().frame(true);
    const current = applyFrame(emptyState(), frame);
    const restarted = applyFrame(current, {
      ...frame,
      instanceId: 'new',
      cursor: 0,
      nodes: [],
      edges: [],
      events: [],
      stats: {},
    });
    expect(restarted.nodes.size).toBe(0);
    expect(restarted.events).toHaveLength(0);
    expect(restarted.reset).toContain('restarted');
  });
  test('latency requires a correlated retained producer and excludes negative clock differences', () => {
    const state = applyFrame(emptyState(), exampleSource().frame(true));
    expect(matchedHops(state, state.events).length).toBeGreaterThan(0);
    expect(
      matchedHops({ ...state, events: state.events.filter((e) => e.kind !== 'publish') }, state.events)
    ).toHaveLength(0);
    const receive = state.events.find((e) => e.kind === 'receive' && state.matches.has(e.observationId))!;
    expect(matchedHops(state, [{ ...receive, occurredAt: 0 }])).toHaveLength(0);
  });
  test('empty latency is unavailable, not zero', () => {
    expect(percentile([], 0.95)).toBeNull();
  });
  const pathway: FlowPathway = {
    id: 'causal-path',
    originObservationId: 'trace:source',
    terminalObservationId: 'trace:terminal',
    originService: 'source',
    terminalService: 'terminal',
    environment: 'dev',
    startNodeId: 'source',
    endNodeId: 'terminal',
    nodeIds: ['source', 'worker', 'terminal'],
    edgeIds: [],
    startedAt: 1000,
    completedAt: 1500,
    durationMs: 500,
    status: 'complete',
    incompleteReason: null,
    evidence: 'otel_parent_links',
    isBatch: false,
    error: false,
    missingSpanIds: [],
    dsmAttributes: {},
    steps: [
      {
        observationId: 'trace:worker-send',
        traceId: 'trace',
        spanId: 'worker-send',
        operation: 'publish',
        service: 'worker',
        nodeId: 'worker',
        topic: 'processed',
        cluster: 'test',
        consumerGroup: null,
        startedAt: 1300,
        completedAt: 1310,
        error: false,
      },
    ],
  };
  test('only complete successful causal samples contribute to endpoint latency, including complete batches', () => {
    const state = {
      ...emptyState(),
      pathways: [
        pathway,
        { ...pathway, id: 'missing', status: 'incomplete' as const },
        { ...pathway, id: 'clock', status: 'clock_skew' as const },
        { ...pathway, id: 'batch', isBatch: true },
        { ...pathway, id: 'failure', error: true },
        { ...pathway, id: 'other', endNodeId: 'another-service' },
        { ...pathway, id: 'expired', completedAt: 0 },
        { ...pathway, id: 'invalid', durationMs: NaN },
      ],
    };
    expect(
      measuredPathways(state, { startId: 'source', endId: 'terminal', now: 1600, seconds: 1 }).map((p) => p.id)
    ).toEqual(['causal-path', 'batch']);
    expect(
      measuredPathways(state, { startId: 'source', endId: 'terminal', environment: 'prod', now: 1600, seconds: 1 })
    ).toEqual([]);
  });
  test('a selected producer inside a complete route uses that send boundary, not the earlier origin', () => {
    const samples = measuredPathways(
      { ...emptyState(), pathways: [pathway] },
      { startId: 'worker', endId: 'terminal', now: 1600, seconds: 1 }
    );
    expect(samples).toHaveLength(1);
    expect(samples[0].durationMs).toBe(200);
    expect(samples[0].originService).toBe('worker');
    expect(samples[0].originObservationId).toBe('trace:worker-send');
  });
  test('late causal corrections replace the retained set instead of retaining obsolete incomplete samples', () => {
    const frame = exampleSource().frame(true);
    const incomplete = applyFrame(emptyState(), {
      ...frame,
      pathways: [{ ...pathway, status: 'incomplete', durationMs: null }],
    });
    const complete = applyFrame(incomplete, {
      ...frame,
      cursor: frame.cursor + 1,
      resetEvents: false,
      replaceTopology: false,
      pathways: [pathway],
    });
    expect(complete.pathways).toEqual([pathway]);
    const restarted = applyFrame(complete, { ...frame, instanceId: 'replacement', cursor: 0, pathways: undefined });
    expect(restarted.pathways).toEqual([]);
  });
});
