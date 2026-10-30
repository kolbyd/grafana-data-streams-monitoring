'use strict';
const $ = id => document.getElementById(id);
const ns = 'http://www.w3.org/2000/svg';
const colors = { publish: '#69d7c4', receive: '#efbd76', error: '#ff9691' };
const state = { nodes: new Map(), edges: new Map(), events: [], matches: new Map(), particles: [], paths: new Map(),
  positions: new Map(), instance: null, cursor: 0, socket: null, key: '', reconnect: 0, retry: null,
  paused: matchMedia('(prefers-reduced-motion: reduce)').matches, replay: false, selected: null,
  demo: null, demoTimers: new Set(), demoTick: 0, demoOffset: 0, signature: '', stats: null,
  serverTime: Date.now(), lastObserved: 0, exportDelay: 0, skipped: 0, frameAt: 0, staleAfterMs: 30000 };
function el(tag, attrs = {}, text = null) {
  const node = document.createElementNS(ns, tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  if (text !== null) node.textContent = text;
  return node;
}
function textNode(tag, text, className) { const n = document.createElement(tag); n.textContent = text; if (className) n.className = className; return n; }
function notice(text) { $('notice').textContent = text; $('notice').hidden = !text; }
function connection(text, mode = '') { $('connection-status').textContent = text; $('connection-dot').className = 'status-dot ' + mode; }
function headers() { return { 'Content-Type': 'application/json', ...(state.key ? { Authorization: 'Bearer ' + state.key } : {}) }; }
function base64url(text) { return btoa(String.fromCharCode(...new TextEncoder().encode(text))).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_'); }
function connect() {
  clearTimeout(state.retry);
  const url = new URL('/api/stream', location.href); url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  if (state.instance) { url.searchParams.set('instanceId', state.instance); url.searchParams.set('after', state.cursor); }
  const protocols = ['liveflow.v1']; if (state.key) protocols.push('liveflow.auth.' + base64url(state.key));
  connection(state.reconnect ? 'Reconnecting' : 'Connecting');
  const socket = state.socket = new WebSocket(url, protocols);
  socket.onopen = () => {
    state.reconnect = 0; connection('Connected', 'online'); notice('');
    fetch('/api/status', { headers: state.key ? { Authorization: 'Bearer ' + state.key } : {} })
      .then(response => response.ok ? response.json() : null).then(status => { if (status) state.staleAfterMs = status.staleAfterSeconds * 1000; }).catch(() => {});
  };
  socket.onmessage = event => {
    try { applyFrame(JSON.parse(event.data)); }
    catch { notice('An update could not be read. Reconnecting for a fresh snapshot.'); socket.close(); }
  };
  socket.onclose = () => {
    if (socket !== state.socket) return;
    connection('Disconnected', 'offline');
    const delay = Math.min(10000, 500 * 2 ** Math.min(state.reconnect++, 5));
    if (state.reconnect > 2) notice('Connection unavailable. Check that the service is running and, if required, enter its API key in connection settings.');
    state.retry = setTimeout(connect, delay + Math.random() * 200);
  };
  socket.onerror = () => connection('Connection unavailable', 'offline');
}
function applyFrame(frame) {
  const oldInstance = state.instance;
  state.instance = frame.instanceId; state.cursor = frame.cursor; state.serverTime = frame.serverTime; state.frameAt = Date.now();
  if (frame.replaceTopology) { state.nodes.clear(); state.edges.clear(); }
  frame.nodes.forEach(n => state.nodes.set(n.id, n)); frame.edges.forEach(e => state.edges.set(e.id, e));
  $('demo-label').hidden = ![...state.nodes.values()].some(n => n.environment === 'demo');
  $('demo-label').textContent = state.demo ? 'Synthetic traffic running' : 'Contains demo observations';
  if (frame.resetEvents) { state.events = []; state.matches.clear(); clearParticles(); }
  if (oldInstance && oldInstance !== frame.instanceId) notice('Service restarted. Showing a fresh in-memory session.');
  else if (frame.reason === 'cursor_expired') notice('The reconnect cursor expired. The current graph and recent observations have been restored.');
  state.skipped += frame.visualEventsSkipped;
  for (const evt of frame.events) {
    if (evt.kind === 'matched') state.matches.set(evt.observationId, evt);
    else { state.events.push(evt); if (!frame.resetEvents && !state.paused && !state.replay) spawn(evt); }
    state.lastObserved = Math.max(state.lastObserved, evt.observedAt);
    if (evt.kind !== 'matched') state.exportDelay = Math.max(0, evt.observedAt - evt.occurredAt);
  }
  state.events = state.events.slice(-1000);
  const retained = new Set(state.events.map(e => e.observationId)); for (const key of state.matches.keys()) if (!retained.has(key)) state.matches.delete(key);
  state.stats = frame.stats;
  for (const key of ['published', 'received', 'matched', 'errors', 'duplicates', 'rejected']) $(key).textContent = frame.stats[key].toLocaleString();
  $('replay').disabled = state.events.length === 0 || state.replay;
  $('coverage-note').textContent = `In-memory session. ${state.skipped.toLocaleString()} visual events omitted by the stream limit; aggregate counts preserved. Sampling limits coverage.`;
  updateFilters(); renderGraph(); renderFeed(); renderDetail(); updateFreshness();
}
function updateFilters() {
  for (const [id, values] of [['environment', [...state.nodes.values()].map(n => n.environment)], ['cluster', [...state.edges.values()].map(e => e.cluster)]]) {
    const select = $(id); const value = select.value; const previous = [...select.options].slice(1).map(o => o.value).join('|');
    const choices = [...new Set(values)].sort(); if (previous === choices.join('|')) continue;
    select.replaceChildren(new Option(id === 'cluster' ? 'All clusters' : 'All environments', ''), ...choices.map(v => new Option(v, v)));
    select.value = choices.includes(value) ? value : '';
  }
}
function visibleGraph() {
  const env = $('environment').value, cluster = $('cluster').value, query = $('search').value.toLowerCase().trim();
  let edges = [...state.edges.values()].filter(e => (!env || e.environment === env) && (!cluster || e.cluster === cluster));
  if (query) edges = edges.filter(e => [e.topic, e.consumerGroup, state.nodes.get(e.source)?.name, state.nodes.get(e.target)?.name].some(s => s?.toLowerCase().includes(query)));
  const ids = new Set(edges.flatMap(e => [e.source, e.target]));
  return { nodes: [...state.nodes.values()].filter(n => ids.has(n.id)), edges };
}
function layout(nodes, edges) {
  // Condense cycles before ranking. Consume/produce loops must not push nodes endlessly to the right.
  const outgoing = new Map(nodes.map(n => [n.id, []])); edges.forEach(e => outgoing.get(e.source)?.push(e.target));
  let index = 0; const stack = [], indices = new Map(), lows = new Map(), onStack = new Set(), components = [], component = new Map();
  function visit(id) {
    indices.set(id, index); lows.set(id, index++); stack.push(id); onStack.add(id);
    for (const to of outgoing.get(id)) {
      if (!indices.has(to)) { visit(to); lows.set(id, Math.min(lows.get(id), lows.get(to))); }
      else if (onStack.has(to)) lows.set(id, Math.min(lows.get(id), indices.get(to)));
    }
    if (lows.get(id) === indices.get(id)) { const members = []; let to; do { to = stack.pop(); onStack.delete(to); component.set(to, components.length); members.push(to); } while (to !== id); components.push(members); }
  }
  nodes.forEach(n => { if (!indices.has(n.id)) visit(n.id); });
  const adj = components.map(() => new Set()), indegree = components.map(() => 0), depth = components.map(() => 0);
  edges.forEach(e => { const a = component.get(e.source), b = component.get(e.target); if (a !== b && !adj[a].has(b)) { adj[a].add(b); indegree[b]++; } });
  const queue = indegree.flatMap((degree, i) => degree === 0 ? [i] : []);
  while (queue.length) { const a = queue.shift(); for (const b of adj[a]) { depth[b] = Math.max(depth[b], depth[a] + 1); if (--indegree[b] === 0) queue.push(b); } }
  const columns = new Map(); nodes.forEach(n => { const col = depth[component.get(n.id)]; if (!columns.has(col)) columns.set(col, []); columns.get(col).push(n); });
  const positions = new Map(); let height = 400;
  for (const [column, members] of columns) {
    members.sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
    height = Math.max(height, members.length * 105 + 90);
    members.forEach((node, row) => positions.set(node.id, { x: 50 + column * 260, y: 70 + row * 105, width: 195, height: 58 }));
  }
  // Center shorter columns against the longest column.
  for (const members of columns.values()) for (const node of members) positions.get(node.id).y += Math.max(0, (height - members.length * 105 - 90) / 2);
  return { positions, width: Math.max(1000, columns.size * 260 + 35), height };
}
function renderGraph() {
  const graph = visibleGraph(); const signature = graph.nodes.map(n => n.id).sort().join('|') + graph.edges.map(e => e.id).sort().join('|');
  $('empty').hidden = graph.nodes.length > 0;
  if (!graph.nodes.length && state.nodes.size) { $('empty').querySelector('h2').textContent = 'No matching relationships.'; $('empty').querySelector('p').textContent = 'Try another service, topic, environment, or cluster.'; }
  else { $('empty').querySelector('h2').textContent = 'Your stream starts here.'; $('empty').querySelector('p').textContent = 'Send Kafka spans to /v1/traces to discover your topology. Or start the synthetic demo to explore the viewer.'; }
  $('graph-summary').textContent = graph.nodes.length ? `${graph.nodes.filter(n => n.kind === 'service').length} services · ${graph.nodes.filter(n => n.kind === 'topic').length} topics · ${graph.edges.length} relationships` : 'No relationships discovered';
  if (signature !== state.signature) {
    state.signature = signature; clearParticles(); state.paths.clear();
    const calculated = layout(graph.nodes, graph.edges); state.positions = calculated.positions;
    $('graph').setAttribute('viewBox', `0 0 ${calculated.width} ${calculated.height}`);
    $('graph').style.minWidth = calculated.width + 'px';
    $('graph').style.height = calculated.height + 'px';
    $('edge-layer').replaceChildren(); $('node-layer').replaceChildren();
    for (const edge of graph.edges) {
      const a = state.positions.get(edge.source), b = state.positions.get(edge.target); if (!a || !b) continue;
      const right = b.x > a.x; const p0 = { x: a.x + (right ? a.width : 0), y: a.y + a.height / 2 };
      const p3 = { x: b.x + (right ? 0 : b.width), y: b.y + b.height / 2 };
      const curve = Math.max(50, Math.abs(p3.x - p0.x) * .5);
      const siblings = graph.edges.filter(e => e.source === edge.source && e.target === edge.target).sort((a, b) => a.id.localeCompare(b.id));
      const lane = (siblings.findIndex(e => e.id === edge.id) - (siblings.length - 1) / 2) * 34;
      const p1 = { x: p0.x + (right ? curve : -curve), y: p0.y + lane }, p2 = { x: p3.x + (right ? -curve : curve), y: p3.y + lane };
      const path = el('path', { d: `M${p0.x},${p0.y} C${p1.x},${p1.y} ${p2.x},${p2.y} ${p3.x},${p3.y}`, class: 'edge-path', 'marker-end': 'url(#arrow)' });
      $('edge-layer').append(path); state.paths.set(edge.id, { p0, p1, p2, p3, edge });
      if (edge.consumerGroup) $('edge-layer').append(el('text', { x: (p0.x + p3.x) / 2, y: (p0.y + p3.y) / 2 - 12 + lane * .75, class: 'edge-label', 'text-anchor': 'middle' }, truncate(edge.consumerGroup, 24)));
    }
    for (const node of graph.nodes) {
      const p = state.positions.get(node.id); const group = el('g', { transform: `translate(${p.x},${p.y})`, class: 'node ' + node.kind, tabindex: '0', role: 'button', 'aria-label': `${node.kind} ${node.name}`, 'data-id': node.id });
      group.append(el('rect', { width: p.width, height: p.height, rx: node.kind === 'topic' ? 28 : 7 }));
      group.append(el('circle', { cx: 19, cy: 23, r: node.kind === 'topic' ? 3 : 2.5, class: 'node-mark' }));
      group.append(el('text', { x: 31, y: 27, class: 'node-title' }, truncate(node.name, 22)));
      group.append(el('text', { x: 31, y: 44, class: 'node-subtitle' }, node.kind === 'topic' ? truncate(node.cluster, 25) : truncate(node.serviceNamespace || 'service', 25)));
      group.append(el('title', {}, `${node.name}\n${node.environment}\n${node.kind === 'topic' ? node.cluster : node.serviceNamespace}`));
      group.onclick = () => selectNode(node.id); group.onkeydown = e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); selectNode(node.id); } };
      $('node-layer').append(group);
    }
  }
  document.querySelectorAll('.node').forEach(node => {
    const data = state.nodes.get(node.dataset.id); node.classList.toggle('selected', state.selected?.nodeId === data?.id);
    node.classList.toggle('stale', data && state.serverTime - data.lastSeen > state.staleAfterMs);
  });
}
function truncate(text, length) { return text.length <= length ? text : text.slice(0, length - 1) + '…'; }
function spawn(evt, force = false) {
  if ((!force && state.paused) || evt.kind === 'matched' || state.particles.length >= 180 || matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const path = [...state.paths.values()].find(p => p.edge.source === evt.source && p.edge.target === evt.target && p.edge.consumerGroup === evt.consumerGroup);
  if (!path) return;
  const circle = el('circle', { r: evt.error ? 4 : 3, fill: evt.error ? colors.error : colors[evt.kind] || colors.publish, opacity: '.95' });
  $('particle-layer').append(circle); state.particles.push({ circle, path, start: performance.now(), duration: 850 + Math.random() * 350 });
}
function clearParticles() { state.particles = []; $('particle-layer').replaceChildren(); }
function animate(now) {
  state.particles = state.particles.filter(p => {
    const t = Math.min(1, (now - p.start) / p.duration); if (t >= 1) { p.circle.remove(); return false; }
    const { p0, p1, p2, p3 } = p.path; const u = 1 - t;
    p.circle.setAttribute('cx', u ** 3 * p0.x + 3 * u ** 2 * t * p1.x + 3 * u * t ** 2 * p2.x + t ** 3 * p3.x);
    p.circle.setAttribute('cy', u ** 3 * p0.y + 3 * u ** 2 * t * p1.y + 3 * u * t ** 2 * p2.y + t ** 3 * p3.y);
    return true;
  }); requestAnimationFrame(animate);
}
function renderFeed() {
  // Keep the inspected row and keyboard navigation stable while observations continue arriving.
  if ($('events').contains(document.activeElement)) { $('activity-note').textContent = 'Feed held for keyboard focus'; return; }
  $('activity-note').textContent = 'Live feed';
  const visible = visibleGraph(); const ids = new Set(visible.nodes.map(n => n.id));
  const events = state.events.filter(e => ids.has(e.source) && ids.has(e.target)).slice(-45).reverse();
  if (!events.length) { const row = document.createElement('tr'), cell = textNode('td', 'No matching Kafka activity observed yet.', 'no-events'); cell.colSpan = 5; row.append(cell); $('events').replaceChildren(row); return; }
  const rows = events.map(evt => {
    const row = document.createElement('tr'); row.className = 'event-row'; row.tabIndex = 0; row.dataset.observationId = evt.observationId;
    const time = textNode('td', new Date(evt.occurredAt).toLocaleTimeString([], { hour12: false }));
    const operation = document.createElement('td'); operation.append(textNode('span', (evt.error ? 'Failed ' : '') + (evt.kind === 'publish' ? 'Publish' : 'Receive'), evt.error ? 'error' : evt.kind));
    const service = textNode('td', evt.service); service.append(textNode('small', evt.topic));
    const record = textNode('td', evt.consumerGroup || '—'); record.append(textNode('small', evt.partition !== null && evt.offset !== null ? `p${evt.partition} / ${evt.offset}` : 'No record coordinates'));
    const correlation = textNode('td', evt.error ? 'Failed operation' : evt.isBatch ? 'Batch observation' : state.matches.has(evt.observationId) ? 'Matched' : evt.kind === 'receive' ? 'Not correlated' : 'Published', state.matches.has(evt.observationId) ? 'correlated' : 'unmatched');
    row.append(time, operation, service, record, correlation);
    const select = () => { state.selected = { nodeId: evt.kind === 'publish' ? evt.source : evt.target, event: evt }; renderDetail(); renderGraph(); };
    row.onclick = select; row.onkeydown = e => { if (e.key === 'Enter') select(); };
    return row;
  }); $('events').replaceChildren(...rows);
}
function selectNode(id) { state.selected = { nodeId: id }; renderDetail(); renderGraph(); }
function renderDetail() {
  const node = state.selected && state.nodes.get(state.selected.nodeId); $('detail').hidden = !node; if (!node) return;
  $('detail-title').textContent = node.name; $('detail-kind').textContent = node.kind === 'topic' ? 'Kafka topic' : 'Application service';
  const values = [['Environment', node.environment], [node.kind === 'topic' ? 'Cluster' : 'Namespace', node.cluster || node.serviceNamespace || '—'], ['Published / received', `${node.published} / ${node.received}`], ['Last observation', new Date(node.lastSeen).toLocaleTimeString()]];
  const evt = state.selected.event;
  if (evt) { values.push(['Trace ID', evt.traceId], ['Span ID', evt.spanId], ['Partition / offset', `${evt.partition ?? '—'} / ${evt.offset ?? '—'}`]); const match = state.matches.get(evt.observationId); if (match) values.push(['Matched via', match.correlationMethod === 'record' ? 'Kafka record coordinates' : 'Explicit producer span link'], ['Producer', match.producerService]); }
  $('detail-values').replaceChildren(...values.flatMap(([key, value]) => [textNode('dt', key), textNode('dd', value)]));
}
function updateFreshness() {
  if (!state.lastObserved) { $('freshness').textContent = 'Waiting for observations'; return; }
  const age = Math.max(0, Date.now() - state.frameAt + state.serverTime - state.lastObserved);
  $('freshness').textContent = `Last observation ${(age / 1000).toFixed(age < 10000 ? 1 : 0)}s ago · export delay ${(state.exportDelay / 1000).toFixed(2)}s`;
}
$('settings-toggle').onclick = () => { $('connection-form').hidden = !$('connection-form').hidden; $('settings-toggle').setAttribute('aria-expanded', !$('connection-form').hidden); if (!$('connection-form').hidden) $('api-key').focus(); };
$('connection-form').onsubmit = e => { e.preventDefault(); state.key = $('api-key').value; state.reconnect = 0; const old = state.socket; state.socket = null; old?.close(); connect(); };
for (const id of ['environment', 'cluster', 'search']) $(id).addEventListener('input', () => { renderGraph(); renderFeed(); });
$('close-detail').onclick = () => { state.selected = null; renderDetail(); renderGraph(); };
$('events').addEventListener('focusout', () => setTimeout(renderFeed, 0));
function updatePause() { $('pause').textContent = state.paused ? 'Resume motion' : 'Pause motion'; $('pause').setAttribute('aria-pressed', state.paused); }
$('pause').onclick = () => { state.paused = !state.paused; if (state.paused) clearParticles(); updatePause(); };
$('fit').onclick = () => { const svg = $('graph'); svg.style.minWidth = svg.style.minWidth === '0px' ? svg.viewBox.baseVal.width + 'px' : '0px'; $('graph-wrap').scrollTo({ left: 0, top: 0 }); };
$('replay').onclick = () => {
  const recent = state.events.filter(e => e.observedAt >= state.serverTime - 15000); if (!recent.length) return;
  state.replay = true; $('replay-label').hidden = false; $('replay').disabled = true; clearParticles();
  const start = recent[0].observedAt, span = Math.max(1000, recent.at(-1).observedAt - start), speed = Math.max(1, span / 6000);
  recent.slice(-180).forEach(evt => setTimeout(() => spawn(evt, true), (evt.observedAt - start) / speed));
  setTimeout(() => { state.replay = false; $('replay-label').hidden = true; $('replay').disabled = false; }, span / speed + 1300);
};

function randomHex(bytes) { return [...crypto.getRandomValues(new Uint8Array(bytes))].map(v => v.toString(16).padStart(2, '0')).join(''); }
function demoSpan(topic, operation, offset, producer = null, error = false) {
  const now = BigInt(Date.now()) * 1000000n;
  const span = { traceId: randomHex(16), spanId: randomHex(8), name: `${topic} ${operation}`, kind: operation === 'publish' ? 4 : 5,
    startTimeUnixNano: (now - 8000000n).toString(), endTimeUnixNano: now.toString(), status: { code: error ? 2 : 1 }, attributes: [
      ['messaging.system', 'kafka'], ['messaging.operation', operation], ['messaging.destination.name', topic], ['messaging.kafka.destination.partition', '0'], ['messaging.kafka.message.offset', String(offset)]
    ].map(([key, value]) => ({ key, value: { stringValue: value } })) };
  if (producer) span.links = [{ traceId: producer.traceId, spanId: producer.spanId }]; return span;
}
async function demoSend(service, span, group) {
  if (!state.demo) return;
  if (group) span.attributes.push({ key: 'messaging.kafka.consumer.group', value: { stringValue: group } });
  const resourceSpans = [{ resource: { attributes: [['service.name', service], ['service.namespace', 'shop'], ['deployment.environment.name', 'demo'], ['kafka.cluster.id', 'demo-kafka']].map(([key, value]) => ({ key, value: { stringValue: value } })) }, scopeSpans: [{ scope: { name: 'liveflow.synthetic-demo' }, spans: [span] }] }];
  try { const result = await fetch('/v1/traces', { method: 'POST', headers: headers(), body: JSON.stringify({ resourceSpans }) }); if (!result.ok) { stopDemo(); notice(`Demo export failed (${result.status}). Check the service connection and API key.`); } }
  catch { stopDemo(); notice('Demo export failed. Check the service connection.'); }
}
function later(action, ms) { const timer = setTimeout(() => { state.demoTimers.delete(timer); if (state.demo) action(); }, ms); state.demoTimers.add(timer); }
function demoCycle() {
  const offset = ++state.demoOffset, failed = ++state.demoTick % 23 === 0;
  const publish = demoSpan('orders.created', 'publish', offset, null, failed); demoSend('orders-api', publish);
  if (failed) return;
  later(() => demoSend('payments', demoSpan('orders.created', 'receive', offset, publish), 'payments-v1'), 180);
  later(() => demoSend('analytics', demoSpan('orders.created', 'receive', offset, publish), 'analytics-v2'), state.demoTick % 5 === 0 ? 2400 : 500);
  later(() => { const outgoing = demoSpan('payments.authorized', 'publish', offset); demoSend('payments', outgoing); later(() => demoSend('fulfilment', demoSpan('payments.authorized', 'receive', offset, outgoing), 'fulfilment-v1'), 220); }, 650);
}
function stopDemo() { clearInterval(state.demo); state.demo = null; for (const timer of state.demoTimers) clearTimeout(timer); state.demoTimers.clear(); $('demo-toggle').textContent = 'Start demo'; $('demo-label').hidden = ![...state.nodes.values()].some(n => n.environment === 'demo'); $('demo-label').textContent = 'Contains demo observations'; }
$('demo-toggle').onclick = () => {
  if (state.demo) return stopDemo();
  state.demoOffset = Date.now(); state.demo = setInterval(demoCycle, 650); $('demo-toggle').textContent = 'Stop demo'; $('demo-label').hidden = false;
  notice('Synthetic observations use environment demo and cluster demo-kafka. No Kafka broker is contacted.'); demoCycle();
};
window.addEventListener('beforeunload', () => { stopDemo(); state.socket?.close(); });
updatePause(); connect(); requestAnimationFrame(animate); setInterval(updateFreshness, 1000);
