// Synthetic standard OTLP JSON fixtures. Does not send telemetry or use Kafka.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const attribute = (key, value) => ({ key, value: { [typeof value === 'number' ? 'intValue' : 'stringValue']: String(value) } });

function fixture(crossTrace = false) {
  const base = BigInt(Date.now()) * 1_000_000n;
  const originTrace = '0123456789abcdef0123456789abcdef';
  const workerTrace = crossTrace ? 'abcdef0123456789abcdef0123456789' : originTrace;
  const at = ms => String(base + BigInt(ms) * 1_000_000n);
  function span(traceId, spanId, parentSpanId, topic, operation, start, end, links = []) {
    return {
      traceId, spanId, ...(parentSpanId ? { parentSpanId } : {}), name: `${operation} ${topic}`,
      kind: operation === 'send' ? 4 : 5, startTimeUnixNano: at(start), endTimeUnixNano: at(end),
      attributes: [
        attribute('messaging.system', 'kafka'), attribute('messaging.operation.name', operation),
        attribute('messaging.operation.type', operation), attribute('messaging.destination.name', topic),
        attribute('messaging.kafka.cluster.id', 'fixture-cluster'), attribute('messaging.destination.partition.id', '0'),
        attribute('messaging.kafka.offset', topic === 'orders' ? 7 : 9),
        ...(operation === 'process' ? [attribute('messaging.consumer.group.name', 'fixture-workers')] : []),
      ],
      ...(links.length ? { links } : {}),
    };
  }
  const sendA = span(originTrace, '1111111111111111', null, 'orders', 'send', 0, 2);
  const processB = span(workerTrace, '2222222222222222', crossTrace ? '9999999999999999' : sendA.spanId,
    'orders', 'process', 5, 25, [{ traceId: originTrace, spanId: sendA.spanId }]);
  const sendB = span(workerTrace, '3333333333333333', processB.spanId, 'payments', 'send', 15, 17);
  const processC = span(workerTrace, '4444444444444444', sendB.spanId, 'payments', 'process', 30, 45,
    [{ traceId: workerTrace, spanId: sendB.spanId }]);
  const workerSpans = [processB, sendB];
  if (crossTrace) workerSpans.unshift({
    traceId: workerTrace, spanId: '9999999999999999', name: 'application job', kind: 1,
    startTimeUnixNano: at(3), endTimeUnixNano: at(27),
  });
  return { resourceSpans: [
    ['fixture-source', [sendA]], ['fixture-worker', workerSpans], ['fixture-target', [processC]],
  ].map(([service, spans]) => ({
    resource: { attributes: [attribute('service.name', service), attribute('deployment.environment.name', 'test')] },
    scopeSpans: [{ scope: { name: 'LiveFlow.DataStreams', version: '0.1.0-preview.1' }, spans }],
  })) };
}

for (const [name, crossTrace] of [['standard-chain.otlp.json', false], ['cross-trace-chain.otlp.json', true]]) {
  const path = fileURLToPath(new URL(name, import.meta.url));
  writeFileSync(path, `${JSON.stringify(fixture(crossTrace), null, 2)}\n`);
  console.log(path);
}
