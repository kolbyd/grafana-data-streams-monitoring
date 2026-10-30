import { cleanFilters, parseViews, SavedView, serializeViews, ViewFilters } from './storage';

const filters: ViewFilters = {
  query: 'orders',
  environment: 'prod',
  cluster: 'cluster-a',
  hiddenNodeIds: ['node-a'],
  errorsOnly: false,
  windowSeconds: 60,
  view: 'Map',
  listKind: 'services',
};
const view: SavedView = { id: 'view-a', name: 'Checkout', filters, updatedAt: 1234 };

describe('saved-view storage validation', () => {
  test('round trips only the whitelisted filters and never credentials', () => {
    const serialized = serializeViews([
      { ...view, filters: { ...filters, apiKey: 'must-not-persist' }, apiKey: 'also-secret' } as SavedView,
    ]);
    expect(serialized).not.toContain('apiKey');
    expect(serialized).not.toContain('secret');
    expect(parseViews(serialized)).toEqual([view]);
  });
  test('rejects malformed, unsupported, excessive and ambiguous stored views', () => {
    for (const raw of [
      '{',
      '[]',
      '{"version":2,"views":[]}',
      JSON.stringify({ version: 1, views: Array(51).fill(view) }),
      JSON.stringify({ version: 1, views: [view, view] }),
      JSON.stringify({ version: 1, views: [{ ...view, name: ' ' }] }),
    ]) {
      expect(() => parseViews(raw)).toThrow();
    }
    expect(() => parseViews('x'.repeat(2 * 1024 * 1024 + 1))).toThrow();
  });
  test('rejects out-of-bounds filters and types rather than silently coercing them', () => {
    for (const changed of [
      { query: 'a'.repeat(257) },
      { hiddenNodeIds: Array(2001).fill('id') },
      { windowSeconds: 0 },
      { windowSeconds: Number.POSITIVE_INFINITY },
      { view: ['Map'] },
      { errorsOnly: 'true' },
      { cluster: null },
    ]) {
      expect(() => cleanFilters({ ...filters, ...changed })).toThrow();
    }
  });
  test('deduplicates hidden node IDs and validates names and timestamps', () => {
    expect(cleanFilters({ ...filters, hiddenNodeIds: ['node-a', 'node-a'] }).hiddenNodeIds).toEqual(['node-a']);
    expect(() => serializeViews([{ ...view, name: 'x'.repeat(81) }])).toThrow();
    expect(() => serializeViews([{ ...view, updatedAt: -1 }])).toThrow();
  });
});

test('round trips dashboard filters and relative or absolute time ranges without extra properties', () => {
  const adHocFilters = [
    { key: 'service', operator: '=~', value: 'orders.*', condition: 'AND', apiKey: 'never-store-this' },
  ];
  const relative = { ...filters, windowSeconds: 604800, adHocFilters, timeRange: { from: 'now-7d/d', to: 'now' } };
  const raw = serializeViews([{ ...view, filters: relative }]);
  expect(raw).not.toContain('apiKey');
  expect(raw).not.toContain('never-store-this');
  expect(parseViews(raw)[0].filters).toEqual({
    ...relative,
    adHocFilters: [{ key: 'service', operator: '=~', value: 'orders.*', condition: 'AND' }],
  });
  const absolute = { from: '2026-10-01T00:00:00.000Z', to: '2026-10-30T12:00:00.000Z' };
  expect(cleanFilters({ ...filters, timeRange: absolute }).timeRange).toEqual(absolute);
  expect(parseViews(serializeViews([view]))).toEqual([view]);
});
test('rejects unsupported or excessive dashboard filters and invalid raw time ranges', () => {
  for (const adHocFilters of [
    [{ key: 'apiKey', operator: '=', value: 'secret' }],
    [{ key: 'service', operator: '>', value: 'orders' }],
    [{ key: 'service', operator: '=', value: 'x'.repeat(257) }],
    [{ key: 'service', operator: '=~', value: '[' }],
    Array(51).fill({ key: 'service', operator: '=', value: 'orders' }),
  ]) {
    expect(() => cleanFilters({ ...filters, adHocFilters })).toThrow();
  }
  for (const timeRange of [
    { from: 'yesterday-ish', to: 'now' },
    { from: 'now-abc', to: 'now' },
    { from: '2026-10-30T12:00:00Z', to: '2026-10-01T00:00:00Z' },
    { from: 'now', to: 'now-1d' },
    { from: 'x'.repeat(129), to: 'now' },
  ]) {
    expect(() => cleanFilters({ ...filters, timeRange })).toThrow();
  }
});
