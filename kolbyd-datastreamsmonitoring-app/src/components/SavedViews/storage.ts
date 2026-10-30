import { dateMath } from '@grafana/data';

export type SavedAdHocFilter = { key: string; operator: string; value: string; condition?: string };
export type SavedTimeRange = { from: string; to: string };

export type ViewFilters = {
  query: string;
  environment: string;
  cluster: string;
  hiddenNodeIds: string[];
  errorsOnly: boolean;
  windowSeconds: number;
  view: 'Map' | 'List';
  listKind: 'services' | 'queues';
  adHocFilters?: SavedAdHocFilter[];
  timeRange?: SavedTimeRange;
};

export type SavedView = { id: string; name: string; filters: ViewFilters; updatedAt: number };
export const MAX_VIEWS = 50;
const MAX_STORAGE_LENGTH = 2 * 1024 * 1024;
const CONTROL_CHARACTERS = /[\u0000-\u001f\u007f]/;

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function boundedString(value: unknown, limit: number): value is string {
  return typeof value === 'string' && value.length <= limit && !CONTROL_CHARACTERS.test(value);
}

export function cleanFilters(value: unknown): ViewFilters {
  if (
    !record(value) ||
    !boundedString(value.query, 256) ||
    !boundedString(value.environment, 256) ||
    !boundedString(value.cluster, 256) ||
    !Array.isArray(value.hiddenNodeIds) ||
    value.hiddenNodeIds.length > 2000 ||
    !value.hiddenNodeIds.every((id) => boundedString(id, 256) && id.length > 0) ||
    typeof value.errorsOnly !== 'boolean' ||
    typeof value.windowSeconds !== 'number' ||
    !Number.isInteger(value.windowSeconds) ||
    value.windowSeconds < 1 ||
    !Number.isSafeInteger(value.windowSeconds) ||
    (value.view !== 'Map' && value.view !== 'List') ||
    (value.listKind !== 'services' && value.listKind !== 'queues')
  ) {
    throw new Error('The filters could not be saved. Check their values and try again.');
  }
  // Construct an explicit whitelist. Credentials and other extra properties never enter storage.
  return {
    query: value.query,
    environment: value.environment,
    cluster: value.cluster,
    hiddenNodeIds: [...new Set(value.hiddenNodeIds as string[])],
    errorsOnly: value.errorsOnly,
    windowSeconds: value.windowSeconds,
    view: value.view as ViewFilters['view'],
    listKind: value.listKind as ViewFilters['listKind'],
    ...(value.adHocFilters !== undefined ? { adHocFilters: cleanAdHocFilters(value.adHocFilters) } : {}),
    ...(value.timeRange !== undefined ? { timeRange: cleanTimeRange(value.timeRange) } : {}),
  };
}

function cleanAdHocFilters(value: unknown): SavedAdHocFilter[] {
  if (!Array.isArray(value) || value.length > 50) {
    throw new Error('Save at most 50 dashboard filters.');
  }
  return value.map((filter) => {
    if (
      !record(filter) ||
      !['service', 'topic', 'consumer_group', 'search'].includes(String(filter.key)) ||
      !['=', '!=', '=~', '!~'].includes(String(filter.operator)) ||
      !boundedString(filter.value, 256) ||
      (filter.condition !== undefined && !['', 'AND', 'OR'].includes(String(filter.condition)))
    ) {
      throw new Error('The dashboard filters could not be saved. Check their keys, operators and values.');
    }
    if (filter.operator === '=~' || filter.operator === '!~') {
      try {
        new RegExp(filter.value);
      } catch {
        throw new Error('A dashboard filter has an invalid regular expression.');
      }
    }
    return {
      key: filter.key as string,
      operator: filter.operator as string,
      value: filter.value,
      ...(filter.condition !== undefined ? { condition: filter.condition as string } : {}),
    };
  });
}
function cleanTimeRange(value: unknown): SavedTimeRange {
  if (!record(value) || !boundedString(value.from, 128) || !boundedString(value.to, 128)) {
    throw new Error('The saved time range is invalid.');
  }
  const validRaw = (raw: string) => raw.startsWith('now') || /^\d{4}-\d{2}-\d{2}T/.test(raw);
  const now = Date.now();
  const from = validRaw(value.from) ? dateMath.toDateTime(value.from, { now }) : undefined;
  const to = validRaw(value.to) ? dateMath.toDateTime(value.to, { now }) : undefined;
  if (!from?.isValid() || !to?.isValid() || from.valueOf() >= to.valueOf()) {
    throw new Error('The saved time range is invalid. Choose a start before its end.');
  }
  return { from: value.from, to: value.to };
}

export function cleanName(value: string): string {
  const name = value.trim();
  if (!name || !boundedString(name, 80)) {
    throw new Error('Enter a view name using 1–80 characters.');
  }
  return name;
}

export function parseViews(raw: string | null): SavedView[] {
  if (raw === null) {
    return [];
  }
  if (raw.length > MAX_STORAGE_LENGTH) {
    throw new Error('Saved views exceed the local storage limit.');
  }
  const value: unknown = JSON.parse(raw);
  if (!record(value) || value.version !== 1 || !Array.isArray(value.views) || value.views.length > MAX_VIEWS) {
    throw new Error('Invalid saved views.');
  }
  const ids = new Set<string>(),
    names = new Set<string>();
  const views = value.views.map((item: unknown) => {
    if (
      !record(item) ||
      !boundedString(item.id, 128) ||
      !item.id ||
      typeof item.name !== 'string' ||
      typeof item.updatedAt !== 'number' ||
      !Number.isSafeInteger(item.updatedAt) ||
      item.updatedAt < 0 ||
      item.updatedAt > 8640000000000000
    ) {
      throw new Error('Invalid saved view.');
    }
    const name = cleanName(item.name);
    if (ids.has(item.id) || names.has(name.toLowerCase())) {
      throw new Error('Duplicate saved view.');
    }
    ids.add(item.id);
    names.add(name.toLowerCase());
    return { id: item.id, name, filters: cleanFilters(item.filters), updatedAt: item.updatedAt };
  });
  return views.sort((a, b) => b.updatedAt - a.updatedAt || a.name.localeCompare(b.name));
}

export function serializeViews(views: SavedView[]): string {
  // Revalidate before writing so callers cannot persist arbitrary extra fields.
  const clean = parseViews(JSON.stringify({ version: 1, views }));
  const serialized = JSON.stringify({ version: 1, views: clean });
  if (serialized.length > MAX_STORAGE_LENGTH) {
    throw new Error('Saved views exceed the local storage limit.');
  }
  return serialized;
}
