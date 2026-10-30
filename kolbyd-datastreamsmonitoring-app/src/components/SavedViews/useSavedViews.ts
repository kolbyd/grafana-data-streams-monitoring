import { useMemo, useSyncExternalStore } from 'react';
import { cleanFilters, cleanName, MAX_VIEWS, parseViews, SavedView, serializeViews, ViewFilters } from './storage';

type StoreState = { key: string; views: SavedView[]; error: string; blocked: boolean };

function load(key: string): StoreState {
  if (!key || key.length > 512) {
    return {
      key,
      views: [],
      error: 'Saved views are unavailable because the browser storage scope is missing.',
      blocked: true,
    };
  }
  let raw: string | null;
  try {
    raw = window.localStorage.getItem(key);
  } catch {
    return {
      key,
      views: [],
      error: 'Browser storage is unavailable. Allow local storage to save personal views.',
      blocked: true,
    };
  }
  try {
    return { key, views: parseViews(raw), error: '', blocked: false };
  } catch {
    return {
      key,
      views: [],
      error: 'Saved views could not be read. Reset local saved views to start again.',
      blocked: true,
    };
  }
}

function newId() {
  if (typeof window.crypto.randomUUID === 'function') {
    return window.crypto.randomUUID();
  }
  return [...window.crypto.getRandomValues(new Uint8Array(16))]
    .map((value) => value.toString(16).padStart(2, '0'))
    .join('');
}

const SAME_TAB_CHANGE = 'dsm-personal-saved-views-change';
function browserStore(key: string) {
  let snapshot = load(key);
  const listeners = new Set<() => void>();
  const refresh = () => {
    snapshot = load(key);
    listeners.forEach((listener) => listener());
  };
  return {
    getSnapshot: () => snapshot,
    setSnapshot: (next: StoreState) => {
      snapshot = next;
      listeners.forEach((listener) => listener());
    },
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      const storageChanged = (event: StorageEvent) => {
        if (event.key === key || event.key === null) {
          refresh();
        }
      };
      const sameTabChanged = (event: Event) => {
        if ((event as CustomEvent<string>).detail === key) {
          refresh();
        }
      };
      window.addEventListener('storage', storageChanged);
      window.addEventListener(SAME_TAB_CHANGE, sameTabChanged);
      return () => {
        listeners.delete(listener);
        window.removeEventListener('storage', storageChanged);
        window.removeEventListener(SAME_TAB_CHANGE, sameTabChanged);
      };
    },
    notifyOtherConsumers: () => window.dispatchEvent(new CustomEvent(SAME_TAB_CHANGE, { detail: key })),
  };
}

export function useSavedViews(storageKey: string) {
  const store = useMemo(() => browserStore(storageKey), [storageKey]);
  const visible = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
  const setStore = store.setSnapshot;
  function change(update: (views: SavedView[]) => SavedView[]): SavedView[] | null {
    const current = load(storageKey);
    if (current.blocked) {
      setStore(current);
      return null;
    }
    try {
      const views = update(current.views);
      const serialized = serializeViews(views);
      try {
        window.localStorage.setItem(storageKey, serialized);
      } catch {
        setStore({ ...current, error: 'The view could not be saved in this browser. Storage may be full or blocked.' });
        return null;
      }
      const saved = parseViews(serialized);
      setStore({ key: storageKey, views: saved, error: '', blocked: false });
      store.notifyOtherConsumers();
      return saved;
    } catch (error) {
      setStore({ ...current, error: error instanceof Error ? error.message : 'The saved view could not be changed.' });
      return null;
    }
  }
  function uniqueName(views: SavedView[], name: string, exceptId = '') {
    if (views.some((view) => view.id !== exceptId && view.name.toLowerCase() === name.toLowerCase())) {
      throw new Error('A view with that name already exists. Choose another name or update the saved view.');
    }
  }
  function create(name: string, filters: ViewFilters): SavedView | null {
    let created: SavedView | null = null;
    const result = change((views) => {
      if (views.length >= MAX_VIEWS) {
        throw new Error('You can save up to 50 views. Delete a view before adding another.');
      }
      const clean = cleanName(name);
      uniqueName(views, clean);
      created = { id: newId(), name: clean, filters: cleanFilters(filters), updatedAt: Date.now() };
      return [...views, created];
    });
    return result ? created : null;
  }
  function update(id: string, filters: ViewFilters): SavedView | null {
    let updated: SavedView | null = null;
    const result = change((views) => {
      if (!views.some((view) => view.id === id)) {
        throw new Error('This view is no longer saved. Refresh the list and try again.');
      }
      return views.map((view) => {
        if (view.id !== id) {
          return view;
        }
        updated = { ...view, filters: cleanFilters(filters), updatedAt: Date.now() };
        return updated;
      });
    });
    return result ? updated : null;
  }
  function rename(id: string, name: string): SavedView | null {
    let renamed: SavedView | null = null;
    const result = change((views) => {
      const clean = cleanName(name);
      uniqueName(views, clean, id);
      if (!views.some((view) => view.id === id)) {
        throw new Error('This view is no longer saved. Refresh the list and try again.');
      }
      return views.map((view) => {
        if (view.id !== id) {
          return view;
        }
        renamed = { ...view, name: clean, updatedAt: Date.now() };
        return renamed;
      });
    });
    return result ? renamed : null;
  }
  function remove(id: string): boolean {
    return change((views) => views.filter((view) => view.id !== id)) !== null;
  }
  function reset(): boolean {
    if (!storageKey || storageKey.length > 512) {
      return false;
    }
    try {
      window.localStorage.removeItem(storageKey);
      setStore(load(storageKey));
      store.notifyOtherConsumers();
      return true;
    } catch {
      setStore({
        key: storageKey,
        views: [],
        blocked: true,
        error: 'Local saved views could not be reset. Browser storage is blocked.',
      });
      return false;
    }
  }
  return {
    views: visible.views,
    error: visible.error,
    blocked: visible.blocked,
    create,
    update,
    rename,
    remove,
    reset,
  };
}
