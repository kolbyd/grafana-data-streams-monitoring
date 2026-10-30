import { useMemo, useSyncExternalStore } from 'react';
import { LiveChannelConnectionState, LiveChannelEventType, LiveChannelScope } from '@grafana/data';
import { getGrafanaLiveSrv } from '@grafana/runtime';
import { PLUGIN_ID } from '../constants';
import { applyFrame, emptyState, FlowFrame } from './model';
import { exampleSource } from './example';

function createStore(example: boolean, _retry: number) {
  let value = { state: emptyState(), status: example ? 'Example data' : 'Connecting', message: '' };
  const listeners = new Set<() => void>();
  let stop: (() => void) | undefined;
  const emit = () => listeners.forEach((listener) => listener());
  const accept = (data: FlowFrame | { type: 'connection'; message: string }) => {
    if ('instanceId' in data && Array.isArray(data.nodes) && Array.isArray(data.events)) {
      value = { state: applyFrame(value.state, data), status: example ? 'Example data' : 'Live', message: '' };
    } else if (data.type === 'connection' && 'message' in data) {
      value = { ...value, status: 'Reconnecting', message: data.message };
    }
    emit();
  };
  const unavailable = () => {
    value = {
      ...value,
      status: 'Disconnected',
      message: 'Unable to subscribe. Check the flow service connection in Setup, then retry.',
    };
    emit();
  };
  return {
    getSnapshot: () => value,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      if (listeners.size === 1) {
        if (example) {
          const source = exampleSource();
          accept(source.frame(true));
          const timer = setInterval(() => accept(source.frame()), 850);
          stop = () => clearInterval(timer);
        } else {
          const subscription = getGrafanaLiveSrv()
            .getStream<FlowFrame | { type: 'connection'; message: string }>({
              scope: LiveChannelScope.Plugin,
              stream: PLUGIN_ID,
              path: 'flow',
            })
            .subscribe({
              next: (event) => {
                if (event.type === LiveChannelEventType.Message) {
                  accept(event.message);
                } else if (event.type === LiveChannelEventType.Status) {
                  if (
                    event.state === LiveChannelConnectionState.Invalid ||
                    event.state === LiveChannelConnectionState.Shutdown
                  ) {
                    unavailable();
                  } else if (event.message && 'instanceId' in event.message) {
                    accept(event.message);
                  } else if (event.state !== LiveChannelConnectionState.Connected) {
                    value = { ...value, status: 'Connecting' };
                    emit();
                  }
                }
              },
              error: unavailable,
            });
          stop = () => subscription.unsubscribe();
        }
      }
      return () => {
        listeners.delete(listener);
        if (!listeners.size) {
          stop?.();
          stop = undefined;
        }
      };
    },
  };
}

export function useFlow(example: boolean, retry: number) {
  const store = useMemo(() => createStore(example, retry), [example, retry]);
  return useSyncExternalStore(store.subscribe, store.getSnapshot);
}
