// Jest setup provided by Grafana scaffolding
import './.config/jest-setup';
import { MessageChannel } from 'worker_threads';

// The scaffold supplies Node MessageChannels for React 19's scheduler. Their
// ports keep a Jest worker alive unless closed after the suite has unmounted.
const testChannels = new Set();
global.MessageChannel = class TestMessageChannel extends MessageChannel {
  constructor() {
    super();
    testChannels.add(this);
  }
};

// Browser layout is exercised by plugin-e2e; jsdom has no ResizeObserver.
if (!global.ResizeObserver) {
  global.ResizeObserver = class TestResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
}

afterAll(() => {
  for (const channel of testChannels) {
    channel.port1.close();
    channel.port2.close();
  }
  testChannels.clear();
});
