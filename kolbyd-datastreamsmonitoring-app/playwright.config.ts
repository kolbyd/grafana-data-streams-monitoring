import type { PluginOptions } from '@grafana/plugin-e2e';
import { defineConfig, devices } from '@playwright/test';
import baseConfig from './.config/playwright.config';

const password = process.env.GRAFANA_ADMIN_PASSWORD || '';
// Use Edge for the local Windows review; CI selects Playwright's bundled Chromium.
const channel = process.env.PLAYWRIGHT_CHANNEL || 'msedge';
const browser = channel === 'chromium' ? { channel: undefined } : { channel };
export default defineConfig<PluginOptions>(baseConfig, {
  workers: 1,
  fullyParallel: false,
  reporter: 'list',
  use: {
    baseURL: process.env.GRAFANA_URL || 'http://127.0.0.1:3001',
    ...browser,
    grafanaAPICredentials: { user: 'admin', password },
    user: { user: 'admin', password, role: 'Admin' },
    userPreferences: { theme: 'light' },
  },
  projects: [
    { ...baseConfig.projects!.find((project) => project.name === 'auth')!, name: 'auth' },
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        ...browser,
        viewport: { width: 1440, height: 1000 },
        storageState: 'playwright/.auth/admin.json',
      },
      dependencies: ['auth'],
    },
  ],
});
