import type { PluginOptions } from '@grafana/plugin-e2e';
import { defineConfig, devices } from '@playwright/test';
import baseConfig from './.config/playwright.config';

const password = process.env.GRAFANA_ADMIN_PASSWORD || '';
export default defineConfig<PluginOptions>(baseConfig, {
  workers: 1,
  fullyParallel: false,
  reporter: 'list',
  use: {
    baseURL: process.env.GRAFANA_URL || 'http://127.0.0.1:3001',
    channel: 'msedge',
    grafanaAPICredentials: { user: 'admin', password },
    user: { user: 'admin', password, role: 'Admin' },
    userPreferences: { theme: 'light' },
  },
  projects: [
    { ...baseConfig.projects!.find(project => project.name === 'auth')!, name: 'auth' },
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'], channel: 'msedge', viewport: { width: 1440, height: 1000 }, storageState: 'playwright/.auth/admin.json' },
      dependencies: ['auth'],
    },
  ],
});
