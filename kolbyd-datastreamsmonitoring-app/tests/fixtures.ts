import { AppConfigPage, AppPage, expect, test as base } from '@grafana/plugin-e2e';
import pluginJson from '../src/plugin.json';

type AppTestFixture = {
  appConfigPage: AppConfigPage;
  gotoPage: (path?: string) => Promise<AppPage>;
};

export const test = base.extend<AppTestFixture>({
  appConfigPage: async ({ gotoAppConfigPage }, use) => {
    const configPage = await gotoAppConfigPage({
      pluginId: pluginJson.id,
    });
    await use(configPage);
  },
  gotoPage: async ({ gotoAppPage, page }, use) => {
    await use(async (path) => {
      const app = await gotoAppPage({ path, pluginId: pluginJson.id });
      await expect(page.getByTestId('dsm-app')).toBeVisible({ timeout: 30000 });
      return app;
    });
  },
});

export { expect } from '@grafana/plugin-e2e';
