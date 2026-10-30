import type { Locator, Page } from '@playwright/test';
import plugin from '../src/plugin.json';
import { expect } from './fixtures';

export async function chooseOption(combobox: Locator, label: string | RegExp) {
  await combobox.click();
  await combobox
    .page()
    .getByRole('option', { name: label, exact: typeof label === 'string' })
    .click();
  await combobox.press('Escape');
  // Native Scenes multi-select variables commit their draft selection on blur.
  await combobox.press('Tab');
}
export async function navigateSidebar(page: Page, path: string) {
  const navigation = page.getByRole('navigation', { name: 'Navigation', exact: true });
  if (!(await navigation.isVisible())) {
    await page.getByRole('button', { name: /^(Main menu|Open menu)$/ }).click();
  }
  const moreApps = navigation.getByRole('button', { name: 'Expand section: More apps', exact: true });
  if (await moreApps.isVisible()) {
    await moreApps.click();
  }
  const expand = navigation.getByRole('button', { name: 'Expand section: ' + plugin.name, exact: true });
  if (await expand.isVisible()) {
    await expand.click();
  }
  await navigation.locator(`a[href="/a/${plugin.id}/${path}"]`).first().click();
  if ((page.viewportSize()?.width ?? 1440) < 768) {
    const close = page.getByRole('button', { name: 'Close menu', exact: true });
    if (await close.isVisible()) {
      await close.click();
    }
  }
}

export async function previewExample(page: Page) {
  await navigateSidebar(page, 'setup');
  await page.getByTestId('dsm-app').getByRole('button', { name: 'Preview example topology', exact: true }).click();
  await expect(page.getByTestId('topology-canvas')).toBeVisible();
}
export async function addDashboardFilter(page: Page, key: string, value: string) {
  const input = page.getByTestId('data-testid ad hoc filters combobox filter input');
  await input.click();
  await input.fill(`${key}=${value}`);
  await input.press('Enter');
  await input.press('Escape');
  await expect(page.getByTestId(`data-testid ad hoc filters combobox filter pill ${key}`)).toContainText(value);
}
export async function removeDashboardFilter(page: Page, key: string) {
  await page.getByTestId(`data-testid ad hoc filters combobox filter pill remove ${key}`).click();
}
