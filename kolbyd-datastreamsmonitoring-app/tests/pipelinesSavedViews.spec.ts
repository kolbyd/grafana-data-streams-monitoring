import { test, expect } from './fixtures';
import type { Locator } from '@playwright/test';
import { chooseOption, previewExample, navigateSidebar } from './nativeControls';

async function chooseService(select: Locator, name: string) {
  await chooseOption(select, new RegExp('^' + name + '(?: ·|$)'));
}

test('Pipelines supports create, detail navigation, edit, map and confirmed removal', async ({ gotoPage, page }) => {
  test.setTimeout(90000);
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');

  await previewExample(page);
  await expect(app.getByTestId('topology-canvas')).toBeVisible();
  await navigateSidebar(page, 'pipelines');
  const pipelines = app.getByTestId('pipelines-page');
  await pipelines.getByRole('button', { name: 'Create Pipeline', exact: true }).click();
  const create = pipelines.getByRole('form', { name: 'Create pipeline definition' });
  await create.getByRole('textbox', { name: 'Pipeline name' }).fill('Order delivery');
  await chooseOption(create.getByRole('combobox', { name: 'Pipeline environment' }), 'example');
  await chooseService(create.getByRole('combobox', { name: 'Pipeline start service' }), 'orders-api');
  await chooseService(create.getByRole('combobox', { name: 'Pipeline end service' }), 'notifications');
  await create.getByRole('button', { name: 'Create pipeline', exact: true }).click();
  await expect(pipelines.getByRole('heading', { name: 'Order delivery', exact: true })).toBeVisible();
  const sections = pipelines.getByRole('navigation', { name: 'Pipeline sections' });
  await expect(sections).toBeVisible();
  await sections.getByRole('button', { name: 'Summary', exact: true }).click();
  for (const title of ['Throughput start', 'Throughput end']) {
    const chart = pipelines.getByRole('group', { name: title, exact: true });
    await expect(chart.locator('canvas').first()).toBeVisible({ timeout: 15000 });
    await expect(chart.getByText(/Panel plugin not found|An unexpected error happened|Query error/)).toHaveCount(0);
  }
  await sections.getByRole('button', { name: 'Definition', exact: true }).click();
  await pipelines.getByRole('button', { name: 'Edit', exact: true }).click();
  const edit = pipelines.getByRole('form', { name: 'Edit pipeline definition' });
  await edit.getByRole('textbox', { name: 'Pipeline name' }).fill('Order delivery updated');
  await edit.getByRole('button', { name: 'Save changes', exact: true }).click();
  await expect(pipelines.getByRole('heading', { name: 'Order delivery updated', exact: true })).toBeVisible();
  await navigateSidebar(page, 'pipelines');
  await expect(pipelines.getByRole('heading', { name: 'Pipeline Catalog', exact: true })).toBeVisible();
  await pipelines.getByRole('button', { name: 'Order delivery updated', exact: true }).click();
  await pipelines.getByRole('button', { name: 'View in Map', exact: true }).click();
  await expect(page).toHaveURL(/\/explore$/);
  await expect(app.getByTestId('topology-canvas')).toBeVisible();
  await navigateSidebar(page, 'pipelines');
  await pipelines.getByRole('button', { name: 'Order delivery updated', exact: true }).click();
  await pipelines.getByRole('button', { name: 'Remove', exact: true }).click();
  await pipelines.getByRole('button', { name: 'Remove pipeline', exact: true }).click();
  await expect(pipelines.getByRole('heading', { name: 'Pipeline Catalog', exact: true })).toBeVisible();
  await expect(pipelines.getByRole('button', { name: 'Order delivery updated', exact: true })).toHaveCount(0);
  expect(errors).toEqual([]);
});
