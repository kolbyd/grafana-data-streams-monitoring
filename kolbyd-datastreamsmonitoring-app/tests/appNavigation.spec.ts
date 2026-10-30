import { test, expect } from './fixtures';
import {
  chooseOption,
  previewExample,
  navigateSidebar,
  addDashboardFilter,
  removeDashboardFilter,
} from './nativeControls';

test('Explore loads live and its explicit example supports map filters and details', async ({ gotoPage, page }) => {
  test.setTimeout(60000);
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');
  await expect(app.getByRole('heading', { name: 'Data Streams Monitoring', exact: true })).toHaveCount(0);
  await previewExample(page);
  await expect(
    app.getByText('Synthetic topology and traffic. No Kafka broker is connected to this preview.')
  ).toBeVisible();
  await expect(app.getByTestId('topology-canvas')).toBeVisible();
  await app.getByRole('button', { name: 'Service payments', exact: true }).click();
  const details = app.getByRole('complementary', { name: 'Selected node details' });
  await expect(details.getByRole('heading', { name: 'payments', exact: true })).toBeVisible();
  await details.getByRole('tab', { name: 'Kafka', exact: true }).click();
  await expect(details.getByText('Consumer lag', { exact: true })).toBeVisible();
  await expect(details.getByText('Not collected', { exact: true })).toHaveCount(2);
  await details.getByRole('button', { name: 'Close details' }).click();
  await addDashboardFilter(page, 'topic', 'orders.created');
  await expect(app.getByRole('button', { name: /^Kafka topic / })).toHaveCount(1);
  await removeDashboardFilter(page, 'topic');
  await app.getByRole('radio', { name: 'List', exact: true }).click();
  await expect(app.getByRole('columnheader', { name: /Operations in/ })).toBeVisible();
  await expect(app.getByRole('columnheader', { name: /Operations out/ })).toBeVisible();
  await app.getByRole('radio', { name: 'Queues', exact: true }).click();
  await expect(app.getByRole('radio', { name: 'Queues', exact: true })).toBeChecked();
  await expect(app.getByRole('button', { name: 'orders.created', exact: true })).toBeVisible();
  await app.getByRole('radio', { name: 'Services', exact: true }).click();
  await navigateSidebar(page, 'measure');
  await expect(app.getByRole('heading', { name: 'Define pathway' })).toBeVisible();
  await chooseOption(app.getByRole('combobox', { name: 'Start service', exact: true }), 'orders-api');
  await chooseOption(app.getByRole('combobox', { name: 'End service', exact: true }), 'notifications');
  const startSize = await app.getByRole('combobox', { name: 'Start service', exact: true }).boundingBox();
  expect(startSize?.width).toBeGreaterThan(120);
  expect(startSize?.height).toBeGreaterThanOrEqual(30);
  await expect(app.getByRole('button', { name: 'Save as pipeline', exact: true })).toBeVisible();
  await expect(app.getByRole('button', { name: 'Service notifications', exact: true })).toBeVisible();
  await app.getByRole('button', { name: 'Swap', exact: true }).click();
  await expect(app.getByText('No observed pathway connects those services.')).toBeVisible();
  await app.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(app.getByRole('combobox', { name: 'Start service', exact: true })).toHaveValue('Select a service…');
  expect(errors).toEqual([]);
});

test('page URLs, browser Back, Kafka Console and setup follow native navigation', async ({ gotoPage, page }) => {
  test.setTimeout(60000);
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');

  await previewExample(page);
  await expect(app.getByTestId('topology-canvas')).toBeVisible();
  await navigateSidebar(page, 'pipelines');
  await expect(page).toHaveURL(/\/pipelines$/);
  await expect(app.getByRole('heading', { name: 'Pipeline Catalog', exact: true })).toBeVisible();
  await navigateSidebar(page, 'measure');
  await expect(page).toHaveURL(/\/measure$/);
  await page.goBack();
  await expect(page).toHaveURL(/\/pipelines$/);
  await expect(app.getByRole('heading', { name: 'Pipeline Catalog', exact: true })).toBeVisible();
  await navigateSidebar(page, 'kafka');
  await expect(page).toHaveURL(/\/kafka$/);
  const console = app.getByTestId('kafka-console');
  await console.getByRole('tab', { name: /Topics/ }).click();
  await console.getByRole('textbox', { name: 'Filter Kafka inventory' }).fill('orders.created');
  const rateChart = console.getByRole('group', { name: 'Observed publish operations per second by topic' });
  await expect(rateChart.locator('canvas').first()).toBeVisible({ timeout: 15000 });
  await expect(rateChart.getByText(/Panel plugin not found|An unexpected error happened|Query error/)).toHaveCount(0);
  await console.getByRole('button', { name: 'Charts', exact: true }).click();
  await expect(rateChart).toHaveCount(0);
  await console.getByRole('button', { name: 'Charts', exact: true }).click();
  await expect(rateChart.locator('canvas').first()).toBeVisible();
  await expect(console.getByText('1 topics', { exact: true })).toBeVisible();
  await console.getByRole('table').getByRole('button', { name: 'orders.created', exact: true }).click();
  await expect(page).toHaveURL(/\/explore$/);
  await expect(app.getByTestId('topology-canvas')).toBeVisible();
  await expect(app.getByRole('button', { name: 'Kafka topic orders.created', exact: true })).toBeVisible();
  await navigateSidebar(page, 'setup');
  await expect(page).toHaveURL(/\/setup$/);
  await expect(app.getByRole('heading', { name: 'Setup', exact: true })).toBeVisible();
  const technologySize = await app.getByRole('combobox', { name: 'Queue technology', exact: true }).boundingBox();
  expect(technologySize?.width).toBeGreaterThan(100);
  expect(technologySize?.height).toBeGreaterThanOrEqual(30);
  await app.getByRole('textbox', { name: 'Filter instrumented services' }).fill('payments');
  await expect(app.getByRole('heading', { name: 'Showing 1 instrumented services', exact: true })).toBeVisible();
  await expect(app.getByRole('table').getByRole('button', { name: 'payments', exact: true })).toBeVisible();
});

test('narrow screens keep controls usable without page overflow', async ({ gotoPage, page }) => {
  test.setTimeout(60000);
  await page.setViewportSize({ width: 390, height: 844 });
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');
  await previewExample(page);
  await expect(app.getByTestId('topology-canvas')).toBeVisible();
  await expect(app.getByRole('combobox', { name: 'Environment', exact: true })).toBeVisible();
  expect(await app.evaluate((element) => element.scrollWidth > element.clientWidth + 1)).toBe(false);
  await app.getByRole('button', { name: 'Zoom in', exact: true }).click();
  await app.getByRole('button', { name: 'Fit map', exact: true }).click();
  await expect(app.getByRole('button', { name: 'Actual size', exact: true })).toBeVisible();
});
