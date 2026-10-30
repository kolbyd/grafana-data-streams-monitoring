import { test, expect } from './fixtures';
import { addDashboardFilter, chooseOption, previewExample } from './nativeControls';

test('dashboard canvas uses native multi-select services queues and filter pills', async ({ gotoPage, page }) => {
  test.setTimeout(90000);
  await page.setViewportSize({ width: 1600, height: 1000 });
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');
  await expect(app.getByRole('heading', { name: 'Data Streams Monitoring', exact: true })).toHaveCount(0);
  await expect(app.getByRole('navigation', { name: 'Data streams sections' })).toHaveCount(0);
  await expect(app.getByRole('button', { name: 'Preview example', exact: true })).toHaveCount(0);
  await expect(app.getByRole('complementary', { name: 'Topology filters' })).toHaveCount(0);
  await expect(app.getByRole('button', { name: 'Views', exact: true })).toHaveCount(0);
  await expect(app.getByRole('button', { name: 'Pause motion', exact: true })).toHaveCount(0);
  await previewExample(page);
  const barBox = await app.getByTestId('dashboard-controls').boundingBox();
  const timeBox = await app.getByTestId('dashboard-time-picker').boundingBox();
  expect(barBox).not.toBeNull();
  expect(timeBox).not.toBeNull();
  expect(Math.abs(timeBox!.y - barBox!.y)).toBeLessThan(4);
  await chooseOption(app.getByRole('combobox', { name: 'Environment', exact: true }), 'example');
  await chooseOption(app.getByRole('combobox', { name: 'Cluster', exact: true }), 'example-kafka');
  await expect(app.getByTestId('dashboard-variable-pills')).toContainText('example-kafka');
  await chooseOption(app.getByRole('combobox', { name: 'Services', exact: true }), 'orders-api');
  await expect(app.getByRole('button', { name: 'Service orders-api', exact: true })).toBeVisible();
  await expect(app.getByRole('button', { name: 'Service payments', exact: true })).toHaveCount(0);
  await chooseOption(app.getByRole('combobox', { name: 'Services', exact: true }), 'All');
  await expect(app.getByRole('button', { name: 'Service payments', exact: true })).toBeVisible();
  await chooseOption(app.getByRole('combobox', { name: 'Queues', exact: true }), 'orders.created');
  await expect(app.getByRole('button', { name: /^Kafka topic / })).toHaveCount(1);
  await chooseOption(app.getByRole('combobox', { name: 'Queues', exact: true }), 'All');
  await expect(app.getByRole('button', { name: /^Kafka topic / })).toHaveCount(5);
  await addDashboardFilter(page, 'topic', 'orders.created');
  await expect(app.getByRole('button', { name: 'Kafka topic orders.created', exact: true })).toBeVisible();
  await expect(app.getByRole('button', { name: /^Kafka topic / })).toHaveCount(1);
});

test('native dashboard time picker supports week presets absolute ranges and time zone settings', async ({
  gotoPage,
  page,
  selectors,
}) => {
  test.setTimeout(90000);
  const appPage = await gotoPage('/explore');
  const picker = appPage.getByGrafanaSelector(selectors.components.TimePicker.openButton);
  await picker.click();
  await appPage
    .getByGrafanaSelector(selectors.components.TimePicker.timeRangeOption('now-7d', 'now'))
    .locator('label')
    .click();
  await expect(picker).toContainText('Last 7 days');
  await picker.click();
  const from = appPage.getByGrafanaSelector(selectors.components.TimePicker.fromField);
  const to = appPage.getByGrafanaSelector(selectors.components.TimePicker.toField);
  await from.fill('2026-10-01 00:00:00');
  await to.fill('2026-10-02 00:00:00');
  await appPage.getByGrafanaSelector(selectors.components.TimePicker.applyTimeRange).click();
  await expect(picker).not.toContainText('Last 7 days');
  await picker.click();
  await expect(from).toHaveValue('2026-10-01 00:00:00');
  await expect(to).toHaveValue('2026-10-02 00:00:00');
  await expect(page.getByRole('region', { name: 'Time zone selection' })).toBeVisible();
  await page.getByRole('button', { name: 'Change time settings', exact: true }).click();
  await expect(page.getByRole('tab', { name: 'Time zone', exact: true })).toBeVisible();
  await page.keyboard.press('Escape');
});

test('native dashboard controls stay on one compact desktop row', async ({ gotoPage, page }) => {
  await page.setViewportSize({ width: 1600, height: 1000 });
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');
  await expect(app.getByRole('combobox', { name: 'Services', exact: true })).toBeVisible();
  await expect(app.getByRole('combobox', { name: 'Queues', exact: true })).toBeVisible();
  const bar = await app.getByTestId('dashboard-controls').boundingBox();
  const time = await app.getByTestId('dashboard-time-picker').boundingBox();
  expect(bar).not.toBeNull();
  expect(time).not.toBeNull();
  expect(bar!.height).toBeLessThanOrEqual(40);
  expect(Math.abs(time!.y - bar!.y)).toBeLessThan(4);
});
