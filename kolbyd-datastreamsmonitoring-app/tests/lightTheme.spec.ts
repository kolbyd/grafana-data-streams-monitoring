import { test, expect } from './fixtures';
import { previewExample } from './nativeControls';

test.use({ userPreferences: { theme: 'light' } });
test('light theme applies to the native app and map cards', async ({ gotoPage, page }) => {
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');
  await expect(app).toBeVisible({ timeout: 15000 });
  await expect(app).not.toHaveClass(/dsm-dark/);
  await previewExample(page);
  await expect(app.getByRole('button', { name: 'Service orders-api', exact: true })).toBeVisible();
  expect(await app.evaluate((element) => getComputedStyle(element).backgroundColor)).not.toBe('rgba(0, 0, 0, 0)');
});
