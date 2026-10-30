import { test, expect } from './fixtures';
import { previewExample } from './nativeControls';

test.use({ userPreferences: { theme: 'dark' } });
test('dark theme applies to the native app and map cards', async ({ gotoPage, page }) => {
  await gotoPage('/explore');
  const app = page.getByTestId('dsm-app');
  await expect(app).toBeVisible({ timeout: 15000 });
  await expect(app).toHaveClass(/dsm-dark/);
  await previewExample(page);
  const service = app.getByRole('button', { name: 'Service orders-api', exact: true });
  await expect(service).toBeVisible();
  const fill = await service.locator('.dsm-service-outline').evaluate((element) => getComputedStyle(element).fill);
  expect(fill).not.toBe('rgb(255, 255, 255)');
  expect(await app.evaluate((element) => getComputedStyle(element).backgroundColor)).not.toBe('rgba(0, 0, 0, 0)');
});
