import { test, expect } from './fixtures';

test('configuration saves the existing URL, preserves the key, and tests the backend connection', async ({
  appConfigPage,
  page,
}) => {
  test.setTimeout(60000);
  const config = page.getByTestId('flow-connection-config');
  await expect(config).toBeVisible({ timeout: 15000 });
  const address = config.getByRole('textbox', { name: 'Flow service URL' });
  await expect(address).not.toHaveValue('');
  const settingsResponse = appConfigPage.waitForSettingsResponse();
  await config.getByRole('button', { name: 'Save and test' }).click();
  await expect(settingsResponse).toBeOK();
  await expect(config.getByRole('status', { name: 'Connected', exact: true })).toContainText(
    'Settings saved. Flow service connected.'
  );
});

test('invalid service URLs cannot be saved', async ({ appConfigPage, page }) => {
  test.setTimeout(60000);
  const config = page.getByTestId('flow-connection-config');
  await expect(config).toBeVisible({ timeout: 15000 });
  await config.getByRole('textbox', { name: 'Flow service URL' }).fill('file:///invalid');
  await expect(config.getByRole('button', { name: 'Save and test' })).toBeDisabled();
  await expect(
    config.getByText('Use an HTTP(S) URL without credentials, query parameters, or a fragment.')
  ).toBeVisible();
});
