import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { of, throwError } from 'rxjs';
import { PluginType } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
import AppConfig, { AppConfigProps } from './AppConfig';

jest.mock('@grafana/runtime', () => ({ ...jest.requireActual('@grafana/runtime'), getBackendSrv: jest.fn() }));

function props(configured = false): AppConfigProps {
  return { plugin: { meta: { id: 'sample-app', name: 'Sample App', type: PluginType.app, enabled: true, pinned: true,
    jsonData: { serviceUrl: 'http://flow-service:4318' }, secureJsonFields: { apiKey: configured } } }, query: {} } as unknown as AppConfigProps;
}

describe('Flow service configuration', () => {
  const fetch = jest.fn();
  beforeEach(() => {
    fetch.mockReset();
    jest.mocked(getBackendSrv).mockReturnValue({ fetch } as unknown as ReturnType<typeof getBackendSrv>);
  });

  test('saves without a key and tests the server-side connection', async () => {
    fetch.mockReturnValue(of({ data: {} }));
    render(<AppConfig {...props()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Save and test' }));
    await waitFor(() => expect(screen.getByText(/Settings saved\. Flow service connected/)).toBeInTheDocument());
    expect(fetch.mock.calls[0][0]).toEqual(expect.objectContaining({ method: 'POST', data: expect.objectContaining({ jsonData: { serviceUrl: 'http://flow-service:4318' } }) }));
    expect(fetch.mock.calls[0][0].data).not.toHaveProperty('secureJsonData');
    expect(fetch.mock.calls[1][0].url).toBe('/api/plugins/sample-app/resources/status');
  });

  test('preserves a configured secret until explicitly reset', async () => {
    fetch.mockReturnValue(of({ data: {} }));
    render(<AppConfig {...props(true)} />);
    fireEvent.click(screen.getByRole('button', { name: 'Save and test' }));
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    expect(fetch.mock.calls[0][0].data).not.toHaveProperty('secureJsonData');
    fireEvent.click(screen.getByRole('button', { name: /reset/i }));
    fireEvent.click(screen.getByRole('button', { name: 'Save and test' }));
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(4));
    expect(fetch.mock.calls[2][0].data.secureJsonData).toEqual({ apiKey: '' });
  });

  test('does not expose raw connection errors and distinguishes saved-but-unreachable', async () => {
    fetch.mockReturnValueOnce(of({ data: {} })).mockReturnValueOnce(throwError(() => new Error('sensitive upstream detail')));
    render(<AppConfig {...props()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Save and test' }));
    await waitFor(() => expect(screen.getByText(/Settings saved, but the flow service could not be reached/)).toBeInTheDocument());
    expect(screen.queryByText(/sensitive upstream detail/)).not.toBeInTheDocument();
  });

  test('rejects URL credentials and non-HTTP protocols before saving', () => {
    render(<AppConfig {...props()} />);
    fireEvent.change(screen.getByRole('textbox', { name: 'Flow service URL' }), { target: { value: 'http://user:password@example.test' } });
    expect(screen.getByRole('button', { name: 'Save and test' })).toBeDisabled();
    fireEvent.change(screen.getByRole('textbox', { name: 'Flow service URL' }), { target: { value: 'file:///tmp/flow' } });
    expect(screen.getByRole('button', { name: 'Save and test' })).toBeDisabled();
    expect(fetch).not.toHaveBeenCalled();
  });
});
