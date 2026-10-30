import React, { SyntheticEvent, useState } from 'react';
import { lastValueFrom } from 'rxjs';
import { css } from '@emotion/css';
import { AppPluginMeta, GrafanaTheme2, PluginConfigPageProps } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
import { Alert, Button, Field, FieldSet, Input, SecretInput, useStyles2 } from '@grafana/ui';

type AppPluginSettings = { serviceUrl?: string };
export interface AppConfigProps extends PluginConfigPageProps<AppPluginMeta<AppPluginSettings>> {}

function validServiceUrl(value: string) {
  try {
    const url = new URL(value);
    return ['http:', 'https:'].includes(url.protocol) && !!url.hostname && !url.username && !url.password && !url.search && !url.hash;
  } catch {
    return false;
  }
}

export default function AppConfig({ plugin }: AppConfigProps) {
  const s = useStyles2(getStyles);
  const { enabled, pinned, jsonData, secureJsonFields } = plugin.meta;
  const [serviceUrl, setServiceUrl] = useState(jsonData?.serviceUrl || '');
  const [apiKey, setApiKey] = useState('');
  const [isKeySet, setIsKeySet] = useState(Boolean(secureJsonFields?.apiKey));
  const [keyChanged, setKeyChanged] = useState(false);
  const [saving, setSaving] = useState(false);
  const [feedback, setFeedback] = useState<{ success: boolean; text: string } | null>(null);
  const isValid = validServiceUrl(serviceUrl.trim());

  async function saveAndTest(event: SyntheticEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isValid || saving) { return; }
    setSaving(true); setFeedback(null);
    let saved = false;
    try {
      await lastValueFrom(getBackendSrv().fetch({
        url: `/api/plugins/${plugin.meta.id}/settings`, method: 'POST', showErrorAlert: false,
        data: {
          enabled, pinned, jsonData: { ...jsonData, serviceUrl: serviceUrl.trim().replace(/\/+$/, '') },
          // Omit unchanged secrets. An explicit reset sends an empty string to remove the saved key.
          ...(keyChanged ? { secureJsonData: { apiKey } } : {}),
        },
      }));
      saved = true;
      if (keyChanged) { setIsKeySet(Boolean(apiKey)); setApiKey(''); setKeyChanged(false); }
      await lastValueFrom(getBackendSrv().fetch({
        url: `/api/plugins/${plugin.meta.id}/resources/status`, method: 'GET', showErrorAlert: false,
      }));
      setFeedback({ success: true, text: 'Settings saved. Flow service connected. Reopen Explore to use the updated connection.' });
    } catch {
      setFeedback({ success: false, text: saved
        ? 'Settings saved, but the flow service could not be reached. Check the URL, API key, and network access, then save and test again.'
        : 'Settings could not be saved. Check that you have permission to configure this plugin, then try again.' });
    } finally { setSaving(false); }
  }

  return <form className={s.form} onSubmit={saveAndTest} data-testid="flow-connection-config">
    <FieldSet label="Flow service connection">
      <p className={s.description}>Grafana connects to the flow service from its backend. Browsers receive topology updates through Grafana Live.</p>
      <Field label="Flow service URL" description="A server-side HTTP(S) address reachable from Grafana. Use HTTPS outside a trusted local network."
        invalid={Boolean(serviceUrl && !isValid)} error="Use an HTTP(S) URL without credentials, query parameters, or a fragment.">
        <Input width={60} id="config-service-url" name="serviceUrl" aria-label="Flow service URL" data-testid="config-service-url" value={serviceUrl} disabled={saving}
          placeholder="http://host.docker.internal:4318" onChange={event => { setServiceUrl(event.currentTarget.value); setFeedback(null); }}/>
      </Field>
      <Field label="API key" description="Optional. Required only when the flow service uses an API key. Grafana encrypts the saved value.">
        <SecretInput width={60} id="config-api-key" name="apiKey" data-testid="config-api-key" value={apiKey} disabled={saving}
          isConfigured={isKeySet} placeholder="Optional flow service API key" onChange={event => { setApiKey(event.currentTarget.value); setKeyChanged(true); setFeedback(null); }}
          onReset={() => { setApiKey(''); setIsKeySet(false); setKeyChanged(true); setFeedback(null); }}/>
      </Field>
      <Button type="submit" disabled={!isValid || saving} data-testid="config-save-test">{saving ? 'Saving and testing…' : 'Save and test'}</Button>
    </FieldSet>
    {feedback && <div className={s.feedback} role="status"><Alert severity={feedback.success ? 'success' : 'error'} title={feedback.success ? 'Connected' : 'Connection needs attention'}>{feedback.text}</Alert></div>}
  </form>;
}

const getStyles = (theme: GrafanaTheme2) => ({
  form: css`padding: ${theme.spacing(3)}; max-width: 820px;`,
  description: css`color: ${theme.colors.text.secondary}; max-width: 65ch; margin-bottom: ${theme.spacing(3)};`,
  feedback: css`margin-top: ${theme.spacing(3)};`,
});
