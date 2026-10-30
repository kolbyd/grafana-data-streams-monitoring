import React from 'react';
import { Alert, LinkButton } from '@grafana/ui';

export function ConfigEditor() {
  return (
    <Alert severity="info" title="Uses the Data Streams Monitoring app connection">
      <p>
        This data source reads the current topology through the app backend. Configure the flow-service URL and API key
        once in the app settings. This data source stores no additional credentials.
      </p>
      <LinkButton href="/plugins/eageraspen2909-datastreamsmonitoring-app?page=configuration" variant="secondary">
        Open app configuration
      </LinkButton>
    </Alert>
  );
}
