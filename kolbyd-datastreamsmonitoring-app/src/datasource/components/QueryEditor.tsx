import React from 'react';
import { QueryEditorProps } from '@grafana/data';
import { Combobox, InlineField, Input, Stack } from '@grafana/ui';
import { DataSource } from '../datasource';
import { FlowDataSourceOptions, FlowQuery, queryViews } from '../types';

type Props = QueryEditorProps<DataSource, FlowQuery, FlowDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const update = (key: keyof FlowQuery, value: string) => onChange({ ...query, [key]: value });
  const fields = [
    ['environment', 'Environment'],
    ['cluster', 'Cluster'],
    ['service', 'Service'],
    ['topic', 'Topic'],
    ['consumerGroup', 'Consumer group'],
  ] as const;
  return (
    <div>
      <InlineField label="View" labelWidth={18}>
        <Combobox
          id={`flow-query-view-${query.refId}`}
          width={40}
          options={queryViews}
          value={query.view || 'summary'}
          onChange={(option) => {
            update('view', option.value || 'summary');
            onRunQuery();
          }}
        />
      </InlineField>
      <InlineField label="Operation" labelWidth={18}>
        <Combobox
          id={`flow-operation-${query.refId}`}
          width={40}
          options={[
            { label: 'All observed operations', value: '' },
            { label: 'Publish', value: 'publish' },
            { label: 'Receive', value: 'receive' },
          ]}
          value={query.operation || ''}
          onChange={(option) => {
            onChange({
              ...query,
              operation: option.value === 'publish' || option.value === 'receive' ? option.value : undefined,
            });
            onRunQuery();
          }}
        />
      </InlineField>
      <Stack direction="row" wrap="wrap">
        {fields.map(([key, label]) => (
          <InlineField key={key} label={label}>
            <Input
              id={`flow-filter-${query.refId}-${key}`}
              value={query[key] || ''}
              placeholder="All"
              width={23}
              onChange={(event) => update(key, event.currentTarget.value)}
              onBlur={onRunQuery}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  onRunQuery();
                }
              }}
            />
          </InlineField>
        ))}
      </Stack>
      <p>
        Current in-memory snapshot. Counts are retained client observations, not broker message counts. Dashboard time
        ranges do not reconstruct history. Filters use exact labels or a single-value template variable.
      </p>
    </div>
  );
}
