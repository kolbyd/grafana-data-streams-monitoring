import React from 'react';
import { Combobox, type ComboboxOption } from '@grafana/ui';

/** String-valued application filters using Grafana's current native combobox. */
export function NativeSelect({
  options,
  value,
  onChange,
  className,
  ...props
}: {
  options: Array<ComboboxOption<string>>;
  value: string;
  onChange: (value: string) => void;
  'aria-label': string;
  id?: string;
  disabled?: boolean;
  invalid?: boolean;
  placeholder?: string;
  width?: number;
  className?: string;
  'data-testid'?: string;
}) {
  return (
    <div className={className}>
      <Combobox<string>
        {...props}
        options={options}
        value={options.find((option) => option.value === value) ?? null}
        onChange={(option) => onChange(option.value)}
      />
    </div>
  );
}
