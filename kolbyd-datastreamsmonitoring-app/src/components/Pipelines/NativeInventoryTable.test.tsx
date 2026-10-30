import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { NativeInventoryTable } from './NativeInventoryTable';

test('sorts actual numeric values and paginates without changing rendered cell values', () => {
  const rows = Array.from({ length: 23 }, (_, i) => ({
    id: String(i),
    name: 'Service ' + i,
    rate: i === 0 ? 100 : i / 10,
    cells: { rate: (i === 0 ? 100 : i / 10).toFixed(1) + ' ops/s' },
  }));
  const columns = [
    { id: 'name', label: 'Service' },
    { id: 'rate', label: 'Rate' },
  ];
  const { rerender } = render(<NativeInventoryTable rows={rows} columns={columns} />);
  expect(screen.getAllByRole('row')).toHaveLength(21);
  fireEvent.click(screen.getByRole('button', { name: 'Sort column Rate' }));
  const table = screen.getByRole('table');
  expect(within(table).getAllByRole('row')[1]).toHaveTextContent('Service 1');
  fireEvent.click(screen.getByRole('button', { name: 'Sort column Rate' }));
  expect(within(table).getAllByRole('row')[1]).toHaveTextContent('100.0 ops/s');
  fireEvent.click(screen.getByRole('button', { name: 'next page' }));
  expect(screen.getAllByRole('row')).toHaveLength(4);
  rerender(
    <NativeInventoryTable
      columns={columns}
      rows={rows.map((row) => ({ ...row, rate: row.rate + 1, cells: { rate: (row.rate + 1).toFixed(1) + ' ops/s' } }))}
    />
  );
  expect(screen.getByRole('button', { name: /^2$/ })).toHaveAttribute('aria-current', 'page');
  expect(within(table).getAllByRole('row')[1]).toHaveTextContent('Service 3');
  expect(within(table).getAllByRole('row')[1]).toHaveTextContent('1.3 ops/s');
});
