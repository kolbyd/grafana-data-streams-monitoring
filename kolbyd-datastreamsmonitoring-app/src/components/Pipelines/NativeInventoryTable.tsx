import React, { ReactNode, useMemo } from 'react';
import { InteractiveTable } from '@grafana/ui';

export type InventoryTableRow = { id: string; cells: Record<string, ReactNode>; [key: string]: unknown };
export function NativeInventoryTable({
  rows,
  columns,
  pageSize = 20,
}: {
  rows: InventoryTableRow[];
  columns: Array<{ id: string; label: string }>;
  pageSize?: number;
}) {
  const nativeColumns = useMemo<React.ComponentProps<typeof InteractiveTable<InventoryTableRow>>['columns']>(
    () =>
      columns.map((column) => ({
        id: column.id,
        header: column.label,
        sortType: (a, b, id) => {
          const left = a.values[id],
            right = b.values[id];
          return typeof left === 'number' && typeof right === 'number'
            ? left - right
            : String(left ?? '').localeCompare(String(right ?? ''), undefined, { numeric: true });
        },
        cell: ({ row }) => row.original.cells[column.id] ?? String(row.original[column.id] ?? ''),
      })),
    [columns]
  );
  const data = useMemo(() => rows, [rows]);
  return (
    <InteractiveTable
      columns={nativeColumns}
      data={data}
      getRowId={(row) => row.id}
      pageSize={pageSize}
      autoResetPage={false}
      initialSortBy={initialSort}
    />
  );
}

const initialSort = [{ id: 'name', desc: false }];
