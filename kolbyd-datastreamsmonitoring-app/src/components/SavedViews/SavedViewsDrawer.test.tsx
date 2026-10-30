import React, { useState } from 'react';
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import SavedViewsDrawer from './SavedViewsDrawer';
import { parseViews, serializeViews, ViewFilters } from './storage';

const storageKey = 'dsm.views.org1.user1';
const filters: ViewFilters = {
  query: 'orders',
  environment: 'prod',
  cluster: 'cluster-a',
  hiddenNodeIds: ['node-a'],
  errorsOnly: false,
  windowSeconds: 60,
  view: 'Map',
  listKind: 'services',
};
function save(name = 'Checkout') {
  fireEvent.change(screen.getByLabelText('View name'), { target: { value: name } });
  fireEvent.click(screen.getByRole('button', { name: 'Save current view' }));
}

describe('personal saved views', () => {
  beforeEach(() => window.localStorage.clear());
  afterEach(async () => {
    await act(async () => {});
    jest.restoreAllMocks();
  });

  test('saves, applies, updates, renames and confirms deletion', () => {
    const onApply = jest.fn(),
      onSaved = jest.fn();
    const props = { open: true, onClose: jest.fn(), currentFilters: filters, onApply, onSaved, storageKey };
    const { rerender } = render(<SavedViewsDrawer {...props} />);
    save();
    const initial = parseViews(window.localStorage.getItem(storageKey))[0];
    expect(initial.filters).toEqual(filters);
    expect(onSaved).toHaveBeenCalledWith(initial);
    fireEvent.click(screen.getByRole('button', { name: 'Apply view Checkout' }));
    expect(onApply).toHaveBeenCalledWith(initial);
    rerender(
      <SavedViewsDrawer
        {...props}
        currentFilters={{ ...filters, query: 'payments', view: 'List', listKind: 'queues' }}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: 'Update with current filters' }));
    expect(parseViews(window.localStorage.getItem(storageKey))[0].filters.query).toBe('payments');
    fireEvent.click(screen.getByRole('button', { name: 'Rename view Checkout' }));
    fireEvent.change(screen.getByRole('textbox', { name: 'Rename Checkout' }), { target: { value: 'Payment queues' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save name' }));
    expect(parseViews(window.localStorage.getItem(storageKey))[0].name).toBe('Payment queues');
    fireEvent.click(screen.getByRole('button', { name: 'Delete view Payment queues' }));
    expect(parseViews(window.localStorage.getItem(storageKey))).toHaveLength(1);
    fireEvent.click(screen.getByRole('button', { name: 'Keep view' }));
    expect(screen.queryByRole('button', { name: 'Delete view' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Delete view Payment queues' }));
    fireEvent.click(screen.getByRole('button', { name: 'Delete view' }));
    expect(parseViews(window.localStorage.getItem(storageKey))).toHaveLength(0);
    expect(screen.getByLabelText('View name')).toHaveFocus();
  });

  test('persists between mounts and keeps organization/user scopes separate', () => {
    const props = { open: true, onClose: jest.fn(), currentFilters: filters, onApply: jest.fn(), storageKey };
    const first = render(<SavedViewsDrawer {...props} />);
    save();
    first.unmount();
    const second = render(<SavedViewsDrawer {...props} />);
    expect(screen.getByRole('button', { name: 'Apply view Checkout' })).toBeInTheDocument();
    second.rerender(<SavedViewsDrawer {...props} storageKey="dsm.views.org2.user1" />);
    expect(screen.queryByRole('button', { name: 'Apply view Checkout' })).not.toBeInTheDocument();
    expect(window.localStorage.getItem(storageKey)).toContain('Checkout');
  });

  test('restores trigger focus on Escape without trapping focus inside the non-modal drawer', () => {
    function Harness() {
      const [open, setOpen] = useState(false);
      return (
        <>
          <button onClick={() => setOpen(true)}>Views trigger</button>
          <button>Outside drawer</button>
          <SavedViewsDrawer
            open={open}
            onClose={() => setOpen(false)}
            currentFilters={filters}
            onApply={jest.fn()}
            storageKey={storageKey}
          />
        </>
      );
    }
    render(<Harness />);
    const trigger = screen.getByRole('button', { name: 'Views trigger' });
    act(() => trigger.focus());
    fireEvent.click(trigger);
    expect(screen.getByRole('button', { name: 'Close' })).toHaveFocus();
    act(() => screen.getByRole('button', { name: 'Outside drawer' }).focus());
    expect(screen.getByRole('button', { name: 'Outside drawer' })).toHaveFocus();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(screen.queryByTestId('saved-views-drawer')).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  test('malformed storage is visible and requires an explicit reset', () => {
    window.localStorage.setItem(storageKey, '{broken');
    render(
      <SavedViewsDrawer open onClose={jest.fn()} currentFilters={filters} onApply={jest.fn()} storageKey={storageKey} />
    );
    expect(screen.getByRole('alert')).toHaveTextContent('Saved views could not be read');
    expect(screen.getByRole('button', { name: 'Save current view' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'Reset local saved views' }));
    expect(window.localStorage.getItem(storageKey)).toBe('{broken');
    fireEvent.click(screen.getByRole('button', { name: 'Reset views' }));
    expect(window.localStorage.getItem(storageKey)).toBeNull();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    save();
    expect(parseViews(window.localStorage.getItem(storageKey))).toHaveLength(1);
  });

  test('failed storage writes do not claim success or retain a phantom view', () => {
    const onSaved = jest.fn();
    jest.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('quota', 'QuotaExceededError');
    });
    render(
      <SavedViewsDrawer
        open
        onClose={jest.fn()}
        currentFilters={filters}
        onApply={jest.fn()}
        onSaved={onSaved}
        storageKey={storageKey}
      />
    );
    save();
    expect(screen.getByRole('alert')).toHaveTextContent('Storage may be full or blocked');
    expect(onSaved).not.toHaveBeenCalled();
    expect(screen.queryByRole('button', { name: 'Apply view Checkout' })).not.toBeInTheDocument();
  });

  test('updates from another tab refresh the scoped view list', () => {
    render(
      <SavedViewsDrawer open onClose={jest.fn()} currentFilters={filters} onApply={jest.fn()} storageKey={storageKey} />
    );
    window.localStorage.setItem(
      storageKey,
      serializeViews([{ id: 'other-tab', name: 'Other tab view', filters, updatedAt: 1234 }])
    );
    fireEvent(window, new StorageEvent('storage', { key: storageKey }));
    expect(
      within(screen.getByTestId('saved-views-drawer')).getByRole('button', { name: 'Apply view Other tab view' })
    ).toBeInTheDocument();
  });

  test('renaming an unapplied view leaves the active badge unchanged and deletion reports the removed view', () => {
    const onSaved = jest.fn(),
      onRemoved = jest.fn();
    render(
      <SavedViewsDrawer
        open
        onClose={jest.fn()}
        currentFilters={filters}
        onApply={jest.fn()}
        onSaved={onSaved}
        onRemoved={onRemoved}
        storageKey={storageKey}
      />
    );
    save('First');
    save('Second');
    onSaved.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Rename view First' }));
    fireEvent.change(screen.getByRole('textbox', { name: 'Rename First' }), { target: { value: 'Renamed first' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save name' }));
    expect(onSaved).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Delete view Second' }));
    fireEvent.click(screen.getByRole('button', { name: 'Delete view' }));
    expect(onRemoved).toHaveBeenCalledWith(expect.objectContaining({ name: 'Second', filters }));
  });
});
