import React, { SyntheticEvent, useEffect, useId, useRef, useState } from 'react';
import { SavedView, ViewFilters } from './storage';
import { useSavedViews } from './useSavedViews';
import { Button, Input, Field, Sidebar, useSidebar, useStyles2, Alert } from '@grafana/ui';
import { getSavedViewStyles } from './saved-views.styles';

export type { SavedView, ViewFilters } from './storage';
export type SavedViewsDrawerProps = {
  open: boolean;
  onClose: () => void;
  currentFilters: ViewFilters;
  onApply: (view: SavedView) => void;
  onSaved?: (view: SavedView) => void;
  onRemoved?: (view: SavedView) => void;
  storageKey: string;
};

function ScopedSavedViewsDrawer({
  open,
  onClose,
  currentFilters,
  onApply,
  onSaved,
  onRemoved,
  storageKey,
}: SavedViewsDrawerProps) {
  const saved = useSavedViews(storageKey);
  const nameId = useId();
  const [name, setName] = useState(''),
    [selectedId, setSelectedId] = useState('');
  const [renaming, setRenaming] = useState(''),
    [renameName, setRenameName] = useState('');
  const [deleting, setDeleting] = useState(''),
    [resetting, setResetting] = useState(false),
    [notice, setNotice] = useState('');
  const shellRef = useRef<HTMLElement>(null),
    nameRef = useRef<HTMLInputElement>(null),
    renameRef = useRef<HTMLInputElement>(null);
  const closeAction = useRef(onClose);
  const selected = saved.views.find((view) => view.id === selectedId);
  const styles = useStyles2(getSavedViewStyles);
  const sidebar = useSidebar({
    hasOpenPane: open,
    position: 'left',
    defaultToDocked: true,
    edgeMargin: 0,
    bottomMargin: 0,
    contentMargin: 0,
    onClosePane: onClose,
  });

  useEffect(() => {
    closeAction.current = onClose;
  }, [onClose]);
  useEffect(() => {
    if (!open) {
      return;
    }
    const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    shellRef.current?.querySelector<HTMLButtonElement>('button[aria-label="Close"]')?.focus();
    const escape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        event.stopPropagation();
        closeAction.current();
      }
    };
    document.addEventListener('keydown', escape, true);
    return () => {
      document.removeEventListener('keydown', escape, true);
      if (previousFocus?.isConnected) {
        previousFocus.focus({ preventScroll: true });
      }
    };
  }, [open]);
  useEffect(() => {
    if (renaming) {
      renameRef.current?.focus();
      renameRef.current?.select();
    }
  }, [renaming]);

  function create(event: SyntheticEvent<HTMLFormElement>) {
    event.preventDefault();
    const view = saved.create(name, currentFilters);
    if (view) {
      setSelectedId(view.id);
      setName('');
      setNotice(`Saved “${view.name}”.`);
      onSaved?.(view);
    }
  }
  function update() {
    if (!selected) {
      return;
    }
    const view = saved.update(selected.id, currentFilters);
    if (view) {
      setNotice(`Updated “${view.name}” with the current filters.`);
      onSaved?.(view);
    }
  }
  function rename(event: SyntheticEvent<HTMLFormElement>) {
    event.preventDefault();
    const view = saved.rename(renaming, renameName);
    if (view) {
      setRenaming('');
      setNotice(`Renamed view to “${view.name}”.`);
      if (selectedId === view.id) {
        onSaved?.(view);
      }
    }
  }
  function remove(view: SavedView) {
    if (saved.remove(view.id)) {
      setDeleting('');
      setRenaming('');
      if (selectedId === view.id) {
        setSelectedId('');
      }
      setNotice(`Deleted “${view.name}”. Current filters were kept.`);
      onRemoved?.(view);
      nameRef.current?.focus();
    }
  }
  if (!open) {
    return null;
  }
  return (
    <aside ref={shellRef} className={styles.shell} aria-label="Saved views" data-testid="saved-views-drawer">
      <Sidebar contextValue={{ ...sidebar, paneWidth: Math.max(320, sidebar.paneWidth), onToggleDock: undefined }}>
        <Sidebar.OpenPane>
          <Sidebar.PaneHeader title="Views" />
          <div className={styles.body}>
            <p className={styles.muted}>
              Save filters to return to a pipeline quickly. Views are personal to this browser.
            </p>
            {saved.error && (
              <Alert title={saved.error} severity="error">
                {saved.blocked && (
                  <Button variant="secondary" type="button" onClick={() => setResetting(true)}>
                    Reset local saved views
                  </Button>
                )}
              </Alert>
            )}
            {resetting && (
              <div className={styles.confirm}>
                <p>Reset all personal views in this browser for the current account?</p>
                <div>
                  <Button
                    variant="secondary"
                    type="button"
                    onClick={() => {
                      if (saved.reset()) {
                        setResetting(false);
                        setSelectedId('');
                        setNotice('Local saved views reset.');
                      }
                    }}
                  >
                    Reset views
                  </Button>
                  <Button variant="secondary" type="button" onClick={() => setResetting(false)}>
                    Cancel reset
                  </Button>
                </div>
              </div>
            )}
            <form className={styles.section} onSubmit={create}>
              <h3>Save current filters</h3>
              <Field label="View name" htmlFor={nameId}>
                <Input
                  id={nameId}
                  ref={nameRef}
                  value={name}
                  maxLength={80}
                  autoComplete="off"
                  placeholder="For example, checkout pipeline"
                  onChange={(event) => setName(event.currentTarget.value)}
                  disabled={saved.blocked}
                />
              </Field>
              <Button
                variant="primary"
                type="submit"
                disabled={!name.trim() || saved.blocked || saved.views.length >= 50}
              >
                Save current view
              </Button>
              {saved.views.length >= 50 && (
                <p className={styles.muted}>The 50-view limit is reached. Delete a view before saving another.</p>
              )}
            </form>
            {selected && (
              <div className={styles.section}>
                <span>
                  Applied view: <strong>{selected.name}</strong>
                </span>
                <Button variant="secondary" type="button" onClick={update} disabled={saved.blocked}>
                  Update with current filters
                </Button>
              </div>
            )}
            <div className={styles.heading}>
              <h3>Saved views</h3>
              <span>{saved.views.length} / 50</span>
            </div>
            {saved.views.length ? (
              <ul className={styles.list}>
                {saved.views.map((view) => (
                  <li key={view.id} className={styles.item}>
                    {renaming === view.id ? (
                      <form className={styles.section} onSubmit={rename}>
                        <Field label="Rename view">
                          <Input
                            ref={renameRef}
                            aria-label={`Rename ${view.name}`}
                            value={renameName}
                            maxLength={80}
                            autoComplete="off"
                            onChange={(event) => setRenameName(event.currentTarget.value)}
                          />
                        </Field>
                        <div>
                          <Button variant="secondary" type="submit" disabled={!renameName.trim()}>
                            Save name
                          </Button>
                          <Button variant="secondary" type="button" onClick={() => setRenaming('')}>
                            Cancel rename
                          </Button>
                        </div>
                      </form>
                    ) : (
                      <>
                        <Button
                          variant="secondary"
                          className={styles.apply}
                          type="button"
                          aria-label={`Apply view ${view.name}`}
                          aria-pressed={selectedId === view.id}
                          onClick={() => {
                            setSelectedId(view.id);
                            setDeleting('');
                            setNotice(`Applied “${view.name}”.`);
                            onApply(view);
                          }}
                        >
                          <strong>{view.name}</strong>
                          <span>
                            {view.filters.view} · {view.filters.environment || 'All environments'} ·{' '}
                            {view.filters.cluster || 'All clusters'}
                          </span>
                        </Button>
                        <div className={styles.actions}>
                          <Button
                            variant="secondary"
                            type="button"
                            aria-label={`Rename view ${view.name}`}
                            onClick={() => {
                              setRenaming(view.id);
                              setRenameName(view.name);
                              setDeleting('');
                            }}
                          >
                            Rename
                          </Button>
                          <Button
                            variant="secondary"
                            type="button"
                            aria-label={`Delete view ${view.name}`}
                            onClick={() => {
                              setDeleting(view.id);
                              setRenaming('');
                            }}
                          >
                            Delete
                          </Button>
                        </div>
                      </>
                    )}
                    {deleting === view.id && (
                      <div className={styles.confirm}>
                        <p>Delete “{view.name}”?</p>
                        <div>
                          <Button variant="destructive" type="button" onClick={() => remove(view)}>
                            Delete view
                          </Button>
                          <Button variant="secondary" type="button" onClick={() => setDeleting('')}>
                            Keep view
                          </Button>
                        </div>
                      </div>
                    )}
                  </li>
                ))}
              </ul>
            ) : (
              <p className={styles.muted}>No saved views yet. Set your filters, then save the current view.</p>
            )}
            <p className={styles.notice} role="status" aria-live="polite">
              {notice}
            </p>
          </div>
        </Sidebar.OpenPane>
      </Sidebar>
    </aside>
  );
}

export function SavedViewsDrawer(props: SavedViewsDrawerProps) {
  return <ScopedSavedViewsDrawer key={props.storageKey} {...props} />;
}

export default SavedViewsDrawer;
