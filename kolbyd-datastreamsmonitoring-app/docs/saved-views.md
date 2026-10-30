# Personal saved views

The **Views** drawer saves the current explorer filters in this browser. It supports create, apply, update with current filters, rename, and inline-confirmed deletion. Escape closes the non-modal drawer and returns focus to its trigger; focus is not trapped inside it.

The parent app supplies a storage key scoped to the current Grafana organization and user. Views are not shared across browsers or synchronized through Grafana's backend. Clearing browser site data removes them. The store listens for local-storage events so other tabs using the same scope see updates.

Only these fields are stored: search query, environment, cluster, hidden node IDs, operation-error filter, observation window, Map/List view, and Services/Queues list selection. Credentials and unrelated properties are excluded by an explicit whitelist.

Limits: 50 views per scope, names up to 80 characters, query/environment/cluster values up to 256 characters, up to 2,000 hidden node IDs, and a 2 MiB serialized-store limit. Observation windows must be positive integer seconds no greater than one day. Names must be unique within the scope, ignoring case.

Malformed or unsupported stored data produces a visible error and blocks overwriting it until the user explicitly confirms **Reset local saved views**. Storage access or quota failures are visible; an unsuccessful write never displays a saved view or claims success.

Integration:

```tsx
import { SavedViewsDrawer, type ViewFilters } from './components/SavedViews';

<SavedViewsDrawer
  open={viewsOpen}
  onClose={() => setViewsOpen(false)}
  currentFilters={currentFilters as ViewFilters}
  onApply={(view) => applyFilters(view.filters)}
  onSaved={(view) => setActiveViewName(view.name)}
  storageKey={`data-streams.views.${orgId}.${userId}`}
/>;
```

`onSaved` fires after successful create, filter update, or rename of the currently selected view. Renaming an unselected view does not change the active-view badge. Optional `onRemoved(view)` fires after deletion so the parent can clear a matching badge. `onApply` does not close the drawer automatically. The drawer uses inherited `--dsm-*` theme variables and is positioned inside the app with `--dsm-drawer-top` (108px default).
