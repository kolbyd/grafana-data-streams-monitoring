import { css } from '@emotion/css';
import { GrafanaTheme2 } from '@grafana/data';

export const getSavedViewStyles = (theme: GrafanaTheme2) => ({
  shell: css({
    position: 'absolute',
    top: 'var(--dsm-drawer-top,108px)',
    bottom: 0,
    left: 0,
    width: 'min(384px,100%)',
    zIndex: theme.zIndex.dropdown,
    '& > div': { maxWidth: '100%' },
  }),
  body: css({ padding: theme.spacing(2), overflowY: 'auto', flex: 1, minHeight: 0 }),
  muted: css({ color: theme.colors.text.secondary }),
  section: css({ display: 'flex', flexDirection: 'column', gap: theme.spacing(1), marginBottom: theme.spacing(2) }),
  confirm: css({
    padding: theme.spacing(1),
    border: '1px solid ' + theme.colors.border.medium,
    marginTop: theme.spacing(1),
    '& > div': { display: 'flex', flexWrap: 'wrap', gap: theme.spacing(1) },
  }),
  heading: css({ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: theme.spacing(1) }),
  list: css({ listStyle: 'none', padding: 0, margin: 0 }),
  item: css({ padding: theme.spacing(1, 0), borderBottom: '1px solid ' + theme.colors.border.weak }),
  apply: css({
    width: '100%',
    height: 'auto',
    '& > span': {
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'flex-start',
      whiteSpace: 'normal',
      textAlign: 'left',
      gap: theme.spacing(0.5),
    },
  }),
  actions: css({ display: 'flex', gap: theme.spacing(1), marginTop: theme.spacing(1) }),
  notice: css({ marginTop: theme.spacing(2), color: theme.colors.text.secondary }),
});
