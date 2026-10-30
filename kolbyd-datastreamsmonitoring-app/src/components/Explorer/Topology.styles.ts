import { css } from '@emotion/css';

// Domain geometry stays scoped to its viewport and uses the active Grafana theme.
export const topologyStyles = css`
  &.dsm-topology-viewport {
    position: relative;
    width: 100%;
    height: 100%;
    min-width: 0;
    min-height: 160px;
    overflow: hidden;
    touch-action: none;
    cursor: grab;
    user-select: none;
  }
  &.is-dragging,
  &.is-dragging .dsm-node,
  &.is-dragging .dsm-edge-hit {
    cursor: grabbing;
  }
  & > svg {
    display: block;
    width: 100%;
    height: 100%;
  }
  & .dsm-connection {
    color: var(--dsm-muted);
  }
  & .dsm-connection.is-path {
    color: var(--dsm-blue);
  }
  & .dsm-connection.has-error {
    color: var(--dsm-error);
  }
  & .dsm-connection:hover,
  & .dsm-connection:focus-within {
    color: var(--dsm-blue);
  }
  & .dsm-connection.has-error:hover,
  & .dsm-connection.has-error:focus-within {
    color: var(--dsm-error);
  }
  & .dsm-connection .dsm-edge {
    fill: none;
    stroke: currentColor;
    stroke-linecap: round;
    stroke-linejoin: round;
    pointer-events: none;
    opacity: 0.85;
  }
  & .dsm-connection:hover .dsm-edge,
  & .dsm-connection:focus-within .dsm-edge {
    opacity: 1;
    stroke-width: 3;
  }
  & .dsm-edge-separation {
    fill: none;
    stroke: var(--dsm-ground);
    stroke-width: 7;
    pointer-events: none;
  }
  & .dsm-edge-hit {
    fill: none;
    stroke: transparent;
    stroke-width: 18;
    pointer-events: stroke;
    cursor: help;
    outline: none;
  }
  & .dsm-particles {
    pointer-events: none;
  }
  & .dsm-observation-particle {
    color: var(--dsm-blue);
  }
  & .dsm-observation-particle.has-error {
    color: var(--dsm-error);
  }
  & .dsm-particle-halo {
    fill: currentColor;
    opacity: 0.22;
  }
  & .dsm-particle-core {
    fill: currentColor;
    stroke: var(--dsm-ground);
    stroke-width: 1.6;
  }
  @media (prefers-reduced-motion: reduce) {
    & .dsm-particles {
      display: none;
    }
  }
`;

// Tooltip content is portaled outside the SVG viewport, so it owns its scope.
export const connectionTooltipStyles = css`
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-width: 420px;
  overflow-wrap: anywhere;

  & strong {
    font-weight: 500;
  }
`;
