import React, { useEffect, useState } from 'react';
import { dateTime, FieldType, LoadingState, toDataFrame, type PanelData } from '@grafana/data';
import {
  EmbeddedScene,
  PanelBuilders,
  SceneDataNode,
  SceneFlexItem,
  SceneFlexLayout,
  SceneTimeRange,
} from '@grafana/scenes';

export interface TimeSeriesSample {
  at: number;
  value: number | null;
}
export interface TimeSeriesInput {
  name: string;
  samples: TimeSeriesSample[];
}

/** Feed retained observations to Grafana's standard time-series visualization. */
export default function NativeTimeSeries({
  samples,
  series,
  title,
  now,
  seconds,
  unit = 'ms',
}: {
  samples?: TimeSeriesSample[];
  series?: TimeSeriesInput[];
  title: string;
  now: number;
  seconds: number;
  unit?: string;
}) {
  const [model] = useState(() => {
    const data = new SceneDataNode();
    const range = new SceneTimeRange({
      from: new Date(now - seconds * 1000).toISOString(),
      to: new Date(now).toISOString(),
    });
    const panel = PanelBuilders.timeseries()
      .setTitle('')
      .setUnit(unit)
      .setMin(0)
      .setOption('legend', { showLegend: Boolean(series && series.length > 1) })
      .setCustomFieldConfig('fillOpacity', 8)
      .setData(data)
      .build();
    const scene = new EmbeddedScene({
      $timeRange: range,
      body: new SceneFlexLayout({ direction: 'column', children: [new SceneFlexItem({ body: panel })] }),
    });
    return { scene, data, range, panel };
  });
  useEffect(() => {
    const timeRange = {
      from: dateTime(now - seconds * 1000),
      to: dateTime(now),
      raw: { from: `now-${seconds}s`, to: 'now' },
    };
    const data: PanelData = {
      state: LoadingState.Done,
      timeRange,
      series: (series ?? [{ name: title, samples: samples ?? [] }]).map((line, index) =>
        toDataFrame({
          name: line.name,
          refId: `S${index}`,
          fields: [
            { name: 'Time', type: FieldType.time, values: line.samples.map((sample) => sample.at) },
            {
              name: line.name,
              type: FieldType.number,
              values: line.samples.map((sample) => sample.value),
              config: { unit, min: 0 },
            },
          ],
        })
      ),
    };
    model.data.setState({ data });
    model.range.setState({ from: timeRange.from.toISOString(), to: timeRange.to.toISOString() });
    model.panel.setState({
      options: {
        ...model.panel.state.options,
        legend: {
          ...model.panel.state.options.legend,
          showLegend: Boolean(series && series.length > 1),
        },
      },
    });
  }, [model, samples, series, title, now, seconds, unit]);
  return (
    <div style={{ height: 220, minWidth: 0 }} data-testid="native-timeseries" role="group" aria-label={title}>
      <model.scene.Component model={model.scene} />
    </div>
  );
}
