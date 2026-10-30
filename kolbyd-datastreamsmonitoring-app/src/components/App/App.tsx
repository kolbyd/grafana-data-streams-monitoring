import React from 'react';
import { AppRootProps } from '@grafana/data';
import Explorer from '../Explorer/Explorer';

function App(props: AppRootProps) {
  return <Explorer {...props} />;
}

export default App;
