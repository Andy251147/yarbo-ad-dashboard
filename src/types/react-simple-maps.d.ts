declare module 'react-simple-maps' {
  import * as React from 'react';
  interface ComposableMapProps {
    projectionConfig?: any;
    projection?: string;
    children?: React.ReactNode;
    style?: React.CSSProperties;
    width?: number | string;
    height?: number | string;
  }
  export const ComposableMap: React.FC<ComposableMapProps>;
  export const ZoomableGroup: React.FC<{ center?: [number, number]; zoom?: number; children?: React.ReactNode }>;
  export const Geographies: React.FC<{ geography: any; children: (props: { geographies: any[] }) => React.ReactNode }>;
  export const Geography: React.FC<{ geography: any; fill?: string; stroke?: string; strokeWidth?: number; style?: any; onMouseEnter?: (e: React.MouseEvent) => void; onMouseMove?: (e: React.MouseEvent) => void; onMouseLeave?: () => void }>;
}
declare module 'topojson-client' {
  export function feature(topology: any, object: any): any;
}
declare module 'world-atlas/countries-110m.json' {
  const value: any;
  export default value;
}
