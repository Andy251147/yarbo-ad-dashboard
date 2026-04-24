'use client';

import { useState, useEffect, useCallback } from 'react';
import { fmt, fmtMoney, formatDuration } from '@/lib/utils';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import {
  ComposableMap,
  Geographies,
  Geography,
} from 'react-simple-maps';
import { feature } from 'topojson-client';
import countries110m from 'world-atlas/countries-110m.json';
import Link from 'next/link';
import { SignalEventsPanel } from './signal-events-panel';
import { HighValueJourneys } from './high-value-journeys';

interface SummaryData {
  totals: Record<string, number>;
  byPlatform: any[];
  daily: any[];
}

export default function Ga4Page() {
  const [summary, setSummary] = useState<SummaryData | null>(null);
  const [loading, setLoading] = useState(true);
  const [syncingChannel, setSyncingChannel] = useState(false);
  const [channelData, setChannelData] = useState<any[]>([]);
  const [dateRange, setDateRange] = useState({
    startDate: (() => {
      const d = new Date();
      d.setDate(d.getDate() - 7);
      return d.toISOString().split('T')[0];
    })(),
    endDate: new Date().toISOString().split('T')[0],
  });

  const fetchSummary = useCallback(async () => {
    setLoading(true);
    const params = new URLSearchParams();
    if (dateRange.startDate) params.set('startDate', dateRange.startDate);
    if (dateRange.endDate) params.set('endDate', dateRange.endDate);

    const res = await fetch(`/api/summary?${params}`);
    if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
    const data = await res.json();
    setSummary(data);
    setLoading(false);
  }, [dateRange]);

  useEffect(() => {
    fetchSummary();
  }, [fetchSummary]);

  const handleSyncChannels = async () => {
    setSyncingChannel(true);
    try {
      const res = await fetch('/api/ga4-source-medium', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(dateRange),
      });
      if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
      const data = await res.json();
      console.log('Channel sync response:', data);
      if (data.error) {
        alert(`同步失败: ${data.error}`);
      } else if (data.data) {
        setChannelData(data.data);
      } else {
        alert('未获取到数据');
      }
    } catch (e) {
      console.error('Channel sync error:', e);
      alert('渠道质量数据同步失败: ' + (e as Error).message);
    }
    setSyncingChannel(false);
  };

  if (loading && !summary) {
    return (
      <div className="min-h-screen bg-[#f4f5f7] flex items-center justify-center">
        <div className="flex flex-col items-center gap-3">
          <div className="w-6 h-6 border-2 border-slate-300 border-t-slate-500 rounded-full animate-spin" />
          <span className="text-slate-400 text-sm">加载中...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#f4f5f7] text-slate-800">
      {/* Header */}
      <header className="sticky top-0 z-50 bg-white/80 backdrop-blur-xl border-b border-slate-200">
        <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <Link href="/" className="text-slate-400 hover:text-slate-600 transition-colors">
              ← 返回看板
            </Link>
            <span className="text-slate-200">|</span>
            <h1 className="text-lg font-semibold tracking-wide text-slate-800">
              GA4 <span className="text-amber-500">分析</span>
            </h1>
          </div>
          <div className="flex gap-2 items-center">
            <Link href="/share" className="text-slate-400 hover:text-slate-600 transition-colors text-sm" title="分享看板">
              ↗ 分享
            </Link>
            <input
              type="date"
              value={dateRange.startDate}
              onChange={(e) => setDateRange({ ...dateRange, startDate: e.target.value })}
              className="bg-slate-50 border border-slate-200 rounded-md px-3 py-1.5 text-sm text-slate-600 focus:outline-none focus:border-blue-400"
            />
            <span className="text-slate-300 text-xs">→</span>
            <input
              type="date"
              value={dateRange.endDate}
              onChange={(e) => setDateRange({ ...dateRange, endDate: e.target.value })}
              className="bg-slate-50 border border-slate-200 rounded-md px-3 py-1.5 text-sm text-slate-600 focus:outline-none focus:border-blue-400"
            />
          </div>
        </div>
      </header>

      <main className="relative max-w-7xl mx-auto px-6 py-6 space-y-5">
        <Ga4Panel daily={summary?.daily?.filter((d: any) => d.platform === 'ga4') ?? []} />
        <HighValueJourneys dateRange={dateRange} />
        <SourceMediumQuality dateRange={dateRange} channelData={channelData} syncing={syncingChannel} onSync={handleSyncChannels} />
        <GeoHeatmap dateRange={dateRange} />
        <SignalEventsPanel dateRange={dateRange} />
      </main>
    </div>
  );
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-white rounded-xl border border-slate-200">
      <div className="px-5 py-3 border-b border-slate-100">
        <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">{title}</h2>
      </div>
      <div className="p-5">{children}</div>
    </div>
  );
}

function Ga4MetricCard({ label, value, accent }: { label: string; value: string; accent: string }) {
  const colors: Record<string, string> = {
    cyan: 'bg-cyan-50 border-cyan-200 text-cyan-600',
    blue: 'bg-blue-50 border-blue-200 text-blue-600',
    violet: 'bg-violet-50 border-violet-200 text-violet-600',
    emerald: 'bg-emerald-50 border-emerald-200 text-emerald-600',
    amber: 'bg-amber-50 border-amber-200 text-amber-600',
    rose: 'bg-rose-50 border-rose-200 text-rose-600',
    indigo: 'bg-indigo-50 border-indigo-200 text-indigo-600',
  };
  const c = colors[accent] ?? colors.blue;

  return (
    <div className={`bg-white rounded-lg border p-4 ${c}`}>
      <div className="text-xs text-slate-400 uppercase tracking-wider">{label}</div>
      <div className="text-xl font-semibold mt-1 tabular-nums">{value}</div>
    </div>
  );
}

function Ga4Panel({ daily }: { daily: any[] }) {
  const total = daily.reduce(
    (acc: any, row: any) => ({
      sessions: acc.sessions + (row.sessions || 0),
      activeUsers: acc.activeUsers + (row.activeUsers || 0),
      totalUsers: acc.totalUsers + (row.totalUsers || 0),
      eventCount: acc.eventCount + (row.eventCount || 0),
      engagedSessions: acc.engagedSessions + (row.engagedSessions || 0),
      conversions: acc.conversions + (row.conversions || 0),
    }),
    { sessions: 0, activeUsers: 0, totalUsers: 0, eventCount: 0, engagedSessions: 0, conversions: 0 }
  );

  const engagementRate = total && total.sessions > 0 ? ((total.engagedSessions / total.sessions) * 100).toFixed(2) + '%' : '0%';

  return (
    <>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Ga4MetricCard label="Sessions" value={fmt(total.sessions)} accent="cyan" />
        <Ga4MetricCard label="Active Users" value={fmt(total.activeUsers)} accent="blue" />
        <Ga4MetricCard label="Total Users" value={fmt(total.totalUsers)} accent="violet" />
        <Ga4MetricCard label="Event Count" value={fmt(total.eventCount)} accent="emerald" />
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Ga4MetricCard label="Engaged Sessions" value={fmt(total.engagedSessions)} accent="amber" />
        <Ga4MetricCard label="Engagement Rate" value={engagementRate} accent="rose" />
        <Ga4MetricCard label="Conversions" value={fmt(total.conversions)} accent="indigo" />
      </div>

      <Card title="GA4 每日趋势">
        <ResponsiveContainer width="100%" height={260}>
          <AreaChart data={daily}>
            <defs>
              <linearGradient id="ga4Sessions" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#06B6D4" stopOpacity={0.2} />
                <stop offset="95%" stopColor="#06B6D4" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="ga4Users" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.2} />
                <stop offset="95%" stopColor="#3B82F6" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
            <XAxis dataKey="date" stroke="#cbd5e1" tick={{ fontSize: 11 }} />
            <YAxis stroke="#cbd5e1" tick={{ fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#fff', border: '1px solid #e2e8f0', borderRadius: '8px', fontSize: '12px', boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}
              labelStyle={{ color: '#64748b' }}
            />
            <Legend wrapperStyle={{ fontSize: '12px' }} />
            <Area type="monotone" dataKey="sessions" stroke="#06B6D4" fill="url(#ga4Sessions)" name="Sessions" strokeWidth={2} />
            <Area type="monotone" dataKey="activeUsers" stroke="#3B82F6" fill="url(#ga4Users)" name="活跃用户" strokeWidth={2} />
          </AreaChart>
        </ResponsiveContainer>
      </Card>

      <Card title="GA4 详细数据">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200">
                {['日期', 'Sessions', '活跃用户', '总用户', '事件数', '互动会话', '互动率', '转化'].map((h) => (
                  <th key={h} className="text-right py-2.5 px-3 font-medium text-xs text-slate-400 uppercase tracking-wider">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {daily.map((row: any, i: number) => {
                const er = row.sessions > 0 ? ((row.engagedSessions / row.sessions) * 100).toFixed(1) + '%' : '-';
                return (
                  <tr key={i} className="border-b border-slate-100 hover:bg-slate-50/80 transition-colors">
                    <td className="py-2.5 px-3 text-slate-600 font-mono text-xs">{row.date}</td>
                    <td className="text-right py-2.5 px-3 text-slate-700 tabular-nums font-medium">{fmt(row.sessions)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-600 tabular-nums">{fmt(row.activeUsers)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums">{fmt(row.totalUsers)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-600 tabular-nums">{fmt(row.eventCount)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums">{fmt(row.engagedSessions)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums">{er}</td>
                    <td className="text-right py-2.5 px-3 text-slate-700 tabular-nums font-medium">{fmt(row.conversions)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </>
  );
}

interface SourceMediumProps {
  dateRange: { startDate: string; endDate: string };
  channelData: any[];
  syncing: boolean;
  onSync: () => void;
}

function SourceMediumQuality({ dateRange, channelData, syncing, onSync }: SourceMediumProps) {
  return (
    <Card title="各渠道的流量质量">
      <div className="flex items-center justify-between mb-4">
        <div className="text-sm text-slate-500">
          共 <span className="font-semibold text-slate-700">{channelData.length}</span> 个渠道来源
        </div>
        <button
          onClick={onSync}
          disabled={syncing}
          className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 shadow-sm"
        >
          {syncing ? '同步中...' : '同步渠道数据'}
        </button>
      </div>
      {channelData.length > 0 ? (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200">
                {['来源/媒介', '总购买收入', '会话数', '跳出率', '每会话浏览', '平均时长', '新用户', '活跃用户', '总用户'].map((h) => (
                  <th key={h} className={`py-2.5 px-3 font-medium text-xs text-slate-400 uppercase tracking-wider ${h !== '来源/媒介' ? 'text-right' : 'text-left'}`}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {channelData.map((row, i) => (
                <tr key={i} className="border-b border-slate-100 hover:bg-slate-50/80 transition-colors">
                  <td className="py-2.5 px-3 text-slate-700 font-medium text-xs">{row.sourceMedium}</td>
                  <td className="text-right py-2.5 px-3 text-emerald-600 tabular-nums font-medium text-xs">{fmtMoney(row.grossPurchaseRevenue)}</td>
                  <td className="text-right py-2.5 px-3 text-slate-700 tabular-nums font-medium text-xs">{fmt(row.sessions)}</td>
                  <td className="text-right py-2.5 px-3 text-rose-500 tabular-nums text-xs">{(row.bounceRate * 100).toFixed(1)}%</td>
                  <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums text-xs">{row.screenPageViewsPerSession.toFixed(1)}</td>
                  <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums text-xs">{formatDuration(row.averageSessionDuration)}</td>
                  <td className="text-right py-2.5 px-3 text-blue-500 tabular-nums text-xs">{fmt(row.newUsers)}</td>
                  <td className="text-right py-2.5 px-3 text-violet-500 tabular-nums text-xs">{fmt(row.activeUsers)}</td>
                  <td className="text-right py-2.5 px-3 text-slate-600 tabular-nums font-medium text-xs">{fmt(row.totalUsers)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="text-center py-12 text-slate-400 text-sm">点击「同步渠道数据」拉取各渠道流量质量</div>
      )}
    </Card>
  );
}

interface GeoData {
  country: string;
  countryCode: string;
  sessions: number;
  activeUsers: number;
  totalUsers: number;
  eventCount: number;
}

const countryMap: Record<string, string> = {
  US: '840', CN: '156', IN: '356', BR: '076', RU: '643',
  JP: '392', DE: '276', GB: '826', FR: '250', IT: '380',
  CA: '124', AU: '036', KR: '410', MX: '484', ES: '724',
  ID: '360', TR: '792', SA: '682', AR: '032', PL: '616',
  TH: '764', NL: '528', EG: '818', MY: '458', PH: '608',
  PK: '586', BD: '050', VN: '704', CO: '170', ZA: '710',
  CH: '756', SE: '752', BE: '056', AT: '040', NO: '578',
  DK: '208', FI: '246', SG: '702', AE: '784', IL: '376',
  CL: '152', NZ: '554', IR: '364', UA: '804', PE: '604',
  VE: '862', PT: '620', RO: '642', GR: '300', CZ: '203',
  HU: '348', IE: '372', KZ: '398', QA: '634', KE: '404',
  NG: '566', ET: '231', GH: '288', TZ: '834', UG: '800',
  LK: '144', NP: '524', MA: '504', TN: '788', DZ: '012',
  EC: '218', GT: '320', CR: '188', PA: '591', DO: '214',
  HN: '340', NI: '558', SV: '222', PY: '600', UY: '858',
  BO: '068', HR: '191', RS: '688', BG: '100', SK: '703',
  LT: '440', LV: '428', EE: '233', SI: '705', AL: '008',
  MK: '807', ME: '499', BA: '070', IS: '352', LU: '442',
  MT: '470', CY: '196', GE: '268', AM: '051', AZ: '031',
  BH: '048', KW: '414', OM: '512', JO: '400', LB: '422',
  IQ: '368', SY: '760', PS: '275', YE: '887', AF: '004',
  UZ: '860', TM: '795', TJ: '762', KG: '417', MN: '496',
  LY: '434', SD: '729', SS: '728', SO: '706', DJ: '262',
  ER: '232', CD: '180', CG: '178', CM: '120',
  CI: '384', SN: '686', ML: '466', BF: '854', NE: '562',
  TD: '148', TG: '768', BJ: '204', GA: '266', AO: '024',
  MZ: '508', ZW: '716', ZM: '894', BW: '072', NA: '516',
  MW: '454', RW: '646', BI: '108', MG: '450', MU: '480',
  SC: '690', CV: '132', SL: '694', LR: '430', GN: '324',
  GM: '270', GW: '624', MR: '478', ST: '678', KM: '174',
  TW: '158', HK: '344', MO: '446',
};

const nameAliases: Record<string, string> = {
  'United States': 'United States of America',
  'Czechia': 'Czech Republic',
  'Congo - Kinshasa': 'Dem. Rep. Congo',
  'Congo - Brazzaville': 'Congo',
  'Russia': 'Russia',
  'Turkey': 'Turkey',
  'Iran': 'Iran',
  'Egypt': 'Egypt',
};

function GeoHeatmap({ dateRange }: { dateRange: { startDate: string; endDate: string } }) {
  const [geoData, setGeoData] = useState<GeoData[]>([]);
  const [loading, setLoading] = useState(false);
  const [topoData, setTopoData] = useState<any>(null);
  const [tooltip, setTooltip] = useState<{ x: number; y: number; country: string; sessions: number } | null>(null);

  useEffect(() => {
    const geojson = feature(countries110m as any, (countries110m as any).objects.countries);
    setTopoData(geojson);
  }, []);

  const handleSyncGeo = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/geo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(dateRange),
      });
      if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
      const data = await res.json();
      if (data.data) setGeoData(data.data);
    } catch (e) {
      alert('地理位置数据同步失败');
    }
    setLoading(false);
  };

  const maxSessions = Math.max(...geoData.map((d) => d.sessions), 1);
  const getColor = (sessions: number) => {
    const ratio = sessions / maxSessions;
    if (ratio === 0) return '#e2e8f0';
    if (ratio < 0.1) return '#dbeafe';
    if (ratio < 0.25) return '#93c5fd';
    if (ratio < 0.5) return '#60a5fa';
    if (ratio < 0.75) return '#3b82f6';
    return '#1d4ed8';
  };
  const totalSessions = geoData.reduce((s, d) => s + d.sessions, 0);

  return (
    <Card title="地理位置">
      <div className="flex items-center justify-between mb-4">
        <div className="text-sm text-slate-500">
          覆盖 <span className="font-semibold text-slate-700">{geoData.length}</span> 个国家/地区，
          共 <span className="font-semibold text-slate-700">{fmt(totalSessions)}</span> sessions
        </div>
        <button
          onClick={handleSyncGeo}
          disabled={loading}
          className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 shadow-sm"
        >
          {loading ? '同步中...' : '同步地理数据'}
        </button>
      </div>
      {topoData ? (
        <div className="relative w-full overflow-hidden rounded-lg" style={{ height: 500 }}>
          <style>{`.map-container svg { width: 100% !important; height: 100% !important; }`}</style>
          <div className="map-container w-full h-full">
          <ComposableMap projectionConfig={{ rotate: [-10, 0, 0], scale: 170 }} width={1200} height={500} style={{ width: '100%', height: '100%' }}>
            <Geographies geography={topoData}>
                {({ geographies }) =>
                  geographies.map((geo: any) => {
                    const code = geo.id;
                    const entry = geoData.find((d) => {
                      if (countryMap[d.countryCode] === code) return true;
                      const aliased = nameAliases[d.country];
                      if (aliased && aliased === geo.properties.name) return true;
                      return false;
                    });
                    const sessions = entry?.sessions || 0;
                    return (
                      <Geography
                        key={geo.rsmKey}
                        geography={geo}
                        fill={getColor(sessions)}
                        stroke="#cbd5e1"
                        strokeWidth={0.3}
                        style={{
                          default: { outline: 'none' },
                          hover: { fill: '#1e40af', outline: 'none', cursor: 'pointer' },
                          pressed: { outline: 'none' },
                        }}
                        onMouseEnter={(e: React.MouseEvent) => {
                          const rect = (e.currentTarget as SVGElement).closest('.rsm-svg')?.getBoundingClientRect();
                          if (rect) {
                            setTooltip({
                              x: e.clientX - rect.left,
                              y: e.clientY - rect.top,
                              country: entry?.country || geo.properties.name,
                              sessions,
                            });
                          }
                        }}
                        onMouseMove={(e: React.MouseEvent) => {
                          const rect = (e.currentTarget as SVGElement).closest('.rsm-svg')?.getBoundingClientRect();
                          if (rect) {
                            setTooltip((prev) =>
                              prev ? { ...prev, x: e.clientX - rect.left, y: e.clientY - rect.top } : null
                            );
                          }
                        }}
                        onMouseLeave={() => setTooltip(null)}
                      />
                    );
                  })
                }
              </Geographies>
          </ComposableMap>
          </div>
          {tooltip && (
            <div
              className="absolute pointer-events-none bg-slate-800 text-white text-xs rounded-md px-3 py-1.5 shadow-lg z-50"
              style={{ left: tooltip.x + 12, top: tooltip.y - 36, whiteSpace: 'nowrap' }}
            >
              <div className="font-semibold">{tooltip.country}</div>
              <div className="text-slate-300">{fmt(tooltip.sessions)} sessions</div>
            </div>
          )}
        </div>
      ) : (
        <div className="h-[500px] flex items-center justify-center text-slate-300 text-sm rounded-lg">加载地图数据中...</div>
      )}
      {geoData.length > 0 && (
        <div className="mt-4 flex items-center gap-3 justify-center">
          <span className="text-xs text-slate-400">低</span>
          <div className="flex gap-0.5">
            {['#e2e8f0', '#dbeafe', '#93c5fd', '#60a5fa', '#3b82f6', '#1d4ed8'].map((c) => (
              <div key={c} className="w-6 h-3 rounded-sm" style={{ backgroundColor: c }} />
            ))}
          </div>
          <span className="text-xs text-slate-400">高</span>
        </div>
      )}
      {geoData.length > 0 && (
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200">
                {['排名', '国家', 'Sessions', '活跃用户', '总用户', '事件数'].map((h) => (
                  <th key={h} className={`py-2 px-3 font-medium text-xs text-slate-400 uppercase tracking-wider ${['Sessions', '活跃用户', '总用户', '事件数'].includes(h) ? 'text-right' : 'text-left'}`}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {geoData.slice(0, 20).map((row, i) => (
                <tr key={i} className="border-b border-slate-100 hover:bg-slate-50/80 transition-colors">
                  <td className="py-2 px-3 text-slate-400 text-xs">{i + 1}</td>
                  <td className="py-2 px-3 text-slate-700 font-medium">{row.country}</td>
                  <td className="text-right py-2 px-3 text-slate-700 tabular-nums font-medium">{fmt(row.sessions)}</td>
                  <td className="text-right py-2 px-3 text-slate-500 tabular-nums">{fmt(row.activeUsers)}</td>
                  <td className="text-right py-2 px-3 text-slate-500 tabular-nums">{fmt(row.totalUsers)}</td>
                  <td className="text-right py-2 px-3 text-slate-500 tabular-nums">{fmt(row.eventCount)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {geoData.length === 0 && (
        <div className="text-center py-12 text-slate-400 text-sm">点击「同步地理数据」拉取按国家分布的流量</div>
      )}
    </Card>
  );
}
