'use client';

import { useState, useEffect, useCallback } from 'react';
import Link from 'next/link';
import { fmt, fmtMoney } from '@/lib/utils';
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

const PLATFORMS = [
  { key: 'google', label: 'Google', color: '#3B82F6' },
  { key: 'meta', label: 'Meta', color: '#06B6D4' },
  { key: 'bing', label: 'Bing', color: '#10B981' },
  { key: 'tiktok', label: 'TikTok', color: '#F43F5E' },
  { key: 'ga4', label: 'GA4', color: '#F59E0B' },
];

const PLATFORM_LABELS: Record<string, string> = {};
PLATFORMS.forEach((p) => (PLATFORM_LABELS[p.key] = p.label));

interface SummaryData {
  totals: Record<string, number>;
  byPlatform: any[];
  daily: any[];
}

export default function Dashboard() {
  const [summary, setSummary] = useState<SummaryData | null>(null);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState<string | null>(null);
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

  const handleSync = async (platform: string) => {
    setSyncing(platform);
    try {
      const res = await fetch(`/api/sync/${platform}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(dateRange),
      });
      if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
      const data = await res.json();
      if (data.error) {
        alert(`同步失败: ${data.error}`);
      } else {
        alert(`同步成功: ${data.message}，${data.count} 条数据`);
      }
      fetchSummary();
    } catch (e) {
      alert('同步请求失败: ' + (e as Error).message);
    }
    setSyncing(null);
  };

  const handleSyncAll = async () => {
    const results = await Promise.allSettled(
      PLATFORMS.map(async (p) => {
        const res = await fetch(`/api/sync/${p.key}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(dateRange),
        });
        if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
        return res.json();
      })
    );

    const errors: string[] = [];
    const successes: string[] = [];
    for (const result of results) {
      if (result.status === 'fulfilled') {
        const d = result.value;
        if (d.error) errors.push(d.error);
        else successes.push(`${d.message}，${d.count} 条数据`);
      } else {
        errors.push(result.reason?.message || '请求失败');
      }
    }

    if (errors.length > 0) alert(`部分同步失败:\n${errors.join('\n')}`);
    if (successes.length > 0) alert(`同步完成:\n${successes.join('\n')}`);
    fetchSummary();
  };

  const totalClicks = summary?.totals?.total_clicks ?? 0;
  const totalImpressions = summary?.totals?.total_impressions ?? 0;
  const globalCtr = totalImpressions > 0 ? ((totalClicks / totalImpressions) * 100).toFixed(2) + '%' : '0.00%';
  const totalSpend = summary?.totals?.total_spend ?? 0;
  const globalCpc = totalClicks > 0 ? '$' + (totalSpend / totalClicks).toFixed(2) : '$0.00';

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
            <div className="w-2 h-2 rounded-full bg-blue-500" />
            <h1 className="text-lg font-semibold tracking-wide">
              <span className="text-slate-800">AD </span>
              <span className="text-blue-500">DASHBOARD</span>
            </h1>
          </div>
          <div className="flex gap-2 items-center">
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
            <button
              onClick={handleSyncAll}
              className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-1.5 rounded-lg text-sm font-medium transition-colors shadow-sm"
            >
              同步全部
            </button>
          </div>
        </div>
      </header>

      <main className="relative max-w-7xl mx-auto px-6 py-6 space-y-5">
        {/* 汇总卡片 */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          <SummaryCard label="花费" value={fmtMoney(summary?.totals?.total_spend ?? 0)} accent="blue" />
          <SummaryCard label="展示" value={fmt(summary?.totals?.total_impressions ?? 0)} accent="indigo" />
          <SummaryCard label="点击" value={fmt(summary?.totals?.total_clicks ?? 0)} accent="violet" />
          <SummaryCard label="转化" value={fmt(summary?.totals?.total_conversions ?? 0)} accent="emerald" />
          <SummaryCard label="CTR" value={globalCtr} accent="amber" />
          <SummaryCard label="CPC" value={globalCpc} accent="rose" />
        </div>

        {/* 平台概览 */}
        <Card title="平台概览">
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            {PLATFORMS.map((p) => {
              const pData = summary?.byPlatform.find((d: any) => d.platform === p.key);
              const isGa4 = p.key === 'ga4';

              if (isGa4) {
                return (
                  <Link
                    key={p.key}
                    href="/ga4"
                    className="bg-white rounded-lg p-4 space-y-2 border border-amber-200 hover:border-amber-400 transition-colors cursor-pointer group"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className="w-2 h-2 rounded-full" style={{ backgroundColor: p.color }} />
                        <span className="text-sm font-medium text-slate-700">{p.label}</span>
                      </div>
                      <span className="text-amber-400 opacity-0 group-hover:opacity-100 transition-opacity text-sm">→</span>
                    </div>
                    <div className="text-lg font-semibold text-slate-900">
                      {pData ? fmt(pData.total_sessions ?? 0) : '--'}
                    </div>
                    {pData && (
                      <div className="text-xs text-slate-400">
                        {fmt(pData.activeUsers ?? 0)} users · {fmt(pData.eventCount ?? 0)} events
                      </div>
                    )}
                    <div className="text-xs text-amber-500 group-hover:text-amber-600 transition-colors">
                      查看 GA4 分析
                    </div>
                  </Link>
                );
              }

              return (
                <div
                  key={p.key}
                  className="bg-white rounded-lg p-4 space-y-2 border border-slate-200 hover:border-slate-300 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full" style={{ backgroundColor: p.color }} />
                    <span className="text-sm font-medium text-slate-700">{p.label}</span>
                  </div>
                  <div className="text-lg font-semibold text-slate-900">
                    {pData ? fmtMoney(pData.total_spend ?? 0) : '--'}
                  </div>
                  {pData && (
                    <div className="text-xs text-slate-400">
                      {fmt(pData.total_clicks ?? 0)} clicks · {fmt(pData.total_conversions ?? 0)} conv
                    </div>
                  )}
                  <button
                    onClick={() => handleSync(p.key)}
                    disabled={syncing === p.key}
                    className="text-xs bg-slate-100 hover:bg-slate-200 text-slate-500 px-3 py-1 rounded transition-colors disabled:opacity-50 w-full"
                  >
                    {syncing === p.key ? '同步中...' : '同步'}
                  </button>
                </div>
              );
            })}
          </div>
        </Card>

        {/* 趋势图 */}
        <Card title="每日趋势">
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart
              data={summary?.daily?.map((d: any) => ({
                ...d,
                platform: PLATFORM_LABELS[d.platform] || d.platform,
              }))}
            >
              <defs>
                <linearGradient id="spendGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#3B82F6" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="clickGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#8B5CF6" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#8B5CF6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="date" stroke="#cbd5e1" tick={{ fontSize: 11 }} />
              <YAxis yAxisId="left" stroke="#3B82F6" tick={{ fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" stroke="#8B5CF6" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#fff',
                  border: '1px solid #e2e8f0',
                  borderRadius: '8px',
                  fontSize: '12px',
                  boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
                }}
                labelStyle={{ color: '#64748b' }}
              />
              <Legend wrapperStyle={{ fontSize: '12px' }} />
              <Area type="monotone" dataKey="spend" yAxisId="left" stroke="#3B82F6" fill="url(#spendGrad)" name="花费 ($)" strokeWidth={2} />
              <Area type="monotone" dataKey="clicks" yAxisId="right" stroke="#8B5CF6" fill="url(#clickGrad)" name="点击" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </Card>

        {/* 平台对比 */}
        <Card title="平台对比">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart
              data={summary?.byPlatform?.map((d: any) => ({
                ...d,
                platform: PLATFORM_LABELS[d.platform] || d.platform,
              }))}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="platform" stroke="#cbd5e1" tick={{ fontSize: 11 }} />
              <YAxis stroke="#cbd5e1" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#fff',
                  border: '1px solid #e2e8f0',
                  borderRadius: '8px',
                  fontSize: '12px',
                  boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
                }}
                labelStyle={{ color: '#64748b' }}
              />
              <Legend wrapperStyle={{ fontSize: '12px' }} />
              <Bar dataKey="total_spend" fill="#3B82F6" name="总花费" radius={[4, 4, 0, 0]} />
              <Bar dataKey="total_clicks" fill="#8B5CF6" name="总点击" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Card>

        {/* 数据表格 */}
        <Card title="详细数据">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  {['日期', '平台', '花费', '展示', '点击', '转化', '收入'].map((h) => (
                    <th key={h} className={`py-2.5 px-3 font-medium text-xs text-slate-400 uppercase tracking-wider ${h !== '日期' && h !== '平台' ? 'text-right' : 'text-left'}`}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {summary?.daily?.map((row: any, i: number) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-slate-50/80 transition-colors">
                    <td className="py-2.5 px-3 text-slate-600 font-mono text-xs">{row.date}</td>
                    <td className="py-2.5 px-3">
                      <span className="inline-flex items-center gap-1.5">
                        {PLATFORMS.find((p) => p.key === row.platform) && (
                          <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: PLATFORMS.find((p) => p.key === row.platform)?.color }} />
                        )}
                        <span className="text-slate-600 text-xs">{PLATFORM_LABELS[row.platform] || row.platform}</span>
                      </span>
                    </td>
                    <td className="text-right py-2.5 px-3 text-slate-700 tabular-nums font-medium">{fmtMoney(row.spend)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums">{fmt(row.impressions)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums">{fmt(row.clicks)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums">{fmt(row.conversions)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-700 tabular-nums font-medium">{fmtMoney(row.revenue)}</td>
                  </tr>
                ))}
                {summary?.daily?.length === 0 && (
                  <tr>
                    <td colSpan={7} className="text-center py-12 text-slate-300 text-sm">
                      暂无数据，请先点击「同步全部」拉取数据
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </main>
    </div>
  );
}

function SummaryCard({ label, value, accent }: { label: string; value: string; accent: string }) {
  const colors: Record<string, string> = {
    blue: 'bg-blue-50 border-blue-200 text-blue-600',
    indigo: 'bg-indigo-50 border-indigo-200 text-indigo-600',
    violet: 'bg-violet-50 border-violet-200 text-violet-600',
    emerald: 'bg-emerald-50 border-emerald-200 text-emerald-600',
    amber: 'bg-amber-50 border-amber-200 text-amber-600',
    rose: 'bg-rose-50 border-rose-200 text-rose-600',
  };
  const c = colors[accent] ?? colors.blue;

  return (
    <div className={`bg-white rounded-lg border p-4 ${c}`}>
      <div className="text-xs text-slate-400 uppercase tracking-wider">{label}</div>
      <div className="text-xl font-semibold mt-1 tabular-nums">{value}</div>
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
