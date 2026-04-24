'use client';

import { useState, useEffect, useCallback } from 'react';
import { fmt } from '@/lib/utils';

// ---- Types ----

interface SignalEvent {
  eventName: string;
  totalCount: number;
  purchasersWithEvent: number;
  nonPurchasersWithEvent: number;
  purchaseGroupRate: number;
  nonPurchaseGroupRate: number;
  lift: number;
  zScore: number;
  pValue: number;
  significant: boolean;
  avgHoursBeforePurchase: number | null;
}

interface JourneySession {
  sessionId: string;
  date: string;
  timeRange: string;
  durationSeconds: number;
  source: string;
  medium: string;
  pages: string[];
  keyEvents: string[];
}

interface JourneyOrder {
  userId: string;
  transactionId: string;
  amount: number;
  currency: string;
  purchaseDate: string;
  products: { name: string; price: number; quantity: number }[];
  sessions: JourneySession[];
}

// ---- Data fetching helpers ----

async function fetchSignalEvents(startDate: string, endDate: string): Promise<SignalEvent[]> {
  const res = await fetch('/api/signal-events', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ startDate, endDate }),
  });
  if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
  const data = await res.json();
  return data.data || [];
}

async function fetchHighValueJourneys(startDate: string, endDate: string): Promise<JourneyOrder[]> {
  const res = await fetch('/api/high-value-journeys', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ startDate, endDate }),
  });
  if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
  const data = await res.json();
  return data.data || [];
}

// ---- Page ----

export default function SharePage() {
  const [startDate, setStartDate] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return d.toISOString().split('T')[0];
  });
  const [endDate, setEndDate] = useState(() => {
    return new Date().toISOString().split('T')[0];
  });
  const [signalEvents, setSignalEvents] = useState<SignalEvent[]>([]);
  const [journeys, setJourneys] = useState<JourneyOrder[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [minLift, setMinLift] = useState(1.0);
  const [expandedOrder, setExpandedOrder] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    setExpandedOrder(null);
    try {
      const [signals, j] = await Promise.all([
        fetchSignalEvents(startDate, endDate),
        fetchHighValueJourneys(startDate, endDate),
      ]);
      setSignalEvents(signals);
      setJourneys(j);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  }, [startDate, endDate]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const totalRevenue = journeys.reduce((s, o) => s + o.amount, 0);
  const significantCount = signalEvents.filter(r => r.significant).length;
  const filteredSignals = signalEvents.filter(r => r.lift >= minLift);

  const fmtDuration = (sec: number) => {
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m`;
    return `${Math.floor(sec / 3600)}h${Math.floor((sec % 3600) / 60)}m`;
  };

  const getLiftColor = (lift: number) => {
    if (lift >= 2.0) return 'text-emerald-600';
    if (lift >= 1.5) return 'text-emerald-500';
    if (lift >= 1.0) return 'text-amber-500';
    return 'text-rose-500';
  };

  return (
    <div className="min-h-screen bg-[#f4f5f7] text-slate-800">
      {/* Header */}
      <header className="sticky top-0 z-50 bg-white/80 backdrop-blur-xl border-b border-slate-200">
        <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
          <div>
            <h1 className="text-lg font-semibold tracking-wide text-slate-800">
              GA4 <span className="text-violet-500">用户旅程分析</span>
            </h1>
            <p className="text-xs text-slate-400 mt-0.5">分享链接 · 数据仅供内部参考</p>
          </div>
          <div className="flex gap-2 items-center">
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="bg-slate-50 border border-slate-200 rounded-md px-3 py-1.5 text-sm text-slate-600 focus:outline-none focus:border-violet-400"
            />
            <span className="text-slate-300 text-xs">→</span>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="bg-slate-50 border border-slate-200 rounded-md px-3 py-1.5 text-sm text-slate-600 focus:outline-none focus:border-violet-400"
            />
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-6 space-y-6">
        {/* Loading */}
        {loading && (
          <div className="flex flex-col items-center justify-center py-20 gap-3">
            <div className="w-8 h-8 border-2 border-slate-300 border-t-violet-500 rounded-full animate-spin" />
            <span className="text-slate-400 text-sm">加载中...</span>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="bg-rose-50 border border-rose-200 rounded-lg p-4 text-sm text-rose-700">
            {error}
          </div>
        )}

        {!loading && !error && (
          <>
            {/* Summary cards */}
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-white rounded-xl border border-slate-200 p-5">
                <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">高价值订单</div>
                <div className="text-2xl font-bold text-slate-800">{journeys.length}</div>
                <div className="text-sm text-emerald-600 font-semibold mt-1">${fmt(Math.round(totalRevenue))}</div>
              </div>
              <div className="bg-white rounded-xl border border-slate-200 p-5">
                <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">信号事件</div>
                <div className="text-2xl font-bold text-slate-800">{signalEvents.length}</div>
                <div className="text-sm text-violet-600 font-semibold mt-1">{significantCount} 个显著 (p{'<'}0.05)</div>
              </div>
              <div className="bg-white rounded-xl border border-slate-200 p-5">
                <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">分析周期</div>
                <div className="text-sm text-slate-600">{startDate}</div>
                <div className="text-sm text-slate-600">→ {endDate}</div>
              </div>
            </div>

            {/* Signal Events Table */}
            <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
              <div className="px-5 py-3 border-b border-slate-100 flex items-center justify-between">
                <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">信号事件 · 购买组 vs 未购买组</h2>
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-slate-400">Lift ≥</span>
                  <input
                    type="number"
                    value={minLift}
                    onChange={(e) => setMinLift(parseFloat(e.target.value) || 0)}
                    step="0.1"
                    min="0"
                    className="w-14 bg-slate-50 border border-slate-200 rounded-md px-2 py-1 text-xs text-slate-600 text-center focus:outline-none focus:border-violet-400"
                  />
                </div>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-slate-200">
                      <th className="text-left py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">事件</th>
                      <th className="text-right py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">Lift</th>
                      <th className="text-right py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">购买组率</th>
                      <th className="text-right py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">未购买组率</th>
                      <th className="text-right py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">P 值</th>
                      <th className="text-right py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">距购买</th>
                      <th className="text-right py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">购买组</th>
                      <th className="text-right py-2.5 px-3 font-medium text-slate-400 uppercase tracking-wider">未购买组</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredSignals.map((row, i) => (
                      <tr key={i} className={`border-b border-slate-100 ${row.significant ? 'bg-violet-50/30' : ''}`}>
                        <td className="py-2 px-3">
                          <span className="font-mono text-slate-700 font-medium">{row.eventName}</span>
                          {row.significant && (
                            <span className="ml-1 text-[10px] bg-violet-100 text-violet-600 px-1 py-0.5 rounded-full">显著</span>
                          )}
                        </td>
                        <td className={`text-right py-2 px-3 tabular-nums font-semibold ${getLiftColor(row.lift)}`}>
                          {row.lift > 0 ? row.lift.toFixed(2) : '-'}
                        </td>
                        <td className="text-right py-2 px-3 text-emerald-600 tabular-nums">{row.purchaseGroupRate}%</td>
                        <td className="text-right py-2 px-3 text-slate-500 tabular-nums">{row.nonPurchaseGroupRate}%</td>
                        <td className={`text-right py-2 px-3 tabular-nums ${row.pValue < 0.01 ? 'text-violet-600 font-semibold' : row.pValue < 0.05 ? 'text-violet-500' : 'text-slate-400'}`}>
                          {row.pValue < 0.0001 ? '<0.0001' : row.pValue.toFixed(4)}
                        </td>
                        <td className="text-right py-2 px-3 text-slate-500 tabular-nums">
                          {row.avgHoursBeforePurchase != null ? `${row.avgHoursBeforePurchase.toFixed(1)}h` : '-'}
                        </td>
                        <td className="text-right py-2 px-3 text-slate-600 tabular-nums">{fmt(row.purchasersWithEvent)}</td>
                        <td className="text-right py-2 px-3 text-slate-500 tabular-nums">{fmt(row.nonPurchasersWithEvent)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {filteredSignals.length === 0 && (
                <div className="text-center py-8 text-slate-400 text-sm">没有 Lift ≥ {minLift} 的事件</div>
              )}
            </div>

            {/* High Value Journeys */}
            <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
              <div className="px-5 py-3 border-b border-slate-100">
                <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">高价值订单 · 用户旅程（&gt;$3000）</h2>
              </div>
              {journeys.length > 0 ? (
                <div className="divide-y divide-slate-100">
                  {journeys.map((order, idx) => (
                    <div key={order.transactionId}>
                      <button
                        onClick={() => setExpandedOrder(expandedOrder === order.transactionId ? null : order.transactionId)}
                        className="w-full flex items-center justify-between px-5 py-3 hover:bg-slate-50/80 transition-colors text-left"
                      >
                        <div className="flex items-center gap-3">
                          <span className="text-xs text-slate-400 font-mono">#{idx + 1}</span>
                          <span className="text-sm font-semibold text-slate-700">{order.transactionId}</span>
                          <span className="text-base font-bold text-emerald-600">${fmt(Math.round(order.amount))}</span>
                          <span className="text-xs text-slate-400">{order.purchaseDate}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-slate-500">{order.sessions.length} sessions · {order.products.length} items</span>
                          <svg className={`w-4 h-4 text-slate-400 transition-transform ${expandedOrder === order.transactionId ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                          </svg>
                        </div>
                      </button>
                      {expandedOrder === order.transactionId && (
                        <div className="px-5 pb-4 bg-slate-50/50">
                          {/* Products */}
                          <div className="mb-3">
                            <div className="text-[10px] text-slate-400 uppercase tracking-wider mb-1">商品</div>
                            <div className="flex flex-wrap gap-1.5">
                              {order.products.map((p, i) => (
                                <span key={i} className="text-[11px] bg-white border border-slate-200 rounded px-2 py-0.5 text-slate-600">
                                  {p.name}{p.quantity > 1 ? ` x${p.quantity}` : ''} <span className="text-slate-400">${fmt(p.price * p.quantity)}</span>
                                </span>
                              ))}
                            </div>
                          </div>
                          {/* Sessions timeline */}
                          <div className="text-[10px] text-slate-400 uppercase tracking-wider mb-2">用户旅程</div>
                          <div className="space-y-0">
                            {order.sessions.map((s, sIdx) => (
                              <div key={s.sessionId} className="relative pl-4 pb-2">
                                {sIdx < order.sessions.length - 1 && (
                                  <div className="absolute left-[3px] top-2.5 bottom-0 w-px bg-slate-200" />
                                )}
                                <div className="absolute left-0 top-2 w-1.5 h-1.5 rounded-full border border-slate-300 bg-white" />
                                <div className="text-[11px] text-slate-500">
                                  <span className="font-mono">{s.date} {s.timeRange}</span>
                                  <span className="mx-1">·</span>
                                  <span>{s.source}/{s.medium}</span>
                                  <span className="mx-1">·</span>
                                  <span>{fmtDuration(s.durationSeconds)}</span>
                                  {s.keyEvents.length > 0 && (
                                    <span>
                                      <span className="mx-1">·</span>
                                      <span className="text-violet-500">{s.keyEvents.slice(0, 5).join(', ')}{s.keyEvents.length > 5 ? '...' : ''}</span>
                                    </span>
                                  )}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-slate-400 text-sm">暂无 &gt;$3000 的订单</div>
              )}
            </div>
          </>
        )}

        {/* Footer */}
        <div className="text-center py-4 text-[10px] text-slate-300">
          数据来源：GA4 BigQuery Export · 自动生成 · 仅供内部参考
        </div>
      </main>
    </div>
  );
}
