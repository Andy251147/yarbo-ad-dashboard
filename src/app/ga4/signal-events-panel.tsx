'use client';

import { useState } from 'react';
import { fmt } from '@/lib/utils';

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

interface SignalEventsPanelProps {
  dateRange: { startDate: string; endDate: string };
}

export function SignalEventsPanel({ dateRange }: SignalEventsPanelProps) {
  const [results, setResults] = useState<SignalEvent[]>([]);
  const [loading, setLoading] = useState(false);
  const [minLift, setMinLift] = useState(1.0);

  const handleAnalyze = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/signal-events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(dateRange),
      });
      if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
      const data = await res.json();
      if (data.data) {
        setResults(data.data);
      } else if (data.error) {
        alert(`分析失败: ${data.error}`);
      }
    } catch (e) {
      console.error('Signal events analysis error:', e);
      alert('用户旅程分析失败: ' + (e as Error).message);
    }
    setLoading(false);
  };

  const filtered = results.filter((r) => r.lift >= minLift);

  const getLiftColor = (lift: number) => {
    if (lift >= 2.0) return 'text-emerald-600';
    if (lift >= 1.5) return 'text-emerald-500';
    if (lift >= 1.0) return 'text-amber-500';
    return 'text-rose-500';
  };

  const getLiftBg = (lift: number) => {
    if (lift >= 2.0) return 'bg-emerald-50';
    if (lift >= 1.5) return 'bg-emerald-50';
    if (lift >= 1.0) return 'bg-amber-50';
    return 'bg-rose-50';
  };

  return (
    <div className="bg-white rounded-xl border border-slate-200">
      <div className="px-5 py-3 border-b border-slate-100">
        <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">用户旅程 · 信号事件分析</h2>
      </div>
      <div className="p-5">
        <div className="flex items-center justify-between mb-4 gap-4">
          <div className="text-sm text-slate-500">
            {results.length > 0 ? (
              <>
                发现 <span className="font-semibold text-slate-700">{results.length}</span> 个事件，
                其中 <span className="font-semibold text-emerald-600">{results.filter((r) => r.significant).length}</span> 个显著 (p{'<'}0.05)
              </>
            ) : (
              '分析购买组 vs 未购买组的事件差异，找出高 Lift 的信号事件'
            )}
          </div>
          <div className="flex items-center gap-2">
            {results.length > 0 && (
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-slate-400">Lift {'>'}=</span>
                <input
                  type="number"
                  value={minLift}
                  onChange={(e) => setMinLift(parseFloat(e.target.value) || 0)}
                  step="0.1"
                  min="0"
                  className="w-14 bg-slate-50 border border-slate-200 rounded-md px-2 py-1 text-xs text-slate-600 text-center focus:outline-none focus:border-blue-400"
                />
              </div>
            )}
            <button
              onClick={handleAnalyze}
              disabled={loading}
              className="bg-violet-500 hover:bg-violet-600 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 shadow-sm whitespace-nowrap"
            >
              {loading ? '分析中...' : '分析用户旅程'}
            </button>
          </div>
        </div>

        {filtered.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  {[
                    { label: '事件名称', align: 'text-left' },
                    { label: 'Lift', align: 'text-right' },
                    { label: '购买组率', align: 'text-right' },
                    { label: '未购买组率', align: 'text-right' },
                    { label: '购买组人数', align: 'text-right' },
                    { label: '未购买组人数', align: 'text-right' },
                    { label: 'P 值', align: 'text-right' },
                    { label: '距购买(小时)', align: 'text-right' },
                    { label: '总次数', align: 'text-right' },
                  ].map((h) => (
                    <th key={h.label} className={`py-2.5 px-3 font-medium text-xs text-slate-400 uppercase tracking-wider ${h.align}`}>
                      {h.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.map((row, i) => (
                  <tr key={i} className={`border-b border-slate-100 hover:bg-slate-50/80 transition-colors ${row.significant ? 'bg-violet-50/30' : ''}`}>
                    <td className="py-2.5 px-3">
                      <div className="flex items-center gap-2">
                        <span className="text-slate-700 font-medium text-xs font-mono">{row.eventName}</span>
                        {row.significant && (
                          <span className="text-[10px] bg-violet-100 text-violet-600 px-1.5 py-0.5 rounded-full font-medium">显著</span>
                        )}
                      </div>
                    </td>
                    <td className={`text-right py-2.5 px-3 tabular-nums font-semibold text-xs ${getLiftColor(row.lift)}`}>
                      {row.lift > 0 ? row.lift.toFixed(2) : '-'}
                    </td>
                    <td className="text-right py-2.5 px-3 text-emerald-600 tabular-nums text-xs">{row.purchaseGroupRate}%</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums text-xs">{row.nonPurchaseGroupRate}%</td>
                    <td className="text-right py-2.5 px-3 text-slate-600 tabular-nums text-xs">{fmt(row.purchasersWithEvent)}</td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums text-xs">{fmt(row.nonPurchasersWithEvent)}</td>
                    <td className={`text-right py-2.5 px-3 tabular-nums text-xs ${row.pValue < 0.01 ? 'text-violet-600 font-semibold' : row.pValue < 0.05 ? 'text-violet-500' : 'text-slate-400'}`}>
                      {row.pValue < 0.0001 ? '<0.0001' : row.pValue.toFixed(4)}
                    </td>
                    <td className="text-right py-2.5 px-3 text-slate-500 tabular-nums text-xs">
                      {row.avgHoursBeforePurchase != null ? row.avgHoursBeforePurchase.toFixed(1) : '-'}
                    </td>
                    <td className="text-right py-2.5 px-3 text-slate-400 tabular-nums text-xs">{fmt(row.totalCount)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : results.length > 0 ? (
          <div className="text-center py-8 text-slate-400 text-sm">
            没有 Lift {'>'}= {minLift} 的事件
          </div>
        ) : (
          <div className="text-center py-12 text-slate-400 text-sm">
            点击「分析用户旅程」开始分析
          </div>
        )}
      </div>
    </div>
  );
}
