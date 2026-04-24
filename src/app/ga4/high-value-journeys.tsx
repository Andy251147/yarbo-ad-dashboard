'use client';

import { useState } from 'react';
import { fmt } from '@/lib/utils';

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

interface HighValueJourneysProps {
  dateRange: { startDate: string; endDate: string };
}

export function HighValueJourneys({ dateRange }: HighValueJourneysProps) {
  const [orders, setOrders] = useState<JourneyOrder[]>([]);
  const [loading, setLoading] = useState(false);
  const [expandedOrder, setExpandedOrder] = useState<string | null>(null);

  const handleFetch = async () => {
    setLoading(true);
    setExpandedOrder(null);
    try {
      const res = await fetch('/api/high-value-journeys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(dateRange),
      });
      if (!res.ok) throw new Error(`API 请求失败: ${res.status}`);
      const data = await res.json();
      if (data.data) {
        setOrders(data.data);
      } else if (data.error) {
        alert(`查询失败: ${data.error}`);
      }
    } catch (e) {
      console.error('High value journeys fetch error:', e);
      alert('高价值订单查询失败: ' + (e as Error).message);
    }
    setLoading(false);
  };

  const totalRevenue = orders.reduce((s, o) => s + o.amount, 0);

  return (
    <div className="bg-white rounded-xl border border-slate-200">
      <div className="px-5 py-3 border-b border-slate-100">
        <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">高价值订单 · 用户旅程追踪</h2>
      </div>
      <div className="p-5">
        <div className="flex items-center justify-between mb-4 gap-4">
          <div className="text-sm text-slate-500">
            {orders.length > 0 ? (
              <>
                共 <span className="font-semibold text-slate-700">{orders.length}</span> 笔订单，
                总金额 <span className="font-semibold text-emerald-600">${fmt(totalRevenue)}</span>
              </>
            ) : (
              '追踪金额 &gt; $3000 订单从首次进站到购买的完整用户旅程'
            )}
          </div>
          <button
            onClick={handleFetch}
            disabled={loading}
            className="bg-emerald-500 hover:bg-emerald-600 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 shadow-sm whitespace-nowrap"
          >
            {loading ? '查询中...' : '查询高价值订单'}
          </button>
        </div>

        {orders.length > 0 ? (
          <div className="space-y-4">
            {orders.map((order, idx) => (
              <OrderJourneyCard
                key={order.transactionId}
                order={order}
                index={idx}
                expanded={expandedOrder === order.transactionId}
                onToggle={() => setExpandedOrder(expandedOrder === order.transactionId ? null : order.transactionId)}
              />
            ))}
          </div>
        ) : (
          <div className="text-center py-12 text-slate-400 text-sm">
            点击「查询高价值订单」拉取金额 &gt; $3000 的订单旅程
          </div>
        )}
      </div>
    </div>
  );
}

function OrderJourneyCard({
  order,
  index,
  expanded,
  onToggle,
}: {
  order: JourneyOrder;
  index: number;
  expanded: boolean;
  onToggle: () => void;
}) {
  const fmtDuration = (sec: number) => {
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m ${sec % 60}s`;
    return `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`;
  };

  const sourceMedium = `${order.sessions[0]?.source || '—'} / ${order.sessions[0]?.medium || '—'}`;

  return (
    <div className="border border-slate-200 rounded-lg overflow-hidden">
      {/* Order header */}
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between px-5 py-3 hover:bg-slate-50/80 transition-colors text-left"
      >
        <div className="flex items-center gap-4">
          <span className="text-xs text-slate-400 font-mono">#{index + 1}</span>
          <div>
            <div className="flex items-center gap-2">
              <span className="text-sm font-semibold text-slate-700">
                订单 {order.transactionId}
              </span>
              <span className="text-lg font-bold text-emerald-600">${fmt(order.amount)}</span>
            </div>
            <div className="text-xs text-slate-400 mt-0.5">
              {order.purchaseDate} · {order.sessions.length} 次会话 · 来源: {sourceMedium}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {/* Products summary */}
          <div className="text-right">
            <div className="text-xs text-slate-500">
              {order.products.slice(0, 2).map(p => `${p.name}${p.quantity > 1 ? ` x${p.quantity}` : ''}`).join(' · ')}
              {order.products.length > 2 && ` +${order.products.length - 2}`}
            </div>
          </div>
          <svg className={`w-5 h-5 text-slate-400 transition-transform ${expanded ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {/* Expanded journey */}
      {expanded && (
        <div className="border-t border-slate-100 bg-slate-50/50 px-5 py-4">
          {/* Session timeline */}
          <div className="space-y-0">
            {order.sessions.map((session, sIdx) => (
              <div key={session.sessionId} className="relative pl-6 pb-4">
                {/* Timeline line */}
                {sIdx < order.sessions.length - 1 && (
                  <div className="absolute left-[5px] top-3 bottom-0 w-px bg-slate-300" />
                )}
                {/* Timeline dot */}
                <div className={`absolute left-0 top-2.5 w-2.5 h-2.5 rounded-full border-2 ${
                  sIdx === order.sessions.length - 1
                    ? 'bg-emerald-500 border-emerald-500'
                    : 'bg-white border-slate-300'
                }`} />

                <div className="bg-white rounded-lg border border-slate-200 p-3">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-semibold text-slate-600">
                        Session {session.sessionId}
                      </span>
                      {sIdx === order.sessions.length - 1 && (
                        <span className="text-[10px] bg-emerald-100 text-emerald-700 px-1.5 py-0.5 rounded-full font-medium">购买</span>
                      )}
                    </div>
                    <div className="text-xs text-slate-400">
                      {session.date} {session.timeRange} · {fmtDuration(session.durationSeconds)}
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2 text-xs">
                    <span className="bg-blue-50 text-blue-600 px-2 py-0.5 rounded">
                      {session.source} / {session.medium}
                    </span>
                    {session.keyEvents.map((ke) => (
                      <span key={ke} className="bg-violet-50 text-violet-600 px-2 py-0.5 rounded font-mono">
                        {ke}
                      </span>
                    ))}
                  </div>

                  {session.pages.length > 0 && (
                    <div className="mt-2 text-xs text-slate-500">
                      访问页面: {session.pages.join(' → ')}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>

          {/* Products detail */}
          <div className="mt-4 pt-3 border-t border-slate-200">
            <div className="text-xs text-slate-400 uppercase tracking-wider mb-2">购买商品</div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-200">
                    <th className="text-left py-1.5 px-2 font-medium text-slate-400">商品</th>
                    <th className="text-right py-1.5 px-2 font-medium text-slate-400">单价</th>
                    <th className="text-right py-1.5 px-2 font-medium text-slate-400">数量</th>
                    <th className="text-right py-1.5 px-2 font-medium text-slate-400">小计</th>
                  </tr>
                </thead>
                <tbody>
                  {order.products.map((p, i) => (
                    <tr key={i} className="border-b border-slate-100">
                      <td className="py-1.5 px-2 text-slate-700">{p.name}</td>
                      <td className="text-right py-1.5 px-2 text-slate-600 tabular-nums">${p.price.toLocaleString()}</td>
                      <td className="text-right py-1.5 px-2 text-slate-600 tabular-nums">{p.quantity}</td>
                      <td className="text-right py-1.5 px-2 text-slate-700 tabular-nums font-medium">${(p.price * p.quantity).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
