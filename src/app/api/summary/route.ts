import { NextRequest, NextResponse } from 'next/server';
import { getGlobalSummary, getDailySummary } from '@/lib/bq-analysis';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const startDate = searchParams.get('startDate') || undefined;
  const endDate = searchParams.get('endDate') || undefined;

  const [global, daily] = await Promise.all([
    getGlobalSummary({ startDate, endDate }),
    getDailySummary({ startDate, endDate }),
  ]);

  const totals = (global as any[]).reduce(
    (acc: Record<string, number>, row: any) => ({
      total_spend: (acc.total_spend || 0) + (Number(row.total_spend) || 0),
      total_impressions: (acc.total_impressions || 0) + (Number(row.total_impressions) || 0),
      total_clicks: (acc.total_clicks || 0) + (Number(row.total_clicks) || 0),
      total_conversions: (acc.total_conversions || 0) + (Number(row.total_conversions) || 0),
      total_revenue: (acc.total_revenue || 0) + (Number(row.total_revenue) || 0),
    }),
    {} as Record<string, number>
  );

  const response = NextResponse.json({
    totals,
    byPlatform: global,
    daily: daily || [],
  });
  response.headers.set('Cache-Control', 'public, max-age=30');
  return response;
}
