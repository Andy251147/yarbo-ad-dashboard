import { NextRequest, NextResponse } from 'next/server';
import { getMetrics } from '@/lib/db';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const platform = searchParams.get('platform') || undefined;
  const startDate = searchParams.get('startDate') || undefined;
  const endDate = searchParams.get('endDate') || undefined;

  const metrics = getMetrics({ platform, startDate, endDate });

  const response = NextResponse.json({ data: metrics });
  response.headers.set('Cache-Control', 'public, max-age=30');
  return response;
}
