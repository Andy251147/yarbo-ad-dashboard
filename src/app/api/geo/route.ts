import { NextRequest, NextResponse } from 'next/server';
import { fetchGA4GeoData } from '@/lib/ingestion';

export async function POST(request: NextRequest) {
  const body = await request.json().catch(() => ({}));
  const { startDate, endDate } = body;

  const end = endDate || new Date().toISOString().split('T')[0];
  const start = startDate || (() => {
    const d = new Date();
    d.setDate(d.getDate() - 7);
    return d.toISOString().split('T')[0];
  })();

  try {
    const data = await fetchGA4GeoData(start, end);
    return NextResponse.json({ data, dateRange: { start, end } });
  } catch (error) {
    console.error('Geo sync failed:', error);
    return NextResponse.json(
      { error: (error as Error).message },
      { status: 500 }
    );
  }
}
