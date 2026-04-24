import { NextRequest, NextResponse } from 'next/server';
import { platformFetchers } from '@/lib/ingestion';
import { upsertMetrics, getGA4Metrics } from '@/lib/bq-analysis';

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ platform: string }> }
) {
  const { platform } = await params;
  const body = await request.json().catch(() => ({}));
  const { startDate, endDate } = body;

  const end = endDate || new Date().toISOString().split('T')[0];
  const start = startDate || (() => {
    const d = new Date();
    d.setDate(d.getDate() - 7);
    return d.toISOString().split('T')[0];
  })();

  try {
    let count: number;

    if (platform === 'ga4') {
      // GA4: query directly from BigQuery events_* tables
      const metrics = await getGA4Metrics(start, end);
      count = metrics.length > 0 ? (await upsertMetrics(metrics)) : 0;
    } else {
      // Ad platforms: fetch from their APIs then write to BigQuery
      const fetcher = platformFetchers[platform];
      if (!fetcher) {
        return NextResponse.json(
          { error: `不支持的平台: ${platform}` },
          { status: 400 }
        );
      }
      const metrics = await fetcher(start, end);
      count = metrics.length > 0 ? (await upsertMetrics(metrics)) : 0;
    }

    if (count === 0) {
      return NextResponse.json({
        message: `从 ${platform} 拉取到 0 条数据`,
        count: 0,
      });
    }

    return NextResponse.json({
      message: `${platform} 同步成功`,
      count,
      dateRange: { start, end },
    });
  } catch (error) {
    console.error(`同步 ${platform} 失败:`, error);
    return NextResponse.json(
      { error: `同步失败: ${(error as Error).message}` },
      { status: 500 }
    );
  }
}
