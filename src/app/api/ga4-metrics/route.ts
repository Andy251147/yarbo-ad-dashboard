import { NextRequest, NextResponse } from 'next/server';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const startDate = searchParams.get('startDate');
  const endDate = searchParams.get('endDate');

  if (!startDate || !endDate) {
    return NextResponse.json({ error: 'startDate 和 endDate 必填' }, { status: 400 });
  }

  const dataset = process.env.GA4_BQ_DATASET;
  const projectId = process.env.GA4_BQ_PROJECT;
  if (!dataset || !projectId) {
    return NextResponse.json({ error: 'GA4 BigQuery 配置缺失' }, { status: 500 });
  }

  const startSuffix = startDate.replace(/-/g, '');
  const endSuffix = endDate.replace(/-/g, '');

  const sql = `
WITH events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_day,
    user_pseudo_id,
    event_name,
    event_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time,
    COALESCE(
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value'),
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value')
    ) AS revenue_value
  FROM \`${projectId}.${dataset}.events_*\`
  WHERE _TABLE_SUFFIX BETWEEN '${startSuffix}' AND '${endSuffix}'
),

session_stats AS (
  SELECT
    event_day,
    user_pseudo_id,
    session_id,
    MAX(engagement_time) AS max_engagement_time,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNTIF(event_name = 'purchase') AS purchase_events,
    SUM(revenue_value) AS revenue
  FROM events
  GROUP BY event_day, user_pseudo_id, session_id
)

SELECT
  CAST(event_day AS STRING) AS date,
  COUNT(DISTINCT CONCAT(CAST(user_pseudo_id AS STRING), '-', CAST(session_id AS STRING))) AS sessions,
  COUNT(DISTINCT user_pseudo_id) AS activeUsers,
  COUNT(DISTINCT user_pseudo_id) AS totalUsers,
  COUNT(*) AS eventCount,
  COUNTIF(max_engagement_time > 0) AS engagedSessions,
  COUNTIF(purchase_events > 0) AS conversions,
  COALESCE(SUM(revenue), 0) AS revenue
FROM session_stats
GROUP BY event_day
ORDER BY event_day ASC
`;

  try {
    const { BigQuery } = await import('@google-cloud/bigquery');
    const { GA4_SERVICE_ACCOUNT_JSON, GA4_SERVICE_ACCOUNT_PATH } = process.env;

    let bqOptions: any = { projectId };
    if (GA4_SERVICE_ACCOUNT_JSON) {
      bqOptions.credentials = JSON.parse(GA4_SERVICE_ACCOUNT_JSON);
    } else if (GA4_SERVICE_ACCOUNT_PATH) {
      bqOptions.keyFilename = GA4_SERVICE_ACCOUNT_PATH;
    }

    const bq = new BigQuery(bqOptions);
    const [rows] = await bq.query({ query: sql });

    return NextResponse.json({ data: rows });
  } catch (error) {
    console.error('GA4 metrics query error:', error);
    return NextResponse.json(
      { error: (error as Error).message },
      { status: 500 }
    );
  }
}
