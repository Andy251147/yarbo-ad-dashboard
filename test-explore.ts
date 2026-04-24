import { BigQuery } from '@google-cloud/bigquery';

const client = new BigQuery({
  projectId: 'yarbo-441803',
  keyFilename: './credentials/ga4-service-account.json',
});

async function main() {
  // 找一个有多 session 的购买用户，看完整旅程
  const q = `
    WITH purchasing_users AS (
      SELECT user_pseudo_id, event_timestamp as purchase_ts,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') as revenue
      FROM \`yarbo-441803.analytics_321746415.events_*\`
      WHERE _TABLE_SUFFIX BETWEEN '20250105' AND '20250615'
      AND event_name = 'purchase'
      AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') > 3000
    ),
    user_sessions AS (
      SELECT
        pu.user_pseudo_id,
        pu.revenue,
        COUNT(DISTINCT (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id')) as session_count
      FROM purchasing_users pu
      JOIN \`yarbo-441803.analytics_321746415.events_*\` e
        ON pu.user_pseudo_id = e.user_pseudo_id
        AND e.event_timestamp <= pu.purchase_ts
      GROUP BY 1, 2
      HAVING session_count > 1
      ORDER BY pu.revenue DESC
      LIMIT 1
    )
    SELECT user_pseudo_id, revenue, session_count FROM user_sessions
  `;
  const [rows] = await client.query({ query: q });
  const user = rows[0];
  console.log('Target user:', JSON.stringify(user, null, 2));

  // 获取该用户的所有事件
  const q2 = `
    SELECT
      event_timestamp,
      event_name,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time,
      traffic_source.source as ts_source,
      traffic_source.medium as ts_medium,
      traffic_source.name as ts_campaign
    FROM \`yarbo-441803.analytics_321746415.events_*\`
    WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20250615'
    AND user_pseudo_id = '${user.user_pseudo_id}'
    ORDER BY event_timestamp ASC
  `;
  const [rows2] = await client.query({ query: q2 });
  console.log('\n=== Full user journey ===');
  let currentSession = '';
  let sessionStart = '';
  for (const row of rows2) {
    const sid = String(row.session_id || '');
    const ts = row.ts_source || row.ts_medium || row.ts_campaign ? `${row.ts_source || '—'} / ${row.ts_medium || '—'}` : '—';
    const url = row.page_location ? new URL(row.page_location).pathname : '';
    const eng = row.engagement_time ? `${(row.engagement_time / 1000).toFixed(0)}s` : '';
    const date = new Date(Number(row.event_timestamp) / 1000).toLocaleString('zh-CN');

    if (sid !== currentSession) {
      currentSession = sid;
      sessionStart = date;
      console.log(`\n  ▸ Session ${sid} (source: ${ts}) started ${sessionStart}`);
    }
    console.log(`    [${date.split(' ')[1].padEnd(8)}] ${row.event_name.padEnd(30)} ${url.padEnd(40)} ${eng}`);
  }
}

main().catch(console.error);
