import fs from 'fs';
import { BigQuery } from '@google-cloud/bigquery';

let bqClient: BigQuery | null = null;

function getClient(): BigQuery {
  if (bqClient) return bqClient;

  const credentialsJson = process.env.GA4_SERVICE_ACCOUNT_JSON;
  if (credentialsJson) {
    bqClient = new BigQuery({
      projectId: process.env.GA4_BQ_PROJECT,
      credentials: JSON.parse(credentialsJson),
    });
    return bqClient;
  }

  const credentialsPath = process.env.GA4_SERVICE_ACCOUNT_PATH;
  if (!credentialsPath) throw new Error('GA4_SERVICE_ACCOUNT_PATH 未配置');
  if (!fs.existsSync(credentialsPath)) throw new Error(`凭据文件不存在: ${credentialsPath}`);
  bqClient = new BigQuery({
    projectId: process.env.GA4_BQ_PROJECT,
    keyFilename: credentialsPath,
  });
  return bqClient;
}

export interface JourneySession {
  sessionId: string;
  date: string;
  timeRange: string; // "01:59 - 02:03"
  durationSeconds: number;
  source: string;
  medium: string;
  pages: string[];
  keyEvents: string[];
}

export interface JourneyOrder {
  userId: string;
  transactionId: string;
  amount: number;
  currency: string;
  purchaseDate: string;
  products: { name: string; price: number; quantity: number }[];
  sessions: JourneySession[];
}

/**
 * 查询金额 > $3000 的订单及其完整用户旅程
 */
export async function fetchHighValueJourneys(
  startDate: string,
  endDate: string,
  minAmount: number = 3000,
  limit: number = 50
): Promise<JourneyOrder[]> {
  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  const startSuffix = startDate.replace(/-/g, '');
  const endSuffix = endDate.replace(/-/g, '');

  const sql = `
WITH
purchases AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    COALESCE(
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value'),
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value')
    ) as revenue,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') as transaction_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency') as currency,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as purchase_session_id
  FROM \`${dataset}.events_*\`
  WHERE _TABLE_SUFFIX BETWEEN '${startSuffix}' AND '${endSuffix}'
  AND event_name = 'purchase'
  AND COALESCE(
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value'),
    (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value')
  ) > ${minAmount}
  ORDER BY revenue DESC
  LIMIT ${limit}
),

user_events AS (
  SELECT
    e.user_pseudo_id,
    e.event_timestamp,
    e.event_name,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') as session_id,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') as page_location,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'engagement_time_msec') as engagement_time,
    e.traffic_source.source as ts_source,
    e.traffic_source.medium as ts_medium,
    p.transaction_id,
    p.revenue,
    p.currency,
    p.purchase_session_id
  FROM \`${dataset}.events_*\` e
  JOIN purchases p
    ON e.user_pseudo_id = p.user_pseudo_id
    AND e.event_timestamp <= p.event_timestamp
),

session_agg AS (
  SELECT
    user_pseudo_id,
    transaction_id,
    session_id,
    ts_source,
    ts_medium,
    MIN(event_timestamp) as session_start,
    MAX(event_timestamp) as session_end,
    TIMESTAMP_DIFF(TIMESTAMP_MICROS(MAX(event_timestamp)), TIMESTAMP_MICROS(MIN(event_timestamp)), SECOND) as duration_sec,
    ARRAY_AGG(STRUCT(event_name, page_location) ORDER BY event_timestamp) as events
  FROM user_events
  GROUP BY 1, 2, 3, 4, 5
),

order_items AS (
  SELECT
    e.user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'transaction_id') as tx_id,
    ARRAY_AGG(STRUCT(item.item_name, item.price as item_price, item.quantity as item_qty)) as items_arr
  FROM \`${dataset}.events_*\` e,
  UNNEST(e.items) as item
  WHERE _TABLE_SUFFIX BETWEEN '${startSuffix}' AND '${endSuffix}'
  AND event_name = 'purchase'
  GROUP BY 1, 2
)

SELECT
  sa.user_pseudo_id,
  sa.transaction_id,
  sa.ts_source,
  sa.ts_medium,
  sa.session_id,
  sa.session_start,
  sa.session_end,
  sa.duration_sec,
  sa.events,
  p.revenue,
  p.currency,
  p.event_timestamp as purchase_ts,
  oi.items_arr as items
FROM session_agg sa
JOIN purchases p ON sa.user_pseudo_id = p.user_pseudo_id AND sa.transaction_id = p.transaction_id
LEFT JOIN order_items oi ON p.transaction_id = oi.tx_id
ORDER BY p.revenue DESC, sa.session_start ASC
`;

  const client = getClient();
  const [rows] = await client.query({ query: sql });

  // Group by order (transaction_id)
  const orderMap = new Map<string, JourneyOrder>();

  for (const row of rows) {
    const txId = row.transaction_id;

    if (!orderMap.has(txId)) {
      const products = (row.items || []).map((item: any) => ({
        name: item.item_name || 'Unknown',
        price: Number(item.item_price) || 0,
        quantity: Number(item.item_qty) || 1,
      }));

      orderMap.set(txId, {
        userId: row.user_pseudo_id,
        transactionId: txId,
        amount: Number(row.revenue) || 0,
        currency: row.currency || 'USD',
        purchaseDate: formatTimestamp(row.purchase_ts),
        products,
        sessions: [],
      });
    }

    const order = orderMap.get(txId)!;
    const pages: string[] = (row.events || [])
      .filter((e: any) => e.event_name === 'page_view' && e.page_location)
      .map((e: any) => {
        try { return new URL(e.page_location).pathname; } catch { return e.page_location; }
      });
    const uniquePages = [...new Set(pages)];

    const keyEventsList = (row.events || [])
      .map((e: any) => e.event_name as string)
      .filter((n: string) => !['page_view', 'session_start', 'user_engagement', 'scroll', 'scroll_first_view', 'GOC-1P-Signal-new'].includes(n));
    const uniqueKeyEvents = [...new Set(keyEventsList)] as string[];

    order.sessions.push({
      sessionId: String(row.session_id || ''),
      date: formatTimestamp(row.session_start).split(' ')[0],
      timeRange: `${formatTime(row.session_start)} - ${formatTime(row.session_end)}`,
      durationSeconds: Number(row.duration_sec) || 0,
      source: row.ts_source || '(direct)',
      medium: row.ts_medium || '(none)',
      pages: uniquePages,
      keyEvents: uniqueKeyEvents,
    });
  }

  // Sort sessions within each order by date
  for (const order of orderMap.values()) {
    order.sessions.sort((a, b) => a.date.localeCompare(b.date));
  }

  return Array.from(orderMap.values()).sort((a, b) => b.amount - a.amount);
}

function formatTimestamp(ts: string | number | bigint): string {
  const ms = Number(ts) / 1000;
  return new Date(ms).toLocaleString('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  });
}

function formatTime(ts: string | number | bigint): string {
  const ms = Number(ts) / 1000;
  const d = new Date(ms);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

export interface SignalEvent {
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

// 购买路径事件 — 必须排除，避免它们作为信号事件
const EXCLUDED_EVENTS = [
  // GA4 标准电商事件
  'purchase', 'add_to_cart', 'begin_checkout', 'view_cart',
  'add_payment_info', 'add_shipping_info', 'remove_from_cart',
  'select_item', 'select_promotion', 'view_item', 'view_item_list',
  // WooFunnels / 自定义结账流程
  'place_order', 'checkout_completed', 'checkout_contact_info_submitted',
  'checkout_started', 'MiniCart_Checkout_Btn', 'WooFunnels_Checkout',
  'WooFunnels_Thankyou', 'Woofunnels_Bump',
  'copy_couponcode', 'open_coupon_popup', 'Add_shipping',
];

/**
 * 构建信号事件分析 SQL
 */
function buildAnalysisSQL(
  dataset: string,
  startDate: string,
  endDate: string,
  windowDays: number
): string {
  const startSuffix = startDate.replace(/-/g, '');
  const endSuffix = endDate.replace(/-/g, '');
  // 时间窗口（微秒）
  const windowUs = windowDays * 86400 * 1_000_000;
  const excludedList = EXCLUDED_EVENTS.map(e => `'${e}'`).join(', ');

  return `
WITH
user_windows AS (
  SELECT
    user_pseudo_id,
    MIN(event_timestamp) AS first_event_ts,
    MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS first_purchase_ts,
    LOGICAL_OR(event_name = 'purchase') AS has_purchase
  FROM \`${dataset}.events_*\`
  WHERE _TABLE_SUFFIX BETWEEN '${startSuffix}' AND '${endSuffix}'
  GROUP BY user_pseudo_id
),

user_events AS (
  SELECT
    e.user_pseudo_id,
    e.event_name,
    e.event_timestamp,
    uw.has_purchase,
    uw.first_purchase_ts,
    TIMESTAMP_DIFF(
      TIMESTAMP_MICROS(COALESCE(uw.first_purchase_ts, uw.first_event_ts + ${windowUs})),
      TIMESTAMP_MICROS(e.event_timestamp),
      HOUR
    ) AS hours_before_conversion
  FROM \`${dataset}.events_*\` e
  JOIN user_windows uw ON e.user_pseudo_id = uw.user_pseudo_id
  WHERE (e.event_timestamp <= uw.first_purchase_ts
     OR (NOT uw.has_purchase
         AND e.event_timestamp <= uw.first_event_ts + ${windowUs}))
    AND e.event_name NOT IN (${excludedList})
),

event_stats AS (
  SELECT
    event_name,
    COUNT(DISTINCT user_pseudo_id) AS total_users_with_event,
    COUNT(DISTINCT CASE WHEN has_purchase THEN user_pseudo_id END) AS purchasers_with_event,
    COUNT(DISTINCT CASE WHEN NOT has_purchase THEN user_pseudo_id END) AS non_purchasers_with_event,
    COUNT(*) AS total_event_count,
    AVG(CASE WHEN has_purchase THEN hours_before_conversion END) AS avg_hours_before_purchase
  FROM user_events
  GROUP BY event_name
),

user_totals AS (
  SELECT
    COUNT(DISTINCT CASE WHEN has_purchase THEN user_pseudo_id END) AS total_purchasers,
    COUNT(DISTINCT CASE WHEN NOT has_purchase THEN user_pseudo_id END) AS total_non_purchasers
  FROM user_windows
)

SELECT
  es.event_name,
  es.total_event_count,
  es.purchasers_with_event,
  es.non_purchasers_with_event,
  SAFE_DIVIDE(es.purchasers_with_event, ut.total_purchasers) AS purchase_group_rate,
  SAFE_DIVIDE(es.non_purchasers_with_event, ut.total_non_purchasers) AS non_purchase_group_rate,
  SAFE_DIVIDE(
    SAFE_DIVIDE(es.purchasers_with_event, ut.total_purchasers),
    SAFE_DIVIDE(es.non_purchasers_with_event, ut.total_non_purchasers)
  ) AS lift,
  CASE
    WHEN ut.total_purchasers = 0 OR ut.total_non_purchasers = 0 THEN NULL
    WHEN SAFE_DIVIDE(es.purchasers_with_event, ut.total_purchasers) = 0
     AND SAFE_DIVIDE(es.non_purchasers_with_event, ut.total_non_purchasers) = 0 THEN 0
    ELSE
      SAFE_DIVIDE(
        SAFE_DIVIDE(es.purchasers_with_event, ut.total_purchasers)
        - SAFE_DIVIDE(es.non_purchasers_with_event, ut.total_non_purchasers),
        SQRT(
          SAFE_DIVIDE(
            SAFE_DIVIDE(es.purchasers_with_event, ut.total_purchasers) * (1 - SAFE_DIVIDE(es.purchasers_with_event, ut.total_purchasers)),
            ut.total_purchasers
          )
          + SAFE_DIVIDE(
            SAFE_DIVIDE(es.non_purchasers_with_event, ut.total_non_purchasers) * (1 - SAFE_DIVIDE(es.non_purchasers_with_event, ut.total_non_purchasers)),
            ut.total_non_purchasers
          )
        )
      )
  END AS z_score,
  es.avg_hours_before_purchase
FROM event_stats es
CROSS JOIN user_totals ut
WHERE es.purchasers_with_event >= 5
ORDER BY lift DESC NULLS LAST
`;
}

/**
 * 根据 z-score 近似计算 p-value（标准正态分布双尾检验）
 */
function zScoreToPValue(z: number): number {
  const absZ = Math.abs(z);
  if (absZ > 10) return 0;
  const p = 0.2316419;
  const b1 = 0.319381530;
  const b2 = -0.356563782;
  const b3 = 1.781477937;
  const b4 = -1.821255978;
  const b5 = 1.330274429;
  const t = 1 / (1 + p * absZ);
  const phi = (1 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * absZ * absZ);
  const tail = phi * (b1 * t + b2 * t * t + b3 * t ** 4 + b4 * t ** 5 + b5 * t ** 6);
  return 2 * tail;
}

/**
 * 分析信号事件
 */
export async function analyzeSignalEvents(
  startDate: string,
  endDate: string,
  windowDays: number = 30
): Promise<SignalEvent[]> {
  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  const sql = buildAnalysisSQL(dataset, startDate, endDate, windowDays);
  const client = getClient();
  const [rows] = await client.query({ query: sql });

  return rows.map((row: any) => {
    const purchaseRate = Number(row.purchase_group_rate) || 0;
    const nonPurchaseRate = Number(row.non_purchase_group_rate) || 0;
    const zScore = Number(row.z_score) || 0;
    const pValue = zScoreToPValue(zScore);

    return {
      eventName: row.event_name,
      totalCount: Number(row.total_event_count) || 0,
      purchasersWithEvent: Number(row.purchasers_with_event) || 0,
      nonPurchasersWithEvent: Number(row.non_purchasers_with_event) || 0,
      purchaseGroupRate: Math.round(purchaseRate * 10000) / 100,
      nonPurchaseGroupRate: Math.round(nonPurchaseRate * 10000) / 100,
      lift: Number(row.lift) || 0,
      zScore: Math.round(zScore * 100) / 100,
      pValue: Math.round(pValue * 10000) / 10000,
      significant: pValue < 0.05 && Number(row.lift) > 1,
      avgHoursBeforePurchase: row.avg_hours_before_purchase != null
        ? Math.round(Number(row.avg_hours_before_purchase) * 10) / 10
        : null,
    };
  });
}

// ==================== Ad Metrics: BigQuery write ====================

export interface AdMetricRow {
  date: string;
  platform: string;
  campaign_id: string;
  campaign_name: string;
  spend: number;
  impressions: number;
  clicks: number;
  conversions: number;
  ctr: number;
  cpc: number;
  revenue: number;
  sessions?: number;
  active_users?: number;
  total_users?: number;
  event_count?: number;
  engaged_sessions?: number;
  engagement_rate?: number;
  avg_session_duration?: number;
}

/**
 * Ensure ad_metrics table exists in BigQuery
 */
export async function ensureAdMetricsTable(): Promise<void> {
  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  const client = getClient();
  const datasetRef = client.dataset(dataset);

  // Check if table exists
  const [tables] = await datasetRef.getTables();
  const tableExists = tables.some((t: any) => t.id === 'ad_metrics');

  if (!tableExists) {
    const schema = [
      { name: 'date', type: 'DATE', mode: 'REQUIRED' },
      { name: 'platform', type: 'STRING', mode: 'REQUIRED' },
      { name: 'campaign_id', type: 'STRING' },
      { name: 'campaign_name', type: 'STRING' },
      { name: 'spend', type: 'FLOAT64' },
      { name: 'impressions', type: 'INT64' },
      { name: 'clicks', type: 'INT64' },
      { name: 'conversions', type: 'INT64' },
      { name: 'ctr', type: 'FLOAT64' },
      { name: 'cpc', type: 'FLOAT64' },
      { name: 'revenue', type: 'FLOAT64' },
      { name: 'sessions', type: 'INT64' },
      { name: 'active_users', type: 'INT64' },
      { name: 'total_users', type: 'INT64' },
      { name: 'event_count', type: 'INT64' },
      { name: 'engaged_sessions', type: 'INT64' },
      { name: 'engagement_rate', type: 'FLOAT64' },
      { name: 'avg_session_duration', type: 'FLOAT64' },
      { name: 'inserted_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
    ];

    await datasetRef.createTable('ad_metrics', { schema });
  }
}

/**
 * Insert metrics into BigQuery and deduplicate.
 * Uses streaming insert (fast) followed by MERGE for dedup.
 */
export async function upsertMetrics(metrics: AdMetricRow[]): Promise<number> {
  if (metrics.length === 0) return 0;

  await ensureAdMetricsTable();

  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  const client = getClient();
  const table = client.dataset(dataset).table('ad_metrics');

  // Prepare rows for streaming insert
  const rows = metrics.map((m) => ({
    insertId: `${m.date}-${m.platform}-${m.campaign_id}-${Date.now()}`,
    json: {
      date: m.date,
      platform: m.platform,
      campaign_id: m.campaign_id || '',
      campaign_name: m.campaign_name || '',
      spend: m.spend || 0,
      impressions: m.impressions || 0,
      clicks: m.clicks || 0,
      conversions: m.conversions || 0,
      ctr: m.ctr || 0,
      cpc: m.cpc || 0,
      revenue: m.revenue || 0,
      sessions: m.sessions || 0,
      active_users: m.active_users || 0,
      total_users: m.total_users || 0,
      event_count: m.event_count || 0,
      engaged_sessions: m.engaged_sessions || 0,
      engagement_rate: m.engagement_rate || 0,
      avg_session_duration: m.avg_session_duration || 0,
      inserted_at: new Date().toISOString(),
    },
  }));

  const [insertResponse] = await table.insert(rows);
  const insertErrors = (insertResponse as any)?.insertErrors;
  if (insertErrors && insertErrors.length > 0) {
    console.error('BigQuery streaming insert errors:', insertErrors);
  }

  // Deduplication via MERGE: keep latest inserted_at per (date, platform, campaign_id)
  const dedupSql = `
    MERGE \`${dataset}.ad_metrics\` T
    USING (
      SELECT * EXCEPT(rn) FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY date, platform, campaign_id
            ORDER BY inserted_at DESC
          ) as rn
        FROM \`${dataset}.ad_metrics\`
      ) WHERE rn = 1
    ) S
    ON T.date = S.date AND T.platform = S.platform AND T.campaign_id = S.campaign_id
    WHEN MATCHED AND T.inserted_at < S.inserted_at THEN
      UPDATE SET
        campaign_name = S.campaign_name, spend = S.spend,
        impressions = S.impressions, clicks = S.clicks,
        conversions = S.conversions, ctr = S.ctr, cpc = S.cpc,
        revenue = S.revenue, sessions = S.sessions,
        active_users = S.active_users, total_users = S.total_users,
        event_count = S.event_count, engaged_sessions = S.engaged_sessions,
        engagement_rate = S.engagement_rate,
        avg_session_duration = S.avg_session_duration,
        inserted_at = S.inserted_at
    WHEN NOT MATCHED THEN
      INSERT ROW
  `;

  await client.query({ query: dedupSql });

  return metrics.length;
}

// ==================== Ad Metrics: BigQuery read ====================

export async function getGlobalSummary(options?: {
  startDate?: string;
  endDate?: string;
}) {
  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  let where = 'WHERE 1=1';
  const params: Record<string, string> = {};
  if (options?.startDate) { where += ' AND date >= @startDate'; params.startDate = options.startDate; }
  if (options?.endDate) { where += ' AND date <= @endDate'; params.endDate = options.endDate; }

  const sql = `
    SELECT platform,
      SUM(spend) as total_spend, SUM(impressions) as total_impressions,
      SUM(clicks) as total_clicks, SUM(conversions) as total_conversions,
      SUM(revenue) as total_revenue, SUM(sessions) as total_sessions,
      SUM(active_users) as activeUsers, SUM(event_count) as eventCount
    FROM \`${dataset}.ad_metrics\`
    ${where}
    GROUP BY platform
  `;

  const client = getClient();
  const [rows] = await client.query({ query: sql, params });
  return rows;
}

export async function getDailySummary(options?: {
  startDate?: string;
  endDate?: string;
}) {
  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  let where = 'WHERE 1=1';
  const params: Record<string, string> = {};
  if (options?.startDate) { where += ' AND date >= @startDate'; params.startDate = options.startDate; }
  if (options?.endDate) { where += ' AND date <= @endDate'; params.endDate = options.endDate; }

  const sql = `
    SELECT date, platform,
      SUM(spend) as spend, SUM(impressions) as impressions,
      SUM(clicks) as clicks, SUM(conversions) as conversions,
      SUM(revenue) as revenue, SUM(sessions) as sessions,
      SUM(active_users) as activeUsers,
      SUM(total_users) as totalUsers,
      SUM(event_count) as eventCount,
      SUM(engaged_sessions) as engagedSessions
    FROM \`${dataset}.ad_metrics\`
    ${where}
    GROUP BY date, platform
    ORDER BY date DESC, platform ASC
  `;

  const client = getClient();
  const [rows] = await client.query({ query: sql, params });
  return rows;
}

export async function getMetrics(options?: {
  platform?: string;
  startDate?: string;
  endDate?: string;
}) {
  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  let where = 'WHERE 1=1';
  const params: Record<string, string> = {};
  if (options?.platform) { where += ' AND platform = @platform'; params.platform = options.platform; }
  if (options?.startDate) { where += ' AND date >= @startDate'; params.startDate = options.startDate; }
  if (options?.endDate) { where += ' AND date <= @endDate'; params.endDate = options.endDate; }

  const sql = `
    SELECT *
    FROM \`${dataset}.ad_metrics\`
    ${where}
    ORDER BY date DESC, platform ASC
  `;

  const client = getClient();
  const [rows] = await client.query({ query: sql, params });
  return rows;
}

// ==================== GA4 metrics directly from events_* ====================

export async function getGA4Metrics(
  startDate: string,
  endDate: string
): Promise<AdMetricRow[]> {
  const dataset = process.env.GA4_BQ_DATASET;
  if (!dataset) throw new Error('GA4_BQ_DATASET 未配置');

  const startSuffix = startDate.replace(/-/g, '');
  const endSuffix = endDate.replace(/-/g, '');

  const sql = `
    SELECT
      PARSE_DATE('%Y%m%d', event_date) as date,
      'ga4' as platform,
      '' as campaign_id,
      '' as campaign_name,
      0.0 as spend,
      0 as impressions,
      0 as clicks,
      COUNTIF(event_name = 'purchase') as conversions,
      0.0 as ctr,
      0.0 as cpc,
      COALESCE(SUM(
        COALESCE(
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value'),
          (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value')
        )
      ), 0) as revenue,
      COUNT(DISTINCT CONCAT(user_pseudo_id, '-',
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
      )) as sessions,
      COUNT(DISTINCT user_pseudo_id) as active_users,
      COUNT(DISTINCT user_pseudo_id) as total_users,
      COUNT(*) as event_count,
      COUNTIF(
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') > 0
      ) as engaged_sessions,
      SAFE_DIVIDE(
        COUNTIF(
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') > 0
        ),
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-',
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
        ))
      ) as engagement_rate,
      SAFE_DIVIDE(
        SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec')),
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-',
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
        ))
      ) as avg_session_duration
    FROM \`${dataset}.events_*\`
    WHERE _TABLE_SUFFIX BETWEEN '${startSuffix}' AND '${endSuffix}'
    GROUP BY date
    ORDER BY date DESC
  `;

  const client = getClient();
  const [rows] = await client.query({ query: sql });
  return rows.map((row: any) => ({
    date: row.date,
    platform: 'ga4',
    campaign_id: '',
    campaign_name: '',
    spend: 0,
    impressions: 0,
    clicks: 0,
    conversions: Number(row.conversions) || 0,
    ctr: 0,
    cpc: 0,
    revenue: Number(row.revenue) || 0,
    sessions: Number(row.sessions) || 0,
    active_users: Number(row.active_users) || 0,
    total_users: Number(row.total_users) || 0,
    event_count: Number(row.event_count) || 0,
    engaged_sessions: Number(row.engaged_sessions) || 0,
    engagement_rate: Number(row.engagement_rate) || 0,
    avg_session_duration: Number(row.avg_session_duration) || 0,
  }));
}
