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
