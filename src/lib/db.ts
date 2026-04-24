import type Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const dbPath = path.join(process.cwd(), 'data', 'ads.db');

let _db: Database.Database | null = null;

function getDb(): Database.Database {
  if (_db) return _db;
  if (!fs.existsSync(path.dirname(dbPath))) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  }
  const Database = require('better-sqlite3').default || require('better-sqlite3');
  const db = new Database(dbPath) as Database.Database;
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS ad_metrics (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      date TEXT NOT NULL,
      platform TEXT NOT NULL,
      campaign_id TEXT,
      campaign_name TEXT,
      spend REAL DEFAULT 0,
      impressions INTEGER DEFAULT 0,
      clicks INTEGER DEFAULT 0,
      conversions INTEGER DEFAULT 0,
      ctr REAL DEFAULT 0,
      cpc REAL DEFAULT 0,
      revenue REAL DEFAULT 0,
      sessions INTEGER DEFAULT 0,
      active_users INTEGER DEFAULT 0,
      total_users INTEGER DEFAULT 0,
      event_count INTEGER DEFAULT 0,
      engaged_sessions INTEGER DEFAULT 0,
      engagement_rate REAL DEFAULT 0,
      avg_session_duration REAL DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_unique
      ON ad_metrics(date, platform, campaign_id);
  `);
  _db = db;
  return _db;
}

export interface AdMetric {
  id?: number;
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
  activeUsers?: number;
  totalUsers?: number;
  eventCount?: number;
  engagedSessions?: number;
  engagementRate?: number;
  avgSessionDuration?: number;
}

export function upsertMetric(metric: Omit<AdMetric, 'id'>): void {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT INTO ad_metrics (date, platform, campaign_id, campaign_name, spend, impressions, clicks, conversions, ctr, cpc, revenue, sessions, active_users, total_users, event_count, engaged_sessions, engagement_rate, avg_session_duration)
    VALUES (@date, @platform, @campaign_id, @campaign_name, @spend, @impressions, @clicks, @conversions, @ctr, @cpc, @revenue, @sessions, @activeUsers, @totalUsers, @eventCount, @engagedSessions, @engagementRate, @avgSessionDuration)
    ON CONFLICT(date, platform, campaign_id) DO UPDATE SET
      campaign_name = excluded.campaign_name,
      spend = excluded.spend, impressions = excluded.impressions,
      clicks = excluded.clicks, conversions = excluded.conversions,
      ctr = excluded.ctr, cpc = excluded.cpc, revenue = excluded.revenue,
      sessions = excluded.sessions, active_users = excluded.active_users,
      total_users = excluded.total_users, event_count = excluded.event_count,
      engaged_sessions = excluded.engaged_sessions,
      engagement_rate = excluded.engagement_rate,
      avg_session_duration = excluded.avg_session_duration,
      created_at = datetime('now')
  `);
  stmt.run(metric);
}

export function upsertMetrics(metrics: Omit<AdMetric, 'id'>[]): void {
  const db = getDb();
  const insert = db.prepare(`
    INSERT INTO ad_metrics (date, platform, campaign_id, campaign_name, spend, impressions, clicks, conversions, ctr, cpc, revenue, sessions, active_users, total_users, event_count, engaged_sessions, engagement_rate, avg_session_duration)
    VALUES (@date, @platform, @campaign_id, @campaign_name, @spend, @impressions, @clicks, @conversions, @ctr, @cpc, @revenue, @sessions, @activeUsers, @totalUsers, @eventCount, @engagedSessions, @engagementRate, @avgSessionDuration)
    ON CONFLICT(date, platform, campaign_id) DO UPDATE SET
      campaign_name = excluded.campaign_name,
      spend = excluded.spend, impressions = excluded.impressions,
      clicks = excluded.clicks, conversions = excluded.conversions,
      ctr = excluded.ctr, cpc = excluded.cpc, revenue = excluded.revenue,
      sessions = excluded.sessions, active_users = excluded.active_users,
      total_users = excluded.total_users, event_count = excluded.event_count,
      engaged_sessions = excluded.engaged_sessions,
      engagement_rate = excluded.engagement_rate,
      avg_session_duration = excluded.avg_session_duration,
      created_at = datetime('now')
  `);
  const insertMany = db.transaction((rows: Omit<AdMetric, 'id'>[]) => {
    for (const row of rows) insert.run(row);
  });
  insertMany(metrics);
}

export function getMetrics(options?: {
  platform?: string;
  startDate?: string;
  endDate?: string;
}): AdMetric[] {
  const db = getDb();
  let sql = 'SELECT * FROM ad_metrics WHERE 1=1';
  const params: Record<string, string> = {};
  if (options?.platform) { sql += ' AND platform = @platform'; params.platform = options.platform; }
  if (options?.startDate) { sql += ' AND date >= @startDate'; params.startDate = options.startDate; }
  if (options?.endDate) { sql += ' AND date <= @endDate'; params.endDate = options.endDate; }
  sql += ' ORDER BY date DESC, platform ASC';
  return db.prepare(sql).all(params) as AdMetric[];
}

export function getDailySummary(options?: { startDate?: string; endDate?: string }) {
  let sql = `
    SELECT date, platform,
      SUM(spend) as spend, SUM(impressions) as impressions,
      SUM(clicks) as clicks, SUM(conversions) as conversions,
      SUM(revenue) as revenue, SUM(sessions) as sessions,
      SUM(active_users) as activeUsers,
      SUM(total_users) as totalUsers,
      SUM(event_count) as eventCount,
      SUM(engaged_sessions) as engagedSessions
    FROM ad_metrics WHERE 1=1
  `;
  const params: Record<string, string> = {};
  if (options?.startDate) { sql += ' AND date >= @startDate'; params.startDate = options.startDate; }
  if (options?.endDate) { sql += ' AND date <= @endDate'; params.endDate = options.endDate; }
  sql += ' GROUP BY date, platform ORDER BY date DESC, platform ASC';
  const db = getDb();
  return db.prepare(sql).all(params);
}

export function getGlobalSummary(options?: { startDate?: string; endDate?: string }) {
  let sql = `
    SELECT platform,
      SUM(spend) as total_spend, SUM(impressions) as total_impressions,
      SUM(clicks) as total_clicks, SUM(conversions) as total_conversions,
      SUM(revenue) as total_revenue, SUM(sessions) as total_sessions,
      SUM(active_users) as activeUsers, SUM(event_count) as eventCount
    FROM ad_metrics WHERE 1=1
  `;
  const params: Record<string, string> = {};
  if (options?.startDate) { sql += ' AND date >= @startDate'; params.startDate = options.startDate; }
  if (options?.endDate) { sql += ' AND date <= @endDate'; params.endDate = options.endDate; }
  sql += ' GROUP BY platform';
  const db = getDb();
  return db.prepare(sql).all(params);
}
