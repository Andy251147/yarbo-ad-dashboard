import axios from 'axios';
import fs from 'fs';
import { GoogleAuth } from 'google-auth-library';

export interface PlatformMetric {
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
}

// ==================== Google Ads ====================
export async function fetchGoogleAdsMetrics(
  startDate: string,
  endDate: string
): Promise<PlatformMetric[]> {
  const developerToken = process.env.GOOADS_DEVELOPER_TOKEN;
  const loginCustomerId = process.env.GOOADS_LOGIN_CUSTOMER_ID;

  if (!developerToken) {
    throw new Error('GOOADS_DEVELOPER_TOKEN 未配置');
  }

  // Google Ads API 使用 gRPC-like REST 接口
  const query = `
    SELECT
      segments.date,
      campaign.id,
      campaign.name,
      metrics.cost_micros,
      metrics.impressions,
      metrics.clicks,
      metrics.conversions,
      metrics.ctr,
      metrics.average_cpc
    FROM campaign
    WHERE segments.date BETWEEN '${startDate}' AND '${endDate}'
  `;

  try {
    const response = await axios.post(
      'https://googleads.googleapis.com/v17/customers/' + loginCustomerId + '/googleAds:search',
      { query },
      {
        headers: {
          'Authorization': `Bearer ${process.env.GOOADS_ACCESS_TOKEN}`,
          'developer-token': developerToken,
          'login-customer-id': loginCustomerId,
        },
      }
    );

    return (response.data.results || []).map((row: any) => ({
      date: row.segments.date,
      platform: 'google',
      campaign_id: String(row.campaign.id),
      campaign_name: row.campaign.name,
      spend: (row.metrics.cost_micros || 0) / 1000000, // 微单位转元
      impressions: row.metrics.impressions || 0,
      clicks: row.metrics.clicks || 0,
      conversions: Math.round(row.metrics.conversions || 0),
      ctr: row.metrics.ctr || 0,
      cpc: (row.metrics.average_cpc || 0) / 1000000,
      revenue: 0,
    }));
  } catch (error) {
    throw new Error('Google Ads 拉取失败: ' + ((error as any)?.response?.data?.error?.message || (error as Error).message));
  }
}

// ==================== Meta Ads ====================
export async function fetchMetaMetrics(
  startDate: string,
  endDate: string
): Promise<PlatformMetric[]> {
  const accessToken = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_ACCOUNT_ID;

  if (!accessToken || !accountId) {
    throw new Error('META_ACCESS_TOKEN 或 META_ACCOUNT_ID 未配置');
  }

  try {
    const response = await axios.get(
      `https://graph.facebook.com/v21.0/act_${accountId}/insights`,
      {
        params: {
          access_token: accessToken,
          level: 'campaign',
          time_range: { since: startDate, until: endDate },
          fields: [
            'date_start',
            'campaign_id',
            'campaign_name',
            'spend',
            'impressions',
            'clicks',
            'actions',
            'ctr',
            'cpc',
          ].join(','),
        },
      }
    );

    return (response.data.data || []).map((row: any) => {
      // Meta 的 conversions 在 actions 数组中
      const actions = row.actions || [];
      const conversions = actions
        .filter((a: any) => a.action_type === 'offsite_conversion')
        .reduce((sum: number, a: any) => sum + (Number(a.value) || 0), 0);

      return {
        date: row.date_start,
        platform: 'meta',
        campaign_id: row.campaign_id,
        campaign_name: row.campaign_name,
        spend: Number(row.spend) || 0,
        impressions: Number(row.impressions) || 0,
        clicks: Number(row.clicks) || 0,
        conversions: Math.round(conversions),
        ctr: Number(row.ctr) || 0,
        cpc: Number(row.cpc) || 0,
        revenue: 0,
      };
    });
  } catch (error) {
    throw new Error('Meta Ads 拉取失败: ' + ((error as any)?.response?.data?.error?.message || (error as Error).message));
  }
}

// ==================== Bing Ads ====================
export async function fetchBingMetrics(
  startDate: string,
  endDate: string
): Promise<PlatformMetric[]> {
  const clientId = process.env.BING_CLIENT_ID;
  const clientSecret = process.env.BING_CLIENT_SECRET;
  const developerToken = process.env.BING_DEVELOPER_TOKEN;
  const accountId = process.env.BING_ACCOUNT_ID;

  if (!developerToken || !accountId) {
    throw new Error('BING_DEVELOPER_TOKEN 或 BING_ACCOUNT_ID 未配置');
  }

  // Bing Ads 需要先获取 OAuth token，然后调用 Reporting API
  // 这里简化处理，假设已有 access_token
  // 实际生产环境需要完整的 OAuth 流程
  const accessToken = process.env.BING_ACCESS_TOKEN;
  if (!accessToken) {
    throw new Error('BING_ACCESS_TOKEN 未配置（需通过 OAuth 流程获取）');
  }

  try {
    // Bing Ads Reporting API - 先提交报告请求，再轮询结果
    // 为简化 MVP，这里使用 CampaignPerformance 的搜索方式
    const response = await axios.get(
      'https://ads.manage.microsoft.com/api/rest/CampaignPerformanceReport',
      {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'CustomerAccountId': accountId,
          'CustomerId': accountId,
          'DeveloperToken': developerToken,
        },
        params: {
          StartTime: startDate,
          EndTime: endDate,
        },
      }
    );

    // Bing 返回 CSV 或 JSON 格式，需要根据实际响应解析
    return (response.data || []).map((row: any) => ({
      date: row.Day || startDate,
      platform: 'bing',
      campaign_id: String(row.CampaignId || ''),
      campaign_name: row.CampaignName || '',
      spend: Number(row.Spend) || 0,
      impressions: Number(row.Impressions) || 0,
      clicks: Number(row.Clicks) || 0,
      conversions: Number(row.Conversions) || 0,
      ctr: Number(row.Ctr) || 0,
      cpc: Number(row.AverageCpc) || 0,
      revenue: Number(row.Revenue) || 0,
    }));
  } catch (error) {
    throw new Error('Bing Ads 拉取失败: ' + ((error as any)?.response?.data?.error?.message || (error as Error).message));
  }
}

// ==================== TikTok Ads ====================
export async function fetchTikTokMetrics(
  startDate: string,
  endDate: string
): Promise<PlatformMetric[]> {
  const accessToken = process.env.TIKTOK_ACCESS_TOKEN;
  const accountId = process.env.TIKTOK_ACCOUNT_ID;

  if (!accessToken || !accountId) {
    throw new Error('TIKTOK_ACCESS_TOKEN 或 TIKTOK_ACCOUNT_ID 未配置');
  }

  try {
    const response = await axios.post(
      'https://business-api.tiktok.com/open_api/v1.3/report/integration/',
      {
        access_token: accessToken,
        advertiser_ids: [accountId],
        report_type: 'BASIC',
        dimensions: ['campaign_id', 'campaign_name', 'stat_time_day'],
        metrics: [
          'spend',
          'impressions',
          'clicks',
          'conversion',
          'ctr',
          'cpc',
        ],
        start_date: startDate,
        end_date: endDate,
      }
    );

    if (response.data.code !== 0) {
      console.error('TikTok API 错误:', response.data.message);
      return [];
    }

    return (response.data.data?.list || []).map((row: any) => ({
      date: row.stat_time_day,
      platform: 'tiktok',
      campaign_id: String(row.campaign_id),
      campaign_name: row.campaign_name,
      spend: Number(row.spend) || 0,
      impressions: Number(row.impressions) || 0,
      clicks: Number(row.clicks) || 0,
      conversions: Number(row.conversion) || 0,
      ctr: Number(row.ctr) || 0,
      cpc: Number(row.cpc) || 0,
      revenue: 0,
    }));
  } catch (error) {
    throw new Error('TikTok Ads 拉取失败: ' + ((error as any)?.response?.data?.message || (error as Error).message));
  }
}

// ==================== GA4 auth helper ====================

function getGA4AuthOptions() {
  const credentialsJson = process.env.GA4_SERVICE_ACCOUNT_JSON;
  if (credentialsJson) {
    return { credentials: JSON.parse(credentialsJson) };
  }
  const credentialsPath = process.env.GA4_SERVICE_ACCOUNT_PATH;
  if (!credentialsPath) throw new Error('GA4_SERVICE_ACCOUNT_JSON 或 GA4_SERVICE_ACCOUNT_PATH 未配置');
  return { keyFilename: credentialsPath };
}

// ==================== GA4 ====================

export async function fetchGA4Metrics(
  startDate: string,
  endDate: string
): Promise<PlatformMetric[]> {
  const propertyId = process.env.GA4_PROPERTY_ID;

  if (!propertyId) {
    throw new Error('GA4_PROPERTY_ID 未配置');
  }

  try {
    const auth = new GoogleAuth({
      ...getGA4AuthOptions(),
      scopes: ['https://www.googleapis.com/auth/analytics.readonly'],
    });

    const client = await auth.getClient();
    const token = await client.getAccessToken();

    const response = await axios.post(
      `https://analyticsdata.googleapis.com/v1beta/properties/${propertyId}:runReport`,
      {
        dateRanges: [{ startDate, endDate }],
        dimensions: [{ name: 'date' }],
        metrics: [
          { name: 'sessions' },
          { name: 'activeUsers' },
          { name: 'totalUsers' },
          { name: 'eventCount' },
          { name: 'engagedSessions' },
          { name: 'engagementRate' },
          { name: 'averageSessionDuration' },
          { name: 'conversions' },
          { name: 'totalRevenue' },
        ],
      },
      {
        headers: {
          Authorization: `Bearer ${token.token}`,
        },
      }
    );

    console.log('GA4 response:', JSON.stringify(response.data, null, 2).slice(0, 2000));

    return (response.data?.rows || []).map((row: any) => {
      // GA4 返回的日期格式是 YYYYMMDD，转为 YYYY-MM-DD
      const rawDate = row.dimensionValues?.[0]?.value || startDate;
      const date = rawDate.length === 8
        ? `${rawDate.slice(0, 4)}-${rawDate.slice(4, 6)}-${rawDate.slice(6)}`
        : rawDate;
      return {
      date,
      platform: 'ga4',
      campaign_id: '',
      campaign_name: '',
      spend: 0,
      impressions: 0,
      clicks: 0,
      conversions: Number(row.metricValues?.[7]?.value) || 0,
      ctr: 0,
      cpc: 0,
      revenue: Number(row.metricValues?.[8]?.value) || 0,
      // GA4 专属字段
      sessions: Number(row.metricValues?.[0]?.value) || 0,
      activeUsers: Number(row.metricValues?.[1]?.value) || 0,
      totalUsers: Number(row.metricValues?.[2]?.value) || 0,
      eventCount: Number(row.metricValues?.[3]?.value) || 0,
      engagedSessions: Number(row.metricValues?.[4]?.value) || 0,
      engagementRate: Number(row.metricValues?.[5]?.value) || 0,
      avgSessionDuration: Number(row.metricValues?.[6]?.value) || 0,
      };
    });
  } catch (error) {
    throw new Error('GA4 拉取失败: ' + ((error as any)?.response?.data?.error?.message || (error as Error).message));
  }
}

// ==================== GA4 地理数据 ====================
export interface GA4GeoData {
  country: string;
  countryCode: string;
  sessions: number;
  activeUsers: number;
  totalUsers: number;
  eventCount: number;
}

export async function fetchGA4GeoData(
  startDate: string,
  endDate: string
): Promise<GA4GeoData[]> {
  const propertyId = process.env.GA4_PROPERTY_ID;

  if (!propertyId) {
    throw new Error('GA4_PROPERTY_ID 未配置');
  }

  try {
    const auth = new GoogleAuth({
      ...getGA4AuthOptions(),
      scopes: ['https://www.googleapis.com/auth/analytics.readonly'],
    });
    const client = await auth.getClient();
    const token = await client.getAccessToken();

    const response = await axios.post(
      `https://analyticsdata.googleapis.com/v1beta/properties/${propertyId}:runReport`,
      {
        dateRanges: [{ startDate, endDate }],
        dimensions: [
          { name: 'country' },
          { name: 'countryIsoCode' },
        ],
        metrics: [
          { name: 'sessions' },
          { name: 'activeUsers' },
          { name: 'totalUsers' },
          { name: 'eventCount' },
        ],
        orderBys: [{ metric: { metricName: 'sessions' }, desc: true }],
        limit: 200,
      },
      {
        headers: {
          Authorization: `Bearer ${token.token}`,
        },
      }
    );

    return (response.data?.rows || []).map((row: any) => ({
      country: row.dimensionValues?.[0]?.value || 'Unknown',
      countryCode: row.dimensionValues?.[1]?.value || '',
      sessions: Number(row.metricValues?.[0]?.value) || 0,
      activeUsers: Number(row.metricValues?.[1]?.value) || 0,
      totalUsers: Number(row.metricValues?.[2]?.value) || 0,
      eventCount: Number(row.metricValues?.[3]?.value) || 0,
    }));
  } catch (error) {
    throw new Error('GA4 地理数据拉取失败: ' + ((error as any)?.response?.data?.error?.message || (error as Error).message));
  }
}

// ==================== GA4 来源/媒介渠道质量 ====================
export interface GA4SourceMediumData {
  sourceMedium: string;
  sessions: number;
  bounceRate: number;
  screenPageViewsPerSession: number;
  averageSessionDuration: number;
  newUsers: number;
  activeUsers: number;
  totalUsers: number;
  grossPurchaseRevenue: number;
}

export async function fetchGA4SourceMediumData(
  startDate: string,
  endDate: string
): Promise<GA4SourceMediumData[]> {
  const propertyId = process.env.GA4_PROPERTY_ID;

  if (!propertyId) {
    throw new Error('GA4_PROPERTY_ID 未配置');
  }

  try {
    const auth = new GoogleAuth({
      ...getGA4AuthOptions(),
      scopes: ['https://www.googleapis.com/auth/analytics.readonly'],
    });
    const client = await auth.getClient();
    const token = await client.getAccessToken();

    const response = await axios.post(
      `https://analyticsdata.googleapis.com/v1beta/properties/${propertyId}:runReport`,
      {
        dateRanges: [{ startDate, endDate }],
        dimensions: [
          { name: 'sessionSourceMedium' },
        ],
        metrics: [
          { name: 'grossPurchaseRevenue' },
          { name: 'sessions' },
          { name: 'bounceRate' },
          { name: 'screenPageViewsPerSession' },
          { name: 'averageSessionDuration' },
          { name: 'newUsers' },
          { name: 'activeUsers' },
          { name: 'totalUsers' },
        ],
        orderBys: [{ metric: { metricName: 'sessions' }, desc: true }],
        limit: 50,
      },
      {
        headers: {
          Authorization: `Bearer ${token.token}`,
        },
      }
    );

    return (response.data?.rows || []).map((row: any) => ({
      sourceMedium: row.dimensionValues?.[0]?.value || '(unknown)',
      sessions: Number(row.metricValues?.[1]?.value) || 0,
      bounceRate: Number(row.metricValues?.[2]?.value) || 0,
      screenPageViewsPerSession: Number(row.metricValues?.[3]?.value) || 0,
      averageSessionDuration: Number(row.metricValues?.[4]?.value) || 0,
      newUsers: Number(row.metricValues?.[5]?.value) || 0,
      activeUsers: Number(row.metricValues?.[6]?.value) || 0,
      totalUsers: Number(row.metricValues?.[7]?.value) || 0,
      grossPurchaseRevenue: Number(row.metricValues?.[0]?.value) || 0,
    }));
  } catch (error) {
    throw new Error('GA4 渠道质量数据拉取失败: ' + ((error as any)?.response?.data?.error?.message || (error as Error).message));
  }
}

// ==================== 统一入口 ====================
export const platformFetchers: Record<
  string,
  (startDate: string, endDate: string) => Promise<PlatformMetric[]>
> = {
  google: fetchGoogleAdsMetrics,
  meta: fetchMetaMetrics,
  bing: fetchBingMetrics,
  tiktok: fetchTikTokMetrics,
  ga4: fetchGA4Metrics,
};
