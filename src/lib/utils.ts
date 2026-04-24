export const fmt = (n: number) => n?.toLocaleString() ?? '0';

export const fmtMoney = (n: number) =>
  '$' + (n ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export const formatDuration = (seconds: number) => {
  if (!seconds) return '-';
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}分${s.toString().padStart(2, '0')}秒`;
};
