import type { HistoryItem, HistoryPage } from "../../core";

export const pageSize = 50;
export function historyKey(item: HistoryItem) {
  return item.kind === "gap" ? "gap:" + item.gapId : "message:" + item.message.rowKey;
}

export function distinctItems(items: HistoryItem[]) {
  const seen = new Set<string>();
  return items.filter(item => {
    const key = historyKey(item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// An older page owns its leading gap. The old leading marker may have
// disappeared while the service crossed the boundary to fetch this page.
export function prependPage(older: HistoryPage, current: HistoryItem[]) {
  const firstMessage = current.findIndex(item => item.kind === "message");
  return distinctItems([...older.items, ...(firstMessage < 0 ? [] : current.slice(firstMessage))]);
}

const dayFormat = new Intl.DateTimeFormat(undefined, { year: "numeric", month: "long", day: "numeric" });
const timeFormat = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" });
export function messageDate(millis: string) {
  if (!/^-?\d+$/.test(millis)) return null;
  const date = new Date(Number(millis));
  if (!Number.isFinite(date.getTime())) return null;
  return { day: dayFormat.format(date), time: timeFormat.format(date), iso: date.toISOString() };
}
