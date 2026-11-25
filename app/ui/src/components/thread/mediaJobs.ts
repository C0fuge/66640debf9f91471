import type { AccountSummary, CoreClient, MediaAccess, MediaJob, MediaJobPage, MediaStart, Message, PluginSummary, ThreadKey } from "../../core";
import { isObject } from "../../core/adapters/client";
import { outcomeUnknown } from "./actions";
import { failureCode, threadId } from "./shared";

const states = new Set(["queued", "running", "choosing", "committing", "cancelling", "succeeded", "failed", "cancelled", "unknown"]);
const reasons = new Set(["notCached", "unsafeCache", "changed", "tooLarge", "destinationExists", "unsupportedDestination", "unavailable", "internal"]);
export const terminalMediaStates = new Set(["succeeded", "failed", "cancelled", "unknown"]);
export interface MediaIntent extends MediaStart { kind: "download" | "save"; phase: "starting" | "unknown" }
interface Ledger {
  sessionId: string | null;
  pending: Map<string, MediaIntent>;
  scopes: Map<string, ThreadKey>;
  uncertainCancels: Set<string>;
}
const ledgers = new WeakMap<CoreClient, Ledger>();
function ledger(client: CoreClient) {
  let value = ledgers.get(client);
  if (!value) {
    value = { sessionId: null, pending: new Map(), scopes: new Map(), uncertainCancels: new Set() };
    ledgers.set(client, value);
  }
  return value;
}
const opaque = (value: unknown): value is string => typeof value === "string" && value.length > 0 && new TextEncoder().encode(value).length <= 256;
const bytes = (value: unknown) => value === null || typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 18446744073709551615n;
const sameThread = (a: ThreadKey, b: ThreadKey) => threadId(a) === threadId(b);
const cancelKey = (job: MediaJob) => JSON.stringify([job.sessionId, threadId(job.thread), job.id]);
const invalidReply = () => ({ code: "unavailable", outcomeUnknown: true });

export function isMediaJob(value: unknown): value is MediaJob {
  if (!isObject(value) || !isObject(value.thread)) return false;
  return opaque(value.id) && opaque(value.sessionId) && typeof value.requestKey === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(value.requestKey)
    && opaque(value.thread.accountId) && opaque(value.thread.chatId) && (value.thread.topicId === null || opaque(value.thread.topicId))
    && opaque(value.messageId) && (value.kind === "download" || value.kind === "save") && states.has(String(value.state))
    && typeof value.cancelRequested === "boolean" && bytes(value.doneBytes) && bytes(value.totalBytes)
    && (value.error === null || isObject(value.error) && failureCode(value.error) !== undefined
      && reasons.has(String(value.error.reason)) && typeof value.error.outcomeUnknown === "boolean");
}

export function isMediaJobPage(value: unknown, thread: ThreadKey, requestKey: string | null): value is MediaJobPage {
  return isObject(value) && opaque(value.sessionId) && Array.isArray(value.jobs) && value.jobs.length <= (requestKey === null ? 16 : 1)
    && new Set(value.jobs.map(job => isObject(job) ? job.id : null)).size === value.jobs.length
    && value.jobs.every(job => isMediaJob(job) && job.sessionId === value.sessionId && sameThread(job.thread, thread)
      && (requestKey === null || job.requestKey === requestKey))
    && (value.nextCursor === null || requestKey === null && typeof value.nextCursor === "string" && value.nextCursor.length > 0 && value.nextCursor.length <= 4096)
    && Number.isSafeInteger(value.remainingRequestKeys) && Number(value.remainingRequestKeys) >= 0 && Number(value.remainingRequestKeys) <= 4096;
}

export function pendingMediaIntents(client: CoreClient, thread: ThreadKey, messageId?: string) {
  return [...ledger(client).pending.values()].filter(intent => sameThread(intent.thread, thread) && (messageId === undefined || intent.messageId === messageId));
}

export function observedMediaScopes(client: CoreClient, thread: ThreadKey) {
  const value = ledger(client);
  const scopes = new Map([[threadId(thread), thread], ...[...value.scopes],
    ...[...value.pending.values()].map(intent => [threadId(intent.thread), intent.thread] as const)]);
  return [...scopes.values()].filter(scope => scope.accountId === thread.accountId && scope.chatId === thread.chatId);
}

// Apply only after the mounted reader's generation/selection guard accepts a page.
export function learnMediaPage(client: CoreClient, thread: ThreadKey, page: MediaJobPage) {
  const value = ledger(client);
  if (value.sessionId !== page.sessionId) {
    value.scopes.clear();
    value.uncertainCancels.clear();
    value.sessionId = page.sessionId;
  }
  if (page.jobs.length && value.scopes.size < 4096) value.scopes.set(threadId(thread), { ...thread });
  for (const job of page.jobs) {
    const intent = value.pending.get(job.requestKey);
    if (intent && intent.sessionId === job.sessionId && sameThread(intent.thread, job.thread)
      && intent.messageId === job.messageId && intent.kind === job.kind) value.pending.delete(job.requestKey);
    value.uncertainCancels.delete(cancelKey(job));
  }
}

export function acknowledgeOldMediaIntent(client: CoreClient, intent: MediaIntent, currentSession: string) {
  const value = ledger(client);
  if (intent.sessionId !== currentSession && value.pending.get(intent.requestKey) === intent) value.pending.delete(intent.requestKey);
}

export async function startMediaIntent(client: CoreClient, thread: ThreadKey, messageId: string, kind: "download" | "save",
  page: MediaJobPage, newKey: () => string = () => crypto.randomUUID()): Promise<MediaJob> {
  const value = ledger(client);
  if (page.remainingRequestKeys === 0 || value.pending.size >= 4096 || pendingMediaIntents(client, thread, messageId).length
    || value.scopes.size >= 4096 && !value.scopes.has(threadId(thread))) throw { code: "conflict" };
  const requestKey = newKey();
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(requestKey) || value.pending.has(requestKey)) throw { code: "invalid_request" };
  const params: MediaStart = { thread: { ...thread }, messageId, sessionId: page.sessionId, requestKey };
  const intent: MediaIntent = { ...params, kind, phase: "starting" };
  value.pending.set(requestKey, intent); // Reserve before the first dispatch/await.
  value.scopes.set(threadId(thread), params.thread);
  try {
    const job = kind === "download" ? await client.startMediaDownload(params) : await client.startMediaSave(params);
    if (!isMediaJob(job) || job.sessionId !== params.sessionId || job.requestKey !== requestKey || job.messageId !== messageId
      || job.kind !== kind || !sameThread(job.thread, thread)) throw invalidReply();
    if (value.pending.get(requestKey) === intent) value.pending.delete(requestKey);
    return job;
  } catch (cause) {
    if (outcomeUnknown(cause) || failureCode(cause) === undefined) intent.phase = "unknown";
    else if (value.pending.get(requestKey) === intent) value.pending.delete(requestKey);
    throw cause;
  }
}

export function mediaCancelNeedsRead(client: CoreClient, job: MediaJob) { return ledger(client).uncertainCancels.has(cancelKey(job)); }
export async function cancelMediaReceipt(client: CoreClient, job: MediaJob): Promise<MediaJob> {
  try {
    const next = await client.cancelMediaJob({ thread: job.thread, sessionId: job.sessionId, jobId: job.id });
    if (!isMediaJob(next) || next.id !== job.id || next.sessionId !== job.sessionId || next.requestKey !== job.requestKey
      || next.messageId !== job.messageId || next.kind !== job.kind || !sameThread(next.thread, job.thread)) throw invalidReply();
    return next;
  } catch (cause) {
    if (outcomeUnknown(cause) || failureCode(cause) === undefined) ledger(client).uncertainCancels.add(cancelKey(job));
    throw cause;
  }
}

export function mediaPermissions(client: CoreClient, plugin: PluginSummary | null | undefined, account: AccountSummary | null | undefined,
  access: MediaAccess | undefined, message: Message) {
  const confirmed = !message.deleted && !message.messageId.startsWith("local:") && !["sending", "failed"].includes(message.delivery ?? "")
    && !!(message.attachment || message.mediaKind);
  const cap = plugin?.capabilities?.["media.download"];
  const tooLarge = !!message.attachment?.bytes && bytes(message.attachment.bytes) && BigInt(message.attachment.bytes) > 2147483648n;
  return { download: confirmed && !tooLarge && !!account?.enabled && account.phase === "running" && plugin?.phase === "running"
      && (cap === true || typeof cap === "object" && cap !== null) && typeof client.startMediaDownload === "function",
    save: confirmed && !tooLarge && !!account?.enabled && access?.saveCopy === true && typeof client.startMediaSave === "function" };
}

const jobErrors: Record<string, string> = {
  notCached: "The attachment is not cached. Download it before saving a copy.",
  unsafeCache: "The cached attachment could not be safely accessed.", changed: "The attachment changed before the operation completed.",
  tooLarge: "This attachment exceeds the 2 GiB limit.", destinationExists: "That name already exists. Choose an unused name for a new copy.",
  unsupportedDestination: "This destination cannot safely publish a new copy.", unavailable: "The operation is unavailable.",
  internal: "The operation could not be completed.",
};
export function mediaJobError(job: MediaJob) {
  const error = job.error;
  return error ? (error.outcomeUnknown ? "The result is unknown. A saved copy may already exist; review before another action. " : "") + jobErrors[error.reason] : "";
}
