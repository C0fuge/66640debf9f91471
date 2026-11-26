import type { CoreClient, FileStage, FileStagePage, Inventory, OutboxItem, ThreadKey } from "../../core";
import { isObject } from "../../core/adapters/client";
import { failureCode, threadId } from "../thread/shared";

const states = new Set(["choosing", "copying", "ready", "verifying", "enqueuing", "enqueued", "admissionUnknown", "cancelling", "cancelled", "failed"]);
const reasons = new Set(["unavailable", "chooserUncertain", "unsafeSource", "sourceChanged", "stageChanged", "tooLarge", "capacity", "targetChanged", "interrupted", "storage", "admissionUnknown", "integrity"]);
const hex = (v: unknown): v is string => typeof v === "string" && /^[0-9a-f]{32}$/.test(v);
const key = (v: unknown): v is string => typeof v === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(v);
const opaque = (v: unknown): v is string => typeof v === "string" && v.length > 0 && new TextEncoder().encode(v).length <= 256;
const bytes = (v: unknown): v is string => typeof v === "string" && /^(0|[1-9][0-9]{0,19})$/.test(v) && BigInt(v) <= 18446744073709551615n;
const same = (a: ThreadKey, b: ThreadKey) => threadId(a) === threadId(b);
const bounded = (v: unknown) => { try { return new TextEncoder().encode(JSON.stringify(v)).length <= 128 * 1024; } catch { return false; } };
const unknown = () => ({ code: "unavailable", outcomeUnknown: true });
export interface FileIntent { thread: ThreadKey; sessionId: string; requestKey: string; pending: boolean }
interface Action { thread: ThreadKey; sessionId: string; stageId: string; kind: "send" | "cancel"; pending: boolean }
interface Ledger { starts: Map<string, FileIntent>; actions: Map<string, Action>; drafts: Map<string, FileSend> }
const ledgers = new WeakMap<CoreClient, Ledger>();
function ledger(client: CoreClient) {
  let l = ledgers.get(client);
  if (!l) { l = { starts: new Map(), actions: new Map(), drafts: new Map() }; ledgers.set(client, l); }
  return l;
}
const actionKey = (s: Pick<FileStage, "sessionId" | "thread" | "stageId">) => JSON.stringify([s.sessionId, threadId(s.thread), s.stageId]);
export const fileInProgress = (s: FileStage) => ["choosing", "copying", "verifying", "enqueuing", "cancelling"].includes(s.state);

export function isFileStage(v: unknown): v is FileStage {
  if (!isObject(v) || !isObject(v.thread) || !hex(v.stageId) || !hex(v.sessionId)
    || !(v.requestKey === null || key(v.requestKey)) || !opaque(v.thread.accountId) || !opaque(v.thread.chatId)
    || !(v.thread.topicId === null || opaque(v.thread.topicId)) || !(typeof v.state === "string" && states.has(v.state))) return false;
  if (!(v.name === null || typeof v.name === "string" && v.name.length > 0 && new TextEncoder().encode(v.name).length <= 255
    && !/[\u0000-\u001f\u007f-\u009f/\\\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]/.test(v.name) && ![".", ".."].includes(v.name))) return false;
  if (!(v.bytes === null || bytes(v.bytes) && BigInt(v.bytes) > 0n && BigInt(v.bytes) <= 2147483648n)
    || !(v.copiedBytes === null || bytes(v.copiedBytes))
    || v.bytes !== null && v.copiedBytes !== null && BigInt(v.copiedBytes as string) > BigInt(v.bytes as string)
    || !(v.format === null || typeof v.format === "string" && ["jpeg", "png", "other"].includes(v.format))
    || !(v.outboxId === null || hex(v.outboxId)) || ![null, "present", "missing", "unchecked"].includes(v.outboxRow as string | null)) return false;
  if (["ready", "verifying", "enqueuing", "enqueued", "admissionUnknown"].includes(String(v.state))
    && (v.name === null || v.bytes === null || v.format === null)) return false;
  if (["enqueued", "admissionUnknown"].includes(String(v.state)) && (v.outboxId === null || v.outboxRow === null)) return false;
  return v.error === null || isObject(v.error) && failureCode(v.error) !== undefined && typeof v.error.reason === "string" && reasons.has(v.error.reason) && typeof v.error.outcomeUnknown === "boolean";
}
export function isFileStagePage(v: unknown, thread: ThreadKey, requestKey: string | null): v is FileStagePage {
  return isObject(v) && bounded(v) && hex(v.sessionId) && Array.isArray(v.items) && v.items.length <= (requestKey === null ? 16 : 1)
    && v.items.every(s => isFileStage(s) && s.sessionId === v.sessionId && same(s.thread, thread) && (requestKey === null || s.requestKey === requestKey))
    && new Set(v.items.map(s => s.stageId)).size === v.items.length
    && (v.nextCursor === null || requestKey === null && typeof v.nextCursor === "string" && v.nextCursor.length > 0 && v.nextCursor.length <= 4096)
    && Number.isSafeInteger(v.remainingRequestKeys) && Number(v.remainingRequestKeys) >= 0 && Number(v.remainingRequestKeys) <= 4096;
}
export function pendingFileStarts(client: CoreClient, thread: ThreadKey) {
  return [...ledger(client).starts.values()].filter(p => same(p.thread, thread));
}
export function fileAction(client: CoreClient, stage: FileStage) { return ledger(client).actions.get(actionKey(stage)); }
// Only call after the mounted exact-thread reader accepts its generation.
export function learnFilePage(client: CoreClient, page: FileStagePage) {
  const l = ledger(client);
  for (const stage of page.items) {
    const start = stage.requestKey === null ? undefined : l.starts.get(stage.requestKey);
    if (start && start.sessionId === stage.sessionId && same(start.thread, stage.thread)) l.starts.delete(start.requestKey);
    const a = l.actions.get(actionKey(stage));
    if (a && !a.pending && (a.kind === "send" ? ["verifying", "enqueuing", "enqueued", "admissionUnknown", "cancelled", "failed"].includes(stage.state)
      : ["cancelled", "failed", "enqueued", "admissionUnknown"].includes(stage.state))) l.actions.delete(actionKey(stage));
  }
}
export function reviewOldFileIntent(client: CoreClient, intent: FileIntent, currentSession: string) {
  if (!intent.pending && intent.sessionId !== currentSession && ledger(client).starts.get(intent.requestKey) === intent) ledger(client).starts.delete(intent.requestKey);
}
function definite(cause: unknown) { const code = failureCode(cause); return code !== undefined && !(isObject(cause) && cause.outcomeUnknown === true); }
export async function selectFile(client: CoreClient, thread: ThreadKey, page: FileStagePage, newKey: () => string = () => crypto.randomUUID()): Promise<FileStage> {
  const l = ledger(client);
  if (!hex(page.sessionId) || page.remainingRequestKeys === 0 || l.starts.size >= 4096 || pendingFileStarts(client, thread).length) throw { code: "conflict" };
  const requestKey = newKey();
  if (!key(requestKey) || l.starts.has(requestKey)) throw { code: "conflict" };
  const intent: FileIntent = { thread: { ...thread }, sessionId: page.sessionId, requestKey, pending: true };
  l.starts.set(requestKey, intent);
  try {
    const s = await client.startFileSelection({ thread: intent.thread, sessionId: intent.sessionId, requestKey });
    if (!bounded(s) || !isFileStage(s) || !same(s.thread, thread) || s.sessionId !== intent.sessionId || s.requestKey !== requestKey) throw unknown();
    l.starts.delete(requestKey); return s;
  } catch (cause) { if (definite(cause)) l.starts.delete(requestKey); throw cause; }
  finally { intent.pending = false; }
}
export interface FileSend { caption: string; asFile: boolean; spoiler: boolean }
export function fileDraft(client: CoreClient, stage: FileStage): FileSend {
  return ledger(client).drafts.get(actionKey(stage)) ?? { caption: "", asFile: false, spoiler: false };
}
export function rememberFileDraft(client: CoreClient, stage: FileStage, intent: FileSend) {
  const l = ledger(client), id = actionKey(stage);
  if (!l.drafts.has(id) && l.drafts.size >= 4096) return false;
  l.drafts.set(id, { ...intent });
  return true;
}
export const validCaption = (s: string) => Array.from(s).length <= 1024 && new TextEncoder().encode(s).length <= 4096;
export async function changeFile(client: CoreClient, stage: FileStage, kind: "send" | "cancel", intent: FileSend = { caption: "", asFile: false, spoiler: false }): Promise<FileStage> {
  const l = ledger(client), id = actionKey(stage), previous = l.actions.get(id);
  if (previous && (previous.pending || kind === "send" || previous.kind === "cancel")) throw { code: "conflict" };
  if (kind === "send" && (stage.state !== "ready" || !validCaption(intent.caption))) throw { code: "invalid_request" };
  if (l.actions.size >= 4096 && !previous) throw { code: "conflict" };
  const a: Action = { thread: { ...stage.thread }, sessionId: stage.sessionId, stageId: stage.stageId, kind, pending: true };
  l.actions.set(id, a);
  const target = { thread: a.thread, sessionId: a.sessionId, stageId: a.stageId };
  try {
    const s = kind === "send" ? await client.enqueueFile({ ...target, ...intent }) : await client.cancelFileSelection(target);
    if (!bounded(s) || !isFileStage(s) || actionKey(s) !== id) throw unknown();
    l.actions.delete(id); return s;
  } catch (cause) { if (definite(cause)) { if (previous) l.actions.set(id, previous); else l.actions.delete(id); } throw cause; }
  finally { a.pending = false; }
}
export function fileAccess(inventory: Inventory | null, thread: ThreadKey) {
  const a = inventory?.accounts.find(a => a.accountId === thread.accountId);
  const p = inventory?.plugins.find(p => p.pluginId === a?.pluginId);
  const cap = p?.capabilities?.["send.file"];
  let maxBytes: number | null = null;
  if (cap === true || isObject(cap) && !Object.hasOwn(cap, "max_bytes")) maxBytes = 2147483648;
  else if (isObject(cap) && typeof cap.max_bytes === "number" && Number.isInteger(cap.max_bytes) && cap.max_bytes > 0
    && BigInt(cap.max_bytes) <= 18446744073709551615n) maxBytes = Math.min(cap.max_bytes, 2147483648);
  const enabled = (k: string) => p?.capabilities?.[k] === true || isObject(p?.capabilities?.[k]);
  return { canSend: !!a?.enabled && a.phase === "running" && p?.phase === "running" && maxBytes !== null,
    canChoose: inventory?.composerAccess?.chooseFile === true, maxBytes, asFile: enabled("send.as_file"), spoiler: enabled("send.spoiler") };
}
export function stageNotice(s: FileStage) {
  if (s.state === "admissionUnknown") return "Could not confirm whether this file was added to the outbox. Check its status; sending again could duplicate it.";
  if (s.state === "enqueued" && s.outboxRow === "missing") return "Added to outbox. Delivery record unavailable.";
  if (s.state === "enqueued" && s.outboxRow === "unchecked") return "Added to outbox. Delivery status could not be checked.";
  const messages: Record<string, string> = {
    capacity: "File storage is full. Sent files are retained, so removing an unsent file may not free enough space.",
    chooserUncertain: "The file dialog did not confirm completion. Close it before restarting the app to choose another file.",
    unsafeSource: "This file location cannot be used. Choose a regular local file outside application data.",
    sourceChanged: "The original file changed while it was copied. Choose it again when it is no longer changing.",
    stageChanged: "The saved copy changed and cannot be sent.", tooLarge: "This file exceeds the account’s file limit.",
    targetChanged: "This conversation or account changed. Check its current state.", interrupted: "File preparation was interrupted.",
    storage: "The file could not be stored or checked.", integrity: "The saved file record could not be verified.",
    unavailable: "File preparation is unavailable for this account.", admissionUnknown: "File admission could not be confirmed. Check its status before choosing again.",
  };
  return s.error ? messages[s.error.reason] : null;
}
export function uploadNotice(item: OutboxItem) {
  if (item.kind !== "sendFile" || item.state !== "sending" || !bytes(item.doneBytes)) return null;
  const done = BigInt(item.doneBytes);
  const total = bytes(item.totalBytes) && BigInt(item.totalBytes) >= done ? BigInt(item.totalBytes) : null;
  return `Uploaded ${done.toLocaleString()} bytes${total === null ? "" : ` of ${total.toLocaleString()}`}`;
}
