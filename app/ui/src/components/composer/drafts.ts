import type { CoreClient, OutboxItem, ThreadKey } from "../../core";
import { failureCode, failureText, threadId } from "../thread/shared";
import { outcomeUnknown } from "../thread/actions";
import { notifyLocalHistory } from "../thread/historySignal";

interface Draft {
  text: string;
  pending: boolean;
  uncertain: "checking" | "unverified" | "review" | null;
  error: string | null;
  outboxPhase: "updating" | "reload" | "reconcile" | null;
  outboxNotice: string | null;
}

export class DraftEntry {
  private state: Draft = { text: "", pending: false, uncertain: null, error: null, outboxPhase: null, outboxNotice: null };
  private readonly listeners = new Set<() => void>();
  snapshot = () => this.state;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  };
  update(change: Partial<Draft>) {
    this.state = { ...this.state, ...change };
    this.listeners.forEach(listener => listener());
  }
}

const stores = new WeakMap<CoreClient, Map<string, DraftEntry>>();

export function draftFor(client: CoreClient, thread: ThreadKey) {
  let drafts = stores.get(client);
  if (!drafts) { drafts = new Map(); stores.set(client, drafts); }
  const key = threadId(thread);
  let draft = drafts.get(key);
  if (!draft) { draft = new DraftEntry(); drafts.set(key, draft); }
  return draft;
}

type Recover = () => Promise<void>;

const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
function decimal(value: unknown): value is string {
  return typeof value === "string" && /^-?(0|[1-9]\d{0,18})$/.test(value)
    && BigInt(value) >= -9223372036854775808n && BigInt(value) <= 9223372036854775807n;
}
function textAcceptance(value: unknown, thread: ThreadKey): value is OutboxItem {
  if (!object(value) || !object(value.thread)) return false;
  return value.kind === "sendText" && typeof value.id === "string" && value.id.length > 0 && value.id.length <= 256
    && new TextEncoder().encode(value.id).length <= 256
    && value.thread.accountId === thread.accountId && value.thread.chatId === thread.chatId
    && value.thread.topicId === thread.topicId
    && typeof value.state === "string" && ["queued", "sending", "sent", "failed", "cancelled"].includes(value.state)
    && typeof value.outcomeUnknown === "boolean" && value.doneBytes === null && value.totalBytes === null
    && (value.echoRowKey === null || decimal(value.echoRowKey) && BigInt(value.echoRowKey) > 0n)
    && (value.nextAttemptAt === null || value.state === "queued" && decimal(value.nextAttemptAt));
}

export async function checkAcceptance(entry: DraftEntry, client: CoreClient, thread: ThreadKey) {
  if (entry.snapshot().uncertain === "checking") return;
  entry.update({ uncertain: "checking", error: null });
  try {
    await Promise.all([
      client.listOutbox({ thread }),
      client.history({ thread, window: { kind: "latest" }, limit: 50 }),
    ]);
    notifyLocalHistory(client, thread);
    entry.update({ uncertain: "review" });
  } catch (cause) {
    entry.update({ uncertain: "unverified", error: failureText(cause) });
  }
}

// The entry, destination and body belong to this invocation even if the user
// switches conversations or the composer is unmounted before the reply arrives.
export async function sendDraft(entry: DraftEntry, client: CoreClient, thread: ThreadKey,
  recover: Recover, duplicateConfirmed = false) {
  const { text, pending, uncertain, outboxPhase } = entry.snapshot();
  if (outboxPhase === "reconcile") return;
  if (pending || !text.trim() || (uncertain && !(duplicateConfirmed && uncertain === "review"))) return;
  entry.update({ pending: true, uncertain: null, error: null });
  try {
    const acceptance = await client.enqueueText({ thread, text });
    if (!textAcceptance(acceptance, thread)) throw { code: "unavailable", outcomeUnknown: true };
    entry.update({ text: entry.snapshot().text === text ? "" : entry.snapshot().text });
  } catch (cause) {
    const code = failureCode(cause);
    if (outcomeUnknown(cause) || code === "unavailable" || code === undefined) {
      await recover();
    } else {
      entry.update({ error: failureText(cause) });
    }
  } finally {
    entry.update({ pending: false });
  }
}

export async function changeOutbox(entry: DraftEntry, client: CoreClient, item: OutboxItem,
  action: "cancel" | "retry", confirmed: boolean) {
  if (action === "retry" && item.kind === "sendFile") throw { code: "unsupported" };
  if (entry.snapshot().outboxPhase || (action === "retry" && item.outcomeUnknown && !confirmed)) return;
  entry.update({ outboxPhase: "updating", outboxNotice: null });
  try {
    const target = { thread: item.thread, id: item.id };
    if (action === "retry") await client.retryOutbox(target);
    else await client.cancelOutbox(target);
    entry.update({ outboxPhase: "reload" });
  } catch (cause) {
    const code = failureCode(cause);
    const ambiguous = outcomeUnknown(cause) || code === "unavailable" || !code;
    entry.update({
      outboxPhase: ambiguous ? "reconcile" : "reload",
      outboxNotice: ambiguous ? "The action could not be confirmed. Check the current outbox state." : failureText(cause),
    });
  }
}
