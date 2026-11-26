import type { CoreClient, OutboxItem, ThreadKey } from "../../core";
import { failureCode, failureText, threadId } from "../thread/shared";
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
    await client.enqueueText({ thread, text });
    entry.update({ text: entry.snapshot().text === text ? "" : entry.snapshot().text });
  } catch (cause) {
    const code = failureCode(cause);
    if (code === "unavailable" || code === undefined) {
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
    const ambiguous = code === "unavailable" || !code;
    entry.update({
      outboxPhase: ambiguous ? "reconcile" : "reload",
      outboxNotice: ambiguous ? "The action could not be confirmed. Check the current outbox state." : failureText(cause),
    });
  }
}
