import type { AccountSummary, ChatFlag, ChatFlagContext, CoreClient, PluginSummary, ThreadKey } from "../core";

export const chatFlags = ["pinned", "markedUnread"] as const;
const object = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
export function flagPermissions(plugin: PluginSummary | null | undefined, account: AccountSummary | null | undefined, thread: ThreadKey) {
  const active = thread.topicId === null && !!thread.chatId && !thread.chatId.startsWith("local:") &&
    account?.accountId === thread.accountId && account.enabled === true && account.phase === "running" && !account.reconnecting &&
    plugin?.pluginId === account.pluginId && plugin.phase === "running";
  const caps = plugin?.capabilities;
  const has = (key: string) => !!active && object(caps) && Object.hasOwn(caps, key) && (caps[key] === true || object(caps[key]));
  const refresh = has("chat.get");
  const write = refresh && has("chat.flags");
  return { refresh, pinned: write && has("chat.pin"), markedUnread: write && has("chat.mark_unread") };
}

function matches(value: unknown, target: ThreadKey): value is ChatFlagContext {
  return object(value) && object(value.thread) && value.thread.accountId === target.accountId &&
    value.thread.chatId === target.chatId && value.thread.topicId === null && target.topicId === null &&
    typeof value.sessionId === "string" && !!value.sessionId &&
    (value.contextToken === null || typeof value.contextToken === "string" && !!value.contextToken) &&
    chatFlags.every(flag => value[flag] === null || typeof value[flag] === "boolean");
}

type Attempt = { state: "pending" | "unknown" };
const maxAttempts = 64;
const ledgers = new WeakMap<CoreClient, Map<string, Attempt>>();
export function flagSession(client: CoreClient, pluginId: string, thread: ThreadKey, permissions: () => ReturnType<typeof flagPermissions>) {
  const ledger = ledgers.get(client) ?? new Map<string, Attempt>();
  ledgers.set(client, ledger);
  const key = (flag: ChatFlag) => JSON.stringify([pluginId, thread.accountId, thread.chatId, flag]);
  let generation = 0;
  let context: ChatFlagContext | null = null;
  let busy = false;
  let message = "";
  let activeAttempt: Attempt | null = null;
  return {
    state: () => ({ context, busy, message, capacityBlocked: ledger.size >= maxAttempts }),
    attempt: (flag: ChatFlag) => ledger.get(key(flag))?.state,
    invalidate() {
      generation++;
      context = null;
      busy = false;
      message = "";
      if (activeAttempt) activeAttempt.state = "unknown";
      activeAttempt = null;
    },
    async refresh() {
      if (busy || !permissions().refresh) return;
      const current = ++generation;
      busy = true;
      context = null;
      message = "";
      try {
        const result = await client.refreshChatFlags({ thread });
        if (current !== generation || !permissions().refresh) return;
        if (!matches(result, thread)) throw Error();
        context = result;
        for (const flag of chatFlags) if (result[flag] !== null && ledger.get(key(flag))?.state === "unknown") ledger.delete(key(flag));
      } catch {
        if (current === generation) message = "Could not refresh flags.";
      } finally {
        if (current === generation) busy = false;
      }
    },
    async set(flag: ChatFlag, value: boolean) {
      if (!chatFlags.includes(flag) || busy || !permissions()[flag] || !context?.contextToken ||
        typeof context[flag] !== "boolean" || value !== !context[flag] || ledger.has(key(flag)) || ledger.size >= maxAttempts) return;
      const { sessionId, contextToken } = context;
      context = null;
      busy = true;
      const current = ++generation;
      const attempt: Attempt = { state: "pending" };
      activeAttempt = attempt;
      ledger.set(key(flag), attempt);
      try {
        if (await client.setChatFlag({ thread, sessionId, contextToken, flag, value }) !== null) throw Error();
        if (current === generation) {
          ledger.delete(key(flag));
          message = "Request acknowledged. Refresh flags to review.";
        }
      } catch (error) {
        if (current === generation) {
          if (object(error) && typeof error.code === "string" && error.outcomeUnknown !== true) {
            ledger.delete(key(flag));
            message = "Request declined. Refresh flags to review.";
          } else {
            attempt.state = "unknown";
            message = "Refresh flags to check the outcome.";
          }
        }
      } finally {
        if (current === generation) { busy = false; activeAttempt = null; }
      }
    },
  };
}
