import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { ChatSummary, CoreChange, CoreClient, CoreRequest, Message, PluginSummary, ThreadKey } from "../../src/core";
import "../../src/styles.css";

const plugin: PluginSummary = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running", authMethods: [], settingsSchema: null,
  capabilities: { edit: true, "send.entities": true, delete: true, "delete.for_me": false, pin: true, unpin: true } };
const accounts = ["Alpha", "Beta"].map(accountId => ({ accountId, label: accountId, pluginId: "fixture", enabled: true, phase: "running" as const, state: "connected", reconnecting: false }));
const listeners = new Set<(event: CoreChange) => void>();
const calls: CoreRequest[] = [];
let next: "normal" | "unknown" | "hold" = "normal";
let release: (() => void) | undefined;
let historyAvailable = true;
const records = new Map<string, Message[]>();
for (const account of accounts) {
  records.set(account.accountId, [
    { rowKey: "1", messageId: "outgoing", topicId: null, direction: "out", senderName: null, sentAt: "0", text: account.accountId + " original 😀", entities: [{ type: "bold", offset: 0, length: 5 }], mediaKind: null, edited: false, deleted: false, delivery: "sent" },
    { rowKey: "2", messageId: "incoming", topicId: "topic-1", direction: "in", senderName: "Fixture sender", sentAt: "1", text: account.accountId + " incoming", entities: [], mediaKind: null, edited: false, deleted: false, delivery: null },
    { rowKey: "3", messageId: "local:fixture", topicId: null, direction: "out", senderName: null, sentAt: "2", text: "Provisional", entities: [], mediaKind: null, edited: false, deleted: false, delivery: "sending" },
  ]);
}
function publish(accountId: string) { listeners.forEach(listener => listener({ kind: "account", accountId })); }
function chat(accountId: string): ChatSummary {
  return { thread: { accountId, chatId: "chat", topicId: null }, title: accountId + " chat", kind: "group", archived: false, pinned: false, muted: false,
    canPost: true, unreadCount: 0, markedUnread: false, lastMessageAt: "0", lastMessagePreview: "Synthetic conversation", lastMessageKind: null };
}
async function mutate(request: CoreRequest, effect: () => void) {
  calls.push(structuredClone(request));
  const mode = next; next = "normal";
  if (mode === "hold") await new Promise<void>(resolve => { release = resolve; });
  effect();
  if (mode === "unknown") throw { code: "unavailable", outcomeUnknown: true, message: "sensitive fixture payload" };
  const params = request.params as { thread: ThreadKey };
  publish(params.thread.accountId);
  return null;
}
function target(thread: ThreadKey, messageId: string) {
  const message = records.get(thread.accountId)!.find(row => row.messageId === messageId)!;
  if (!message || message.topicId !== thread.topicId) throw new Error("Wrong fixture target");
  return message;
}
const unused = async () => { throw new Error("Unexpected fixture operation"); };
const client: CoreClient = {
  votePoll: unused, closePoll: unused, refreshPoll: unused,
  subscribe: async listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
  inventory: async () => structuredClone({ plugins: [plugin], accounts }),
  listChats: async ({ accountId }) => [chat(accountId)],
  history: async ({ thread }) => {
    if (!historyAvailable) throw { code: "unavailable" };
    return { items: structuredClone(records.get(thread.accountId)!).map(message => ({ kind: "message" as const, message })), moreOlder: false, moreNewer: false };
  },
  listOutbox: async () => [],
  editText: params => mutate({ method: "editText", params }, () => {
    const message = target(params.thread, params.messageId); message.text = params.text; message.entities = params.entities; message.edited = true;
  }),
  deleteMessages: params => mutate({ method: "deleteMessages", params }, () => {
    params.messageIds.forEach(id => target(params.thread, id));
    records.set(params.thread.accountId, records.get(params.thread.accountId)!.filter(row => !params.messageIds.includes(row.messageId)));
  }),
  pinMessage: params => mutate({ method: "pinMessage", params }, () => { target(params.thread, params.messageId).isPinned = true; }),
  unpinMessage: params => mutate({ method: "unpinMessage", params }, () => { target(params.thread, params.messageId).isPinned = false; }),
  fillGap: unused, enqueueText: unused, cancelOutbox: unused, retryOutbox: unused, search: unused, addAccount: unused,
  getSettings: unused, saveSettings: unused, getAuthStep: unused, beginAuth: unused, submitAuth: unused, cancelAuth: unused,
};
Object.assign(window, { actionFixture: {
  calls: () => calls, next(mode: typeof next) { next = mode; }, release() { release?.(); release = undefined; },
  historyAvailable(value: boolean) { historyAvailable = value; },
  capability(key: string, value: boolean) { plugin.capabilities![key] = value; listeners.forEach(listener => listener({ kind: "inventory" })); },
} });
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
