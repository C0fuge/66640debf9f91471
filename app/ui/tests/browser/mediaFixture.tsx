import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { Attachment, ChatSummary, CoreChange, CoreClient, Message } from "../../src/core";
import "../../src/styles.css";

const plugin = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running" as const,
  capabilities: {}, authMethods: [], settingsSchema: null };
const accounts = ["Alpha", "Beta"].map(accountId => ({ accountId, pluginId: "fixture", label: accountId,
  enabled: true, phase: "running" as const, state: "connected", reconnecting: false }));
const requests: string[] = [];
const listeners = new Set<(change: CoreChange) => void>();
const changedNames = new Map<string, string>();
function attachment(values: Partial<Attachment> = {}): Attachment {
  return { kind: "document", name: null, mime: null, bytes: null, width: null, height: null,
    durationSeconds: null, hasSpoiler: false, ...values };
}
function chat(accountId: string): ChatSummary {
  return { thread: { accountId, chatId: "same-chat", topicId: null }, title: `${accountId} media chat`, kind: "group",
    archived: false, pinned: false, muted: false, canPost: true, unreadCount: 0, markedUnread: false,
    lastMessageAt: "0", lastMessagePreview: "Synthetic media", lastMessageKind: "document" };
}
function records(accountId: string): Message[] {
  const base: Message = { rowKey: "1", messageId: "same-id", topicId: null, direction: "in", senderName: "Media sender",
    sentAt: "0", text: "Photo caption", entities: [], mediaKind: "photo", edited: false, deleted: false, delivery: null };
  return [
    { ...base, attachment: attachment({ kind: "photo", name: "photo.jpg", mime: "image/jpeg", bytes: "2048", width: 640, height: 480 }) },
    { ...base, rowKey: "2", messageId: "same-secret-id", topicId: "same-topic", direction: "out", senderName: null,
      text: "First spoiler caption", mediaKind: "document", attachment: attachment({ name: changedNames.get(accountId) ?? `${accountId} secret <img src=x onerror=fixture>.txt`, mime: "text/plain", bytes: "9007199254740993", hasSpoiler: true }) },
    { ...base, rowKey: "3", messageId: "second-secret", text: "Second spoiler caption", mediaKind: "document",
      attachment: attachment({ name: "Second hidden name", hasSpoiler: true }) },
    { ...base, rowKey: "4", messageId: "deleted", deleted: true, text: "Deleted caption", mediaKind: "document",
      attachment: attachment({ name: "Deleted hidden name", mime: "Deleted hidden MIME" }) },
    { ...base, rowKey: "5", messageId: "local:fixture", delivery: "sending", text: "Pending caption", mediaKind: "document",
      attachment: attachment({ name: "Local upload private filename", mime: "Local hidden MIME" }) },
    { ...base, rowKey: "6", messageId: "voice", text: "Voice caption", mediaKind: "voice",
      attachment: attachment({ kind: "voice", mime: "audio/ogg", durationSeconds: 65 }) },
  ];
}
const unused = async () => { throw new Error("Unexpected fixture mutation"); };
const client: CoreClient = {
  votePoll: unused, closePoll: unused, refreshPoll: unused,
  subscribe: async listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
  inventory: async () => { requests.push("inventory"); return { plugins: [plugin], accounts }; },
  listChats: async ({ accountId }) => { requests.push("listChats"); return [chat(accountId)]; },
  history: async ({ thread }) => { requests.push("history"); return { items: records(thread.accountId).map(message => ({ kind: "message" as const, message })), moreOlder: false, moreNewer: false }; },
  listOutbox: async () => { requests.push("listOutbox"); return []; },
  fillGap: unused, enqueueText: unused, editText: unused, deleteMessages: unused, pinMessage: unused,
  unpinMessage: unused, cancelOutbox: unused, retryOutbox: unused, search: unused, addAccount: unused,
  getSettings: unused, saveSettings: unused, getAuthStep: unused, beginAuth: unused, submitAuth: unused, cancelAuth: unused,
};
declare global { interface Window { mediaFixture: { requests(): string[]; changeAttachment(accountId: string): void } } }
window.mediaFixture = {
  requests: () => [...requests],
  changeAttachment(accountId) {
    changedNames.set(accountId, `${accountId} replaced secret`);
    listeners.forEach(listener => listener({ kind: "account", accountId }));
  },
};
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
