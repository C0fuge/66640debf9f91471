import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { ChatSummary, CoreClient, CoreChange, CoreRequest, ThreadKey, PluginSummary } from "../../src/core";
import "../../src/styles.css";

const accounts = ["Alpha", "Beta"].map(accountId => ({ accountId, pluginId: "fixture", label: accountId, enabled: true,
  phase: "running" as const, state: "connected", reconnecting: false }));
const plugin: PluginSummary = { pluginId: "fixture", name: "Fixture", phase: "running", capabilities: {
  "chat.get": {}, "chat.flags": true, "chat.pin": true, "chat.mark_unread": {} }, authMethods: [], settingsSchema: null, brandColor: null };
const listeners = new Set<(change: CoreChange) => void>();
const requests: CoreRequest[] = [];
let delayed = false, lose = false, partial = false;
let release = () => {};
const emit = (change: CoreChange) => listeners.forEach(f => f(change));
const flags = new Map<string, { pinned: boolean; markedUnread: boolean }>();
const current = (t: ThreadKey) => flags.get(JSON.stringify(t)) ?? { pinned: false, markedUnread: false };
const unused = async () => { throw Error("Unexpected fixture operation"); };
const client: CoreClient = {
  subscribe: async cb => { listeners.add(cb); return () => { listeners.delete(cb); }; },
  inventory: async () => structuredClone({ accounts, plugins: [plugin], composerAccess: { chooseFile: false }, mediaAccess: { saveCopy: false } }),
  listChats: async ({ accountId }) => Array.from({ length: 101 }, (_, i): ChatSummary => ({
    thread: { accountId, chatId: String(i), topicId: null }, title: `${accountId} dialog ${i}`, archived: i === 100,
    kind: "channel", canPost: false, pinned: false, markedUnread: false, muted: null, unreadCount: 7,
    lastMessageAt: null, lastMessagePreview: null, lastMessageKind: null })),
  refreshChatFlags: async params => {
    requests.push({ method: "refreshChatFlags", params });
    const data = { ...params, sessionId: "run", contextToken: String(requests.length), ...current(params.thread),
      pinned: partial ? null : current(params.thread).pinned };
    if (delayed) { delayed = false; await new Promise<void>(resolve => { release = resolve; }); }
    return data;
  },
  setChatFlag: async params => {
    requests.push({ method: "setChatFlag", params });
    flags.set(JSON.stringify(params.thread), { ...current(params.thread), [params.flag]: params.value });
    emit({ kind: "account", accountId: params.thread.accountId });
    if (lose) { lose = false; throw { code: "unavailable", outcomeUnknown: true }; }
    return null;
  },
  history: async () => ({ items: [], moreOlder: false, moreNewer: false }), listOutbox: async () => [],
  listFileStages: async () => ({ sessionId: "run", items: [], nextCursor: null, remainingRequestKeys: 4096 }),
  listMediaJobs: async () => ({ sessionId: "run", jobs: [], nextCursor: null, remainingRequestKeys: 4096 }),
  startFileSelection: unused, cancelFileSelection: unused, enqueueFile: unused, startMediaDownload: unused, startMediaSave: unused,
  cancelMediaJob: unused, editText: unused, deleteMessages: unused, pinMessage: unused, unpinMessage: unused, votePoll: unused,
  closePoll: unused, refreshPoll: unused, refreshMessageReactions: unused, setMessageReactions: unused, listMessageReactors: unused,
  enqueueText: unused, fillGap: unused, cancelOutbox: unused, retryOutbox: unused, search: unused, getSettings: unused,
  saveSettings: unused, addAccount: unused, getAuthStep: unused, beginAuth: unused, cancelAuth: unused, submitAuth: unused,
};
Object.assign(window, { flagFixture: { requests: () => structuredClone(requests), delay: () => { delayed = true; },
  release: () => release(), lose: () => { lose = true; }, partial: (v: boolean) => { partial = v; },
  reset: () => emit({ kind: "reset" }), cache: () => emit({ kind: "account", accountId: "Alpha" }),
  disableManual: () => { plugin.capabilities!["chat.mark_unread"] = false; emit({ kind: "inventory" }); } } });
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
