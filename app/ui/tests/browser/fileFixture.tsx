import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { ChatSummary, CoreChange, CoreClient, CoreRequest, FileStage, OutboxItem, ThreadKey } from "../../src/core";
import "../../src/styles.css";

// Typed receipt fixture only. No files, native dialogs, remote accounts or bytes.
const sessionId = "1".repeat(32);
const plugin = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running" as const,
  capabilities: { "send.text": true, "send.file": true, "send.as_file": true, "send.spoiler": true }, authMethods: [], settingsSchema: null };
const accounts = ["Alpha", "Beta"].map(accountId => ({ accountId, pluginId: "fixture", label: accountId,
  enabled: true, phase: "running" as const, state: "connected", reconnecting: false }));
const listeners = new Set<(c: CoreChange) => void>();
const requests: CoreRequest[] = [];
const stages: FileStage[] = [];
let serial = 2, mode: "unknown" | "consumed" = "unknown", hold = false, release: (() => void) | null = null;
const equal = (a: ThreadKey, b: ThreadKey) => JSON.stringify(a) === JSON.stringify(b);
const emit = (accountId: string) => listeners.forEach(l => l({ kind: "account", accountId }));
function chat(accountId: string): ChatSummary {
  return { thread: { accountId, chatId: "chat", topicId: "42" }, title: `${accountId} files`, kind: "group", archived: false,
    pinned: false, muted: false, canPost: true, unreadCount: 0, markedUnread: false, lastMessageAt: "0", lastMessagePreview: null, lastMessageKind: null };
}
const unused = async () => { throw { code: "unsupported" }; };
const client: CoreClient = {
  subscribe: async listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
  inventory: async () => structuredClone({ plugins: [plugin], accounts, mediaAccess: { saveCopy: false }, composerAccess: { chooseFile: true } }),
  listChats: async ({ accountId }) => [chat(accountId)],
  history: async () => ({ items: [], moreOlder: false, moreNewer: false }),
  listOutbox: async ({ thread }): Promise<OutboxItem[]> => [
    { kind: "sendFile", id: "previous-file", thread, state: "sending", doneBytes: "5", totalBytes: "12", echoRowKey: null, nextAttemptAt: null, outcomeUnknown: false },
    { kind: "sendFile", id: "failed-file", thread, state: "failed", doneBytes: null, totalBytes: null, echoRowKey: null, nextAttemptAt: null, outcomeUnknown: true },
  ],
  listFileStages: async params => {
    const items = stages.filter(s => equal(s.thread, params.thread) && (params.requestKey === null || s.requestKey === params.requestKey));
    return structuredClone({ sessionId, items: items.slice(0, 16), nextCursor: null, remainingRequestKeys: 4096 - stages.length });
  },
  startFileSelection: async params => {
    requests.push(structuredClone({ method: "startFileSelection", params }));
    const stage: FileStage = { ...params, stageId: (++serial).toString(16).padStart(32, "0"), state: "copying", name: "<report>.txt",
      bytes: "12", copiedBytes: "3", format: "other", outboxId: null, outboxRow: null, error: null };
    stages.unshift(stage); emit(params.thread.accountId);
    if (hold) {
      hold = false;
      return new Promise(resolve => { release = () => { stage.state = "ready"; stage.copiedBytes = "12"; emit(stage.thread.accountId); resolve(structuredClone(stage)); }; });
    }
    throw { code: "unavailable", outcomeUnknown: true }; // A real retained receipt recovers this lost reply.
  },
  enqueueFile: async params => {
    requests.push(structuredClone({ method: "enqueueFile", params }));
    const stage = stages.find(s => s.stageId === params.stageId && equal(s.thread, params.thread) && s.sessionId === params.sessionId);
    if (!stage || stage.state !== "ready") throw { code: "conflict" };
    stage.outboxId = (serial + 100).toString(16).padStart(32, "0"); stage.outboxRow = "missing";
    stage.state = mode === "unknown" ? "admissionUnknown" : "enqueued";
    stage.error = mode === "unknown" ? { code: "unavailable", reason: "admissionUnknown", outcomeUnknown: true } : null;
    emit(stage.thread.accountId);
    if (mode === "unknown") throw { code: "unavailable", outcomeUnknown: true };
    return structuredClone(stage);
  },
  cancelFileSelection: async params => {
    requests.push(structuredClone({ method: "cancelFileSelection", params }));
    const stage = stages.find(s => s.stageId === params.stageId && equal(s.thread, params.thread));
    if (!stage || !["ready", "copying", "choosing"].includes(stage.state)) throw { code: "conflict" };
    stage.state = "cancelled"; emit(stage.thread.accountId); return structuredClone(stage);
  },
  retryOutbox: unused, cancelOutbox: unused,
  fillGap: unused, enqueueText: unused, editText: unused, deleteMessages: unused, pinMessage: unused, unpinMessage: unused,
  votePoll: unused, closePoll: unused, refreshPoll: unused, refreshMessageReactions: unused, setMessageReactions: unused, listMessageReactors: unused,
  startMediaDownload: unused, startMediaSave: unused, listMediaJobs: unused, cancelMediaJob: unused, search: unused,
  addAccount: unused, getSettings: unused, saveSettings: unused, getAuthStep: unused, beginAuth: unused, submitAuth: unused, cancelAuth: unused,
};
declare global { interface Window { fileFixture: {
  requests(): CoreRequest[]; ready(account: string): void; mode(value: "unknown" | "consumed"): void;
  hold(): void; release(): void; capacity(): void;
} } }
window.fileFixture = {
  requests: () => structuredClone(requests),
  ready(account) { const s = stages.find(s => s.thread.accountId === account && s.state === "copying"); if (s) { s.state = "ready"; s.copiedBytes = "12"; emit(account); } },
  mode(value) { mode = value; }, hold() { hold = true; }, release() { release?.(); release = null; },
  capacity() {
    const s = stages.find(s => s.thread.accountId === "Alpha" && s.state === "ready");
    if (s) { s.state = "failed"; s.error = { code: "conflict", reason: "capacity", outcomeUnknown: false }; emit("Alpha"); }
  },
};
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
