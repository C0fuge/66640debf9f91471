import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { ChatSummary, CoreChange, CoreClient, CoreRequest, MediaJob, MediaStart, Message, ThreadKey } from "../../src/core";
import "../../src/styles.css";

// Synthetic receipts only: no native chooser, local file, bytes or account access.
const plugin = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running" as const,
  capabilities: { "media.download": true }, authMethods: [], settingsSchema: null };
const accounts = ["Alpha", "Beta"].map(accountId => ({ accountId, pluginId: "fixture", label: accountId,
  enabled: true, phase: "running" as "running" | "stopped", state: "connected", reconnecting: false }));
const listeners = new Set<(change: CoreChange) => void>();
const requests: CoreRequest[] = [];
const jobs: MediaJob[] = [];
let saveCopy = false, serial = 0, copies = 0;
let first: { params: MediaStart; reject: (cause: unknown) => void } | null = null;
let firstUsed = false;
const deleted = new Set<string>();
const sessionId = "fixture-session";
const topic = (accountId: string): ThreadKey => ({ accountId, chatId: "same-chat", topicId: "42" });
const equalThread = (a: ThreadKey, b: ThreadKey) => a.accountId === b.accountId && a.chatId === b.chatId && a.topicId === b.topicId;
const emit = (accountId: string) => listeners.forEach(listener => listener({ kind: "account", accountId }));
function chat(accountId: string): ChatSummary {
  return { thread: { accountId, chatId: "same-chat", topicId: null }, title: `${accountId} attachments`, kind: "group",
    archived: false, pinned: false, muted: false, canPost: true, unreadCount: 0, markedUnread: false,
    lastMessageAt: "0", lastMessagePreview: "Synthetic attachment", lastMessageKind: "document" };
}
function records(accountId: string): Message[] {
  const message: Message = { rowKey: "1", messageId: "same-message", topicId: "42", direction: "out", senderName: null,
    sentAt: "0", text: `${accountId} download caption`, entities: [], mediaKind: "document", edited: false, deleted: deleted.has(accountId), delivery: null,
    attachment: { kind: "document", name: "spoiler-fixture.bin", mime: "application/octet-stream", bytes: "7",
      width: null, height: null, durationSeconds: null, hasSpoiler: true } };
  return [message, { ...message, rowKey: "2", messageId: "local:pending", topicId: null, text: "Pending attachment", deleted: false, delivery: "sending" },
    { ...message, rowKey: "3", messageId: "deleted-id", topicId: "42", deleted: true, text: "Deleted attachment" },
    { ...message, rowKey: "4", messageId: "last", topicId: null, direction: "in", senderName: "Fixture sender", deleted: false,
      text: "Last caption", attachment: { ...message.attachment!, hasSpoiler: false } }];
}
for (let i = 1; i <= 17; i++) jobs.unshift({ id: `Alpha-old-${i}`, sessionId, requestKey: `old-${i}`, thread: topic("Alpha"),
  messageId: i === 1 ? "deleted-id" : `previous-${i}`, kind: "download", state: i === 1 ? "running" : "succeeded",
  cancelRequested: false, doneBytes: i === 1 ? "1" : "7", totalBytes: "7", error: null });
function reserve(params: MediaStart, kind: "download" | "save"): MediaJob {
  const job: MediaJob = { ...structuredClone(params), id: `${params.thread.accountId}-new-${++serial}`, kind,
    state: kind === "save" ? "choosing" : "running", cancelRequested: false, doneBytes: "3", totalBytes: "7", error: null };
  jobs.unshift(job);
  return structuredClone(job);
}
const unused = async () => { throw new Error("Unexpected fixture mutation"); };
const client: CoreClient = {
  subscribe: async listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
  inventory: async () => structuredClone({ plugins: [plugin], accounts, mediaAccess: { saveCopy } }),
  listChats: async ({ accountId }) => [chat(accountId)],
  history: async ({ thread }) => ({ items: records(thread.accountId).map(message => ({ kind: "message" as const, message })), moreOlder: false, moreNewer: false }),
  listOutbox: async () => [],
  listMediaJobs: async params => {
    requests.push(structuredClone({ method: "listMediaJobs", params }));
    const filtered = jobs.filter(job => equalThread(job.thread, params.thread));
    const key = JSON.stringify(params.thread);
    let offset = 0;
    if (params.cursor !== null) {
      const decoded = JSON.parse(params.cursor) as [string, number];
      if (decoded[0] !== key) throw { code: "invalid_request" };
      offset = decoded[1];
    }
    const rows = params.requestKey === null ? filtered.slice(offset, offset + 16) : filtered.filter(job => job.requestKey === params.requestKey);
    return structuredClone({ sessionId, jobs: rows, nextCursor: params.requestKey === null && offset + 16 < filtered.length ? JSON.stringify([key, offset + 16]) : null,
      remainingRequestKeys: 4096 - jobs.length });
  },
  startMediaDownload: async params => {
    requests.push(structuredClone({ method: "startMediaDownload", params }));
    if (!firstUsed) {
      firstUsed = true;
      return new Promise((_resolve, reject) => { first = { params: structuredClone(params), reject }; });
    }
    const job = reserve(params, "download"); emit(params.thread.accountId); return job;
  },
  startMediaSave: async params => {
    requests.push(structuredClone({ method: "startMediaSave", params }));
    const job = reserve(params, "save"); emit(params.thread.accountId); return job;
  },
  cancelMediaJob: async params => {
    requests.push(structuredClone({ method: "cancelMediaJob", params }));
    const job = jobs.find(job => job.id === params.jobId && job.sessionId === params.sessionId && equalThread(job.thread, params.thread));
    if (!job) throw { code: "not_found" };
    job.cancelRequested = true;
    job.state = job.kind === "save" ? "cancelling" : "cancelled";
    emit(params.thread.accountId); return structuredClone(job);
  },
  fillGap: unused, enqueueText: unused, editText: unused, deleteMessages: unused, pinMessage: unused, unpinMessage: unused,
  votePoll: unused, closePoll: unused, refreshPoll: unused, cancelOutbox: unused, retryOutbox: unused, search: unused,
  addAccount: unused, getSettings: unused, saveSettings: unused, getAuthStep: unused, beginAuth: unused, submitAuth: unused, cancelAuth: unused,
};
const fixture = {
  requests: () => structuredClone(requests), copies: () => copies,
  loseFirst() { first!.reject({ code: "unavailable", outcomeUnknown: true }); },
  admitFirst() { reserve(first!.params, "download"); }, // No notification: explicit key recovery must find it.
  finishDownload() { const job = jobs.find(job => job.requestKey === first!.params.requestKey)!; job.state = "succeeded"; job.doneBytes = "7"; emit("Alpha"); },
  provider(value: boolean) { saveCopy = value; listeners.forEach(listener => listener({ kind: "inventory" })); },
  settleChooser() {
    const job = jobs.find(job => job.kind === "save" && (job.state === "choosing" || job.state === "cancelling"))!;
    if (job.cancelRequested) job.state = "cancelled";
    else { job.state = "succeeded"; job.doneBytes = "7"; copies++; }
    emit(job.thread.accountId);
  },
  offline() { accounts[0].phase = "stopped"; accounts[0].state = "disconnected"; emit("Alpha"); },
  deleteTarget() { deleted.add("Alpha"); emit("Alpha"); },
  reset() { listeners.forEach(listener => listener({ kind: "reset" })); },
};
declare global { interface Window { mediaJobsFixture: typeof fixture } }
window.mediaJobsFixture = fixture;
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
