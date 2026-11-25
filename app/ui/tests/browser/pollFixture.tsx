import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { ChatSummary, CoreChange, CoreClient, CoreRequest, Message, PluginSummary, PollSnapshot, ThreadKey } from "../../src/core";
import "../../src/styles.css";
const plugin: PluginSummary = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running", authMethods: [], settingsSchema: null,
  capabilities: { "polls.vote": true, "polls.close": true, "messages.get": true } };
const accounts = ["Alpha", "Beta"].map(accountId => ({ accountId, label: accountId, pluginId: "fixture", enabled: true, phase: "running" as const, state: "connected", reconnecting: false }));
const listeners = new Set<(event: CoreChange) => void>();
const calls: CoreRequest[] = [];
let mode: "normal" | "unknown" | "missing" | "hold" = "normal";
let release: (() => void) | undefined;
function snapshot(quiz: boolean): PollSnapshot {
  return { id: quiz ? "quiz" : "poll", question: quiz ? "Unanswered quiz" : "Literal <question>", options: [
    { id: "b:AA", text: "First <answer>", votes: null, chosen: quiz ? null : false, correct: null },
    { id: "b:_w", text: "Second", votes: "18446744073709551615", chosen: quiz ? null : false, correct: null }],
    totalVoters: null, closed: false, multiple: false, quiz, anonymous: false, closeAt: null, solution: null, solutionEntities: [] };
}
const records = new Map<string, Message[]>();
for (const account of accounts) records.set(account.accountId, [false, true].map((quiz, index) => ({
  rowKey: String(index + 1), messageId: quiz ? "quiz" : "older", topicId: "topic-1", direction: "out", senderName: null,
  sentAt: String(index), text: null, entities: [], mediaKind: null, poll: snapshot(quiz), edited: false, deleted: false, delivery: "sent",
})));
function chat(accountId: string): ChatSummary {
  return { thread: { accountId, chatId: "chat", topicId: null }, title: accountId + " chat", kind: "group", archived: false, pinned: false,
    muted: false, canPost: true, unreadCount: 0, markedUnread: false, lastMessageAt: "0", lastMessagePreview: "Synthetic polls", lastMessageKind: null };
}
function target(thread: ThreadKey, id: string) {
  const row = records.get(thread.accountId)!.find(row => row.messageId === id)!;
  if (!row || thread.chatId !== "chat" || row.topicId !== thread.topicId) throw Error("Wrong poll target");
  return row;
}
const unused = async () => { throw Error("Unexpected fixture operation"); };
async function pollCall(request: CoreRequest) {
  calls.push(structuredClone(request));
  const params = request.params as { thread: ThreadKey; messageId: string; optionIds?: string[] };
  const row = target(params.thread, params.messageId);
  const next = mode; mode = "normal";
  if (next === "hold") await new Promise<void>(resolve => { release = resolve; });
  if (request.method === "votePoll") row.poll!.options.forEach(option => { option.chosen = params.optionIds!.includes(option.id); });
  if (request.method === "closePoll") row.poll!.closed = true;
  if (next === "unknown") throw { code: "unavailable", outcomeUnknown: true, message: "sensitive fixture error" };
  listeners.forEach(listener => listener({ kind: "account", accountId: params.thread.accountId }));
  return { poll: next === "missing" ? null : structuredClone(row.poll!) };
}
const client: CoreClient = {
  startMediaDownload: unused, startMediaSave: unused, cancelMediaJob: unused,
  listMediaJobs: async () => ({ sessionId: "fixture-session", jobs: [], nextCursor: null, remainingRequestKeys: 4096 }),
  subscribe: async listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
  inventory: async () => structuredClone({ mediaAccess: { saveCopy: false }, plugins: [plugin], accounts }), listChats: async ({ accountId }) => [chat(accountId)],
  history: async ({ thread }) => ({ items: structuredClone(records.get(thread.accountId)!).map(message => ({ kind: "message" as const, message })), moreOlder: false, moreNewer: false }),
  listOutbox: async () => [],
  votePoll: params => pollCall({ method: "votePoll", params }), closePoll: params => pollCall({ method: "closePoll", params }), refreshPoll: params => pollCall({ method: "refreshPoll", params }),
  editText: unused, deleteMessages: unused, pinMessage: unused, unpinMessage: unused, fillGap: unused, enqueueText: unused, cancelOutbox: unused, retryOutbox: unused,
  search: unused, addAccount: unused, getSettings: unused, saveSettings: unused, getAuthStep: unused, beginAuth: unused, submitAuth: unused, cancelAuth: unused,
};
Object.assign(window, { pollFixture: { calls: () => calls, next(value: typeof mode) { mode = value; }, release() { release?.(); release = undefined; } } });
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
