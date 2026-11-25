import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { ChatSummary, CoreChange, CoreClient, CoreRequest, Message, OrdinaryReactionKey, ReactionContext, ReactionSnapshot, ReactorItem, ThreadKey } from "../../src/core";
import "../../src/styles.css";

// All messages, proofs and pages are synthetic. No plugin, asset or file access.
const heart: OrdinaryReactionKey = { kind: "emoji", emoji: "❤️" }, plain: OrdinaryReactionKey = { kind: "emoji", emoji: "❤" };
const custom: OrdinaryReactionKey = { kind: "custom", id: "9007199254740993" };
const plugin = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running" as const,
  capabilities: { "messages.get": true, "reactions.set": { max: 16 as number | null }, "reactions.custom": true, "reactions.who": true }, authMethods: [], settingsSchema: null };
const accounts = ["Alpha", "Beta"].map(accountId => ({ accountId, pluginId: "fixture", label: accountId, enabled: true,
  phase: "running" as const, state: "connected", reconnecting: false }));
const listeners = new Set<(change: CoreChange) => void>(), requests: CoreRequest[] = [];
let clock = 1000, serial = 0, absentNext = false, loseWrite = false, delayNext = false;
Date.now = () => clock; // Page-local test clock; never the host clock.
const keyId = (key: OrdinaryReactionKey | null) => key === null ? "all" : JSON.stringify(key.kind === "emoji" ? [key.kind, key.emoji] : [key.kind, key.id]);
const targetId = (thread: ThreadKey, messageId: string) => JSON.stringify([thread.accountId, thread.chatId, thread.topicId, messageId]);
const snapshots = new Map<string, ReactionSnapshot>();
const emit = (accountId: string) => listeners.forEach(listener => listener({ kind: "account", accountId }));
function snapshot(thread: ThreadKey, messageId: string) {
  const target = targetId(thread, messageId);
  if (!snapshots.has(target)) snapshots.set(target, { items: (messageId === "other-id" ? [{ kind: "emoji" as const, emoji: "👍" }]
    : thread.accountId === "Alpha" ? [heart, custom] : [plain]).map(key => ({ key, count: "1", mine: true })) });
  return structuredClone(snapshots.get(target)!);
}
interface Context { target: string; value: ReactionContext; attempts: number; pages: Map<string, { items: ReactorItem[]; nextCursor: string | null }> }
const contexts = new Map<string, Context>();
let delayed: (() => void) | null = null;
function refresh(thread: ThreadKey, messageId: string, absent: boolean): ReactionContext {
  const target = targetId(thread, messageId);
  for (const [token, context] of contexts) if (context.target === target) contexts.delete(token);
  const contextToken = `proof-${++serial}`;
  const value: ReactionContext = { sessionId: "reaction-fixture-session", contextToken, expiresAt: String(clock + 60000),
    snapshot: absent ? null : snapshot(thread, messageId), availability: { keys: [heart, plain, custom], allowCustom: false, maxPerMessage: "3" },
    canSet: !absent && typeof plugin.capabilities["reactions.set"].max === "number" && plugin.capabilities["reactions.set"].max >= 1 };
  contexts.set(contextToken, { target, value, attempts: 0, pages: new Map() });
  emit(thread.accountId);
  return structuredClone(value);
}
function proof(params: { thread: ThreadKey; messageId: string; sessionId: string; contextToken: string }) {
  const context = contexts.get(params.contextToken);
  if (!context || context.target !== targetId(params.thread, params.messageId) || context.value.sessionId !== params.sessionId
    || Number(context.value.expiresAt) <= clock) throw { code: "conflict" };
  return context;
}
function chat(accountId: string): ChatSummary {
  return { thread: { accountId, chatId: "same-chat", topicId: null }, title: `${accountId} reactions`, kind: "group", archived: false,
    pinned: false, muted: false, canPost: true, unreadCount: 0, markedUnread: false, lastMessageAt: "0", lastMessagePreview: "Synthetic reactions", lastMessageKind: null };
}
function records(accountId: string): Message[] {
  const emojis = ["❤️", "❤", "👍", "👎", "😀", "🎉", "🔥", "🙏", "👀", "✅", "💯", "🤔"];
  const current = snapshots.get(targetId({ accountId, chatId: "same-chat", topicId: "42" }, "same-id"));
  const cached = current ? current.items.length ? { items: current.items.map(item => ({ ...item, mine: null })) } : null
    : { items: emojis.map((emoji, index) => ({ key: { kind: "emoji" as const, emoji }, count: index === 0 ? "9007199254740993" : null, mine: null })) };
  const base: Message = { rowKey: "1", messageId: "same-id", topicId: "42", direction: "out", senderName: null, sentAt: "0",
    text: `${accountId} reaction caption`, entities: [], mediaKind: null, edited: false, deleted: false, delivery: null, reactions: cached };
  return [base, { ...base, rowKey: "2", messageId: "other-id", topicId: "84", direction: "in", senderName: "Other sender", text: "Other topic caption", reactions: null },
    { ...base, rowKey: "3", messageId: "local:pending", delivery: "sending", text: "Pending reaction caption" },
    { ...base, rowKey: "4", messageId: "deleted", deleted: true, text: "Deleted reaction caption" }];
}
const unused = async () => { throw new Error("Unexpected fixture operation"); };
const client: CoreClient = {
  subscribe: async listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
  inventory: async () => structuredClone({ plugins: [plugin], accounts, mediaAccess: { saveCopy: false } }),
  listChats: async ({ accountId }) => [chat(accountId)],
  history: async ({ thread }) => ({ items: records(thread.accountId).map(message => ({ kind: "message" as const, message })), moreOlder: false, moreNewer: false }),
  listOutbox: async () => [],
  refreshMessageReactions: async params => {
    requests.push(structuredClone({ method: "refreshMessageReactions", params }));
    const absent = absentNext; absentNext = false;
    if (delayNext) { delayNext = false; return new Promise(resolve => { delayed = () => resolve(refresh(params.thread, params.messageId, absent)); }); }
    return refresh(params.thread, params.messageId, absent);
  },
  setMessageReactions: async params => {
    requests.push(structuredClone({ method: "setMessageReactions", params }));
    const context = proof(params); contexts.delete(params.contextToken);
    if (!context.value.canSet || params.keys.length > 3 || params.keys.some(key => !context.value.availability!.keys!.some(allowed => keyId(allowed) === keyId(key)))
      || params.keys.some(key => key.kind === "custom" && !plugin.capabilities["reactions.custom"])) throw { code: "unsupported" };
    const next = { items: params.keys.map(key => ({ key, count: "1", mine: true })) };
    snapshots.set(targetId(params.thread, params.messageId), structuredClone(next)); emit(params.thread.accountId);
    if (loseWrite) { loseWrite = false; throw { code: "unavailable", outcomeUnknown: true }; }
    return structuredClone({ snapshot: next });
  },
  listMessageReactors: async params => {
    requests.push(structuredClone({ method: "listMessageReactors", params }));
    const context = proof(params), filter = keyId(params.key), cacheKey = JSON.stringify([filter, params.cursor]);
    const cached = context.pages.get(cacheKey); if (cached) return structuredClone(cached);
    if (context.attempts >= 8) throw { code: "unsupported" };
    let index = 0;
    if (params.cursor !== null) {
      const [token, knownFilter, value] = JSON.parse(params.cursor) as [string, string, number];
      if (token !== params.contextToken || knownFilter !== filter || !Number.isInteger(value) || value < 1) throw { code: "invalid_request" };
      index = value;
    }
    context.attempts++;
    const next = { items: index === 0 ? [] : [{ userId: `user-${index}`, key: params.key ?? heart,
      name: '<img src=x onerror="fixture">', at: "0" }], nextCursor: JSON.stringify([params.contextToken, filter, index + 1]) };
    context.pages.set(cacheKey, next); return structuredClone(next);
  },
  startMediaDownload: unused, startMediaSave: unused, cancelMediaJob: unused,
  listMediaJobs: async () => ({ sessionId: "unrelated-media-session", jobs: [], nextCursor: null, remainingRequestKeys: 4096 }),
  fillGap: unused, enqueueText: unused, editText: unused, deleteMessages: unused, pinMessage: unused, unpinMessage: unused,
  votePoll: unused, closePoll: unused, refreshPoll: unused, cancelOutbox: unused, retryOutbox: unused, search: unused,
  addAccount: unused, getSettings: unused, saveSettings: unused, getAuthStep: unused, beginAuth: unused, submitAuth: unused, cancelAuth: unused,
};
const fixture = {
  requests: () => structuredClone(requests), absentNext() { absentNext = true; }, loseNextWrite() { loseWrite = true; },
  delayNextRefresh() { delayNext = true; }, releaseDelayed() { delayed!(); delayed = null; },
  advance() { clock += 61000; emit("Alpha"); },
  custom(value: boolean) { plugin.capabilities["reactions.custom"] = value; listeners.forEach(listener => listener({ kind: "inventory" })); },
  malformedMax(value: boolean) { plugin.capabilities["reactions.set"].max = value ? null : 16; listeners.forEach(listener => listener({ kind: "inventory" })); },
  reset() { listeners.forEach(listener => listener({ kind: "reset" })); },
};
declare global { interface Window { reactionFixture: typeof fixture } }
window.reactionFixture = fixture;
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
