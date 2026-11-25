import assert from "node:assert/strict";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { AccountSummary, CoreClient, Message, OrdinaryReactionKey, PluginSummary, ReactionContext, ReactionSnapshot } from "../src/core";
import { ReactionPanel, ReactionSummary } from "../src/components/thread/ReactionPanel";
import { isReactionKey, isReactionSnapshot, reactionKeyId, reactionPermissions, reactionSession, type ReactionPermissions, type ReactionPlan } from "../src/components/thread/reactions";

const heart: OrdinaryReactionKey = { kind: "emoji", emoji: "❤️" }, plain: OrdinaryReactionKey = { kind: "emoji", emoji: "❤" };
const custom: OrdinaryReactionKey = { kind: "custom", id: "9007199254740993" };
const thread = { accountId: "Alpha", chatId: "same-chat", topicId: "42" };
const original: ReactionSnapshot = { items: [{ key: heart, count: "9007199254740993", mine: true }, { key: custom, count: "1", mine: true }] };
const fresh = (overrides: Partial<ReactionContext> = {}): ReactionContext => ({ sessionId: "reaction-session", contextToken: "proof", expiresAt: "61000",
  snapshot: structuredClone(original), availability: { keys: [heart, plain, custom], allowCustom: false, maxPerMessage: null }, canSet: true, ...overrides });
const allowed: ReactionPermissions = { refresh: true, set: true, who: true, custom: true, declaredMax: 16 };
const core = (methods: Partial<CoreClient>) => methods as CoreClient;
const plan = (value: ReactionPlan | string) => { assert.notEqual(typeof value, "string"); return value as ReactionPlan; };
const account: AccountSummary = { accountId: "Alpha", pluginId: "fixture", label: null, enabled: true, phase: "running", state: "connected", reconnecting: false };
const plugin: PluginSummary = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running",
  capabilities: { "messages.get": true, "reactions.set": { max: 16 }, "reactions.custom": true, "reactions.who": true }, authMethods: [], settingsSchema: null };
const message: Message = { rowKey: "1", messageId: "same-id", topicId: "42", direction: "out", sentAt: "0", senderName: null,
  text: "Caption <svg/onload=fixture>", entities: [], mediaKind: null, edited: false, deleted: false, delivery: null, reactions: original };

test("cached component chips are bounded/inert and mounting the real panel makes no reaction RPC", () => {
  let calls = 0;
  const client = core({ refreshMessageReactions: async () => { calls++; return fresh(); }, setMessageReactions: async () => { calls++; return { snapshot: null }; },
    listMessageReactors: async () => { calls++; return { items: [], nextCursor: null }; } });
  const values = Array.from({ length: 12 }, (_, i) => ({ key: { kind: "custom" as const, id: i === 0 ? '<img src=x onerror="fixture">' : `id-${i}` }, count: null, mine: null }));
  const markup = renderToStaticMarkup(createElement(ReactionSummary, { client, thread, message: { ...message, reactions: { items: values } }, plugin, disabled: false, select: () => {} }));
  assert.equal((markup.match(/<li>/g) ?? []).length, 8);
  assert.match(markup, /partial view|View\/refresh reactions/); assert.match(markup, /count unknown.*your choice unknown/);
  assert.match(markup, /&lt;img/); assert.doesNotMatch(markup, /<img|src=\"|href=|checked/);
  const panel = renderToStaticMarkup(createElement(ReactionPanel, { selection: { thread, message }, client, plugin, account, unavailable: false, close: () => {} }));
  assert.match(panel, /Caption &lt;svg/); assert.match(panel, /refresh required/); assert.doesNotMatch(panel, /<svg|checked|reorder|chosen.order/i);
  assert.equal(calls, 0);
  for (const value of [{ ...message, deleted: true }, { ...message, messageId: "local:echo" }, { ...message, delivery: "failed" }]) {
    assert.equal(renderToStaticMarkup(createElement(ReactionSummary, { client, thread, message: value, plugin, disabled: false, select: () => {} })), "");
  }
});

test("strict key identity preserves sequences/custom IDs and malformed handshake max disables writes", () => {
  assert.notEqual(reactionKeyId(heart), reactionKeyId(plain));
  assert.equal(isReactionKey({ kind: "emoji", emoji: "👩‍💻" }), true);
  for (const key of [{ kind: "paid", count: 1 }, { kind: "custom", id: "x", emoji: "❤" }, { kind: "emoji", emoji: "" }, { kind: "emoji", emoji: "😀".repeat(33) }]) assert.equal(isReactionKey(key), false);
  const client = core({ refreshMessageReactions: async () => fresh(), setMessageReactions: async () => ({ snapshot: null }), listMessageReactors: async () => ({ items: [], nextCursor: null }) });
  for (const cap of [true, {}]) {
    const p = reactionPermissions(client, { ...plugin, capabilities: { ...plugin.capabilities, "reactions.set": cap } }, account, thread, message);
    assert.equal(p.set, true); assert.equal(p.declaredMax, 1);
  }
  for (const max of [null, 0, -1, 1.5, "3"]) {
    const p = reactionPermissions(client, { ...plugin, capabilities: { ...plugin.capabilities, "reactions.set": { max } } }, account, thread, message);
    assert.equal(p.set, false); assert.equal(p.declaredMax, null);
  }
  assert.equal(reactionPermissions(client, plugin, { ...account, accountId: "Beta" }, thread, message).refresh, false);
});

test("fresh absent, known-empty and sparse personal state remain distinct", async () => {
  const client = core({ refreshMessageReactions: async () => fresh({ snapshot: null, canSet: false }) });
  const session = reactionSession(client, thread, "same-id", null, () => allowed, () => 1000);
  assert.equal(typeof session.plan([plain]), "string");
  await session.refresh(); assert.equal(session.snapshot, null); assert.equal(session.canSet, false); assert.equal(session.canWho, true);
  client.refreshMessageReactions = async () => fresh({ snapshot: { items: [] } });
  await session.refresh(); assert.deepEqual(session.snapshot, { items: [] }); assert.equal(session.canSet, true);
  assert.equal(typeof session.plan([]), "string", "known-empty clear is a no-op");
  client.refreshMessageReactions = async () => fresh({ snapshot: { items: [{ key: heart, count: null, mine: null }] } });
  await session.refresh(); assert.equal(session.canSet, false, "unknown never becomes an unchecked personal choice");
  assert.equal(isReactionSnapshot({ items: [{ key: heart, count: "-1", mine: null }] }), false);
});

test("newest refresh generation alone installs proof and expiry reads the current clock without RPC", async () => {
  const waiting: ((value: ReactionContext) => void)[] = [];
  let calls = 0, clock = 1000;
  const client = core({ refreshMessageReactions: async () => { calls++; return new Promise(resolve => { waiting.push(resolve); }); } });
  const session = reactionSession(client, thread, "same-id", original, () => allowed, () => clock);
  const old = session.refresh(), latest = session.refresh();
  waiting[1](fresh({ contextToken: "new-proof" })); await latest;
  waiting[0](fresh({ contextToken: "old-proof" })); assert.equal((await old).stale, true);
  assert.equal(session.context?.contextToken, "new-proof");
  clock = 61000;
  assert.equal(session.canSet, false); assert.equal(session.canWho, false);
  assert.equal(typeof session.plan([plain]), "string"); await session.loadMore(); assert.equal(calls, 2);
  const pending = session.refresh(); session.invalidate(); waiting[2](fresh()); assert.equal((await pending).stale, true);
  assert.equal(session.context, null, "unmount/target replacement cannot resurrect the old proof");
});

test("complete-set plan lists removed custom choices, honors finite catalog and consumes proof before dispatch", async () => {
  let accept!: (value: { snapshot: ReactionSnapshot | null }) => void;
  const writes: unknown[] = [];
  const client = core({ refreshMessageReactions: async () => fresh(), setMessageReactions: async p => { writes.push(p); return new Promise(resolve => { accept = resolve; }); } });
  const session = reactionSession(client, thread, "same-id", null, () => allowed, () => 1000);
  await session.refresh();
  const replacement = plan(session.plan([plain]));
  assert.deepEqual(replacement.removed, [heart, custom]);
  assert.equal(typeof session.plan([{ kind: "emoji", emoji: "👍" }]), "string");
  const pending = session.replace(replacement);
  assert.equal(session.context, null); assert.equal(typeof session.plan([heart]), "string");
  accept({ snapshot: { items: [{ key: plain, count: "1", mine: true }] } }); assert.equal((await pending).ok, true);
  assert.deepEqual(writes, [{ thread, messageId: "same-id", sessionId: "reaction-session", contextToken: "proof", keys: [plain] }]);
  assert.equal(session.canSet, false); assert.deepEqual(session.snapshot?.items[0].key, plain);
});

test("listed custom with allowCustom false is selectable; runtime unknown is separate from known limits", async () => {
  let context = fresh(), permissions = { ...allowed };
  const client = core({ refreshMessageReactions: async () => structuredClone(context) });
  const session = reactionSession(client, thread, "same-id", null, () => permissions, () => 1000);
  await session.refresh(); assert.equal(typeof session.plan([custom]), "object");
  permissions.custom = false; assert.equal(typeof session.plan([custom]), "string"); permissions.custom = true;
  context = fresh({ availability: { keys: [heart, plain, custom], allowCustom: true, maxPerMessage: "1" } });
  await session.refresh(); assert.equal(typeof session.plan([plain, custom]), "string");
  context.availability!.maxPerMessage = null;
  await session.refresh(); assert.equal(typeof session.plan([plain, custom]), "object");
  context = fresh({ availability: { keys: [], allowCustom: false, maxPerMessage: "0" } });
  await session.refresh(); assert.equal(typeof session.plan([]), "object", "clear can remove existing choices under empty/zero policy");
  context = fresh({ availability: { keys: null, allowCustom: true, maxPerMessage: null } });
  await session.refresh(); assert.equal(typeof session.plan([custom]), "string", "no arbitrary custom-ID selection in unrestricted mode");
  const many = Array.from({ length: 17 }, (_, i) => ({ kind: "emoji" as const, emoji: `emoji-${i}` }));
  assert.equal(typeof session.plan(many), "string");
  context = fresh({ snapshot: { items: [{ key: heart, count: "1", mine: true }, { key: plain, count: "1", mine: true }] } });
  await session.refresh(); assert.equal(typeof session.plan([plain, heart]), "string", "order-only proposals are not membership changes");
});

test("lost/mismatched writes never replay and an absent optional confirmation stays unknown", async () => {
  let writes = 0;
  const client = core({ refreshMessageReactions: async () => fresh(), setMessageReactions: async () => { writes++; throw { code: "unavailable", outcomeUnknown: true }; } });
  const session = reactionSession(client, thread, "same-id", null, () => allowed, () => 1000);
  await session.refresh();
  assert.match((await session.replace(plan(session.plan([plain])))).error, /may have completed/);
  await session.replace({ generation: 0, contextToken: "proof", keys: [plain], removed: [] }); assert.equal(writes, 1);
  await session.refresh(); client.setMessageReactions = async () => ({ snapshot: { items: [{ key: plain, count: "1", mine: false }] } });
  assert.match((await session.replace(plan(session.plan([plain])))).error, /may have completed/);
  assert.deepEqual(session.snapshot, original, "mismatched confirmation does not invent local selection");
  await session.refresh(); client.setMessageReactions = async () => ({ snapshot: null });
  assert.equal((await session.replace(plan(session.plan([plain])))).ok, true);
  assert.equal(session.snapshot, null); assert.equal(session.canSet, false);
});

test("reactor short/empty continuations are explicit and filter/cache traversal never resets total budget", async () => {
  const calls: unknown[] = [];
  const pages = [{ items: [], nextCursor: "a" }, { items: [{ userId: "u", key: heart, name: "<img>", at: "0" }], nextCursor: "b" }, { items: [], nextCursor: null }];
  const client = core({ refreshMessageReactions: async () => fresh(), listMessageReactors: async p => { calls.push(p); return pages[calls.length - 1]; } });
  const session = reactionSession(client, thread, "same-id", null, () => allowed, () => 1000);
  await session.refresh(); assert.equal(calls.length, 0);
  await session.loadMore(); assert.equal(session.nextCursor, "a"); assert.equal(session.canLoadMore, true);
  await session.loadMore(); await session.loadMore(); assert.equal(session.nextCursor, null); assert.equal(session.canLoadMore, false);
  assert.deepEqual(calls.map(value => (value as { cursor: string | null }).cursor), [null, "a", "b"]);
  session.chooseFilter(heart); assert.equal(calls.length, 3);
  session.chooseFilter(null); await session.loadMore(); assert.equal(calls.length, 3, "cached first page is reused only after an explicit load");
});

test("reactor limits retain continuation and permit free cached-page review", async () => {
  let calls = 0;
  const client = core({ refreshMessageReactions: async () => fresh(), listMessageReactors: async () => ({
    items: Array.from({ length: 32 }, (_, i) => ({ userId: `${calls}-${i}`, key: heart, name: null, at: null })), nextCursor: `cursor-${++calls}` }) });
  const session = reactionSession(client, thread, "same-id", null, () => allowed, () => 1000);
  await session.refresh();
  for (let i = 0; i < 8; i++) assert.equal((await session.loadMore()).ok, true);
  assert.equal(session.reactors.length, 256); assert.equal(session.viewLimit, true); assert.equal(session.nextCursor, "cursor-8");
  assert.equal((await session.loadMore()).ok, false); assert.equal(calls, 8);
  session.chooseFilter(plain); assert.equal(session.canLoadMore, false);
  session.chooseFilter(null); assert.equal(session.canLoadMore, true); await session.loadMore(); assert.equal(calls, 8);
});

test("late filter/expired context pages and cyclic cursors cannot install old rows or proof", async () => {
  const waiting: ((value: { items: { userId: string; key: OrdinaryReactionKey; name: null; at: null }[]; nextCursor: string | null }) => void)[] = [];
  const client = core({ refreshMessageReactions: async () => fresh(), listMessageReactors: async () => new Promise(resolve => { waiting.push(resolve); }) });
  let clock = 1000;
  const session = reactionSession(client, thread, "same-id", null, () => allowed, () => clock);
  await session.refresh(); const old = session.loadMore(); session.chooseFilter(plain); const current = session.loadMore();
  waiting[1]({ items: [{ userId: "new", key: plain, name: null, at: null }], nextCursor: "a" }); await current;
  waiting[0]({ items: [{ userId: "old", key: heart, name: null, at: null }], nextCursor: "old" }); assert.equal((await old).stale, true);
  assert.deepEqual(session.reactors.map(item => item.userId), ["new"]);
  const cyclic = session.loadMore(); waiting[2]({ items: [], nextCursor: "a" }); assert.equal((await cyclic).ok, false);
  assert.equal(session.context, null); assert.equal(session.canSet, false);
  await session.refresh();
  const expired = session.loadMore(); clock = 61000;
  waiting[3]({ items: [{ userId: "late", key: heart, name: null, at: null }], nextCursor: "late" });
  assert.equal((await expired).stale, true); assert.deepEqual(session.reactors, []); assert.equal(session.canWho, false);
});

test("oversized fresh state or malformed read reply issues no usable proof", async () => {
  const oversized = Array.from({ length: 129 }, (_, i) => ({ key: { kind: "emoji" as const, emoji: `emoji-${i}` }, count: "1", mine: false }));
  const client = core({ refreshMessageReactions: async () => fresh({ snapshot: { items: oversized } }) });
  const session = reactionSession(client, thread, "same-id", null, () => allowed, () => 1000);
  assert.equal((await session.refresh()).ok, false); assert.equal(session.context, null);
  client.refreshMessageReactions = async () => fresh({ availability: { keys: Array.from({ length: 256 }, (_, i) => ({ kind: "emoji", emoji: "\0".repeat(125) + String(i) })), allowCustom: false, maxPerMessage: null } });
  assert.equal((await session.refresh()).ok, false); assert.equal(session.context, null, "combined 128-KiB result bound cannot become a truncated proof");
});
