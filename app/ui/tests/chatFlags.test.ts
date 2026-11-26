import assert from "node:assert/strict";
import test from "node:test";
import type { AccountSummary, ChatFlagContext, CoreClient, PluginSummary } from "../src/core";
import { flagPermissions, flagSession } from "../src/components/chatFlags";

const thread = { accountId: "Alpha", chatId: "same", topicId: null };
const account: AccountSummary = { accountId: "Alpha", label: null, pluginId: "fixture", enabled: true, phase: "running", state: "connected", reconnecting: false };
const plugin: PluginSummary = { pluginId: "fixture", name: "Fixture", phase: "running", brandColor: null, capabilities: {
  "chat.get": {}, "chat.flags": true, "chat.pin": {}, "chat.mark_unread": false }, authMethods: [], settingsSchema: null };
const allowed = () => flagPermissions(plugin, account, thread);
const read = (override: Partial<ChatFlagContext> = {}): ChatFlagContext => ({ thread, sessionId: "run", contextToken: "one", pinned: false, markedUnread: null, ...override });
const core = (methods: Partial<CoreClient>) => methods as CoreClient;

test("exact running account and individual live capability gates", () => {
  assert.deepEqual(allowed(), { refresh: true, pinned: true, markedUnread: false });
  for (const cap of [false, null, 1, "enabled", [], undefined]) {
    assert.equal(flagPermissions({ ...plugin, capabilities: { ...plugin.capabilities, "chat.get": cap as never } }, account, thread).refresh, false);
  }
  for (const patch of [{ enabled: false }, { phase: "stopped" as const }, { accountId: "Beta" }, { pluginId: "other" }, { reconnecting: true }])
    assert.equal(flagPermissions(plugin, { ...account, ...patch }, thread).refresh, false);
  assert.equal(flagPermissions(plugin, account, { ...thread, topicId: "topic" }).refresh, false);
  assert.equal(flagPermissions({ ...plugin, phase: "stopped" }, account, thread).refresh, false);
});

test("write attempts consume proof synchronously and loss survives remount until a matching field read", async () => {
  const writes: unknown[] = [];
  let result = read();
  let lose!: () => void;
  const client = core({ refreshChatFlags: async () => result, setChatFlag: async params => {
    writes.push(params); return new Promise((_, reject) => { lose = () => reject({ outcomeUnknown: true }); });
  } });
  const session = () => flagSession(client, "fixture", thread, allowed);
  const first = session();
  await first.set("pinned", true);
  await first.refresh();
  await first.set("bogus" as never, false);
  await first.set("markedUnread", false);
  const writing = first.set("pinned", true);
  await first.set("pinned", true);
  assert.deepEqual(writes, [{ thread, sessionId: "run", contextToken: "one", flag: "pinned", value: true }]);
  first.invalidate();
  const next = session();
  assert.equal(next.attempt("pinned"), "unknown");
  lose(); await writing;
  assert.equal(flagSession(client, "other", thread, allowed).attempt("pinned"), undefined);
  assert.equal(flagSession(client, "fixture", { ...thread, accountId: "Beta" }, allowed).attempt("pinned"), undefined);
  for (result of [read({ thread: { ...thread, accountId: "Beta" } }), read({ pinned: "false" as never }), read({ pinned: null })]) {
    await next.refresh(); assert.equal(next.attempt("pinned"), "unknown");
  }
  result = read(); await next.refresh(); assert.equal(next.attempt("pinned"), undefined);
});

test("late reads and malformed acknowledgements grant no further authority", async () => {
  let finish!: (result: ChatFlagContext) => void;
  const client = core({ refreshChatFlags: () => new Promise(resolve => { finish = resolve; }), setChatFlag: async () => ({} as never) });
  const session = flagSession(client, "fixture", thread, allowed);
  const late = session.refresh(); session.invalidate(); finish(read()); await late;
  assert.equal(session.state().context, null);
  const current = session.refresh(); finish(read({ pinned: true })); await current;
  await session.set("pinned", false);
  assert.equal(session.attempt("pinned"), "unknown");
  assert.equal(session.state().context, null);
});

test("unresolved attempts have bounded admission and exact refresh frees capacity without eviction", async () => {
  let writes = 0;
  const client = core({
    refreshChatFlags: async ({ thread }) => read({ thread }),
    setChatFlag: async () => { writes++; throw { code: "unavailable", outcomeUnknown: true }; },
  });
  const sessions = Array.from({ length: 65 }, (_, i) =>
    flagSession(client, "fixture", { ...thread, chatId: String(i) }, allowed));
  for (const session of sessions) { await session.refresh(); await session.set("pinned", true); }
  assert.equal(writes, 64);
  assert.equal(sessions[64].state().capacityBlocked, true);
  for (const session of sessions.slice(0, 64)) assert.equal(session.attempt("pinned"), "unknown");
  assert.equal(sessions[64].attempt("pinned"), undefined);
  await sessions[0].refresh();
  assert.equal(sessions[0].attempt("pinned"), undefined);
  assert.equal(sessions[64].state().capacityBlocked, false);
  await sessions[64].set("pinned", true);
  assert.equal(writes, 65);
  assert.equal(sessions[64].attempt("pinned"), "unknown");
});
