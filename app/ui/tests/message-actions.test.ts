import assert from "node:assert/strict";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { CoreClient, CoreRequest, Message, PluginSummary, ThreadKey } from "../src/core";
import { createNativeClient } from "../src/core/adapters/native";
import { createBrowserClient } from "../src/core/adapters/browser";
import { actionSession, messageActions, type MessageAction } from "../src/components/thread/actions";
import { MessageActionButtons, MessageActionDialog } from "../src/components/thread/MessageActionDialog";
import { draftFor } from "../src/components/composer/drafts";

const thread: ThreadKey = { accountId: "fixture-account", chatId: "chat", topicId: null };
const message: Message = { rowKey: "1", messageId: "confirmed", topicId: null, direction: "out", sentAt: "0", senderName: null,
  text: "original 😀", entities: [{ type: "bold", offset: 9, length: 2 }], mediaKind: null, edited: false, deleted: false, delivery: "sent" };
const plugin: PluginSummary = { pluginId: "fake", name: "Fixture", brandColor: null, phase: "running", authMethods: [], settingsSchema: null,
  capabilities: { edit: true, "send.entities": true, delete: true, "delete.for_me": true, pin: true, unpin: true } };
const mutations: MessageAction[] = [
  { kind: "edit", messageId: "confirmed", text: "A😀B", entities: [{ type: "bold", offset: 1, length: 2 }] },
  { kind: "delete", messageIds: ["confirmed"], revoke: false },
  { kind: "pin", messageId: "confirmed", silent: false },
  { kind: "unpin", messageId: "confirmed" },
];
const expected: CoreRequest[] = [
  { method: "editText", params: { thread, messageId: "confirmed", text: "A😀B", entities: [{ type: "bold", offset: 1, length: 2 }] } },
  { method: "deleteMessages", params: { thread, messageIds: ["confirmed"], revoke: false } },
  { method: "pinMessage", params: { thread, messageId: "confirmed", silent: false } },
  { method: "unpinMessage", params: { thread, messageId: "confirmed" } },
];

test("native adapter carries all four exact typed actions with explicit false parameters", async () => {
  const requests: CoreRequest[] = [];
  const core = createNativeClient(async (command, args) => {
    assert.equal(command, "core_request"); requests.push(args.request as CoreRequest);
    return { ok: true, value: null };
  });
  const session = actionSession(core, thread);
  for (const action of mutations) assert.equal((await session.perform(action)).ok, true);
  assert.deepEqual(requests, expected);
});

class Socket {
  protocol = "ch-core-v1";
  sent: Record<string, unknown>[] = [];
  onopen?: () => void; onmessage?: (event: { data: string }) => void; onclose?: () => void;
  send(frame: string) { this.sent.push(JSON.parse(frame)); }
  close() { this.onclose?.(); }
  receive(frame: unknown) { this.onmessage?.({ data: JSON.stringify(frame) }); }
  ready() { this.onopen?.(); this.receive({ ready: true }); }
}

test("browser adapter carries each action once; lost replies remain unknown after reconnect", async context => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const sockets: Socket[] = [];
  const core = createBrowserClient("synthetic", () => { const socket = new Socket(); sockets.push(socket); return socket as unknown as WebSocket; });
  const subscribed = core.subscribe(() => {}); sockets[0].ready(); const stop = await subscribed;
  for (const [index, action] of mutations.entries()) {
    const session = actionSession(core, thread);
    const pending = session.perform(action);
    const frame = sockets[0].sent.at(-1)!;
    assert.deepEqual(frame.request, expected[index]);
    sockets[0].receive({ id: frame.id, reply: { ok: true, value: null } });
    assert.equal((await pending).ok, true);
  }
  const uncertain = actionSession(core, thread);
  const pending = uncertain.perform(mutations[0]); sockets[0].close();
  assert.equal((await pending).ok, false);
  assert.equal(uncertain.reloadRequired, true);
  context.mock.timers.tick(350); sockets[1].ready();
  assert.deepEqual(sockets[1].sent, [{ token: "synthetic" }]);
  assert.equal((await uncertain.perform(mutations[0])).ok, false);
  assert.equal(sockets[1].sent.length, 1);
  stop(); sockets[1].close();
});

test("ambiguous core replies keep safe messages and block mutation until reload plus explicit retry", async () => {
  let attempts = 0;
  const core = createNativeClient(async () => {
    if (++attempts === 1) return { ok: false, error: { code: "internal", message: "sensitive settings", outcomeUnknown: true } };
    return { ok: true, value: null };
  });
  const session = actionSession(core, thread);
  const result = await session.perform(mutations[1]);
  assert.match(result.error, /may have completed/); assert.doesNotMatch(result.error, /sensitive/);
  await session.perform(mutations[1]); assert.equal(attempts, 1);
  session.reloaded(); assert.equal(attempts, 1);
  assert.equal((await session.perform(mutations[1])).ok, true); assert.equal(attempts, 2);
});

test("transport failures flag all four mutations as unknown while keeping reads generic", async () => {
  for (const action of mutations) {
    let attempts = 0;
    const core = createNativeClient(async () => { attempts++; throw new Error("sensitive transport payload"); });
    const session = actionSession(core, thread);
    assert.equal((await session.perform(action)).ok, false);
    assert.equal(session.reloadRequired, true);
    assert.equal((await session.perform(action)).ok, false);
    assert.equal(attempts, 1);
    await assert.rejects(core.inventory({}), error => {
      assert.deepEqual(error, { code: "unavailable", message: "The core is unavailable." }); return true;
    });
  }
});

test("browser timeouts mark all in-flight actions unknown without replaying or affecting read errors", async context => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const socket = new Socket();
  const core = createBrowserClient("synthetic", () => socket as unknown as WebSocket);
  const subscribed = core.subscribe(() => {}); socket.ready(); const stop = await subscribed;
  const sessions = mutations.map(() => actionSession(core, thread));
  const pending = mutations.map((action, index) => sessions[index].perform(action));
  const read = assert.rejects(core.inventory({}), error => {
    assert.deepEqual(error, { code: "unavailable", message: "The core is unavailable." }); return true;
  });
  context.mock.timers.tick(30_000);
  const outcomes = await Promise.all(pending); await read;
  assert.equal(outcomes.every(outcome => !outcome.ok), true);
  assert.equal(sessions.every(session => session.reloadRequired), true);
  assert.equal(socket.sent.length, 6);
  context.mock.timers.tick(30_000);
  assert.equal(socket.sent.length, 6);
  stop(); socket.close();
});

test("pending action cannot double mutate and keeps account/topic composer drafts separate", async () => {
  let accept!: () => void; const targets: ThreadKey[] = [];
  const core = { pinMessage: async ({ thread }: { thread: ThreadKey }) => { targets.push(thread); await new Promise<void>(r => { accept = r; }); return null; } } as unknown as CoreClient;
  const first = draftFor(core, thread); first.update({ text: "keep main draft" });
  const otherThread = { ...thread, accountId: "other-account", topicId: "topic" };
  const other = draftFor(core, otherThread); other.update({ text: "keep other account draft" });
  const session = actionSession(core, thread);
  const pending = session.perform(mutations[2], "topic");
  assert.equal((await session.perform(mutations[2])).ok, false);
  accept(); await pending;
  assert.deepEqual(targets, [{ ...thread, topicId: "topic" }]);
  assert.equal(first.snapshot().text, "keep main draft"); assert.equal(other.snapshot().text, "keep other account draft");
});

test("visible action buttons respect live capabilities and disable provisional IDs", () => {
  const core = createNativeClient(async () => ({ ok: true, value: null }));
  const markup = (m: Message, p: PluginSummary = plugin) => renderToStaticMarkup(createElement(MessageActionButtons, { client: core, plugin: p, message: m, disabled: false, select() {} }));
  assert.match(markup(message), />Edit</); assert.match(markup(message), />Delete</); assert.match(markup(message), />Pin</);
  assert.match(markup({ ...message, isPinned: true }), />Unpin</);
  assert.doesNotMatch(markup({ ...message, direction: "in" }), />Edit</);
  const provisional = markup({ ...message, messageId: "local:fixture", delivery: "sending" });
  assert.equal((provisional.match(/disabled=""/g) ?? []).length, 3);
  assert.doesNotMatch(markup(message, { ...plugin, capabilities: { edit: true, "send.entities": false } }), />Edit</);
  assert.doesNotMatch(markup(message, { ...plugin, phase: "stopped" }), /<button/);
  assert.equal(messageActions(core, { ...plugin, capabilities: { delete: {}, "delete.for_me": false } }, message).deleteForMe, false);
});

test("delete dialog requires an explicit supported scope and edit text stays outside composer", () => {
  const dialog = (kind: "delete" | "edit", deleteForMe: boolean) => renderToStaticMarkup(createElement(MessageActionDialog, { selection: { kind, message, deleteForMe }, busy: false, close() {}, submit() {} }));
  const deletion = dialog("delete", false);
  assert.match(deletion, /Everyone/); assert.doesNotMatch(deletion, /Only me/); assert.doesNotMatch(deletion, /checked/);
  assert.match(deletion, /type="submit" disabled=""/); assert.match(dialog("delete", true), /Only me/);
  assert.match(dialog("edit", false), /<textarea[^>]*>original 😀<\/textarea>/);
});
