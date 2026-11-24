import assert from "node:assert/strict";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createNativeClient } from "../src/core/adapters/native";
import { createBrowserClient } from "../src/core/adapters/browser";
import type { CoreRequest, Message, PluginSummary, PollSnapshot } from "../src/core";
import { pollPermissions, pollSession } from "../src/components/thread/polls";
import { PollSummary } from "../src/components/thread/PollCard";
const thread = { accountId: "synthetic", chatId: "chat", topicId: "older-topic" };
const poll: PollSnapshot = { id: "poll", question: "<script>literal</script>", options: [{ id: "opaque", text: "<b>answer</b>", votes: null, chosen: null, correct: null }], totalVoters: null, closed: false, multiple: false, quiz: true, anonymous: false, closeAt: null, solution: null, solutionEntities: [] };
const message: Message = { rowKey: "1", messageId: "older", topicId: "older-topic", direction: "out", senderName: null, sentAt: "0", text: null, entities: [], mediaKind: null, edited: false, deleted: false, delivery: "sent", poll };
const plugin: PluginSummary = { pluginId: "fixture", name: "Fixture", phase: "running", brandColor: null, authMethods: [], settingsSchema: null, capabilities: { "polls.vote": true, "polls.close": true, "messages.get": true } };

test("poll session only explicit scoped refresh releases uncertainty; null never invents votes", async () => {
  const calls: CoreRequest[] = [];
  let result: unknown = { ok: true, value: { poll } };
  const client = createNativeClient(async (_name, args) => { calls.push(args.request as CoreRequest); return result; });
  const session = pollSession(client, thread, "older", poll);
  assert.equal((await session.perform("vote", ["opaque"])).ok, false);
  assert.equal(calls.length, 0, "a fresh cached snapshot cannot authorize a write");
  await session.perform("refresh");
  result = { ok: false, error: { code: "unavailable", outcomeUnknown: true } };
  assert.equal((await session.perform("vote", ["opaque"])).ok, false);
  assert.equal(session.needsRefresh, true);
  await session.perform("close"); assert.equal(calls.length, 2);
  result = { ok: true, value: { poll: null } };
  await session.perform("refresh"); assert.equal(session.needsRefresh, true); assert.equal(session.poll.options[0].chosen, null);
  result = { ok: true, value: { poll: { ...poll, closed: true } } };
  await session.perform("refresh"); assert.equal(session.needsRefresh, false); assert.equal(session.poll.closed, true);
  assert.deepEqual(calls.map(call => call.method), ["refreshPoll", "votePoll", "refreshPoll", "refreshPoll"]);
  assert.deepEqual(calls[3].params, { thread, messageId: "older" });
});

test("native failures classify only vote and close as writes", async () => {
  const core = createNativeClient(async () => { throw Error("sensitive"); });
  for (const operation of [core.votePoll({ thread, messageId: "older", optionIds: [] }), core.closePoll({ thread, messageId: "older" })])
    await assert.rejects(operation, error => (error as { outcomeUnknown?: boolean }).outcomeUnknown === true);
  await assert.rejects(core.refreshPoll({ thread, messageId: "older" }), error => !(error as { outcomeUnknown?: boolean }).outcomeUnknown);
});

test("quiz unknown choice is read only; literal content is escaped and provisional polls hidden", () => {
  const client = createNativeClient(async () => ({ ok: true, value: { poll } }));
  assert.equal(pollPermissions(client, plugin, message, poll).vote, false);
  assert.equal(pollPermissions(client, plugin, message, { ...poll, options: poll.options.map(option => ({ ...option, chosen: false })) }).vote, true);
  assert.equal(pollPermissions(client, plugin, message, { ...poll, options: poll.options.map(option => ({ ...option, chosen: true })) }).vote, false);
  const html = renderToStaticMarkup(createElement(PollSummary, { thread, message, disabled: false, select: () => { throw Error("automatic vote"); } }));
  assert.match(html, /&lt;script&gt;/); assert.match(html, /Public voting/); assert.doesNotMatch(html, /<script>/);
  assert.equal(renderToStaticMarkup(createElement(PollSummary, { thread, message: { ...message, messageId: "local:fixture" }, disabled: false, select: () => {} })), "");
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
test("browser disconnect leaves poll write unknown without replay", async context => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const sockets: Socket[] = [];
  const core = createBrowserClient("fixture", () => { const socket = new Socket(); sockets.push(socket); return socket as unknown as WebSocket; });
  const pendingSubscription = core.subscribe(() => {}); sockets[0].ready(); const stop = await pendingSubscription;
  const session = pollSession(core, thread, "older", poll);
  const read = session.perform("refresh");
  const readFrame = sockets[0].sent.at(-1)!;
  sockets[0].receive({ id: readFrame.id, reply: { ok: true, value: { poll } } });
  await read;
  const pending = session.perform("vote", ["opaque"]); sockets[0].close();
  assert.equal((await pending).ok, false); assert.equal(session.needsRefresh, true);
  context.mock.timers.tick(350); sockets[1].ready();
  await session.perform("vote", ["opaque"]);
  assert.deepEqual(sockets[1].sent, [{ token: "fixture" }]);
  stop(); sockets[1].close();
});

test("malformed successful write value blocks replay until scoped recovery", async () => {
  let calls = 0;
  const client = createNativeClient(async () => { calls++; return { ok: true, value: calls === 1 ? { poll } : null }; });
  const session = pollSession(client, thread, "older", poll);
  await session.perform("refresh");
  assert.equal((await session.perform("close")).ok, false);
  assert.equal(session.needsRefresh, true);
  await session.perform("close");
  assert.equal(calls, 2);
});

test("fresh sessions after remount or poll ID discovery require their own targeted read", async () => {
  const calls: CoreRequest[] = [];
  const client = createNativeClient(async (_name,args) => { calls.push(args.request as CoreRequest); return { ok: true, value: { poll } }; });
  const first = pollSession(client, thread, "older", { ...poll, id: null });
  await first.perform("refresh");
  await first.perform("vote", ["opaque"]);
  for (const snapshot of [poll, { ...poll, id: null }]) {
    const reopened = pollSession(client, thread, "older", snapshot);
    assert.equal((await reopened.perform("vote", ["opaque"])).ok, false);
    assert.equal((await reopened.perform("close")).ok, false);
    assert.equal(reopened.needsRefresh, true);
  }
  assert.equal(calls.length, 2);
});

test("initial targeted read failure does not claim a write may have completed", async () => {
  const client = createNativeClient(async () => { throw Error("sensitive"); });
  const session = pollSession(client, thread, "older", poll);
  const result = await session.perform("refresh");
  assert.equal(result.ok, false);
  assert.equal(session.needsRefresh, true);
  assert.doesNotMatch(result.error, /may have completed|sensitive/);
});
