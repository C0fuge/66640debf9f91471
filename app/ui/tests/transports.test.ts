import assert from "node:assert/strict";
import test from "node:test";
import { createBrowserClient, takeToken } from "../src/core/adapters/browser";
import { createClient } from "../src/core/adapters/client";
import { createNativeClient } from "../src/core/adapters/native";

class FakeSocket {
  protocol = "ch-core-v1";
  sent: Record<string, unknown>[] = [];
  onopen?: () => void;
  onmessage?: (event: { data: string }) => void;
  onclose?: () => void;
  send(frame: string) { this.sent.push(JSON.parse(frame)); }
  close() { this.onclose?.(); }
  receive(value: unknown) { this.onmessage?.({ data: JSON.stringify(value) }); }
  authenticate() {
    this.onopen?.();
    this.receive({ ready: true });
  }
}

test("only a loopback fragment supplies the token and it is removed from browser history", () => {
  function read(url: string) {
    let replaced: string | undefined;
    const history = { replaceState: (_state: unknown, _unused: string, path: string) => { replaced = path; } };
    const token = takeToken(new URL(url) as unknown as Location, history as unknown as History);
    return { token, replaced };
  }
  assert.deepEqual(read("http://127.0.0.1:5173/?tab=a#token=temporary&tab=b"),
    { token: "temporary", replaced: "/?tab=a#tab=b" });
  assert.equal(read("http://127.0.0.1:5173/?token=query").token, null);
  assert.equal(read("http://localhost:5173/#token=temporary").token, null);
});

test("core failures use safe messages, unexpected replies and transport errors reject generically", async () => {
  for (const [send, code] of [
    [async () => ({ ok: false, error: { code: "conflict", message: "<private>" } }), "conflict"],
    [async () => ({ ok: true }), "unavailable"],
    [async () => { throw new Error("<private>"); }, "unavailable"],
  ] as const) {
    const client = createClient(send, async () => () => {});
    await assert.rejects(client.inventory({}), error => {
      const failure = error as { code: string; message: string };
      return failure.code === code && !failure.message.includes("<private>");
    });
  }
});

test("browser correlates out-of-order replies, rejects pending sends and never replays them after reconnect", async () => {
  const sockets: FakeSocket[] = [];
  let reconnectStarted!: () => void;
  const reconnected = new Promise<void>(resolve => { reconnectStarted = resolve; });
  const client = createBrowserClient("ephemeral", () => {
    const socket = new FakeSocket();
    sockets.push(socket);
    if (sockets.length === 2) reconnectStarted();
    return socket as unknown as WebSocket;
  });
  const events: string[] = [];
  const subscribed = client.subscribe(change => events.push(change.kind));
  const first = sockets[0];
  first.authenticate();
  const stop = await subscribed;
  assert.deepEqual([...first.sent], [{ token: "ephemeral" }]);
  const inventory = client.inventory({});
  const chats = client.listChats({ accountId: "account", archived: false });
  first.receive({ id: first.sent[2].id, reply: { ok: true, value: [] } });
  first.receive({ id: first.sent[1].id, reply: { ok: true, value: { plugins: [], accounts: [], mediaAccess: { saveCopy: false } } } });
  assert.deepEqual(await inventory, { plugins: [], accounts: [], mediaAccess: { saveCopy: false } });
  assert.deepEqual(await chats, []);
  first.receive({ change: { kind: "inventory" } });
  const send = client.enqueueText({ thread: { accountId: "account", chatId: "chat", topicId: null }, text: "draft" });
  const rejected = assert.rejects(send, { code: "unavailable" });
  first.close();
  await rejected;
  await reconnected;
  const second = sockets[1];
  second.authenticate();
  assert.deepEqual(second.sent, [{ token: "ephemeral" }]);
  assert.deepEqual(events, ["reset", "inventory", "reset", "reset"]);
  stop();
  stop();
  second.receive({ change: { kind: "inventory" } });
  assert.equal(events.length, 4);
  second.close();
});

test("browser retains all 16 server slots through local timeouts until a reply arrives", async context => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const socket = new FakeSocket();
  const client = createBrowserClient("ephemeral", () => socket as unknown as WebSocket);
  const subscribed = client.subscribe(() => {});
  socket.authenticate();
  const stop = await subscribed;
  const thread = { accountId: "account", chatId: "chat", topicId: null };
  const timedOut = assert.rejects(client.enqueueText({ thread, text: "first" }), { code: "unavailable" });
  context.mock.timers.tick(1);
  const accepted = client.enqueueText({ thread, text: "second" });
  const reads = Array.from({ length: 14 }, () => client.inventory({}));
  await assert.rejects(client.inventory({}), { code: "unavailable" });
  assert.equal(socket.sent.length, 17); // Handshake plus 16 requests.

  context.mock.timers.tick(29_999);
  await timedOut;
  await assert.rejects(client.inventory({}), { code: "unavailable" });
  assert.equal(socket.sent.length, 17);

  // The late reply frees one slot. The next request cannot consume a second slot.
  socket.receive({ id: socket.sent[1].id, reply: { ok: true, value: { id: "first" } } });
  const replacement = client.inventory({});
  await assert.rejects(client.inventory({}), { code: "unavailable" });
  assert.equal(socket.sent.length, 18);
  socket.receive({ id: socket.sent[2].id, reply: { ok: true, value: { id: "second" } } });
  assert.equal((await accepted).id, "second");
  const inventory = { accounts: [], plugins: [], mediaAccess: { saveCopy: false } };
  for (const frame of socket.sent.slice(3)) {
    socket.receive({ id: frame.id, reply: { ok: true, value: inventory } });
  }
  assert.deepEqual(await replacement, inventory);
  await Promise.all(reads);
  stop();
  socket.close();
});

test("native media starts and cancellation classify lost replies as unknown while listing stays a read", async () => {
  const client = createNativeClient(async () => { throw new Error("private local path"); });
  const thread = { accountId: "a", chatId: "c", topicId: "topic" };
  const start = { thread, messageId: "m", sessionId: "session", requestKey: "intent" };
  for (const request of [() => client.startMediaDownload(start), () => client.startMediaSave(start),
    () => client.cancelMediaJob({ thread, sessionId: "session", jobId: "job" })]) {
    await assert.rejects(request(), { code: "unavailable", outcomeUnknown: true });
  }
  await assert.rejects(client.listMediaJobs({ thread, cursor: null, requestKey: "intent" }), cause => {
    const error = cause as { outcomeUnknown?: boolean; message: string };
    return error.outcomeUnknown !== true && !error.message.includes("private");
  });
});

test("browser disconnect never replays media side effects and keeps keys and thread on the wire", async () => {
  const socket = new FakeSocket();
  const client = createBrowserClient("ephemeral", () => socket as unknown as WebSocket);
  const subscribed = client.subscribe(() => {});
  socket.authenticate();
  const stop = await subscribed;
  const thread = { accountId: "a", chatId: "c", topicId: "topic" };
  const start = { thread, messageId: "m", sessionId: "session", requestKey: "intent" };
  const pending = [client.startMediaDownload(start), client.startMediaSave(start),
    client.cancelMediaJob({ thread, sessionId: "session", jobId: "job" })]
    .map(reply => assert.rejects(reply, { code: "unavailable", outcomeUnknown: true }));
  assert.deepEqual((socket.sent[1].request as { params: unknown }).params, start);
  const read = assert.rejects(client.listMediaJobs({ thread, cursor: null, requestKey: null }), cause =>
    (cause as { outcomeUnknown?: boolean }).outcomeUnknown !== true);
  socket.close();
  await Promise.all([...pending, read]);
  stop();
  assert.equal(socket.sent.length, 5, "only explicit requests were sent");
});
