import assert from "node:assert/strict";
import test from "node:test";
import type { AccountSummary, CoreClient, MediaJob, MediaJobPage, Message, PluginSummary, ThreadKey } from "../src/core";
import { acknowledgeOldMediaIntent, cancelMediaReceipt, isMediaJob, isMediaJobPage, learnMediaPage, mediaCancelNeedsRead,
  mediaJobError, mediaPermissions, observedMediaScopes, pendingMediaIntents, startMediaIntent } from "../src/components/thread/mediaJobs";

const thread: ThreadKey = { accountId: "a", chatId: "same-chat", topicId: "42" };
const receipt: MediaJob = { id: "job", sessionId: "session", requestKey: "intent", thread, messageId: "same-message", kind: "download",
  state: "running", cancelRequested: false, doneBytes: "0", totalBytes: null, error: null };
const page: MediaJobPage = { sessionId: "session", jobs: [], nextCursor: null, remainingRequestKeys: 4096 };
const message: Message = { rowKey: "1", messageId: "same-message", topicId: "42", direction: "out", senderName: null,
  sentAt: "0", text: "Caption", entities: [], mediaKind: "document", attachment: { kind: "document", name: "fixture.bin",
    mime: null, bytes: "7", width: null, height: null, durationSeconds: null, hasSpoiler: false }, edited: false, deleted: false, delivery: null };
const plugin: PluginSummary = { pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running", capabilities: { "media.download": true }, authMethods: [], settingsSchema: null };
const account: AccountSummary = { accountId: "a", pluginId: "fixture", label: null, enabled: true, phase: "running", state: "connected", reconnecting: false };
const core = (methods: Partial<CoreClient>) => methods as CoreClient;

test("unknown starts keep their exact key/session across remount and account/topic views, without replay", async () => {
  const calls: unknown[] = [];
  let reject!: (cause: unknown) => void;
  const client = core({ startMediaDownload: async params => { calls.push(params); return new Promise((_resolve, fail) => { reject = fail; }); } });
  const pending = startMediaIntent(client, thread, message.messageId, "download", page, () => "intent");
  const failed = assert.rejects(pending, { outcomeUnknown: true });
  assert.equal(pendingMediaIntents(client, thread)[0].phase, "starting");
  assert.deepEqual(pendingMediaIntents(client, { ...thread, accountId: "b" }), []);
  assert.deepEqual(pendingMediaIntents(client, { ...thread, topicId: null }), []);
  reject({ code: "unavailable", outcomeUnknown: true });
  await failed;
  const remembered = pendingMediaIntents(client, thread)[0];
  assert.equal(remembered.requestKey, "intent"); assert.equal(remembered.sessionId, "session");
  assert.equal(remembered.phase, "unknown");
  learnMediaPage(client, thread, page); // Zero-key/list results can race original admission.
  assert.equal(pendingMediaIntents(client, thread).length, 1);
  await assert.rejects(startMediaIntent(client, thread, message.messageId, "download", page, () => "new-key"), { code: "conflict" });
  assert.equal(calls.length, 1);
  assert.deepEqual(observedMediaScopes(client, { ...thread, topicId: null }).map(scope => scope.topicId), [null, "42"]);
  learnMediaPage(client, thread, { ...page, jobs: [receipt] });
  assert.equal(pendingMediaIntents(client, thread).length, 0, "a matching retained receipt resolves uncertainty");
  assert.equal(calls.length, 1, "recovery is a read, never a second start");
});

test("a new app session requires explicit old-request review and cannot claim a prior Save did not publish", async () => {
  let calls = 0;
  const client = core({ startMediaSave: async () => { calls++; throw { outcomeUnknown: true }; } });
  await assert.rejects(startMediaIntent(client, thread, message.messageId, "save", page, () => "save-intent"));
  const intent = pendingMediaIntents(client, thread)[0];
  learnMediaPage(client, thread, { ...page, sessionId: "new-session" });
  assert.equal(pendingMediaIntents(client, thread).length, 1);
  acknowledgeOldMediaIntent(client, intent, "session");
  assert.equal(pendingMediaIntents(client, thread).length, 1);
  acknowledgeOldMediaIntent(client, intent, "new-session");
  assert.equal(pendingMediaIntents(client, thread).length, 0);
  assert.equal(calls, 1);
});

test("foreign or malformed successful start replies remain uncertain and cannot release the key", async () => {
  for (const value of [null, { ...receipt, thread: { ...thread, accountId: "b" } }, { ...receipt, thread: { ...thread, topicId: null } },
    { ...receipt, messageId: "other" }, { ...receipt, sessionId: "foreign" }, { ...receipt, requestKey: "foreign" }, { ...receipt, kind: "save" }]) {
    let calls = 0;
    const client = core({ startMediaDownload: async () => { calls++; return value as MediaJob; } });
    await assert.rejects(startMediaIntent(client, thread, message.messageId, "download", page, () => "intent"), { outcomeUnknown: true });
    assert.equal(pendingMediaIntents(client, thread)[0].requestKey, "intent");
    assert.equal(calls, 1);
  }
});

test("definite admission failures release an unaccepted key and quota exhaustion makes no call", async () => {
  let calls = 0;
  const client = core({ startMediaDownload: async () => { calls++; throw { code: "unsupported" }; } });
  await assert.rejects(startMediaIntent(client, thread, message.messageId, "download", page, () => "intent"), { code: "unsupported" });
  assert.deepEqual(pendingMediaIntents(client, thread), []);
  await assert.rejects(startMediaIntent(client, thread, message.messageId, "download", { ...page, remainingRequestKeys: 0 }), { code: "conflict" });
  assert.equal(calls, 1);
});

test("bounded pages validate exact topic/account and distinguish normal pages from key recovery", () => {
  assert.equal(isMediaJobPage({ ...page, jobs: [receipt] }, thread, "intent"), true);
  for (const value of [{ ...page, jobs: Array(17).fill(receipt) }, { ...page, jobs: [receipt], nextCursor: "next" },
    { ...page, jobs: [receipt, receipt] }, { ...page, sessionId: "foreign", jobs: [receipt] }]) {
    assert.equal(isMediaJobPage(value, thread, "intent"), false);
  }
  assert.equal(isMediaJobPage({ ...page, jobs: [receipt] }, { ...thread, topicId: null }, null), false);
  assert.equal(isMediaJobPage({ ...page, jobs: [receipt] }, { ...thread, accountId: "b" }, null), false);
  assert.equal(isMediaJobPage({ ...page, nextCursor: "server-continuation" }, thread, null), true, "an empty page may still carry continuation");
  assert.equal(isMediaJobPage({ ...page, remainingRequestKeys: 4097 }, thread, null), false);
  assert.equal(isMediaJobPage({ ...page, jobs: [receipt, receipt] }, thread, null), false, "duplicate receipts cannot create ambiguous UI rows");
});

test("byte evidence stays exact and raw job errors cannot be displayed", () => {
  assert.equal(isMediaJob({ ...receipt, doneBytes: "9007199254740993", totalBytes: null }), true);
  for (const doneBytes of ["01", "-1", "1.2", "18446744073709551616"]) assert.equal(isMediaJob({ ...receipt, doneBytes }), false);
  assert.equal(isMediaJob({ ...receipt, error: { code: "internal", reason: "/private/source", outcomeUnknown: false } }), false);
  const error: MediaJob = { ...receipt, state: "unknown", error: { code: "conflict", reason: "destinationExists", outcomeUnknown: true } };
  assert.match(mediaJobError(error), /may already exist.*unused name/);
});

test("unknown cancellation requires a read; verified completion can win cancellation", async () => {
  let calls = 0;
  const client = core({ cancelMediaJob: async () => { calls++; throw { code: "unavailable", outcomeUnknown: true }; } });
  await assert.rejects(cancelMediaReceipt(client, receipt), { outcomeUnknown: true });
  assert.equal(mediaCancelNeedsRead(client, receipt), true);
  assert.equal(mediaCancelNeedsRead(client, { ...receipt, thread: { ...thread, accountId: "b" } }), false);
  assert.equal(mediaCancelNeedsRead(client, { ...receipt, thread: { ...thread, topicId: null } }), false);
  learnMediaPage(client, thread, { ...page, jobs: [{ ...receipt, state: "succeeded", cancelRequested: true }] });
  assert.equal(mediaCancelNeedsRead(client, receipt), false);
  assert.equal(calls, 1, "a fresh receipt does not replay cancellation");
  client.cancelMediaJob = async () => ({ ...receipt, state: "succeeded", cancelRequested: true });
  assert.equal((await cancelMediaReceipt(client, receipt)).state, "succeeded");
});

test("offline native saving is separate from downloading; provider false and provisional messages gate controls", () => {
  const client = core({ startMediaDownload: async () => receipt, startMediaSave: async () => ({ ...receipt, kind: "save" }) });
  assert.deepEqual(mediaPermissions(client, plugin, account, { saveCopy: false }, message), { download: true, save: false });
  assert.deepEqual(mediaPermissions(client, { ...plugin, phase: "stopped", capabilities: null }, { ...account, phase: "stopped" }, { saveCopy: true }, message), { download: false, save: true });
  for (const value of [{ ...message, deleted: true }, { ...message, messageId: "local:echo" }, { ...message, delivery: "failed" },
    { ...message, attachment: { ...message.attachment!, bytes: "2147483649" } }]) {
    assert.deepEqual(mediaPermissions(client, plugin, account, { saveCopy: true }, value), { download: false, save: false });
  }
  assert.deepEqual(mediaPermissions(client, plugin, { ...account, enabled: false }, { saveCopy: true }, message), { download: false, save: false });
});
