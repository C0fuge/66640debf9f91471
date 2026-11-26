import assert from "node:assert/strict";
import { test } from "node:test";
import type { CoreClient, FileStage, FileStagePage, Inventory } from "../src/core";
import { changeFile, fileAccess, fileAction, fileDraft, isFileStage, isFileStagePage, learnFilePage, pendingFileStarts,
  rememberFileDraft, reviewOldFileIntent, selectFile, stageNotice, uploadNotice, validCaption } from "../src/components/composer/fileIngress";
import { changeOutbox, draftFor } from "../src/components/composer/drafts";

const thread = { accountId: "account", chatId: "chat", topicId: "topic" };
const sessionId = "1".repeat(32), stageId = "2".repeat(32);
const page: FileStagePage = { sessionId, items: [], nextCursor: null, remainingRequestKeys: 4096 };
const ready: FileStage = { stageId, sessionId, requestKey: "selection", thread, state: "ready", name: "report.txt",
  bytes: "12", format: "other", copiedBytes: "12", outboxId: null, outboxRow: null, error: null };
const core = (methods: Partial<CoreClient>) => methods as CoreClient;

test("a lost start retains exact scope/key and reads never open a second dialog", async () => {
  let reject!: (cause: unknown) => void, calls = 0;
  const client = core({ startFileSelection: async p => { calls++; assert.deepEqual(p, { thread, sessionId, requestKey: "selection" }); return new Promise((_r, fail) => { reject = fail; }); } });
  const selected = assert.rejects(selectFile(client, thread, page, () => "selection"), { outcomeUnknown: true });
  assert.equal(pendingFileStarts(client, thread)[0].pending, true);
  assert.deepEqual(pendingFileStarts(client, { ...thread, topicId: null }), []);
  reject({ code: "unavailable", outcomeUnknown: true }); await selected;
  learnFilePage(client, page);
  await assert.rejects(selectFile(client, thread, page, () => "again"), { code: "conflict" });
  assert.equal(calls, 1);
  learnFilePage(client, { ...page, items: [ready] });
  assert.equal(pendingFileStarts(client, thread).length, 0);
  assert.equal(calls, 1);
});

test("foreign start receipts and old sessions stay unresolved until explicit review", async () => {
  const client = core({ startFileSelection: async () => ({ ...ready, thread: { ...thread, accountId: "other" } }) });
  await assert.rejects(selectFile(client, thread, page, () => "selection"), { outcomeUnknown: true });
  const old = pendingFileStarts(client, thread)[0];
  learnFilePage(client, { ...page, sessionId: "3".repeat(32) });
  assert.equal(pendingFileStarts(client, thread).length, 1);
  reviewOldFileIntent(client, old, sessionId); assert.equal(pendingFileStarts(client, thread).length, 1);
  reviewOldFileIntent(client, old, "3".repeat(32)); assert.equal(pendingFileStarts(client, thread).length, 0);
});

test("definite start denial and quota exhaustion do not retain accepted keys", async () => {
  let calls = 0;
  const client = core({ startFileSelection: async () => { calls++; throw { code: "unsupported" }; } });
  await assert.rejects(selectFile(client, thread, page, () => "selection"), { code: "unsupported" });
  assert.equal(pendingFileStarts(client, thread).length, 0);
  await assert.rejects(selectFile(client, thread, { ...page, remainingRequestKeys: 0 }), { code: "conflict" });
  assert.equal(calls, 1);
});

test("file pages validate exact session/thread, bounded payloads, unique IDs and canonical sizes", () => {
  assert.equal(isFileStagePage({ ...page, items: [ready] }, thread, "selection"), true);
  assert.equal(isFileStagePage({ ...page, items: [{ ...ready, requestKey: null }] }, thread, null), true);
  for (const bad of [{ ...page, items: [ready, ready] }, { ...page, items: Array(17).fill(ready) },
    { ...page, sessionId: "3".repeat(32), items: [ready] }, { ...page, remainingRequestKeys: 4097 },
    { ...page, items: [ready], nextCursor: "cursor" }]) assert.equal(isFileStagePage(bad, thread, "selection"), false);
  for (const bad of [{ ...ready, bytes: "01" }, { ...ready, bytes: "2147483649" }, { ...ready, copiedBytes: "13" },
    { ...ready, name: "../private" }, { ...ready, name: "a\u202eb" }, { ...ready, error: { code: "internal", reason: "/secret", outcomeUnknown: false } }]) assert.equal(isFileStage(bad), false);
  assert.equal(isFileStagePage({ ...page, items: [ready] }, { ...thread, topicId: null }, null), false);
  assert.equal(isFileStagePage({ ...page, padding: "x".repeat(128 * 1024) }, thread, null), false);
});

test("ambiguous send is never replayed by a ready read and keeps the separate caption and text draft", async () => {
  let calls = 0;
  const client = core({ enqueueFile: async p => { calls++; assert.equal(p.caption, " literal \n"); throw { code: "unavailable", outcomeUnknown: true }; } });
  draftFor(client, thread).update({ text: "keep text" });
  const intent = { caption: " literal \n", asFile: true, spoiler: false };
  rememberFileDraft(client, ready, intent);
  await assert.rejects(changeFile(client, ready, "send", intent));
  learnFilePage(client, { ...page, items: [ready] }); // An older ready read can race the original request.
  assert.ok(fileAction(client, ready));
  await assert.rejects(changeFile(client, ready, "send", intent), { code: "conflict" });
  assert.equal(calls, 1); assert.equal(draftFor(client, thread).snapshot().text, "keep text");
  assert.deepEqual(fileDraft(client, ready), intent);
  learnFilePage(client, { ...page, items: [{ ...ready, state: "admissionUnknown", outboxId: "4".repeat(32), outboxRow: "missing" }] });
  assert.equal(fileAction(client, ready), undefined); assert.equal(calls, 1);
});

test("duplicate pending send and changed intent cannot call enqueue twice", async () => {
  let finish!: (s: FileStage) => void, calls = 0;
  const client = core({ enqueueFile: async () => { calls++; return new Promise(resolve => { finish = resolve; }); } });
  const pending = changeFile(client, ready, "send");
  await assert.rejects(changeFile(client, ready, "send", { caption: "changed", asFile: false, spoiler: false }), { code: "conflict" });
  finish({ ...ready, state: "verifying" }); await pending;
  assert.equal(calls, 1);
});

test("file retry is denied even through the shared action helper; text retry keeps exact item thread", async () => {
  let retries = 0;
  const item = { kind: "sendFile" as const, thread, id: "outbox", doneBytes: null, totalBytes: null, echoRowKey: null,
    nextAttemptAt: null, state: "failed" as const, outcomeUnknown: true };
  const client = core({ retryOutbox: async p => { retries++; assert.deepEqual(p.thread, thread); return { ...item, kind: "sendText" }; } });
  await assert.rejects(changeOutbox(draftFor(client, thread), client, item, "retry", true), { code: "unsupported" });
  await changeOutbox(draftFor(client, thread), client, { ...item, kind: "sendText" }, "retry", true);
  assert.equal(retries, 1);
});

test("admission uncertainty and proven local acceptance with a missing row have distinct messages", () => {
  assert.match(stageNotice({ ...ready, state: "admissionUnknown" })!, /could duplicate/);
  assert.equal(stageNotice({ ...ready, state: "enqueued", outboxRow: "missing" }), "Added to outbox. Delivery record unavailable.");
  assert.match(stageNotice({ ...ready, error: { code: "conflict", reason: "capacity", outcomeUnknown: false } })!, /Sent files are retained/);
});

test("capabilities fail closed on malformed limits and retain exact literal caption limits", () => {
  const inventory: Inventory = { mediaAccess: { saveCopy: false }, composerAccess: { chooseFile: true },
    accounts: [{ accountId: thread.accountId, pluginId: "p", enabled: true, label: null, phase: "running", state: "connected", reconnecting: false }],
    plugins: [{ pluginId: "p", name: "P", phase: "running", capabilities: { "send.file": true }, brandColor: null, authMethods: [], settingsSchema: null }] };
  assert.equal(fileAccess(inventory, thread).canSend, true);
  for (const max_bytes of [null, false, "12", 0, -1, 1.5, Number.MAX_VALUE]) {
    inventory.plugins[0].capabilities = { "send.file": { max_bytes } };
    assert.equal(fileAccess(inventory, thread).canSend, false);
  }
  inventory.plugins[0].capabilities = { "send.file": { max_bytes: 12 } };
  assert.equal(fileAccess(inventory, thread).maxBytes, 12);
  inventory.accounts[0].enabled = false; assert.equal(fileAccess(inventory, thread).canSend, false);
  assert.equal(validCaption("😀".repeat(1024)), true); assert.equal(validCaption("😀".repeat(1025)), false);
});


test("upload progress remains exact and never borrows local copy progress", () => {
  const item = { kind: "sendFile" as const, thread, id: "outbox", doneBytes: "9007199254740993", totalBytes: "9007199254740994",
    echoRowKey: null, nextAttemptAt: null, state: "sending" as const, outcomeUnknown: false };
  assert.equal(uploadNotice(item), `Uploaded ${9007199254740993n.toLocaleString()} bytes of ${9007199254740994n.toLocaleString()}`);
  assert.equal(uploadNotice({ ...item, totalBytes: "1" }), `Uploaded ${9007199254740993n.toLocaleString()} bytes`);
  assert.equal(uploadNotice({ ...item, doneBytes: null }), null);
  assert.equal(uploadNotice({ ...item, doneBytes: "01" }), null);
  assert.equal(uploadNotice({ ...item, kind: "sendText" }), null);
});


test("an unconfirmed cancellation cannot repeat from an older ready snapshot", async () => {
  let calls = 0;
  const client = core({ cancelFileSelection: async () => { calls++; throw { code: "unavailable", outcomeUnknown: true }; } });
  await assert.rejects(changeFile(client, ready, "cancel"));
  learnFilePage(client, { ...page, items: [ready] });
  await assert.rejects(changeFile(client, ready, "cancel"), { code: "conflict" });
  assert.equal(calls, 1);
  learnFilePage(client, { ...page, items: [{ ...ready, state: "cancelled" }] });
  assert.equal(fileAction(client, ready), undefined);
});
