import assert from "node:assert/strict";
import { test } from "node:test";
import type { CoreClient, HistoryItem, Message, OutboxItem, ThreadKey } from "../src/core";
import { changeOutbox, draftFor, checkAcceptance, sendDraft } from "../src/components/composer/drafts";
import { distinctItems, messageDate, prependPage } from "../src/components/thread/history";
import { watchLocalHistory } from "../src/components/thread/historySignal";

const thread = { accountId: "fixture-a", chatId: "c", topicId: null };
const topic = { ...thread, topicId: "topic" };
function client(methods: Partial<CoreClient>): CoreClient { return methods as CoreClient; }
function item(target: ThreadKey): OutboxItem {
  return { kind: "sendText", doneBytes: null, totalBytes: null, thread: target, id: "1".repeat(32), state: "queued", outcomeUnknown: false, echoRowKey: null, nextAttemptAt: null };
}
function msg(rowKey: string, messageId = rowKey): HistoryItem {
  const message: Message = {
    rowKey, messageId, topicId: null, direction: "out", senderName: null,
    sentAt: "0", text: null, entities: [], mediaKind: null, edited: false, deleted: false, delivery: null,
  };
  return { kind: "message", message };
}

test("prepend removes a crossed gap and adopts the current ID for an overlapping local row", () => {
  const gap: HistoryItem = { kind: "gap", gapId: "1" };
  const items = prependPage({ items: [msg("1"), msg("2", "final")], moreOlder: false, moreNewer: true },
    [gap, msg("2", "provisional"), msg("3")]);
  assert.deepEqual(items, [msg("1"), msg("2", "final"), msg("3")]);
  assert.deepEqual(distinctItems([gap, gap, msg("1")]), [gap, msg("1")]);
  assert.equal(messageDate("-1")?.iso, "1969-12-31T23:59:59.999Z");
  assert.equal(messageDate("8640000000000001"), null);
});

test("a delayed acceptance preserves another topic's draft and cannot double enqueue", async () => {
  let accept!: (value: OutboxItem) => void;
  const destinations: ThreadKey[] = [];
  const core = client({ enqueueText: p => {
    destinations.push(p.thread);
    assert.equal(p.text, " original \n");
    return new Promise(resolve => { accept = resolve; });
  } });
  const draft = draftFor(core, thread);
  draft.update({ text: " original \n" });
  const recover = async () => assert.fail("unexpected reconciliation");
  const pending = sendDraft(draft, core, thread, recover);
  const other = draftFor(core, topic);
  other.update({ text: "new topic" });
  await sendDraft(draft, core, thread, recover);
  accept(item(thread));
  await pending;
  assert.deepEqual(destinations, [thread]);
  assert.equal(draftFor(core, thread).snapshot().text, "");
  assert.equal(other.snapshot().text, "new topic");
  assert.notEqual(draftFor(core, { ...thread, accountId: "fixture-b" }), draft);
});

test("a lost reply requires successful cached reads and a separate choice before resending", async () => {
  let sends = 0;
  let available = false;
  let reads = 0;
  let notifications = 0;
  const core = client({
    enqueueText: async () => {
      if (++sends === 1) throw { code: "unavailable" };
      return item(thread);
    },
    listOutbox: async p => { assert.deepEqual(p.thread, thread); return []; },
    history: async p => {
      ++reads;
      assert.deepEqual(p.window, { kind: "latest" });
      if (!available) throw { code: "unavailable" };
      return { items: [], moreOlder: false, moreNewer: false };
    },
  });
  const stop = watchLocalHistory(core, thread, () => { notifications++; });
  const draft = draftFor(core, thread);
  draft.update({ text: "original" });
  const recover = () => checkAcceptance(draft, core, thread);
  await sendDraft(draft, core, thread, recover);
  assert.equal(draft.snapshot().uncertain, "unverified");
  assert.equal(draft.snapshot().text, "original");
  await sendDraft(draft, core, thread, recover, true);
  assert.equal(sends, 1);
  available = true;
  await recover();
  assert.equal(reads, 2);
  assert.equal(notifications, 1);
  assert.equal(draft.snapshot().uncertain, "review");
  await sendDraft(draft, core, thread, recover);
  assert.equal(sends, 1);
  await sendDraft(draft, core, thread, recover, true);
  assert.equal(sends, 2);
  assert.equal(draft.snapshot().text, "");
  stop();
});

test("a typed rejection preserves the draft without displaying raw errors", async () => {
  const core = client({ enqueueText: async () => { throw { code: "conflict", message: "sensitive fixture" }; } });
  const draft = draftFor(core, thread);
  draft.update({ text: "keep" });
  await sendDraft(draft, core, thread, async () => assert.fail("unexpected reconciliation"));
  assert.equal(draft.snapshot().text, "keep");
  assert.equal(draft.snapshot().uncertain, null);
  assert.doesNotMatch(draft.snapshot().error ?? "", /sensitive/);
});

test("retry targets the item's exact topic and an ambiguous reply blocks another attempt", async () => {
  const targets: ThreadKey[] = [];
  const core = client({
    retryOutbox: async target => {
      targets.push(target.thread);
      throw { code: "unavailable" };
    },
  });
  const draft = draftFor(core, thread);
  const failed: OutboxItem = { ...item(topic), state: "failed", outcomeUnknown: true };
  await changeOutbox(draft, core, failed, "retry", false);
  assert.equal(targets.length, 0);
  await changeOutbox(draft, core, failed, "retry", true);
  assert.deepEqual(targets, [topic]);
  assert.equal(draftFor(core, thread).snapshot().outboxPhase, "reconcile");
  await changeOutbox(draft, core, failed, "retry", true);
  assert.equal(targets.length, 1);
});

test("malformed or foreign text acceptance retains the draft and never automatically enqueues again", async () => {
  const good = item(thread);
  const invalid = [null, {}, [], { ...good, id: "" }, { ...good, id: 17 }, { ...good, id: "x".repeat(257) }, { ...good, id: "😀".repeat(65) },
    { ...good, kind: "sendFile" }, { ...good, thread: topic },
    { ...good, thread: { ...thread, chatId: "other" } }, { ...good, thread: { ...thread, accountId: "other" } },
    { ...good, state: "accepted" }, { ...good, outcomeUnknown: "false" },
    { ...good, doneBytes: "0" }, { ...good, totalBytes: "1" },
    { ...good, echoRowKey: "0" }, { ...good, echoRowKey: "9223372036854775808" },
    { ...good, nextAttemptAt: "tomorrow" }, { ...good, state: "sent", nextAttemptAt: "1" }];
  for (const acceptance of invalid) {
    let sends = 0;
    const core = client({
      enqueueText: async () => { sends++; return acceptance as unknown as OutboxItem; },
      listOutbox: async () => [],
      history: async () => ({ items: [], moreOlder: false, moreNewer: false }),
    });
    const draft = draftFor(core, thread);
    draft.update({ text: "keep the exact body" });
    await sendDraft(draft, core, thread, () => checkAcceptance(draft, core, thread));
    assert.equal(draft.snapshot().text, "keep the exact body");
    assert.equal(draft.snapshot().uncertain, "review");
    await sendDraft(draft, core, thread, () => checkAcceptance(draft, core, thread));
    assert.equal(sends, 1, "cached absence and another normal click cannot replay an unknown enqueue");
  }
});

test("realistic settled or queued text receipts confirm admission without conflating delivery uncertainty", async () => {
  for (const state of ["queued", "sending", "sent", "failed", "cancelled"] as const) {
    const core = client({ enqueueText: async () => ({ ...item(thread), state, echoRowKey: "12",
      nextAttemptAt: state === "queued" ? "1767225600000" : null, outcomeUnknown: state === "failed" }) });
    const draft = draftFor(core, thread);
    draft.update({ text: "accepted" });
    await sendDraft(draft, core, thread, async () => assert.fail("valid receipt cannot require admission recovery"));
    assert.equal(draft.snapshot().text, "");
    assert.equal(draft.snapshot().uncertain, null);
  }
});

test("explicit unknown admission with an internal code blocks duplicate clicks throughout recovery", async () => {
  let finishRead!: () => void;
  let sends = 0;
  let reads = 0;
  const reading = new Promise<void>(resolve => { finishRead = resolve; });
  const core = client({
    enqueueText: async () => { sends++; throw { code: "internal", outcomeUnknown: true, message: "sensitive fixture" }; },
    listOutbox: async () => { reads++; await reading; return []; },
    history: async () => ({ items: [], moreOlder: false, moreNewer: false }),
  });
  const draft = draftFor(core, thread);
  draft.update({ text: "keep" });
  const recover = () => checkAcceptance(draft, core, thread);
  const pending = sendDraft(draft, core, thread, recover);
  await Promise.resolve();
  assert.equal(reads, 1);
  assert.equal(draft.snapshot().pending, true);
  assert.equal(draft.snapshot().uncertain, "checking");
  await sendDraft(draft, core, thread, recover);
  await sendDraft(draft, core, thread, recover, true);
  assert.equal(sends, 1);
  finishRead(); await pending;
  assert.equal(draft.snapshot().pending, false);
  assert.equal(draft.snapshot().uncertain, "review");
  assert.equal(draft.snapshot().text, "keep");
  assert.doesNotMatch(draft.snapshot().error ?? "", /sensitive/);
  await sendDraft(draft, core, thread, recover);
  assert.equal(sends, 1);
});

test("cancel and retry preserve an explicit unknown outcome independently from the error code", async () => {
  for (const action of ["cancel", "retry"] as const) {
    let changes = 0;
    const fail = async () => { changes++; throw { code: "internal", outcomeUnknown: true }; };
    const core = client({ cancelOutbox: fail, retryOutbox: fail });
    const draft = draftFor(core, thread);
    const failed = { ...item(thread), state: "failed" as const };
    await changeOutbox(draft, core, failed, action, true);
    assert.equal(draft.snapshot().outboxPhase, "reconcile");
    await changeOutbox(draft, core, failed, action, true);
    assert.equal(changes, 1);
  }
});
