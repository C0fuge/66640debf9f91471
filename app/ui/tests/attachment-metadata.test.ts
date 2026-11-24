import assert from "node:assert/strict";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { Attachment, Message, ThreadKey } from "../src/core";
import { AttachmentSummary, attachmentBytes, attachmentFacts, attachmentRevealKey } from "../src/components/thread/AttachmentSummary";
import { RichText } from "../src/components/thread/RichText";

const thread: ThreadKey = { accountId: "first", chatId: "chat", topicId: null };
const attachment: Attachment = { kind: "document", name: "fixture.txt", mime: "text/plain", bytes: "1024",
  width: null, height: null, durationSeconds: null, hasSpoiler: false };
const message: Message = { rowKey: "1", messageId: "same-id", topicId: null, direction: "in", sentAt: "0",
  senderName: null, text: "Caption stays 😀", entities: [], mediaKind: "document", attachment,
  edited: false, deleted: false, delivery: null };
function render(value: Message) {
  return renderToStaticMarkup(createElement(AttachmentSummary, { thread, message: value }));
}

test("attachment metadata remains inert text and captions stay alongside the summary", () => {
  const value = { ...message, attachment: { ...attachment, kind: "<script>kind</script>", name: '<img src=x onerror="fixture">', mime: "<svg/onload=fixture>" } };
  const markup = renderToStaticMarkup(createElement("section", null,
    createElement(AttachmentSummary, { thread, message: value }),
    createElement(RichText, { text: value.text!, entities: value.entities })));
  assert.match(markup, /&lt;img src=x onerror=&quot;fixture&quot;&gt;/);
  assert.match(markup, /&lt;script&gt;kind&lt;\/script&gt;/);
  assert.match(markup, /&lt;svg\/onload=fixture&gt;/);
  assert.match(markup, /Caption stays 😀/);
  assert.doesNotMatch(markup, /<(img|svg|script|a)\b|href=|src=\"|<button/);
  assert.deepEqual(attachmentFacts({ ...attachment, kind: "__proto__" }), ["__proto__", "text/plain", "1,024 bytes"]);
});

test("photo and voice facts use safe reported dimensions and duration without inventing absent values", () => {
  assert.deepEqual(attachmentFacts({ ...attachment, kind: "photo", mime: null, width: 640, height: 480 }), ["1,024 bytes", "640 × 480 px"]);
  assert.deepEqual(attachmentFacts({ ...attachment, kind: "voice", bytes: null, mime: "audio/ogg", durationSeconds: 65 }), ["audio/ogg", "1:05"]);
  assert.deepEqual(attachmentFacts({ ...attachment, mime: null, bytes: null, width: Number.MAX_SAFE_INTEGER + 1,
    height: -1, durationSeconds: NaN }), []);
  assert.equal(attachmentBytes("9007199254740993"), "9,007,199,254,740,993 bytes");
  assert.equal(attachmentBytes("18446744073709551615"), "18,446,744,073,709,551,615 bytes");
  assert.equal(attachmentBytes("0"), "0 bytes");
  assert.equal(attachmentBytes("1"), "1 byte");
  for (const value of [null, "-1", "1.5", "01", "1e6", "18446744073709551616", "9".repeat(1000)]) assert.equal(attachmentBytes(value), null);
});

test("spoilers, deleted messages and local echoes withhold details; old mediaKind remains usable", () => {
  const hidden = render({ ...message, attachment: { ...attachment, name: "Secret filename", hasSpoiler: true } });
  assert.match(hidden, /Reveal attachment details/);
  assert.doesNotMatch(hidden, /Secret filename|text\/plain|1,024/);
  assert.equal(render({ ...message, deleted: true }), "");
  for (const value of [{ ...message, messageId: "local:fixture" }, { ...message, delivery: "sending" }, { ...message, delivery: "failed" }]) {
    assert.equal(render(value), '<div class="thread-media">Attachment</div>');
  }
  assert.match(render({ ...message, attachment: undefined, mediaKind: "photo" }), /Photo attachment/);
  assert.equal(render({ ...message, attachment: null, mediaKind: null }), "");
});

test("local spoiler identity cannot carry between accounts, chats, topics, rows, IDs or changed metadata", () => {
  const first = attachmentRevealKey(thread, message);
  for (const other of [{ ...thread, accountId: "second" }, { ...thread, chatId: "second" }, { ...thread, topicId: "second" }]) {
    assert.notEqual(attachmentRevealKey(other, message), first);
  }
  for (const other of [{ ...message, topicId: "second" }, { ...message, rowKey: "2" }, { ...message, messageId: "second" },
    { ...message, attachment: { ...attachment, name: "changed attachment" } }]) {
    assert.notEqual(attachmentRevealKey(thread, other), first);
  }
});
