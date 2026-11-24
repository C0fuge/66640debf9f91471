import assert from "node:assert/strict";
import { test } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { RichText } from "../src/components/thread/RichText";
import type { TextEntity } from "../src/core";

function render(text: string, entities: readonly TextEntity[]) {
  return renderToStaticMarkup(createElement(RichText, { text, entities }));
}

test("UTF-16 ranges nest after an emoji and preserve equal-range order and input", () => {
  const entities = Object.freeze([
    Object.freeze({ type: "italic", offset: 3, length: 1 }),
    Object.freeze({ type: "bold", offset: 2, length: 3 }),
    Object.freeze({ type: "underline", offset: 2, length: 3 }),
    Object.freeze({ type: "strike", offset: 5, length: 1 }),
  ]);
  assert.equal(render("👋abc!", entities), "👋<strong><u>a<em>b</em>c</u></strong><s>!</s>");
});

test("text and untrusted metadata stay inert while supported child styles survive", () => {
  const text = "<script> & 😀";
  const wrap = { offset: 0, length: text.length };
  const expected = "<strong>&lt;script&gt;</strong> &amp; 😀";
  for (const entity of [
    { type: "future_entity" }, { type: "mention_name", userId: "1" },
    { type: "custom_emoji", customEmojiId: "42" }, { type: "bank_card" },
    { type: "text_url", url: "javascript:alert(1)" },
  ]) {
    assert.equal(render(text, [{ ...wrap, ...entity }, { type: "bold", offset: 0, length: 8 }]), expected);
  }
  assert.equal(render("JS", [{ type: "text_url", offset: 0, length: 2, url: "https://example.test/" }]),
    '<span class="thread-reference">JS</span>');
});

test("a malformed, crossing or excessively nested range returns the entire original text", () => {
  const text = "😀ab😀cd";
  const bold = { type: "bold", offset: 0, length: 4 };
  for (const bad of [
    { offset: -1, length: 2 }, { offset: 3, length: 0 }, { offset: 3, length: -1 },
    { offset: 2.5, length: 1 }, { offset: 2, length: 1.5 }, { offset: 3, length: Infinity },
    { offset: 9, length: 1 }, { offset: 7, length: 2 }, { offset: 1, length: 2 },
    { offset: 3, length: 2 }, { offset: 3, length: 4 },
  ]) {
    assert.equal(render(text, [bold, { type: "unknown", ...bad }]), text);
  }
  assert.equal(render(text, Array(1000).fill(bold)), text);
});

test("code, quotes and spoilers keep their text and expose native disclosure controls", () => {
  const entity = { offset: 0, length: 4 };
  assert.equal(render("test", [{ ...entity, type: "code" }]), "<code>test</code>");
  assert.equal(render("test", [{ ...entity, type: "pre" }]), "<pre><code>test</code></pre>");
  assert.equal(render("test", [{ ...entity, type: "blockquote" }]), "<blockquote>test</blockquote>");
  assert.equal(render("test", [{ ...entity, type: "blockquote", collapsed: true }]),
    '<details class="thread-quote"><summary aria-label="Quoted text"></summary><blockquote>test</blockquote></details>');
  assert.equal(render("test", [{ ...entity, type: "spoiler" }]),
    '<button type="button" class="thread-spoiler" aria-label="Reveal spoiler"><span aria-hidden="true">test</span></button>');
});
