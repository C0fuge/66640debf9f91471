import { toTree } from "@telegraf/entity";
import { Fragment, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { TextEntity } from "../../core";

type Tree = ReturnType<typeof toTree>;
type Entity = NonNullable<Parameters<typeof toTree>[0]["entities"]>[number];
const maxDepth = 48;

function splitsSurrogate(text: string, index: number) {
  const before = text.charCodeAt(index - 1);
  const after = text.charCodeAt(index);
  return before >= 0xd800 && before <= 0xdbff && after >= 0xdc00 && after <= 0xdfff;
}

function httpAddress(address: string | undefined) {
  if (!address) return false;
  try {
    const { protocol } = new URL(address);
    return protocol === "http:" || protocol === "https:";
  } catch {
    return false;
  }
}

function asEntity(text: string, entity: TextEntity): Entity | undefined {
  const { offset, length, type } = entity;
  const span = { offset, length };
  switch (type) {
    case "bold": case "italic": case "underline": case "code": case "spoiler":
    case "pre": case "mention": case "hashtag": case "cashtag": case "bot_command": case "email":
      return { ...span, type };
    case "strike": return { ...span, type: "strikethrough" };
    case "blockquote": return { ...span, type: entity.collapsed ? "expandable_blockquote" : "blockquote" };
    case "phone": return { ...span, type: "phone_number" };
    case "url":
      if (httpAddress(text.slice(offset, offset + length))) return { ...span, type };
      break;
    case "text_url":
      if (entity.url && httpAddress(entity.url)) return { ...span, type: "text_link", url: entity.url };
      break;
    case "custom_emoji":
      if (entity.customEmojiId) return { ...span, type, custom_emoji_id: entity.customEmojiId };
      break;
  }
}

function messageTree(text: string, entities: readonly TextEntity[]): Tree {
  if (!Array.isArray(entities) || !entities.length) return [text];
  for (const entity of entities) {
    if (!entity) return [text];
    const { offset, length } = entity;
    if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || offset < 0 || length <= 0 ||
      length > text.length - offset || splitsSurrogate(text, offset) || splitsSurrogate(text, offset + length)) {
      return [text];
    }
  }

  // Stable sort keeps input order for identical spans; longer spans must enclose shorter ones.
  const sorted = [...entities].sort((a, b) => a.offset - b.offset || b.length - a.length);
  const ends: number[] = [];
  const mapped: Entity[] = [];
  for (const entity of sorted) {
    while (ends.length && entity.offset >= ends[ends.length - 1]) ends.pop();
    const end = entity.offset + entity.length;
    if ((ends.length && end > ends[ends.length - 1]) || ends.length >= maxDepth) return [text];
    ends.push(end);
    const converted = asEntity(text, entity);
    if (converted) mapped.push(converted);
  }
  return toTree({ text, entities: mapped });
}

function focusRevealedText(element: HTMLSpanElement | null) {
  element?.focus({ preventScroll: true });
}

function Spoiler({ text, children }: { text: string; children: ReactNode }) {
  const [revealed, setRevealed] = useState(false);
  if (revealed) return <span ref={focusRevealedText} tabIndex={-1}>{children}</span>;
  return <button type="button" className="thread-spoiler" aria-label="Reveal spoiler" onClick={() => setRevealed(true)}>
    <span aria-hidden="true">{text}</span>
  </button>;
}

function renderTree(tree: Tree): ReactNode {
  return tree.map((node, i) => {
    if (typeof node === "string") return node;
    const children = renderTree(node.children);
    switch (node.type) {
      case "bold": return <strong key={i}>{children}</strong>;
      case "italic": return <em key={i}>{children}</em>;
      case "underline": return <u key={i}>{children}</u>;
      case "strikethrough": return <s key={i}>{children}</s>;
      case "code": return <code key={i}>{children}</code>;
      case "pre": return <pre key={i}><code>{children}</code></pre>;
      case "blockquote": return <blockquote key={i}>{children}</blockquote>;
      case "expandable_blockquote": return <details className="thread-quote" key={i}>
        <summary aria-label="Quoted text" /><blockquote>{children}</blockquote>
      </details>;
      case "spoiler": return <Spoiler key={node.offset + ":" + node.text} text={node.text}>{children}</Spoiler>;
      case "url": case "text_link": case "mention": case "hashtag": case "cashtag":
      case "bot_command": case "email": case "phone_number":
        return <span key={i} className="thread-reference">{children}</span>;
      default: return <Fragment key={i}>{children}</Fragment>;
    }
  });
}

export function RichText({ text, entities }: { text: string; entities: readonly TextEntity[] }) {
  return useMemo(() => renderTree(messageTree(text, entities)), [text, entities]);
}
