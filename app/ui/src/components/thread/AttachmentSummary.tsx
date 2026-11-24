import { useState } from "react";
import type { Attachment, Message, ThreadKey } from "../../core";

const labels: Record<string, string> = {
  photo: "Photo", video: "Video", gif: "GIF", sticker: "Sticker", voice: "Voice message",
  video_note: "Video note", music: "Audio", document: "Document", other: "Attachment",
};

function label(kind: string) {
  return Object.hasOwn(labels, kind) ? labels[kind] : "Attachment";
}

export function attachmentBytes(value: string | null): string | null {
  if (value === null || value.length > 20 || !/^(0|[1-9][0-9]*)$/.test(value)) return null;
  const count = BigInt(value);
  if (count > 18446744073709551615n) return null;
  return `${count.toLocaleString("en-US")} ${count === 1n ? "byte" : "bytes"}`;
}

function safeNumber(value: number | null): value is number {
  return value !== null && Number.isSafeInteger(value) && value >= 0;
}

export function attachmentFacts(attachment: Attachment): string[] {
  const facts: string[] = [];
  if (!Object.hasOwn(labels, attachment.kind)) facts.push(attachment.kind);
  if (attachment.mime) facts.push(attachment.mime);
  const bytes = attachmentBytes(attachment.bytes);
  if (bytes !== null) facts.push(bytes);
  if (safeNumber(attachment.width) && safeNumber(attachment.height)) facts.push(`${attachment.width} × ${attachment.height} px`);
  else if (safeNumber(attachment.width)) facts.push(`${attachment.width} px wide`);
  else if (safeNumber(attachment.height)) facts.push(`${attachment.height} px tall`);
  if (safeNumber(attachment.durationSeconds)) {
    const seconds = attachment.durationSeconds;
    const minutes = Math.floor(seconds / 60);
    facts.push(`${minutes}:${String(seconds % 60).padStart(2, "0")}`);
  }
  return facts;
}

export function attachmentRevealKey(thread: ThreadKey, message: Message): string {
  const a = message.attachment;
  return JSON.stringify([thread.accountId, thread.chatId, thread.topicId, message.topicId, message.rowKey, message.messageId,
    a?.kind, a?.name, a?.mime, a?.bytes, a?.width, a?.height, a?.durationSeconds, a?.hasSpoiler]);
}

function focusDetails(element: HTMLDivElement | null) {
  element?.focus({ preventScroll: true });
}

export function AttachmentSummary({ thread, message }: { thread: ThreadKey; message: Message }) {
  const [revealed, setRevealed] = useState<string | null>(null);
  if (message.deleted) return null;
  if (message.messageId.startsWith("local:") || message.delivery === "sending" || message.delivery === "failed") {
    return message.mediaKind || message.attachment ? <div className="thread-media">Attachment</div> : null;
  }
  const attachment = message.attachment;
  if (!attachment) {
    const kind = message.mediaKind ? label(message.mediaKind) : null;
    return kind ? <div className="thread-media">{kind === "Attachment" ? kind : `${kind} attachment`}</div> : null;
  }
  const identity = attachmentRevealKey(thread, message);
  if (attachment.hasSpoiler && revealed !== identity) {
    return <button type="button" className="thread-attachment-reveal" onClick={() => setRevealed(identity)}>
      Reveal attachment details
    </button>;
  }
  const facts = attachmentFacts(attachment);
  return <div className="thread-media thread-attachment" ref={attachment.hasSpoiler ? focusDetails : undefined}
    tabIndex={attachment.hasSpoiler ? -1 : undefined}>
    <strong>{label(attachment.kind)}</strong>
    {attachment.name && <span className="thread-attachment-name" dir="auto">{attachment.name}</span>}
    {facts.length > 0 && <span className="thread-attachment-facts" dir="auto">{facts.join(" · ")}</span>}
  </div>;
}
