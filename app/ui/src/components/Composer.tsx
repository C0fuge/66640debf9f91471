import { useEffect, useLayoutEffect, useRef, useState, useSyncExternalStore } from "react";
import type { ChatSummary, ComposerProps, CoreClient, OutboxItem } from "../core";
import { changeOutbox, checkAcceptance, draftFor, sendDraft } from "./composer/drafts";
import { threadId } from "./thread/shared";
import { notifyLocalHistory } from "./thread/historySignal";
import { useAccountReads } from "./thread/useAccountReads";
import { uploadNotice } from "./composer/fileIngress";
import { FileComposer } from "./composer/FileComposer";
import "./composer.css";

const stateLabel: Record<OutboxItem["state"], string> = {
  queued: "Queued", sending: "Sending", sent: "Sent", failed: "Failed", cancelled: "Cancelled",
};

function SelectedComposer({ client, chat }: { client: CoreClient; chat: ChatSummary }) {
  const { thread } = chat;
  const entry = draftFor(client, thread);
  const draft = useSyncExternalStore(entry.subscribe, entry.snapshot);
  const [items, setItems] = useState<OutboxItem[]>([]);
  const [open, setOpen] = useState(false);
  const [confirmItem, setConfirmItem] = useState<string | null>(null);
  const textarea = useRef<HTMLTextAreaElement>(null);
  const outbox = useAccountReads(client, thread.accountId, async active => {
    const phase = entry.snapshot().outboxPhase;
    const next = await client.listOutbox({ thread });
    if (phase === "reconcile") {
      await client.history({ thread, window: { kind: "latest" }, limit: 50 });
      notifyLocalHistory(client, thread);
    }
    if (active()) {
      setItems(next);
      if ((phase === "reconcile" || phase === "reload") && entry.snapshot().outboxPhase === phase) {
        entry.update({ outboxPhase: null });
      }
    }
  });

  useLayoutEffect(() => {
    const el = textarea.current;
    if (el) { el.style.height = "auto"; el.style.height = el.scrollHeight + "px"; }
  }, [draft.text]);

  // Also reconcile the visible composer if the user returned to this thread
  // while a previous mount was waiting for an ambiguous send reply.
  useEffect(() => {
    if (draft.uncertain === "review") { setOpen(true); outbox.reload(); }
  }, [draft.uncertain, outbox.reload]);
  useEffect(() => {
    if (draft.outboxPhase === "reload" || draft.outboxPhase === "reconcile") outbox.reload();
  }, [draft.outboxPhase, outbox.reload]);
  const recover = () => checkAcceptance(entry, client, thread);

  const restricted = chat.canPost === false;
  const outboxLocked = outbox.busy || !!outbox.error || draft.outboxPhase !== null;
  const canSend = !restricted && draft.outboxPhase !== "reconcile" && outbox.subscribed && !draft.pending && !!draft.text.trim();
  function send(duplicateConfirmed = false) {
    if (canSend) void sendDraft(entry, client, thread, recover, duplicateConfirmed);
  }

  function updateOutbox(item: OutboxItem, action: "cancel" | "retry", confirmed = false) {
    if (action === "retry" && (restricted || item.kind === "sendFile")) return;
    if (action === "retry" && item.outcomeUnknown && !confirmed) {
      setConfirmItem(item.id);
      return;
    }
    outbox.run(async () => {
      setConfirmItem(null);
      await changeOutbox(entry, client, item, action, confirmed);
    });
  }

  const attention = items.filter(item => item.state === "failed" || item.state === "queued" || item.state === "sending").length;
  return <section className="thread-composer" aria-label="Message composer">
    {(items.length > 0 || outbox.error || open) && <div className="composer-outbox">
      <button type="button" className="composer-disclosure" aria-expanded={open} onClick={() => setOpen(!open)}>
        Outbox{attention ? ' · ' + attention + ' pending or failed' : ' · ' + items.length} {open ? "▴" : "▾"}
      </button>
      {open && <ul className="composer-outbox-list" aria-label="Message outbox">
        {items.map(item => <li key={JSON.stringify([threadId(item.thread), item.id])}>
          <div className="composer-outbox-row">
            <span className="composer-item-label">{item.kind === "sendFile" ? "File · " : ""}{stateLabel[item.state]} <small title={item.id}>{item.id}</small>
              {thread.topicId === null && item.thread.topicId !== null && <small>Topic {item.thread.topicId}</small>}
            </span>
            {(item.state === "queued" || item.state === "sending") &&
              <button type="button" disabled={outboxLocked} onClick={() => updateOutbox(item, "cancel")}>Cancel</button>}
            {item.kind === "sendText" && item.state === "failed" && confirmItem !== item.id &&
              <button type="button" disabled={outboxLocked || restricted} onClick={() => updateOutbox(item, "retry")}>Retry</button>}
          </div>
          {uploadNotice(item) && <div className="composer-file-meta">{uploadNotice(item)}</div>}
          {item.kind === "sendFile" && item.outcomeUnknown && <div className="composer-warning">Delivery could not be confirmed. Sending this file again could duplicate it.</div>}
          {item.kind === "sendText" && item.state === "failed" && confirmItem === item.id && <div className="composer-warning">
            This message may already have been delivered. Retrying could send a duplicate.
            <div><button type="button" disabled={outboxLocked || restricted} onClick={() => updateOutbox(item, "retry", true)}>Retry anyway</button>
              <button type="button" onClick={() => setConfirmItem(null)}>Keep failed</button></div>
          </div>}
        </li>)}
        {!items.length && <li>No messages in the outbox.</li>}
      </ul>}
    </div>}
    {(outbox.error || draft.outboxNotice) && <div className="composer-status" role="status">
      {outbox.error || draft.outboxNotice} <button type="button" disabled={outbox.busy} onClick={outbox.reload}>Reload outbox</button>
    </div>}
    {draft.uncertain && <div className="composer-warning" role="status">
      Your send may have been accepted. Sending again could deliver a duplicate.
      {draft.uncertain === "checking" && <span> Checking history and outbox…</span>}
      {draft.uncertain === "unverified" && <button type="button" onClick={() => void recover()}>Check history and outbox</button>}
      {draft.uncertain === "review" && <div>
        <button type="button" disabled={!canSend || outbox.busy || !!outbox.error} onClick={() => send(true)}>Send again anyway</button>
        <button type="button" onClick={() => entry.update({ uncertain: null, error: null, text: "" })}>Discard draft</button>
      </div>}
    </div>}
    {draft.error && <div className="composer-status" role="alert">{draft.error}</div>}
    {restricted && <div className="composer-status">This conversation is read-only.</div>}
    <FileComposer client={client} chat={chat} />
    <form className="composer-form" onSubmit={event => { event.preventDefault(); send(); }}>
      <textarea ref={textarea} rows={1} aria-label="Message" placeholder={restricted ? "Read-only conversation" : "Write a message…"}
        disabled={restricted} readOnly={draft.pending || draft.uncertain !== null}
        value={draft.text} onChange={event => entry.update({ text: event.target.value, error: null })}
        onKeyDown={event => {
          if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing || event.nativeEvent.keyCode === 229) return;
          event.preventDefault();
          if (!event.repeat) send();
        }} />
      <button type="submit" className="composer-send" disabled={!canSend || draft.uncertain !== null}>
        {draft.pending ? "Sending…" : "Send"}
      </button>
    </form>
  </section>;
}

export function Composer({ client, chat }: ComposerProps) {
  if (!chat) return null;
  return <SelectedComposer key={threadId(chat.thread)} client={client} chat={chat} />;
}
