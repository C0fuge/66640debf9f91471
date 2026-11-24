import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { ChatSummary, CoreClient, HistoryItem, HistoryPage, Message, PluginSummary, ThreadKey, ThreadPaneProps } from "../core";
import { distinctItems, historyKey, messageDate, pageSize, prependPage } from "./thread/history";
import { RichText } from "./thread/RichText";
import { watchLocalHistory } from "./thread/historySignal";
import { failureCode, failureText, threadId } from "./thread/shared";
import { useAccountReads } from "./thread/useAccountReads";
import { actionSession, type MessageAction } from "./thread/actions";
import { MessageActionButtons, MessageActionDialog, type ActionSelection } from "./thread/MessageActionDialog";
import { AttachmentSummary } from "./thread/AttachmentSummary";
import { PollPanel, PollSummary, type PollSelection } from "./thread/PollCard";
import "./thread.css";

function chatKind(kind: string) {
  switch (kind) {
    case "private": case "direct": case "dm": return "Direct message";
    case "group": case "supergroup": return "Group";
    case "channel": return "Channel";
    default: return kind || "Chat";
  }
}

function Bubble({ thread, message, date, showSender, client, plugin, disabled, select, selectPoll }: {
  thread: ThreadKey; message: Message; date: ReturnType<typeof messageDate>; showSender: boolean;
  client: CoreClient; plugin?: PluginSummary | null; disabled: boolean; select: (action: ActionSelection) => void; selectPoll: (poll: PollSelection) => void;
}) {
  const sender = message.direction === "out" ? "You" : message.senderName;
  return <article className={'thread-bubble thread-bubble-' + message.direction} aria-label={sender || "Incoming message"}>
    {showSender && sender && <div className="thread-sender" dir="auto">{sender}</div>}
    {message.deleted ? <span className="thread-secondary">Deleted message</span> : <>
      <AttachmentSummary thread={thread} message={message} />
      <PollSummary thread={thread} message={message} disabled={disabled} select={selectPoll} />
      {message.text && <div className="thread-message-body" dir="auto">
        <RichText text={message.text} entities={message.entities} />
      </div>}
      {!message.mediaKind && !message.attachment && !message.poll && !message.text && <span className="thread-secondary">Empty message</span>}
    </>}
    <footer className="thread-message-meta">
      {message.isPinned && !message.deleted && <span>Pinned</span>}
      {message.edited && !message.deleted && <span>edited</span>}
      {date ? <time dateTime={date.iso}>{date.time}</time> : <span>Unknown time</span>}
      {message.direction === "out" && message.delivery && <span>{message.delivery}</span>}
    </footer>
    <MessageActionButtons client={client} plugin={plugin} message={message} disabled={disabled} select={select} />
  </article>;
}

type ScrollTarget = { bottom: true } | { key: string; inset: number };

function SelectedThread({ client, chat, plugin }: { client: CoreClient; chat: ChatSummary; plugin?: PluginSummary | null }) {
  const { thread } = chat;
  const [page, setPage] = useState<HistoryPage | null>(null);
  const latestLimit = useRef(pageSize);
  const [notice, setNotice] = useState("");
  const [action, setAction] = useState<ActionSelection | null>(null);
  const [poll, setPoll] = useState<PollSelection | null>(null);
  const [actionBusy, setActionBusy] = useState(false);
  const [reloadRequired, setReloadRequired] = useState(false);
  const actions = useMemo(() => actionSession(client, thread), [client, thread.accountId, thread.chatId, thread.topicId]);
  const scroller = useRef<HTMLDivElement>(null);
  const scrollTarget = useRef<ScrollTarget | null>({ bottom: true });
  const lastScroll = useRef(0);
  const [away, setAway] = useState(false);

  const rows = useMemo(() => {
    let previousDay: string | undefined;
    let previousSender: string | undefined;
    return (page?.items ?? []).map(item => {
      if (item.kind === "gap") {
        previousSender = undefined;
        previousDay = undefined;
        return { key: historyKey(item), item, date: null, day: null, showSender: false };
      }
      const message = item.message;
      const date = messageDate(message.sentAt);
      const day = date?.day ?? "Unknown date";
      const sender = JSON.stringify([message.direction, message.senderName, message.topicId]);
      const showSender = sender !== previousSender || day !== previousDay;
      const newDay = day !== previousDay;
      previousSender = sender;
      previousDay = day;
      return { key: historyKey(item), item, date, day: newDay ? day : null, showSender };
    });
  }, [page]);

  const hasOlder = Boolean(page?.moreOlder);
  const leading = hasOlder ? 1 : 0;
  const virtual = useVirtualizer({
    count: rows.length + leading, getScrollElement: () => scroller.current,
    estimateSize: index => index < leading ? 44 : 82, overscan: 8,
    getItemKey: index => index < leading ? "older" : rows[index - leading].key,
  });

  function rememberPosition(prepending: boolean) {
    const element = scroller.current;
    if (!element || (!prepending && element.scrollHeight - element.scrollTop - element.clientHeight < 48)) {
      scrollTarget.current = { bottom: true };
      return;
    }
    const visible = virtual.getVirtualItems().find(row => row.index >= leading && row.end > element.scrollTop);
    if (visible) scrollTarget.current = { key: String(visible.key), inset: element.scrollTop - visible.start };
  }

  function applyPage(next: HistoryPage, items: HistoryItem[], prepending = false) {
    rememberPosition(prepending);
    latestLimit.current = Math.min(200, Math.max(pageSize, items.filter(item => item.kind === "message").length));
    setPage({ ...next, items });
  }

  async function readCached(active: () => boolean) {
    const next = await client.history({ thread, window: { kind: "latest" }, limit: latestLimit.current });
    if (active()) applyPage(next, distinctItems(next.items));
  }
  const reads = useAccountReads(client, thread.accountId, readCached);
  useEffect(() => watchLocalHistory(client, thread, reads.reload), [client, thread, reads.reload]);

  function submitAction(mutation: MessageAction) {
    reads.run(async active => {
      setActionBusy(true);
      const result = await actions.perform(mutation, action?.message.topicId ?? thread.topicId);
      if (!active()) return;
      setActionBusy(false);
      setAction(null);
      setNotice(result.error);
      setReloadRequired(actions.reloadRequired);
      if (result.ok) await readCached(active);
    });
  }

  function reloadActions() {
    reads.run(async active => {
      await readCached(active);
      if (!active()) return;
      actions.reloaded();
      setReloadRequired(false);
      setNotice("History reloaded. Review the message before choosing another action.");
    });
  }

  useLayoutEffect(() => {
    const target = scrollTarget.current;
    const el = scroller.current;
    if (!target || !el || !page) return;
    scrollTarget.current = null;
    if ("bottom" in target) {
      if (rows.length) virtual.scrollToIndex(rows.length - 1 + leading, { align: "end" });
    } else {
      const index = rows.findIndex(row => row.key === target.key);
      if (index >= 0) {
        const offset = virtual.getOffsetForIndex(index + leading, "start");
        if (offset) virtual.scrollToOffset(offset[0] + target.inset);
      }
    }
    lastScroll.current = el.scrollTop;
    setAway(el.scrollHeight - el.scrollTop - el.clientHeight > 100);
  }, [page, leading, rows, virtual]);

  function loadOlder() {
    if (!page?.moreOlder) return;
    const head = page.items.find(item => item.kind === "message");
    reads.run(async active => {
      setNotice("");
      try {
        const next = await client.history({
          thread, window: { kind: "older", before: head?.message.messageId ?? null }, limit: pageSize,
        });
        if (active()) applyPage(next, prependPage(next, page.items), true);
      } catch (cause) {
        if (!active()) return;
        if (failureCode(cause) !== "invalid_request" && failureCode(cause) !== "not_found") throw cause;
        setNotice("The history cursor changed. Showing the latest messages.");
        await readCached(active);
      }
    });
  }

  function refresh() {
    reads.run(async active => {
      setNotice("");
      const next = await client.history({ thread, window: { kind: "refresh" }, limit: latestLimit.current });
      if (active()) applyPage(next, distinctItems(next.items));
    });
  }

  async function rereadGapWindow(active: () => boolean, count: number) {
    const next = await client.history({ thread, window: { kind: "latest" }, limit: latestLimit.current });
    let items = distinctItems(next.items);
    let moreOlder = next.moreOlder;
    // A user can fill a gap several pages back. Rebuild the previously loaded
    // range, stopping at a remaining marker so this click fills only one gap.
    while (active() && moreOlder && items.length < count && items[0]?.kind === "message") {
      const before = items[0].message.messageId;
      const older = await client.history({
        thread, window: { kind: "older", before }, limit: Math.min(200, count - items.length),
      });
      const merged = prependPage(older, items);
      moreOlder = older.moreOlder;
      if (merged.length === items.length) break;
      items = merged;
    }
    if (active()) applyPage({ ...next, moreOlder }, items);
  }

  function fillGap(gapId: string) {
    reads.run(async active => {
      setNotice("");
      try {
        const result = await client.fillGap({ thread, gapId });
        if (active() && !result.closed) setNotice("More messages remain in this gap.");
      } catch (cause) {
        if (active()) setNotice(failureText(cause));
      }
      if (active()) {
        try {
          await rereadGapWindow(active, page?.items.length ?? pageSize);
        } catch (cause) {
          if (!active()) return;
          if (failureCode(cause) !== "invalid_request" && failureCode(cause) !== "not_found") throw cause;
          await readCached(active);
        }
      }
    });
  }

  const title = chat.title || thread.chatId;
  const initials = title.trim().split(/\s+/u).slice(0, 2).map(word => Array.from(word)[0]).join("");
  return <section className="thread-pane" aria-label="Conversation">
    <header className="thread-header">
      <span className="thread-avatar" aria-hidden="true">{initials || "#"}</span>
      <div className="thread-heading"><h2 dir="auto">{title}</h2>
        <span>{chatKind(chat.kind)}{thread.topicId !== null && " · Topic " + thread.topicId}</span>
      </div>
      <button type="button" className="thread-action" onClick={refresh} disabled={reads.busy || !reads.subscribed}>Refresh</button>
    </header>
    {(reads.error || notice) && <div className="thread-status" role="status">
      <span>{reads.error || notice}</span>
      {reads.error && <button type="button" onClick={reads.reload} disabled={reads.busy}>Reload history</button>}
      {reloadRequired && <button type="button" onClick={reloadActions} disabled={reads.busy || !reads.subscribed}>Reload before another action</button>}
    </div>}
    <div className="thread-stage">
      <div ref={scroller} className="thread-scroll" tabIndex={0} aria-label="Message history"
        aria-busy={reads.busy} onScroll={event => {
          const el = event.currentTarget;
          const upward = el.scrollTop < lastScroll.current;
          lastScroll.current = el.scrollTop;
          setAway(el.scrollHeight - el.scrollTop - el.clientHeight > 100);
          if (upward && el.scrollTop < 72 && !reads.busy) loadOlder();
        }}>
        <div className="thread-virtual" style={{ height: virtual.getTotalSize() }}>
          {virtual.getVirtualItems().map(cell => {
            const row = rows[cell.index - leading];
            return <div className="thread-row" key={cell.key} data-index={cell.index} ref={virtual.measureElement}
              style={{ transform: 'translateY(' + cell.start + 'px)' }}>
              {!row ? <button type="button" className="thread-page-button" onClick={loadOlder} disabled={reads.busy}>Load earlier messages</button>
                : row.item.kind === "gap" ? <button type="button" className="thread-page-button" disabled={reads.busy}
                  onClick={() => { if (row.item.kind === "gap") fillGap(row.item.gapId); }}>Load missing messages</button>
                : <>{row.day && <div className="thread-day">{row.day}</div>}
                  <Bubble thread={thread} message={row.item.message} date={row.date} showSender={row.showSender} client={client} plugin={plugin}
                    disabled={reads.busy || !reads.subscribed || actionBusy || reloadRequired} select={setAction} selectPoll={setPoll} /></>}
            </div>;
          })}
        </div>
        {rows.length === 0 && <div className="thread-empty" role="status">
          {reads.busy ? "Loading history…" : page ? "No cached messages" : "History unavailable"}
        </div>}
      </div>
      {away && rows.length > 0 && <button type="button" className="thread-latest" onClick={() => {
        virtual.scrollToIndex(rows.length - 1 + leading, { align: "end" });
      }}>Latest ↓</button>}
    </div>
    {poll && <PollPanel selection={poll} client={client} plugin={plugin} close={() => setPoll(null)} />}
    {action && <MessageActionDialog selection={action} busy={actionBusy} close={() => setAction(null)} submit={submitAction} />}
  </section>;
}

export function ThreadPane({ client, chat, plugin }: ThreadPaneProps) {
  if (!chat) return <section className="thread-pane thread-empty" aria-label="Conversation">Select a conversation</section>;
  return <SelectedThread key={threadId(chat.thread)} client={client} chat={chat} plugin={plugin} />;
}
