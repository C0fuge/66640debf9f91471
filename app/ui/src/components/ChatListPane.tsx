import { useCallback, useEffect, useRef, useState } from "react";
import { Archive, ChevronRight, MessageCircle, MoreHorizontal, Pin, Search, VolumeX, X } from "lucide-react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { ChatListPaneProps, ChatSummary, CoreChange, PluginSummary, AccountSummary } from "../core";
import { useSnapshot } from "./useSnapshot";
import { ChatFlagsPanel } from "./ChatFlagsPanel";
import "./chat-list.css";

const clock = new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" });
const shortDate = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" });

function rowDate(millis: string | null) {
  if (millis === null || !/^-?\d+$/.test(millis)) return "";
  const date = new Date(Number(millis));
  if (Number.isNaN(date.getTime())) return "";
  return (date.toDateString() === new Date().toDateString() ? clock : shortDate).format(date);
}

export function initials(text: string) {
  const names = text.trim().split(/\s+/u);
  return [names[0], names.length > 1 ? names[names.length - 1] : ""]
    .map(name => Array.from(name ?? "")[0] ?? "").join("").toLocaleUpperCase() || "•";
}

function avatarTone(id: string) {
  let hash = 0;
  for (const letter of id) hash = (hash * 31 + letter.charCodeAt(0)) | 0;
  return Math.abs(hash) % 5;
}

interface Props extends ChatListPaneProps {
  onSnapshot?: (rows: ChatSummary[]) => void;
  account?: AccountSummary | null;
  plugin?: PluginSummary | null;
  accountName?: string;
  platformName?: string;
  platformColor?: string;
}

export function ChatListPane(props: Props) {
  const { accountId } = props;
  if (accountId === null) {
    return <section className="chat-pane" aria-label="Chats">
      <header className="chats-heading"><h1>Chats</h1></header>
      <div className="pane-message"><MessageCircle aria-hidden="true" /><p>Select an account to see its chats.</p></div>
    </section>;
  }
  return <AccountChats key={JSON.stringify([props.plugin?.pluginId, accountId])} {...props} accountId={accountId} />;
}

function AccountChats({ accountId, selectedThread, onSelectChat, onSnapshot, client, account, plugin, accountName, platformName, platformColor }: Props & { accountId: string }) {
  const [menu, setMenu] = useState<string | null>(null);
  const opener = useRef<HTMLButtonElement | null>(null);
  const read = useCallback(() => client.listChats({ accountId, archived: null }), [client, accountId]);
  const relevant = useCallback((e: CoreChange) => {
    if (e.kind === "reset") { setMenu(null); return true; }
    return e.kind === "account" && e.accountId === accountId;
  }, [accountId]);
  const { data, error, retry } = useSnapshot(client, read, relevant);
  const [query, setQuery] = useState("");
  const [archive, setArchive] = useState(false);
  const scroller = useRef<HTMLDivElement>(null);
  const searchInput = useRef<HTMLInputElement>(null);
  useEffect(() => { if (data) onSnapshot?.(data); }, [data, onSnapshot]);
  const term = query.trim().toLocaleLowerCase();
  const rows = (data ?? []).filter(row => row.archived === archive &&
    (!term || (row.title || row.thread.chatId).toLocaleLowerCase().includes(term) ||
      row.lastMessagePreview?.toLocaleLowerCase().includes(term)));
  const target = rows.find(row => row.thread.chatId === menu && row.thread.accountId === accountId && row.thread.topicId === null);
  useEffect(() => {
    if (menu !== null && data && !target) {
      setMenu(null);
      searchInput.current?.focus();
    }
  }, [menu, data, target]);
  const virtual = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scroller.current,
    estimateSize: () => 78,
    getItemKey: index => rows[index].thread.chatId,
    overscan: 5,
  });
  useEffect(() => { scroller.current?.scrollTo({ top: 0 }); }, [archive, query]);

  return <section className="chat-pane" aria-label="Chats">
    <header className="chats-heading">
      <h1>Chats</h1>
      <div className="account-caption"><span>{accountName || accountId}</span>{platformName && platformName !== accountName && <span className="platform-name">{platformName}</span>}</div>
      <div className="chat-search">
        <Search size={16} aria-hidden="true" />
        <input id="chat-filter" type="search" ref={searchInput} aria-label="Search chats" placeholder="Search chats" value={query} onChange={e => { setMenu(null); setQuery(e.target.value); }} />
        {query ? <button type="button" aria-label="Clear search" onClick={() => { setMenu(null); setQuery(""); searchInput.current?.focus(); }}><X size={14} /></button> : <kbd>Ctrl K</kbd>}
      </div>
      <div className="chat-folders" aria-label="Chat folders">
        <button type="button" aria-pressed={!archive} onClick={() => { setMenu(null); setArchive(false); }}><MessageCircle size={14} />Inbox</button>
        <button type="button" aria-pressed={archive} onClick={() => { setMenu(null); setArchive(true); }}><Archive size={14} />Archive</button>
      </div>
    </header>
    {error && <div className="pane-error" role="status"><span>{data ? "Chat updates are offline." : "Could not load chats."}</span><button onClick={retry}>Retry</button></div>}
    {!data && !error && <div className="pane-message" role="status">Loading chats…</div>}
    {data && !rows.length && <div className="pane-message" role="status">{archive ? <Archive /> : <MessageCircle />}<p>{term ? "No matching chats" : archive ? "No archived chats" : "No chats yet"}</p></div>}
    <div className="chat-scroll" ref={scroller}>
      <ul className="chat-list" style={{ height: virtual.getTotalSize() }} aria-label={archive ? "Archived chats" : "Inbox chats"}>
        {virtual.getVirtualItems().map(item => {
          const row = rows[item.index];
          const title = row.title || row.thread.chatId;
          const active = selectedThread?.accountId === accountId && selectedThread.chatId === row.thread.chatId;
          const unread = row.unreadCount > 0 || row.markedUnread;
          const preview = row.lastMessagePreview || row.lastMessageKind || "No messages";
          return <li key={item.key} style={{ height: item.size, transform: `translateY(${item.start}px)` }} aria-posinset={item.index + 1} aria-setsize={rows.length}>
            <button className="chat-row" aria-current={active ? "true" : undefined} onClick={() => { setMenu(null); onSelectChat(row); }}>
              <span className={`row-avatar tone-${avatarTone(row.thread.chatId)}`} aria-hidden="true">{initials(title)}
                {platformName && <i title={platformName} className="platform-mark" style={{ color: platformColor }}>{Array.from(platformName)[0]}</i>}
              </span>
              <span className="row-copy">
                <span className="row-title-line"><strong>{title}</strong><time>{rowDate(row.lastMessageAt)}</time></span>
                <span className="row-preview-line"><span className="preview">{preview}</span>
                  {row.pinned && <Pin size={13} aria-label="Pinned" />}
                  {row.muted && <VolumeX size={14} aria-label="Muted" />}
                  {unread && <span aria-label={row.unreadCount ? `${row.unreadCount} unread` : "Unread"} className={`unread ${row.muted ? "is-muted" : ""} ${!row.unreadCount ? "dot" : ""}`}>{row.unreadCount ? (row.unreadCount > 999 ? "999+" : row.unreadCount) : ""}</span>}
                  {active && !unread && <ChevronRight size={13} aria-hidden="true" />}
                </span>
              </span>
            </button>
            {account && plugin && row.thread.accountId === accountId && row.thread.topicId === null && !row.thread.chatId.startsWith("local:") &&
              <button type="button" className="chat-menu-button" aria-haspopup="dialog" aria-expanded={menu === row.thread.chatId}
                aria-label={`Chat preferences for ${title}`} onClick={e => { opener.current = e.currentTarget; setMenu(row.thread.chatId); }}>
                <MoreHorizontal size={16} aria-hidden="true" /></button>}
          </li>;
        })}
      </ul>
    </div>
    {target && plugin && account && !error && <ChatFlagsPanel key={target.thread.chatId} client={client} row={target}
      account={account} plugin={plugin} close={() => {
        setMenu(null);
        if (opener.current?.isConnected) opener.current.focus(); else searchInput.current?.focus();
      }} />}
  </section>;
}
