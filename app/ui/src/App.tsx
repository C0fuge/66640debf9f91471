import { useCallback, useEffect, useState } from "react";
import { ArrowLeft, LogIn, MessagesSquare, PanelsTopLeft, Plus, Settings } from "lucide-react";
import type { AccountSummary, ChatSummary, CoreClient } from "./core";
import { ChatListPane, initials } from "./components/ChatListPane";
import { ThreadPane } from "./components/ThreadPane";
import { Composer } from "./components/Composer";
import { useSnapshot } from "./components/useSnapshot";
import { AddAccount, EditSettings } from "./components/accounts/AccountSettings";
import { SignIn, supportsSignIn } from "./components/accounts/SignIn";
import "./components/accounts/accounts.css";

type AccountPanel = { kind: "add" } | { kind: "settings" | "auth"; id: string };
const anyChange = () => true;

function accountStatus(account: AccountSummary) {
  if (!account.enabled) return "Disabled";
  if (account.reconnecting) return "Reconnecting";
  if (account.phase !== "running") return account.phase;
  return account.state || account.phase;
}

function pluginColor(color: string | null | undefined) {
  return color && /^#[\da-f]{6}$/i.test(color) ? color : undefined;
}

export function App({ client, missingToken = false }: { client: CoreClient; missingToken?: boolean }) {
  const read = useCallback(() => client.inventory({}), [client]);
  const { data: inventory, error, retry } = useSnapshot(client, read, anyChange);
  const [chosenAccount, setChosenAccount] = useState<string | null>(null);
  const [selection, setSelection] = useState<ChatSummary | null>(null);
  const [showThread, setShowThread] = useState(false);
  const [panel, setPanel] = useState<AccountPanel | null>(null);
  const closePanel = useCallback(() => setPanel(null), []);
  const account = inventory?.accounts.find(a => a.accountId === chosenAccount) ?? inventory?.accounts[0];
  const accountId = account?.accountId ?? null;
  const platform = inventory?.plugins.find(p => p.pluginId === account?.pluginId);
  const chat = selection?.thread.accountId === accountId ? selection : null;
  const status = account ? accountStatus(account) : "";
  useEffect(() => {
    setChosenAccount(accountId);
    setPanel(open => open && open.kind !== "add" && open.id !== accountId ? null : open);
    setSelection(prev => prev?.thread.accountId === accountId ? prev : null);
  }, [accountId]);
  useEffect(() => {
    function shortcut(event: KeyboardEvent) {
      if (document.querySelector("dialog[open]")) return;
      if (!(event.ctrlKey || event.metaKey) || event.altKey || event.key.toLowerCase() !== "k") return;
      event.preventDefault();
      setShowThread(false);
      requestAnimationFrame(() => document.getElementById("chat-filter")?.focus());
    }
    window.addEventListener("keydown", shortcut);
    return () => window.removeEventListener("keydown", shortcut);
  }, []);
  const reconcile = useCallback((rows: ChatSummary[]) => {
    setSelection(prev => {
      if (!prev || prev.thread.accountId !== accountId) return prev;
      return rows.find(r => r.thread.accountId === accountId && r.thread.chatId === prev.thread.chatId &&
        r.thread.topicId === prev.thread.topicId) ?? null;
    });
  }, [accountId]);
  const selectChat = useCallback((row: ChatSummary) => {
    setSelection(row);
    setShowThread(true);
  }, []);

  return <main className="hub" data-thread-open={!!chat && showThread}>
    <header className="app-bar">
      <div className="app-name"><MessagesSquare size={23} aria-hidden="true" />Contact Hub</div>
      {account && <div className="bar-status" role="status"><span className="status-dot" data-online={!error && status.toLowerCase() === "connected"} />{error ? "Core offline" : status}</div>}
    </header>
    <div className="workspace">
      <nav className="account-rail" aria-label="Accounts">
        <span className="rail-title">Accounts</span>
        <div className="account-list">{inventory?.accounts.map(a => {
          const plugin = inventory.plugins.find(p => p.pluginId === a.pluginId);
          const name = a.label || a.accountId;
          const current = a.accountId === accountId;
          const state = accountStatus(a);
          return <button key={a.accountId} aria-pressed={current} className="account-button"
            aria-label={`${name}, ${plugin?.name || a.pluginId}, ${state}`}
            title={`${name} · ${a.accountId} · ${state}`}
            onClick={() => {
              if (!current) {
                setChosenAccount(a.accountId);
                setSelection(null);
              }
              setShowThread(false);
            }}>
            <span className="account-avatar" style={{ color: pluginColor(plugin?.brandColor) }}>{initials(name)}
              <i className="status-dot" data-online={!error && state.toLowerCase() === "connected"} />
            </span><span className="rail-account-name">{name}</span>
          </button>;
        })}</div>
        <div className="account-tools">
          <button disabled={!inventory || !!error} onClick={() => setPanel({ kind: "add" })} aria-label="Add account"><Plus size={18} />Add</button>
          {account && platform && <>
            <button disabled={!!error} onClick={() => setPanel({ kind: "settings", id: account.accountId })} aria-label="Account settings">
              <Settings size={17} />Settings</button>
            {account.enabled && account.phase === "running" && account.state === "auth_required" && supportsSignIn(platform) &&
              <button disabled={!!error} onClick={() => setPanel({ kind: "auth", id: account.accountId })}><LogIn size={17} />Sign in</button>}
          </>}
        </div>
      </nav>
      <div className="chat-column">
        {error && <div className="pane-error" role="status"><span>{missingToken ? "Open the development URL with its #token fragment to connect." : "Could not connect to the core."}</span>
          {!missingToken && <button onClick={retry}>Retry</button>}</div>}
        {!inventory && !error ? <div className="pane-message" role="status">Loading accounts…</div>
          : inventory && !inventory.accounts.length ? <div className="pane-message"><PanelsTopLeft /><p>No accounts configured</p></div>
          : <ChatListPane client={client} accountId={accountId} selectedThread={chat?.thread ?? null}
              accountName={account?.label || accountId || undefined} platformName={platform?.name} platformColor={pluginColor(platform?.brandColor)}
              onSnapshot={reconcile} onSelectChat={selectChat} />}
      </div>
      <div className="thread-column">
        {chat && <button className="back-to-chats" onClick={() => setShowThread(false)}><ArrowLeft size={16} />Chats</button>}
        <ThreadPane client={client} chat={chat} />
        <Composer client={client} chat={chat} />
      </div>
    </div>
    {panel?.kind === "add" && inventory && <AddAccount client={client} plugins={inventory.plugins} close={closePanel}
      added={id => { setChosenAccount(id); setSelection(null); setShowThread(false); closePanel(); retry(); }} />}
    {account && platform && panel?.kind === "settings" && panel.id === accountId &&
      <EditSettings key={accountId} client={client} account={account} plugin={platform} close={closePanel} />}
    {account && platform && panel?.kind === "auth" && panel.id === accountId &&
      <SignIn key={accountId} client={client} account={account} plugin={platform} close={closePanel} />}
  </main>;
}
