import { useLayoutEffect, useMemo, useReducer, useRef } from "react";
import type { AccountSummary, ChatSummary, CoreClient, PluginSummary } from "../core";
import { chatFlags, flagPermissions, flagSession } from "./chatFlags";

export function ChatFlagsPanel({ client, row, account, plugin, close }: {
  client: CoreClient; row: ChatSummary; account: AccountSummary; plugin: PluginSummary; close: () => void;
}) {
  const { accountId, chatId, topicId } = row.thread;
  const allowed = flagPermissions(plugin, account, row.thread);
  const permissions = useRef(allowed);
  permissions.current = allowed;
  const session = useMemo(() => flagSession(client, plugin.pluginId, { accountId, chatId, topicId }, () => permissions.current),
    [client, plugin.pluginId, accountId, chatId, topicId]);
  const [, render] = useReducer(n => n + 1, 0);
  const closeButton = useRef<HTMLButtonElement>(null);
  useLayoutEffect(() => { closeButton.current?.focus(); }, []);
  useLayoutEffect(() => { session.invalidate(); render(); return () => session.invalidate(); },
    [session, allowed.refresh, allowed.pinned, allowed.markedUnread]);
  const run = (work: Promise<void>) => { render(); void work.finally(render); };
  const { context, busy, message, capacityBlocked } = session.state();
  return <section className="chat-flags-panel" role="dialog" aria-label={`Chat preferences for ${row.title || chatId}`}
    onKeyDown={e => { if (e.key === "Escape") close(); }}>
    <button type="button" ref={closeButton} onClick={close}>Close</button>
    <strong>{row.title || chatId}</strong>
    <button type="button" disabled={!allowed.refresh || busy} onClick={() => run(session.refresh())}>Refresh flags</button>
    {chatFlags.map(flag => {
      const observed = context?.[flag];
      const attempt = session.attempt(flag);
      const name = flag === "pinned" ? "Pinned" : "Manual unread";
      return <div key={flag}><span>{name} · {attempt === "unknown" ? "Outcome unknown" : attempt === "pending" ? "Pending" :
        observed == null ? "Unknown" : observed ? "Yes" : "No"}</span>
        {typeof observed === "boolean" && <button type="button" disabled={!allowed[flag] || !context?.contextToken || busy || !!attempt || capacityBlocked}
          onClick={() => run(session.set(flag, !observed))}>{flag === "pinned" ? observed ? "Unpin chat" : "Pin chat" :
            observed ? "Clear unread mark" : "Mark as unread"}</button>}</div>;
    })}
    {(message || capacityBlocked) && <p role="status">{message || "Refresh unresolved flags before changing another chat."}</p>}
  </section>;
}
