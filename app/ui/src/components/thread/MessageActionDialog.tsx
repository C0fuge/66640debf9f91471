import { useId, useLayoutEffect, useRef, useState } from "react";
import type { CoreClient, Message, PluginSummary } from "../../core";
import { messageActions, type MessageAction } from "./actions";

export type ActionSelection = { kind: "edit" | "delete" | "pin" | "unpin"; message: Message; deleteForMe: boolean };

export function MessageActionButtons({ client, plugin, message, disabled, select }: {
  client: CoreClient; plugin?: PluginSummary | null; message: Message; disabled: boolean; select: (action: ActionSelection) => void;
}) {
  const actions = messageActions(client, plugin, message);
  const choose = (kind: ActionSelection["kind"]) => select({ kind, message, deleteForMe: actions.deleteForMe });
  return <div className="thread-message-actions" aria-label="Message actions">
    {actions.showEdit && <button type="button" disabled={disabled || !actions.edit} onClick={() => choose("edit")}>Edit</button>}
    {actions.showDelete && <button type="button" disabled={disabled || !actions.delete} onClick={() => choose("delete")}>Delete</button>}
    {message.isPinned ? actions.showUnpin && <button type="button" disabled={disabled || !actions.unpin} onClick={() => choose("unpin")}>Unpin</button>
      : actions.showPin && <button type="button" disabled={disabled || !actions.pin} onClick={() => choose("pin")}>Pin</button>}
  </div>;
}

export function MessageActionDialog({ selection, busy, close, submit }: {
  selection: ActionSelection; busy: boolean; close: () => void; submit: (action: MessageAction) => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const heading = useId();
  const [text, setText] = useState(selection.message.text ?? "");
  // The user chooses an explicit scope; never silently pick a destructive default.
  const [scope, setScope] = useState("");
  const [silent, setSilent] = useState(false);
  useLayoutEffect(() => {
    const dialog = ref.current!;
    dialog.showModal();
    return () => dialog.close();
  }, []);
  const { kind, message } = selection;
  const title = kind === "edit" ? "Edit message" : kind === "delete" ? "Delete message" : kind === "pin" ? "Pin message" : "Unpin message";
  return <dialog ref={ref} className="message-action-dialog" aria-labelledby={heading}
    onCancel={event => { event.preventDefault(); if (!busy) close(); }}>
    <form onSubmit={event => {
      event.preventDefault();
      if (busy) return;
      if (kind === "edit") submit({ kind, messageId: message.messageId, text, entities: text === message.text ? message.entities : [] });
      else if (kind === "delete" && scope) submit({ kind, messageIds: [message.messageId], revoke: scope === "everyone" });
      else if (kind === "pin") submit({ kind, messageId: message.messageId, silent });
      else if (kind === "unpin") submit({ kind, messageId: message.messageId });
    }}>
      <h2 id={heading}>{title}</h2>
      {kind === "edit" && <><label>Message text<textarea autoFocus value={text} disabled={busy} onChange={event => setText(event.target.value)} /></label>
        {message.entities.length > 0 && text !== message.text && <p>Replacing the text clears its formatting.</p>}</>}
      {kind === "delete" && <fieldset disabled={busy}><legend>Choose who this deletion affects</legend>
        {selection.deleteForMe && <label><input type="radio" name="delete-scope" value="me" checked={scope === "me"} onChange={() => setScope("me")} />Only me</label>}
        <label><input type="radio" name="delete-scope" value="everyone" checked={scope === "everyone"} onChange={() => setScope("everyone")} />Everyone</label>
        <p>This permanently deletes the selected message from your local history.</p>
      </fieldset>}
      {kind === "pin" && <label><input type="checkbox" checked={silent} disabled={busy} onChange={event => setSilent(event.target.checked)} />Pin silently</label>}
      {kind === "unpin" && <p>Unpin this message?</p>}
      <div className="message-action-buttons"><button type="button" disabled={busy} onClick={close}>Cancel</button>
        <button type="submit" disabled={busy || kind === "delete" && !scope || kind === "edit" && !text.trim()}>{busy ? "Applying…" : title}</button></div>
    </form>
  </dialog>;
}
