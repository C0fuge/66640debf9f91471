import { useRef, useState } from "react";
import type { ChatSummary, CoreClient, FileStage, FileStagePage, Inventory } from "../../core";
import { failureText, threadId } from "../thread/shared";
import { useAccountReads } from "../thread/useAccountReads";
import { changeFile, fileAccess, fileAction, fileDraft, fileInProgress, isFileStagePage, learnFilePage,
  pendingFileStarts, rememberFileDraft, reviewOldFileIntent, selectFile, stageNotice, validCaption } from "./fileIngress";

const labels: Record<FileStage["state"], string> = { choosing: "Choosing file…", copying: "Copying file…", ready: "Ready to send",
  verifying: "Checking saved file…", enqueuing: "Adding to outbox…", enqueued: "Added to outbox", admissionUnknown: "Admission unconfirmed",
  cancelling: "Cancelling…", cancelled: "Cancelled", failed: "File preparation failed" };
function size(n: string | null) { return n === null ? "Size unknown" : `${BigInt(n).toLocaleString()} bytes`; }
type Access = ReturnType<typeof fileAccess>;

function StageRow({ client, stage, access, restricted, busy, mutate }: {
  client: CoreClient; stage: FileStage; access: Access; restricted: boolean; busy: boolean;
  mutate: (stage: FileStage, kind: "send" | "cancel") => void;
}) {
  const [draft, setDraft] = useState(() => fileDraft(client, stage));
  const action = fileAction(client, stage);
  function update(change: Partial<typeof draft>) { const next = { ...draft, ...change }; rememberFileDraft(client, stage, next); setDraft(next); }
  const editable = stage.state === "ready" && !action;
  const tooLarge = access.maxBytes !== null && stage.bytes !== null && BigInt(stage.bytes) > BigInt(access.maxBytes);
  const send = editable && !busy && !restricted && access.canSend && !tooLarge && validCaption(draft.caption)
    && (!draft.asFile || access.asFile) && (!draft.spoiler || access.spoiler);
  const canCancel = ["choosing", "copying", "ready", "verifying"].includes(stage.state) && !action?.pending;
  const notice = stageNotice(stage);
  return <li className="composer-file-stage">
    <div className="composer-outbox-row"><strong>{stage.name ?? "Selected file"}</strong><span>{labels[stage.state]}</span></div>
    {stage.bytes !== null && <div className="composer-file-meta">{size(stage.bytes)}
      {stage.format !== null && <span> · {stage.format === "other" ? "File" : `${stage.format.toUpperCase()} header`}</span>}</div>}
    {stage.state === "copying" && <div className="composer-status">Copied {size(stage.copiedBytes)} of {size(stage.bytes)}</div>}
    {notice && <div className="composer-warning" role="status">{notice}</div>}
    {action && !action.pending && <div className="composer-warning" role="status">
      The {action.kind === "send" ? "send" : "cancel"} request could not be confirmed. Reload file status; it will not be repeated automatically.
    </div>}
    {stage.state === "ready" && <div className="composer-file-review">
      <label>File caption<textarea rows={2} aria-label="File caption" value={draft.caption} readOnly={!editable}
        onChange={e => update({ caption: e.target.value })} /></label>
      {!validCaption(draft.caption) && <span role="status">Caption is too long (up to 1,024 characters).</span>}
      {(access.asFile || draft.asFile) && <label><input type="checkbox" checked={draft.asFile} disabled={!editable} onChange={e => update({ asFile: e.target.checked })} /> Send as file</label>}
      {(access.spoiler || draft.spoiler) && <label><input type="checkbox" checked={draft.spoiler} disabled={!editable} onChange={e => update({ spoiler: e.target.checked })} /> Spoiler</label>}
      {tooLarge && <span role="status">This file exceeds the current account limit.</span>}
      <button type="button" disabled={!send} onClick={() => mutate(stage, "send")}>Send file</button>
    </div>}
    {canCancel && <button type="button" disabled={busy} onClick={() => mutate(stage, "cancel")}>{stage.state === "ready" ? "Remove unsent file" : "Cancel file preparation"}</button>}
  </li>;
}

export function FileComposer({ client, chat }: { client: CoreClient; chat: ChatSummary }) {
  const [inventory, setInventory] = useState<Inventory | null>(null);
  const [page, setPage] = useState<FileStagePage | null>(null);
  const [open, setOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [, changed] = useState(0);
  const cursor = useRef<string | null>(null);
  const { thread } = chat;
  async function read(active: () => boolean) {
    const [inv, next] = await Promise.all([client.inventory({}), client.listFileStages({ thread, requestKey: null, cursor: cursor.current })]);
    if (!isFileStagePage(next, thread, null)) throw { code: "unavailable" };
    const items = [...next.items];
    for (const intent of pendingFileStarts(client, thread)) {
      const recovered = await client.listFileStages({ thread, requestKey: intent.requestKey, cursor: null });
      if (!isFileStagePage(recovered, thread, intent.requestKey) || recovered.sessionId !== next.sessionId) throw { code: "unavailable" };
      for (const s of recovered.items) if (!items.some(item => item.stageId === s.stageId)) items.unshift(s);
    }
    if (active()) {
      learnFilePage(client, { ...next, items });
      setInventory(inv); setPage({ ...next, items }); changed(n => n + 1);
    }
  }
  const reads = useAccountReads(client, thread.accountId, read);
  const access = fileAccess(inventory, thread);
  const starts = pendingFileStarts(client, thread);
  const hasActive = page?.items.some(fileInProgress) ?? false;
  const choose = access.canChoose && access.canSend && chat.canPost !== false && page !== null && page.remainingRequestKeys > 0
    && starts.length === 0 && !hasActive && reads.subscribed && !reads.busy && !reads.error;
  function begin() {
    if (!choose || !page) return;
    setNotice(null); setOpen(true);
    reads.run(async active => {
      try { await selectFile(client, thread, page); }
      catch (cause) { if (active()) setNotice(failureText(cause)); }
      finally { if (active()) { cursor.current = null; changed(n => n + 1); reads.reload(); } }
    });
  }
  function mutate(stage: FileStage, kind: "send" | "cancel") {
    setNotice(null);
    reads.run(async active => {
      try {
        const next = await changeFile(client, stage, kind, fileDraft(client, stage));
        if (active()) setPage(p => p && p.sessionId === next.sessionId ? { ...p, items: p.items.map(s => s.stageId === next.stageId ? next : s) } : p);
      } catch (cause) { if (active()) setNotice(failureText(cause)); }
      finally { if (active()) { changed(n => n + 1); reads.reload(); } }
    });
  }
  function move(next: string | null) { cursor.current = next; reads.reload(); }
  return <div className="composer-files" data-thread={threadId(thread)}>
    <div className="composer-file-toolbar">
      <button type="button" disabled={!choose} onClick={begin}>Choose file</button>
      <button type="button" className="composer-disclosure" aria-expanded={open} onClick={() => setOpen(!open)}>Files{page?.items.length ? ` · ${page.items.length}` : ""} {open ? "▴" : "▾"}</button>
    </div>
    {open && <div className="composer-file-panel" aria-label="Files for this conversation">
      <p className="composer-file-meta">Choose a file, review it, then send. Sent copies stay in app storage; the current limit is 16 retained files.</p>
      {!access.canChoose && <p className="composer-status">File selection is available in the desktop app.</p>}
      {page && page.remainingRequestKeys === 0 && <p className="composer-status">The selection limit for this app session was reached.</p>}
      {starts.map(intent => <div key={intent.requestKey} className="composer-warning" role="status">
        {intent.pending ? "Waiting for the selection request…" : "Selection could not be confirmed. Reload to find the original request; choosing again may open another dialog."}
        {page && intent.sessionId !== page.sessionId && !intent.pending && <button type="button" onClick={() => { reviewOldFileIntent(client, intent, page.sessionId); changed(n => n + 1); }}>Review complete; allow a new selection</button>}
      </div>)}
      <ul className="composer-file-list">{page?.items.map(stage => <StageRow key={JSON.stringify([stage.sessionId, stage.stageId])}
        client={client} stage={stage} access={access} restricted={chat.canPost === false} busy={reads.busy || !reads.subscribed} mutate={mutate} />)}</ul>
      {page?.items.length === 0 && !starts.length && <p className="composer-status">No prepared files for this conversation.</p>}
      <div className="composer-file-toolbar">
        <button type="button" disabled={reads.busy} onClick={reads.reload}>Reload file status</button>
        {cursor.current !== null && <button type="button" disabled={reads.busy} onClick={() => move(null)}>Newest files</button>}
        {page?.nextCursor && <button type="button" disabled={reads.busy} onClick={() => move(page.nextCursor)}>Older files</button>}
      </div>
      {(reads.error || notice) && <p className="composer-status" role="status">{reads.error || notice}</p>}
    </div>}
  </div>;
}
