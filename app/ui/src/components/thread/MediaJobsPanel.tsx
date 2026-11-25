import { useId, useLayoutEffect, useRef, useState } from "react";
import type { AccountSummary, CoreClient, MediaAccess, MediaJob, MediaJobPage, Message, PluginSummary, ThreadKey } from "../../core";
import { attachmentBytes } from "./AttachmentSummary";
import { outcomeUnknown } from "./actions";
import { acknowledgeOldMediaIntent, cancelMediaReceipt, isMediaJobPage, learnMediaPage, mediaCancelNeedsRead, mediaJobError,
  mediaPermissions, observedMediaScopes, pendingMediaIntents, startMediaIntent, terminalMediaStates, type MediaIntent } from "./mediaJobs";
import { failureText, threadId } from "./shared";
import { useAccountReads } from "./useAccountReads";

export interface MediaSelection { thread: ThreadKey; message: Message | null }
interface Props {
  selection: MediaSelection; client: CoreClient; plugin?: PluginSummary | null; account?: AccountSummary | null;
  mediaAccess?: MediaAccess; changed: boolean; close: () => void;
}

export function AttachmentActions({ thread, message, client, plugin, mediaAccess, disabled, select }: {
  thread: ThreadKey; message: Message; client: CoreClient; plugin?: PluginSummary | null; mediaAccess?: MediaAccess;
  disabled: boolean; select: (selection: MediaSelection) => void;
}) {
  const exact = { ...thread, topicId: message.topicId };
  const cap = plugin?.capabilities?.["media.download"];
  if (typeof client.listMediaJobs !== "function" || message.deleted || message.messageId.startsWith("local:")
    || ["sending", "failed"].includes(message.delivery ?? "") || !(message.attachment || message.mediaKind)
    || !(cap === true || typeof cap === "object" && cap !== null || mediaAccess?.saveCopy || pendingMediaIntents(client, exact, message.messageId).length)) return null;
  return <button type="button" className="thread-attachment-action" disabled={disabled}
    onClick={() => select({ thread: exact, message })}>Attachment actions</button>;
}

export function MediaJobsPanel(props: Props) {
  const [scope, setScope] = useState(props.selection.thread);
  return <MediaJobsDialog key={threadId(scope)} {...props} selection={{ ...props.selection, thread: scope }}
    scopes={observedMediaScopes(props.client, props.selection.thread)} chooseScope={setScope} />;
}

function JobRow({ job, client, disabled, cancel, check }: {
  job: MediaJob; client: CoreClient; disabled: boolean; cancel: () => void; check: () => void;
}) {
  const done = attachmentBytes(job.doneBytes), total = attachmentBytes(job.totalBytes);
  const determinate = done !== null && total !== null && BigInt(job.totalBytes!) > 0n && BigInt(job.doneBytes!) <= BigInt(job.totalBytes!);
  const progress = determinate ? Number(BigInt(job.doneBytes!) * 1000n / BigInt(job.totalBytes!)) : undefined;
  const labels: Record<MediaJob["state"], string> = { queued: "Queued", running: job.kind === "download" ? "Downloading" : "Saving copy",
    choosing: "Choosing destination", committing: "Finishing copy", cancelling: "Cancellation requested; waiting for completion",
    succeeded: job.kind === "download" ? "Download completed" : "Saved a new copy", failed: "Failed", cancelled: "Cancelled", unknown: "Outcome unknown" };
  const error = mediaJobError(job);
  return <li className="media-job" data-job-id={job.id}>
    <strong>{job.kind === "download" ? "Download" : "Save copy"} · Message <span dir="auto">{job.messageId}</span></strong>
    <span role="status">{labels[job.state]}</span>
    {(done !== null || total !== null) && <span>{done ?? "Progress unknown"}{total !== null && ` / ${total}`}</span>}
    {!terminalMediaStates.has(job.state) && <progress max={1000} value={progress} aria-label={`${job.kind === "download" ? "Download" : "Save copy"} progress`} />}
    {error && <p>{error}</p>}
    {job.state === "unknown" && !error && <p>The result is unknown. A saved copy may already exist; review before another action.</p>}
    <div className="message-action-buttons">
      <button type="button" disabled={disabled} onClick={check}>Check job</button>
      {!terminalMediaStates.has(job.state) && <button type="button" disabled={disabled || job.cancelRequested || mediaCancelNeedsRead(client, job)} onClick={cancel}>Cancel job</button>}
    </div>
  </li>;
}

function MediaJobsDialog({ selection, client, plugin, account, mediaAccess, changed, close, scopes, chooseScope }: Props & {
  scopes: ThreadKey[]; chooseScope: (scope: ThreadKey) => void;
}) {
  const { thread, message } = selection;
  const dialog = useRef<HTMLDialogElement>(null), heading = useId();
  const cursor = useRef<string | null>(null);
  const focus = useRef<MediaJob | null>(null);
  const [page, setPage] = useState<MediaJobPage | null>(null);
  const [focused, setFocused] = useState<MediaJob | null>(null);
  const [notice, setNotice] = useState("");

  async function lookup(requestKey: string, active: () => boolean) {
    const next = await client.listMediaJobs({ thread, cursor: null, requestKey });
    if (!active()) return null;
    if (!isMediaJobPage(next, thread, requestKey)) throw { code: "unavailable" };
    learnMediaPage(client, thread, next);
    setPage(current => current?.sessionId === next.sessionId ? { ...current, remainingRequestKeys: next.remainingRequestKeys } : { ...next, jobs: [], nextCursor: null });
    if (next.jobs[0]) { focus.current = next.jobs[0]; setFocused(next.jobs[0]); }
    else if (focus.current?.requestKey === requestKey) { focus.current = null; setFocused(null); }
    return next;
  }

  async function read(active: () => boolean) {
    const next = await client.listMediaJobs({ thread, cursor: cursor.current, requestKey: null });
    if (!active()) return;
    if (!isMediaJobPage(next, thread, null)) throw { code: "unavailable" };
    learnMediaPage(client, thread, next);
    setPage(next);
    const current = focus.current;
    if (current) {
      const job = next.jobs.find(job => job.id === current.id);
      if (job) { focus.current = job; setFocused(job); }
      else if (current.sessionId === next.sessionId) await lookup(current.requestKey, active);
      else { focus.current = null; setFocused(null); }
    }
  }
  const reads = useAccountReads(client, thread.accountId, read);
  useLayoutEffect(() => { const element = dialog.current!; element.showModal(); return () => element.close(); }, []);
  const pending = pendingMediaIntents(client, thread, message?.messageId);
  const jobs = [...(focused && !page?.jobs.some(job => job.id === focused.id) ? [focused] : []), ...(page?.jobs ?? [])];
  const permissions = message ? mediaPermissions(client, plugin, account, mediaAccess, message) : { download: false, save: false };
  const blocked = reads.busy || !reads.subscribed || !!reads.error;
  const unresolved = message && pending.length > 0;
  const activeTarget = message && jobs.some(job => job.messageId === message.messageId && !terminalMediaStates.has(job.state));
  const startsBlocked = blocked || !page || page.remainingRequestKeys === 0 || !!unresolved || !!activeTarget || changed;

  function start(kind: "download" | "save") {
    if (!message || !page || startsBlocked) return;
    reads.run(async active => {
      try {
        const job = await startMediaIntent(client, thread, message.messageId, kind, page);
        if (!active()) return;
        focus.current = job; setFocused(job); cursor.current = null;
        setNotice("The job was accepted. Review its reported state below.");
        await read(active);
      } catch (cause) {
        if (!active()) return;
        setNotice(pendingMediaIntents(client, thread, message.messageId).length ?
          "The start result is unknown. Recover this request before another action." : failureText(cause));
      }
    });
  }
  function recover(intent: MediaIntent) {
    reads.run(async active => {
      const next = await lookup(intent.requestKey, active);
      if (!next) return;
      setNotice(next.sessionId !== intent.sessionId ? "The app session changed. An earlier saved copy may exist; review before acknowledging this request."
        : next.jobs.length ? "Recovered the existing job. No operation was restarted."
          : "No receipt has appeared. The original start may still be pending; keep this request and check again.");
    });
  }
  function check(job: MediaJob) {
    reads.run(async active => {
      const next = await lookup(job.requestKey, active);
      if (next) setNotice(next.jobs.length ? "Review the job's reported state." : "This job was not returned in the current app session.");
    });
  }
  function cancel(job: MediaJob) {
    reads.run(async active => {
      try {
        const next = await cancelMediaReceipt(client, job);
        if (!active()) return;
        focus.current = next; setFocused(next);
        setNotice("Review the reported state. A verified completion may win a late cancellation.");
        await read(active);
      } catch (cause) {
        if (active()) setNotice(outcomeUnknown(cause) || mediaCancelNeedsRead(client, job)
          ? "The cancellation result is unknown. Check the job before another cancellation." : failureText(cause));
      }
    });
  }

  return <dialog ref={dialog} className="message-action-dialog media-jobs-panel" aria-labelledby={heading}
    onCancel={event => { event.preventDefault(); close(); }}>
    <h2 id={heading}>{message ? "Attachment actions" : "Attachment jobs"}</h2>
    {!message && scopes.length > 1 && <label>Job scope <select value={threadId(thread)} disabled={reads.busy}
      onChange={event => { const scope = scopes.find(scope => threadId(scope) === event.target.value); if (scope) chooseScope(scope); }}>
      {scopes.map(scope => <option key={threadId(scope)} value={threadId(scope)}>{scope.topicId === null ? "Main chat" : `Topic ${scope.topicId}`}</option>)}
    </select></label>}
    {message?.text && !changed && <p className="media-job-caption" dir="auto">{message.text}</p>}
    {changed && <p role="status">The attachment changed. Close and reopen its actions before starting an operation.</p>}
    {message && <div className="media-job-starts">
      <button type="button" disabled={startsBlocked || !permissions.download} onClick={() => start("download")}>Download attachment</button>
      {mediaAccess?.saveCopy === true && <>
        <p>Save a new copy — existing files will not be replaced. Saving never downloads implicitly; a completed download may later be evicted from the cache.</p>
        <button type="button" disabled={startsBlocked || !permissions.save} onClick={() => start("save")}>Save a new copy</button>
      </>}
    </div>}
    {page?.remainingRequestKeys === 0 && <p>Session request limit reached. Existing jobs can still be checked and cancelled.</p>}
    {pending.map(intent => <div key={intent.requestKey} className="media-job-recovery">
      <p>{intent.kind === "download" ? "Download" : "Save-copy"} start {intent.phase === "starting" ? "is awaiting a receipt" : "has an unknown result"} · Message <span dir="auto">{intent.messageId}</span></p>
      {page && page.sessionId !== intent.sessionId ? <>
        <p>An earlier saved copy may already exist. This previous-session request cannot be recovered here.</p>
        <button type="button" disabled={blocked} onClick={() => { acknowledgeOldMediaIntent(client, intent, page.sessionId); setNotice("Previous-session warning acknowledged. Another operation requires a new explicit action."); }}>Acknowledge previous session</button>
      </> : <button type="button" disabled={blocked} onClick={() => recover(intent)}>Recover request</button>}
    </div>)}
    {(reads.error || notice) && <p role="status">{reads.error || notice}</p>}
    <ul className="media-job-list">{jobs.map(job => <JobRow key={job.id} job={job} client={client} disabled={blocked} cancel={() => cancel(job)} check={() => check(job)} />)}</ul>
    {page && !jobs.length && <p>No jobs in this page.</p>}
    <div className="message-action-buttons">
      <button type="button" onClick={close}>Close panel</button>
      <button type="button" disabled={reads.busy} onClick={() => { cursor.current = null; reads.reload(); }}>Latest jobs</button>
      {page?.nextCursor && <button type="button" disabled={blocked} onClick={() => { cursor.current = page.nextCursor; reads.reload(); }}>Earlier jobs</button>}
    </div>
  </dialog>;
}
