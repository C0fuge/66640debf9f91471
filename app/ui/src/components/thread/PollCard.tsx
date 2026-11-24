import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { CoreClient, Message, PluginSummary, PollSnapshot, ThreadKey } from "../../core";
import { RichText } from "./RichText";
import { pollPermissions, pollSession } from "./polls";

export type PollSelection = { thread: ThreadKey; message: Message; poll: PollSnapshot };
export function PollSummary({ thread, message, disabled, select }: {
  thread: ThreadKey; message: Message; disabled: boolean; select: (value: PollSelection) => void;
}) {
  const poll = message.poll;
  if (!poll || message.deleted || message.messageId.startsWith("local:") || ["sending", "failed"].includes(message.delivery ?? "")) return null;
  return <div className="poll-card">
    <strong dir="auto">{poll.question}</strong>
    <p>{poll.quiz ? "Quiz" : poll.multiple ? "Multiple choice poll" : "Poll"} · {poll.anonymous ? "Anonymous voting" : "Public voting"}{poll.closed && " · Closed"}</p>
    <ul>{poll.options.map(option => <li key={option.id} dir="auto">{option.text}{option.votes !== null && ` — ${option.votes}`}{option.chosen === true && " · Your choice"}</li>)}</ul>
    <p>{poll.totalVoters === null ? "Voter count unknown" : `${poll.totalVoters} voters`}</p>
    <button type="button" disabled={disabled} onClick={() => select({ thread: { ...thread, topicId: message.topicId }, message, poll })}>Open poll</button>
  </div>;
}

export function PollPanel({ selection, client, plugin, close }: {
  selection: PollSelection; client: CoreClient; plugin?: PluginSummary | null; close: () => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const heading = useId();
  const mounted = useRef(true);
  const session = useMemo(() => pollSession(client, selection.thread, selection.message.messageId, selection.poll), [client, selection]);
  const [poll, setPoll] = useState(session.poll);
  const [choices, setChoices] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("Refresh this poll to get current state before voting or closing.");
  const permissions = pollPermissions(client, plugin, selection.message, poll);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useLayoutEffect(() => { const el = dialog.current!; el.showModal(); return () => el.close(); }, []);
  async function perform(kind: "vote" | "close" | "refresh", ids = choices) {
    if (session.busy) return;
    setBusy(true);
    const result = await session.perform(kind, ids);
    if (!mounted.current) return;
    setPoll(session.poll); setBusy(false); setNotice(result.error);
    // Retain tentative choices after uncertainty; a reported snapshot is separate evidence.
    if (result.ok && !session.needsRefresh) setChoices([]);
  }
  const blocked = busy || session.needsRefresh;
  return <dialog ref={dialog} className="message-action-dialog poll-panel" aria-labelledby={heading}
    onCancel={event => { event.preventDefault(); if (!busy) close(); }}>
    <h2 id={heading} dir="auto">{poll.question}</h2>
    <p>{poll.quiz ? "Quiz" : "Poll"} · {poll.anonymous ? "Anonymous voting" : "Public voting"}{poll.closed && " · Closed"}</p>
    {poll.options.some(option => option.chosen === null) && <p>Your current choice is unknown.</p>}
    <fieldset disabled={blocked || !permissions.vote}><legend>Choose answers to submit</legend>
      {poll.options.map(option => <label key={option.id}>
        <input type={poll.multiple ? "checkbox" : "radio"} name={heading} checked={choices.includes(option.id)}
          onChange={() => setChoices(current => poll.multiple ? current.includes(option.id) ? current.filter(id => id !== option.id) : [...current, option.id] : [option.id])} />
        <span dir="auto">{option.text}</span>{option.votes !== null && <span> — {option.votes}</span>}
        {option.chosen === true && <span> · Your choice</span>}{poll.quiz && (poll.closed || poll.options.some(answer => answer.chosen === true)) && option.correct === true && <span> · Correct</span>}
      </label>)}
    </fieldset>
    {poll.quiz && (poll.closed || poll.options.some(option => option.chosen === true)) && poll.solution &&
      <div><RichText text={poll.solution} entities={poll.solutionEntities} /></div>}
    {notice && <p role="status">{notice}</p>}
    <div className="message-action-buttons">
      <button type="button" disabled={busy} onClick={close}>Close panel</button>
      <button type="button" disabled={busy || !permissions.refresh} onClick={() => void perform("refresh")}>Refresh poll</button>
      <button type="button" disabled={blocked || !permissions.vote || choices.length === 0} onClick={() => void perform("vote")}>Submit vote</button>
      {permissions.retract && <button type="button" disabled={blocked} onClick={() => void perform("vote", [])}>Retract vote</button>}
      {permissions.close && <button type="button" disabled={blocked} onClick={() => void perform("close")}>Close poll</button>}
    </div>
  </dialog>;
}
