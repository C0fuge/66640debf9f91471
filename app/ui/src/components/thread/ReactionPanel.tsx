import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { AccountSummary, CoreClient, Message, OrdinaryReactionKey, PluginSummary, ReactionItem, ThreadKey } from "../../core";
import { isReactionItem, isReactionKey, reactionKeyId, reactionKeyLabel, reactionPermissions, reactionSession, type ReactionPlan } from "./reactions";
import { messageDate } from "./history";

export interface ReactionSelection { thread: ThreadKey; message: Message }
function Item({ item }: { item: ReactionItem }) {
  return <span><bdi>{reactionKeyLabel(item.key)}</bdi> · {item.count ?? "count unknown"} · {item.mine === null ? "your choice unknown" : item.mine ? "your choice" : "not your choice"}</span>;
}
export function ReactionSummary({ client, thread, message, plugin, disabled, select }: {
  client: CoreClient; thread: ThreadKey; message: Message; plugin?: PluginSummary | null; disabled: boolean; select: (selection: ReactionSelection) => void;
}) {
  if (message.deleted || message.messageId.startsWith("local:") || ["sending", "failed"].includes(message.delivery ?? "")) return null;
  const cached = message.reactions;
  const cap = plugin?.capabilities;
  if (!cached && !(cap?.["reactions.set"] || cap?.["reactions.who"])) return null;
  const shown = cached?.items.filter(isReactionItem).slice(0, 8) ?? [];
  return <div className="reaction-summary">
    {cached && <>
      <span className="thread-secondary">Cached reactions · partial view</span>
      <ul>{shown.map(item => <li key={reactionKeyId(item.key)}><Item item={item} /></li>)}</ul>
      {cached.items.length === 0 && <span className="thread-secondary">Cached snapshot reports no ordinary entries.</span>}
    </>}
    {typeof client.refreshMessageReactions === "function" && <button type="button" disabled={disabled}
      onClick={() => select({ thread: { ...thread, topicId: message.topicId }, message })}>View/refresh reactions</button>}
  </div>;
}

export function ReactionPanel({ selection, client, plugin, account, unavailable, close }: {
  selection: ReactionSelection; client: CoreClient; plugin?: PluginSummary | null; account?: AccountSummary | null; unavailable: boolean; close: () => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null), heading = useId();
  const [, render] = useState(0);
  const redraw = () => render(value => value + 1);
  const permissions = reactionPermissions(client, plugin, account, selection.thread,
    unavailable ? { ...selection.message, deleted: true } : selection.message);
  const live = useRef(permissions);
  useLayoutEffect(() => { live.current = permissions; });
  const session = useMemo(() => reactionSession(client, selection.thread, selection.message.messageId,
    selection.message.reactions ?? null, () => live.current), [client, selection]);
  const mounted = useRef(true);
  const [choices, setChoices] = useState<OrdinaryReactionKey[]>([]);
  const [literal, setLiteral] = useState("");
  const [notice, setNotice] = useState("Refresh explicitly to obtain a scoped view before replacing choices or loading reactors.");
  const [confirmation, setConfirmation] = useState<ReactionPlan | null>(null);
  useLayoutEffect(() => { const element = dialog.current!; element.showModal(); return () => element.close(); }, []);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; session.invalidate(); }; }, [session]);
  useEffect(() => { if (!permissions.refresh) { session.invalidate(); setConfirmation(null); redraw(); } }, [permissions.refresh, session]);
  const expiresAt = session.context?.expiresAt;
  useEffect(() => {
    if (!expiresAt) return;
    const timer = setInterval(() => {
      if (session.context && !session.fresh) {
        session.invalidate(); setConfirmation(null); setNotice("This reaction view expired. Refresh explicitly."); clearInterval(timer);
      }
      redraw(); // Date.now is evaluated by the session on every redraw/action.
    }, 1000);
    return () => clearInterval(timer);
  }, [expiresAt, session]);

  async function refresh() {
    setConfirmation(null); setNotice("");
    const pending = session.refresh(); redraw();
    const result = await pending;
    if (!mounted.current || result.stale) return;
    if (result.ok) { setChoices([]); setLiteral(""); }
    setNotice(result.error); redraw();
  }
  async function replace(plan: ReactionPlan) {
    setConfirmation(null); setNotice("");
    const pending = session.replace(plan); redraw();
    const result = await pending;
    if (!mounted.current || result.stale) return;
    setNotice(result.error); redraw();
  }
  async function loadMore() {
    setNotice("");
    const pending = session.loadMore(); redraw();
    const result = await pending;
    if (!mounted.current || result.stale) return;
    setNotice(result.error); redraw();
  }
  function review(keys: OrdinaryReactionKey[]) {
    const plan = session.plan(keys);
    if (typeof plan === "string") setNotice(plan);
    else setConfirmation(plan);
  }
  function toggle(key: OrdinaryReactionKey) {
    setChoices(current => current.some(value => reactionKeyId(value) === reactionKeyId(key))
      ? current.filter(value => reactionKeyId(value) !== reactionKeyId(key)) : [...current, key]);
  }
  const context = session.context, policy = context?.availability ?? null, snapshot = session.snapshot;
  const maximum = Math.min(16, permissions.declaredMax ?? 16, policy?.maxPerMessage === null || policy?.maxPerMessage === undefined
    ? 16 : Number(BigInt(policy.maxPerMessage) < 16n ? BigInt(policy.maxPerMessage) : 16n));
  const canChoose = session.canSet && !session.busy && !confirmation;
  const preview = session.plan(choices), clear = session.plan([]);
  const filterKeys = new Map([...(context?.snapshot?.items.map(item => item.key) ?? []), ...(policy?.keys ?? [])].map(key => [reactionKeyId(key), key]));
  const currentFilter = session.filter === null ? "all" : reactionKeyId(session.filter);
  const end = session.started && session.nextCursor === null;
  return <dialog ref={dialog} className="message-action-dialog reaction-panel" aria-labelledby={heading}
    onCancel={event => { event.preventDefault(); close(); }}>
    <h2 id={heading}>Ordinary reactions</h2>
    {!unavailable && selection.message.text && <p className="reaction-caption" dir="auto">{selection.message.text}</p>}
    {unavailable && <p>This message is no longer available for reaction operations.</p>}
    <p>{session.reported ? "Reported ordinary reactions" : "Cached or previous view · refresh required"}</p>
    {snapshot === null ? <p>Reaction state is unknown. Missing state is not a known-empty selection.</p>
      : snapshot.items.length === 0 ? <p>The snapshot reports no ordinary reactions.</p>
        : <ul className="reaction-state">{snapshot.items.map(item => <li key={reactionKeyId(item.key)}><Item item={item} /></li>)}</ul>}
    {context && <p>{session.fresh ? `Scoped view expires at ${context.expiresAt === null ? "an unknown time" : messageDate(context.expiresAt)?.iso ?? "an unknown time"}.` : "This reaction view expired. Refresh explicitly."}</p>}
    {policy && <p>Account limit: {policy.maxPerMessage ?? "unknown"}. Plugin limit: {permissions.declaredMax ?? "unavailable"}. This panel supports at most 16 selected choices.</p>}
    {policy === null && <p>The reaction policy is unknown; replacement controls require an explicit successful refresh.</p>}
    {!confirmation && <>
      <fieldset disabled={!canChoose}><legend>Build the complete new ordinary set</legend>
        {policy?.keys !== null && policy?.keys !== undefined && policy.keys.map(key => {
          const selected = choices.some(value => reactionKeyId(value) === reactionKeyId(key));
          return <label key={reactionKeyId(key)}><input type="checkbox" checked={selected}
            disabled={!canChoose || !selected && choices.length >= maximum || key.kind === "custom" && !permissions.custom} onChange={() => toggle(key)} />
            <bdi>{reactionKeyLabel(key)}</bdi>{key.kind === "custom" && !permissions.custom && <span> · custom selection unavailable</span>}</label>;
        })}
        {policy?.keys === null && <div className="reaction-literal">
          <label>Literal emoji sequence<input value={literal} onChange={event => setLiteral(event.target.value)} maxLength={128} /></label>
          <button type="button" disabled={!canChoose || choices.length >= maximum} onClick={() => {
            const key: OrdinaryReactionKey = { kind: "emoji", emoji: literal };
            if (!isReactionKey(key)) setNotice("Enter a nonempty emoji sequence of at most 128 UTF-8 bytes.");
            else if (choices.some(value => reactionKeyId(value) === reactionKeyId(key))) setNotice("That exact sequence is already selected.");
            else { setChoices(current => [...current, key]); setLiteral(""); }
          }}>Add to complete set</button>
          {choices.map(key => <button type="button" key={reactionKeyId(key)} onClick={() => toggle(key)}>Remove <bdi>{reactionKeyLabel(key)}</bdi></button>)}
        </div>}
        <p>Only listed custom emoji can be selected here. Selecting a checkbox sends nothing.</p>
      </fieldset>
      <div className="message-action-buttons">
        <button type="button" disabled={typeof preview === "string"} onClick={() => review(choices)}>Review complete replacement</button>
        <button type="button" disabled={typeof clear === "string"} onClick={() => review([])}>Clear ordinary set</button>
      </div>
    </>}
    {confirmation && <section className="reaction-confirmation" aria-label="Complete-set confirmation">
      <h3>Replace all current ordinary reaction choices</h3>
      <p>Review every selected and removed ordinary choice before confirming.</p>
      <strong>New complete set</strong>
      {confirmation.keys.length ? <ul>{confirmation.keys.map(key => <li key={reactionKeyId(key)}><bdi>{reactionKeyLabel(key)}</bdi></li>)}</ul> : <p>No ordinary choices will remain.</p>}
      <strong>Choices that will be removed</strong>
      {confirmation.removed.length ? <ul>{confirmation.removed.map(key => <li key={reactionKeyId(key)}><bdi>{reactionKeyLabel(key)}</bdi></li>)}</ul> : <p>No current ordinary choices will be removed.</p>}
      <div className="message-action-buttons"><button type="button" onClick={() => setConfirmation(null)}>Back to choices</button>
        <button type="button" disabled={!session.canSet || session.busy} onClick={() => void replace(confirmation)}>Confirm replacement</button></div>
    </section>}
    <section className="reaction-reactors" aria-label="Reactor metadata">
      <h3>Reactor metadata</h3>
      <label>Reaction filter<select value={currentFilter} disabled={!session.canWho || !!confirmation} onChange={event => {
        const key = event.target.value === "all" ? null : filterKeys.get(event.target.value);
        if (key !== undefined) { session.chooseFilter(key); redraw(); }
      }}><option value="all">All ordinary reactions</option>
        {[...filterKeys].map(([id, key]) => <option key={id} value={id}>{reactionKeyLabel(key)}</option>)}
      </select></label>
      <ul>{session.reactors.map((item, index) => <li key={index}><span dir="auto">{item.name || `User ${item.userId}`}</span> · <bdi>{reactionKeyLabel(item.key)}</bdi>
        {item.at !== null && <span> · {messageDate(item.at)?.iso ?? "Unknown time"}</span>}</li>)}</ul>
      {session.viewLimit && !session.canLoadMore && !end && <p>Reactor view limit reached. Refresh explicitly for a fresh view; more results may exist.</p>}
      <button type="button" disabled={!session.canLoadMore || !!confirmation}
        onClick={() => void loadMore()}>{session.started ? "Load more reactors" : "Load reactors"}</button>
    </section>
    {notice && <p role="status">{notice}</p>}
    <div className="message-action-buttons"><button type="button" onClick={close}>Close panel</button>
      <button type="button" disabled={!permissions.refresh || session.busy || !!confirmation} onClick={() => void refresh()}>Refresh reactions</button></div>
  </dialog>;
}
