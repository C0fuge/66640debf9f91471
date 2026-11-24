import { useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import { QRCodeSVG } from "qrcode.react";
import type { AccountSummary, AuthStep, CoreClient, PluginSummary } from "../../core";
import { AccountDialog } from "./AccountDialog";
import { expiry, initialAuth, openAuthSession, type AuthAction } from "./authSession";

const methodLabels = new Map([
  ["phone_code", "Phone code"], ["password_2fa", "Two-step password"], ["qr", "QR code"],
  ["email_code", "Email code"], ["token", "Token"], ["oauth", "OAuth"], ["username_password", "Username and password"],
]);
const inputs = new Map([
  ["phone", ["Phone number", "tel"]], ["code", ["Sign-in code", "text"]], ["email_code", ["Email code", "text"]],
  ["password", ["Password", "password"]], ["email", ["Email address", "email"]], ["text", ["Response", "text"]],
]);

export function supportsSignIn(plugin: PluginSummary | undefined) {
  const interactive = plugin?.capabilities?.["auth.interactive"];
  return !!interactive && !!plugin?.authMethods.some(m => m !== "none");
}

function Credential({ step, disabled, send }: { step: AuthStep; disabled: boolean; send: (value: string) => void }) {
  const [value, setValue] = useState("");
  const id = useId();
  const [label, type] = inputs.get(step.kind)!;
  return <form autoComplete="off" onSubmit={e => {
    e.preventDefault();
    if (disabled || !value) return;
    send(value);
    setValue("");
  }}>
    <label className="account-field" htmlFor={id}>{label}
      <input id={id} type={type} value={value} placeholder={step.placeholder ?? undefined}
        autoFocus autoComplete="off" autoCapitalize="none" spellCheck={false} required
        minLength={step.minLength ?? undefined} maxLength={step.maxLength ?? undefined}
        disabled={disabled} onChange={e => setValue(e.target.value)} />
    </label>
    <button className="account-primary" disabled={disabled || !value}>Continue</button>
  </form>;
}

function useExpired(at: number | null) {
  const [, tick] = useState(0);
  const remaining = at === null ? null : at - Date.now();
  useEffect(() => {
    if (remaining === null || remaining <= 0) return;
    const timer = window.setTimeout(() => tick(n => n + 1), Math.min(remaining, 2147483647));
    return () => window.clearTimeout(timer);
  }, [remaining]);
  return remaining !== null && remaining <= 0;
}

function AuthPanel({ client, accountId, methods, retry, close }: {
  client: CoreClient; accountId: string; methods: string[]; retry: () => void; close: () => void;
}) {
  const [view, setView] = useState(initialAuth);
  const session = useRef<ReturnType<typeof openAuthSession> | null>(null);
  const [chosen, choose] = useState("");
  const [startedMethod, setStartedMethod] = useState("");
  const method = methods.includes(chosen) ? chosen : methods.length === 1 ? methods[0] : "";
  useLayoutEffect(() => {
    const current = openAuthSession(client, accountId, setView);
    session.current = current;
    return () => { current.close(); session.current = null; };
  }, [client, accountId]);
  const step = view.step;
  const done = step?.kind === "done";
  useEffect(() => { if (done) close(); }, [done, close]);
  const at = expiry(step);
  const expired = useExpired(at);
  function act(action: AuthAction) {
    if (action.kind === "begin" || action.kind === "resend") setStartedMethod(action.method);
    void session.current?.act(step, action);
  }
  const resend = step && !done && (step.canResend || expired && step.kind === "qr");
  const methodPicker = <label className="account-field">Sign-in method<select disabled={view.busy} value={method}
    onChange={e => choose(e.target.value)}>
    <option value="" disabled>Choose a method</option>
    {methods.map(m => <option key={m} value={m}>{methodLabels.get(m) ?? m}</option>)}
  </select></label>;
  return <>
    {view.error && <p className="account-error" role="alert">{view.error} {!view.ready && <button onClick={retry}>Retry</button>}</p>}
    {!view.ready && !view.error && <p role="status">Loading sign-in…</p>}
    {view.ready && !step && <>
      {view.awaiting && <p role="status">Waiting for the next sign-in step…</p>}
      {methodPicker}
      <button className="account-primary" disabled={!method || view.busy}
        onClick={() => act({ kind: "begin", method })}>{view.awaiting ? "Restart sign-in" : "Start sign-in"}</button>
    </>}
    {view.ready && step && !done && <>
      {step.title && <h3>{step.title}</h3>}
      {step.hint && <p>{step.hint}</p>}
      {step.error && <p role="alert" className="account-error">{step.error}</p>}
      {at !== null && <p role="status">{expired ? "This step has expired." : <>Expires at <time dateTime={new Date(at).toISOString()}>{new Date(at).toLocaleTimeString()}</time></>}</p>}
      {inputs.has(step.kind) ? <Credential key={JSON.stringify([accountId, step.flow, step.stepId, step.kind])} step={step}
        disabled={view.busy || view.awaiting || expired} send={value => act({ kind: "submit", value })} />
        : step.kind === "qr" ? !expired && (step.data && new TextEncoder().encode(step.data).length <= 2953
          ? <QRCodeSVG value={step.data} level="L" size={208} marginSize={4} className="account-qr" role="img" title="Scan to sign in" />
          : <p role="alert">The platform did not provide a usable QR code.</p>)
        : step.kind === "info" ? !step.hint && !step.title && <p>Waiting for confirmation from the platform.</p>
        : <p role="alert">This version cannot display the “{step.kind}” sign-in step.</p>}
      {view.awaiting && <p role="status">Waiting for the next sign-in step…</p>}
      {resend && !methods.includes(startedMethod) && methods.length > 1 && methodPicker}
      <div className="account-auth-actions">{resend && <button disabled={view.busy || !method} onClick={() => act({ kind: "resend", method })}>
        {step.kind === "qr" ? "Get new QR code" : "Resend code"}
      </button>}
      <button disabled={view.busy} onClick={() => act({ kind: "cancel" })}>Cancel sign-in</button></div>
    </>}
  </>;
}

export function SignIn({ client, account, plugin, close }: {
  client: CoreClient; account: AccountSummary; plugin: PluginSummary; close: () => void;
}) {
  const [attempt, setAttempt] = useState(0);
  const connected = account.state === "connected";
  useEffect(() => { if (connected) close(); }, [connected, close]);
  return <AccountDialog title="Sign in" subtitle={account.label || account.accountId} close={close}>
    {supportsSignIn(plugin) && account.phase === "running" ? <AuthPanel key={attempt} client={client} accountId={account.accountId}
      methods={plugin.authMethods.filter(m => m !== "none")} retry={() => setAttempt(n => n + 1)} close={close} />
      : <p>Interactive sign-in is unavailable for this account.</p>}
  </AccountDialog>;
}
