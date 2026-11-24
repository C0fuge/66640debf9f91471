import { useCallback, useLayoutEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import type { AccountSettings, AccountSummary, CoreChange, CoreClient, JsonObject, PluginSummary } from "../../core";
import { useSnapshot } from "../useSnapshot";
import { AccountDialog } from "./AccountDialog";
import { SettingsField } from "./SettingsFields";
import { emptySettings, fieldsFor, settingsPayload, type Edits } from "./settings";

const usable = (plugin: PluginSummary) => plugin.phase !== "unavailable" && plugin.phase !== "incompatible";

function SettingsForm({ plugin, settings, submit, submitLabel, children, success, restart, readError = false }: {
  plugin: PluginSummary; settings: AccountSettings; submit: (values: JsonObject) => Promise<string | void>;
  submitLabel: string; children?: ReactNode; success: (id?: string) => void; restart?: boolean; readError?: boolean;
}) {
  const [edits, setEdits] = useState<Edits>(() => new Map());
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const active = useRef(false);
  const inFlight = useRef(false);
  useLayoutEffect(() => {
    active.current = true;
    return () => { active.current = false; };
  }, []);
  const fields = fieldsFor(plugin.settingsSchema);
  async function save(event: FormEvent) {
    event.preventDefault();
    if (inFlight.current || readError) return;
    let values: JsonObject;
    try { values = settingsPayload(fields, settings, edits); }
    catch (err) {
      setError(err instanceof Error ? err.message : "Check the settings.");
      return;
    }
    inFlight.current = true;
    setPending(true);
    setError("");
    try {
      const result = await submit(values);
      if (active.current) success(result ?? undefined);
    } catch {
      if (active.current) setError("The core could not apply these settings. Check the fields and account status.");
    } finally {
      inFlight.current = false;
      if (active.current) setPending(false);
    }
  }
  return <form onSubmit={save} autoComplete="off">
    <fieldset disabled={pending}>
      {children}
      {fields.map(field => <SettingsField key={field.name} field={field} settings={settings} edits={edits}
        edit={(name, value) => setEdits(old => new Map(old).set(name, value))} />)}
    </fieldset>
    {error && <p role="alert" className="account-error">{error}</p>}
    <footer>{restart && <p>Saving restarts this account.</p>}
      <button className="account-primary" disabled={pending || readError}>{pending ? "Applying…" : submitLabel}</button>
    </footer>
  </form>;
}

export function AddAccount({ client, plugins, close, added }: {
  client: CoreClient; plugins: PluginSummary[]; close: () => void; added: (id: string) => void;
}) {
  const available = plugins.filter(usable);
  const [selected, setSelected] = useState(available[0]?.pluginId ?? "");
  const [label, setLabel] = useState("");
  const plugin = available.find(p => p.pluginId === selected) ?? available[0];
  return <AccountDialog title="Add account" close={close}>
    {plugin ? <SettingsForm key={plugin.pluginId} plugin={plugin} settings={emptySettings} submitLabel="Add account"
      submit={async settings => (await client.addAccount({ pluginId: plugin.pluginId, label: label.trim() || null, settings })).accountId}
      success={id => { if (id !== undefined) added(id); }}>
      <label className="account-field">Platform<select value={plugin.pluginId} onChange={e => setSelected(e.target.value)}>
        {available.map(p => <option key={p.pluginId} value={p.pluginId}>{p.name}</option>)}
      </select></label>
      <label className="account-field">Account label<input value={label} onChange={e => setLabel(e.target.value)} /></label>
    </SettingsForm> : <p>No platforms are available to add.</p>}
  </AccountDialog>;
}

export function EditSettings({ client, account, plugin, close }: {
  client: CoreClient; account: AccountSummary; plugin: PluginSummary; close: () => void;
}) {
  const accountId = account.accountId;
  const read = useCallback(() => client.getSettings({ accountId }), [client, accountId]);
  const relevant = useCallback((event: CoreChange) => event.kind === "reset" || event.kind === "account" && event.accountId === accountId, [accountId]);
  const { data, error, retry } = useSnapshot(client, read, relevant);
  const restart = account.phase !== "stopped";
  return <AccountDialog title="Account settings" subtitle={account.label || accountId} close={close}>
    {error && <p role="alert" className="account-error">Could not read account settings. <button onClick={retry}>Retry</button></p>}
    {data && usable(plugin) ? <SettingsForm plugin={plugin} settings={data} restart={restart} readError={error} submitLabel={restart ? "Save and restart" : "Save settings"}
      submit={async values => { await client.saveSettings({ accountId, values }); }} success={close} />
      : !usable(plugin) ? <p>The platform is unavailable.</p> : !error && <p role="status">Loading settings…</p>}
  </AccountDialog>;
}
