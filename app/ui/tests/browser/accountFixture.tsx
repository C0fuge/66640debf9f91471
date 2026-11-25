import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import type { AccountSettings, AccountSummary, AuthStep, CoreChange, CoreClient, JsonObject, PluginSummary } from "../../src/core";
import "../../src/styles.css";

const plugin: PluginSummary = {
  pluginId: "fixture", name: "Fixture", brandColor: null, phase: "running",
  capabilities: { "auth.interactive": true }, authMethods: ["phone_code", "qr"],
  settingsSchema: { properties: { name: { type: "string", title: "Name" }, token: { type: "string", "x-secret": true } } },
};
let accounts: AccountSummary[] = ["Alpha", "Beta"].map(id => ({
  accountId: id, label: id, pluginId: "fixture", enabled: true, phase: "running", state: "auth_required", reconnecting: false,
}));
const listeners = new Set<(event: CoreChange) => void>();
let settings: AccountSettings = { values: { name: "stored" }, secretsSet: ["token"] };
let saved: JsonObject | undefined;
let begins = 0;
const steps = new Map<string, AuthStep>();
function publish(accountId: string) { listeners.forEach(f => f({ kind: "account", accountId })); }
function step(accountId: string, kind: string, stepId: string) {
  steps.set(accountId, { flow: "sign_in", stepId, kind, title: null, hint: null, error: null, placeholder: null,
    data: kind === "qr" ? "ch-fixture://scan/test" : null, minLength: null, maxLength: null, expiresAt: null, canResend: false });
  publish(accountId);
}
const unused = async () => { throw new Error("Unexpected thread operation"); };
const client: CoreClient = {
  startMediaDownload: unused, startMediaSave: unused, cancelMediaJob: unused,
  listMediaJobs: async () => ({ sessionId: "fixture-session", jobs: [], nextCursor: null, remainingRequestKeys: 4096 }),
  votePoll: unused, closePoll: unused, refreshPoll: unused,
  editText: unused, deleteMessages: unused, pinMessage: unused, unpinMessage: unused,
  history: unused, fillGap: unused, enqueueText: unused, listOutbox: unused, cancelOutbox: unused, retryOutbox: unused, search: unused,
  subscribe: async cb => { listeners.add(cb); return () => { listeners.delete(cb); }; },
  inventory: async () => ({ mediaAccess: { saveCopy: false }, plugins: [plugin], accounts: accounts.map(a => ({ ...a })) }),
  listChats: async () => [],
  getSettings: async () => settings,
  saveSettings: async ({ values }) => { saved = values; return null; },
  addAccount: async ({ label }) => {
    const id = label || "New";
    accounts = [...accounts, { ...accounts[0], accountId: id, label }];
    publish(id);
    return { accountId: id };
  },
  getAuthStep: async ({ accountId }) => steps.get(accountId) ?? null,
  beginAuth: async ({ accountId, method }) => { begins++; step(accountId, method === "qr" ? "qr" : "phone", "first"); return null; },
  cancelAuth: async ({ accountId }) => { steps.delete(accountId); publish(accountId); return null; },
  submitAuth: async ({ accountId }) => { step(accountId, "code", "next"); return null; },
};
const fixture = {
  refresh() { settings = { ...settings, values: { name: "changed externally" } }; publish("Alpha"); },
  saved: () => saved, begins: () => begins, step,
};
Object.assign(window, { accountFixture: fixture });
createRoot(document.getElementById("root")!).render(<StrictMode><App client={client} /></StrictMode>);
