import test from "node:test";
import assert from "node:assert/strict";
import type { AccountSettings, AuthStep, CoreChange, CoreClient } from "../src/core";
import { displayValue, fieldsFor, settingsPayload } from "../src/components/accounts/settings";
import { initialAuth, openAuthSession } from "../src/components/accounts/authSession";

test("settings retain redacted secrets and unrelated keys, with explicit replace and clear", () => {
  const fields = fieldsFor({ properties: {
    token: { type: "string", "x-secret": true, default: "must-never-prefill" },
    count: { type: "integer", default: 2 }, aliases: { type: "array" }, mode: { enum: ["one", 2] },
  } });
  const stored: AccountSettings = { values: { extra: true }, secretsSet: ["token"] };
  const token = fields[0];
  assert.equal(displayValue(token, stored, new Map()), "");
  assert.equal(displayValue(fields[1], stored, new Map()), "2");
  assert.deepEqual(settingsPayload(fields, stored, new Map()), { extra: true });
  assert.deepEqual(settingsPayload(fields, stored, new Map([["token", ""]])), { extra: true });
  assert.equal(settingsPayload(fields, stored, new Map([["token", null]])).token, null);
  assert.equal(settingsPayload(fields, stored, new Map([["token", "replacement"]])).token, "replacement");
  const edits = new Map([["count", "4"], ["aliases", '["a"]'], ["mode", "1"]]);
  assert.deepEqual(settingsPayload(fields, stored, edits), { extra: true, count: 4, aliases: ["a"], mode: 2 });
  assert.throws(() => settingsPayload(fields, stored, new Map([["aliases", "{}"]])), /JSON array/);
  assert.throws(() => settingsPayload(fields, stored, new Map([["aliases", "{"]])), /valid JSON/);
  assert.throws(() => settingsPayload(fields, stored, new Map([["count", "1.5"]])), /integer/);
  assert.equal(displayValue(fields[1], { ...stored, values: { count: 8 } }, edits), "4");
});

test("required secrets can use an existing stored value", () => {
  const fields = fieldsFor({ required: ["key"], properties: { key: { type: "string", "x-secret": true } } });
  assert.deepEqual(settingsPayload(fields, { values: {}, secretsSet: ["key"] }, new Map()), {});
  assert.throws(() => settingsPayload(fields, { values: {}, secretsSet: [] }, new Map()), /required/);
});

const tick = () => new Promise<void>(resolve => setImmediate(resolve));
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}
const code: AuthStep = { flow: "login", stepId: "1", kind: "code", title: null, hint: null, placeholder: null,
  data: null, expiresAt: null, error: null, minLength: null, maxLength: null, canResend: true };

function fakeAuth(initial: AuthStep | null) {
  let step = initial;
  let listener: ((event: CoreChange) => void) | undefined;
  const calls: string[] = [];
  const client: Pick<CoreClient, "subscribe" | "getAuthStep" | "beginAuth" | "submitAuth" | "cancelAuth"> = {
    subscribe: async cb => { calls.push("listen"); listener = cb; return () => { listener = undefined; }; },
    getAuthStep: async ({ accountId }) => { calls.push("read " + accountId); return step; },
    beginAuth: async ({ accountId, method }) => { calls.push(`begin ${accountId} ${method}`); return null; },
    submitAuth: async ({ accountId, flow, stepId }) => { calls.push(`submit ${accountId} ${flow} ${stepId}`); return null; },
    cancelAuth: async ({ accountId, flow }) => { calls.push(`cancel ${accountId} ${flow}`); return null; },
  };
  return { client, calls, replace(next: AuthStep | null) { step = next; },
    invalidate(id = "a") { listener?.({ kind: "account", accountId: id }); } };
}

test("an auth session subscribes first, drops invalidated reads, ignores unrelated accounts and never auto-begins", async () => {
  const core = fakeAuth(null);
  const first = deferred<AuthStep | null>();
  const steady = core.client.getAuthStep;
  core.client.getAuthStep = args => { core.client.getAuthStep = steady; core.calls.push("first " + args.accountId); return first.promise; };
  const accepted: string[] = [];
  const session = openAuthSession(core.client, "a", v => accepted.push(v.step?.stepId ?? "none"));
  await tick();
  assert.deepEqual(core.calls, ["listen", "first a"]);
  core.replace({ ...code, stepId: "2" });
  core.invalidate();
  first.resolve(code);
  await tick();
  core.invalidate("b");
  await tick();
  assert.deepEqual(core.calls, ["listen", "first a", "read a"]);
  assert.deepEqual(accepted, ["2"]);
  session.close();
});

test("submit and cancel recheck the exact current step; closing drops a pending credential submission", async () => {
  const core = fakeAuth(code);
  let view = initialAuth;
  const session = openAuthSession(core.client, "a", v => { view = v; });
  await tick();
  const newer = { ...code, flow: "different" };
  core.replace(newer);
  await session.act(code, { kind: "submit", value: "dummy" });
  assert.deepEqual(view.step, newer);
  await session.act(code, { kind: "cancel" });
  assert.equal(core.calls.some(call => /^(submit|cancel)/.test(call)), false);
  await session.act(newer, { kind: "cancel" });
  assert.ok(core.calls.includes("cancel a different"));
  const check = deferred<AuthStep | null>();
  core.client.getAuthStep = () => check.promise;
  const action = session.act(newer, { kind: "submit", value: "dummy" });
  session.close();
  const other = fakeAuth(null);
  const nextSession = openAuthSession(other.client, "b", () => {});
  check.resolve(newer);
  await action;
  await tick();
  assert.equal(core.calls.some(call => call.startsWith("submit")), false);
  assert.deepEqual(other.calls, ["listen", "read b"]);
  nextSession.close();
});

test("a step delivered before begin acknowledgement remains actionable; acknowledged submits cannot repeat", async () => {
  const core = fakeAuth(null);
  const ack = deferred<null>();
  core.client.beginAuth = () => ack.promise;
  let view = initialAuth;
  const session = openAuthSession(core.client, "a", v => { view = v; });
  await tick();
  const begin = session.act(null, { kind: "begin", method: "phone_code" });
  await tick();
  core.replace(code);
  core.invalidate();
  await tick();
  ack.resolve(null);
  await begin;
  assert.equal(view.awaiting, false);
  assert.equal(view.step, code);
  await session.act(code, { kind: "submit", value: "dummy" });
  await session.act(code, { kind: "submit", value: "dummy" });
  assert.equal(core.calls.filter(call => call.startsWith("submit")).length, 1);
  session.close();
});
