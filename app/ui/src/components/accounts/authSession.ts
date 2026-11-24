import type { AuthStep, CoreClient } from "../../core";

export interface AuthView {
  step: AuthStep | null;
  ready: boolean;
  busy: boolean;
  error: string;
  awaiting: boolean;
}

export type AuthAction =
  | { kind: "begin"; method: string }
  | { kind: "resend"; method: string }
  | { kind: "submit"; value: string }
  | { kind: "cancel" };

export const initialAuth: AuthView = { step: null, ready: false, busy: false, error: "", awaiting: false };

export function sameStep(a: AuthStep | null, b: AuthStep | null) {
  return a === b || !!a && !!b && a.flow === b.flow && a.stepId === b.stepId && a.kind === b.kind;
}

export function expiry(step: AuthStep | null): number | null {
  if (step?.expiresAt == null) return null;
  const at = new Date(Number(step.expiresAt)).getTime();
  return Number.isFinite(at) ? at : null;
}

// Each mounted account dialog has its own subscribed view of the core's transient step.
// A read which straddled an invalidation cannot authorize a write.
type AuthClient = Pick<CoreClient, "subscribe" | "getAuthStep" | "beginAuth" | "submitAuth" | "cancelAuth">;

export function openAuthSession(client: AuthClient, accountId: string, publish: (view: AuthView) => void) {
  let view = initialAuth;
  let closed = false;
  let revision = 0;
  let reading = false;
  let listen: (() => void) | undefined;
  let awaitingStep: AuthStep | null | undefined;

  function update(patch: Partial<AuthView>) {
    if (!closed) publish(view = { ...view, ...patch });
  }
  function accept(step: AuthStep | null) {
    if (awaitingStep !== undefined && !sameStep(awaitingStep, step)) awaitingStep = undefined;
    update({ step, ready: true, awaiting: awaitingStep !== undefined, error: view.ready ? view.error : "" });
  }
  async function refresh() {
    revision++;
    if (closed || !listen || reading) return;
    reading = true;
    let seen: number;
    do {
      seen = revision;
      try {
        const next = await client.getAuthStep({ accountId });
        if (!closed && seen === revision) accept(next);
      } catch {
        if (!closed && seen === revision) update({ ready: false, step: null, error: "Could not read the sign-in step." });
      }
    } while (!closed && seen !== revision);
    reading = false;
  }
  void client.subscribe(event => {
    if (event.kind === "reset" || (event.kind === "account" && event.accountId === accountId)) void refresh();
  }).then(stop => {
    if (closed) { stop(); return; }
    listen = stop;
    void refresh();
  }, () => update({ error: "Could not connect to sign-in updates." }));

  async function act(expected: AuthStep | null, action: AuthAction) {
    if (closed || !view.ready || view.busy || !sameStep(view.step, expected)) return;
    if ((action.kind === "begin") !== (expected === null)) return;
    if (action.kind === "submit" && view.awaiting) return;
    update({ busy: true, error: "" });
    try {
      const seen = revision;
      const current = await client.getAuthStep({ accountId });
      if (closed) return;
      if (seen !== revision || !sameStep(current, expected)) {
        update({ error: "The sign-in step changed. Check the current step and try again." });
        return;
      }
      if (action.kind === "begin" || action.kind === "resend") {
        await client.beginAuth({ accountId, method: action.method });
      } else if (current && action.kind === "submit") {
        const at = expiry(current);
        if (at !== null && at <= Date.now()) { update({ error: "This step has expired." }); return; }
        await client.submitAuth({ accountId, flow: current.flow, stepId: current.stepId, value: action.value });
      } else if (current && action.kind === "cancel") {
        await client.cancelAuth({ accountId, flow: current.flow });
      }
      if (!closed) {
        // The next step can be delivered before the command acknowledgement.
        awaitingStep = action.kind !== "cancel" && sameStep(view.step, expected) ? expected : undefined;
        update({ awaiting: awaitingStep !== undefined });
      }
    } catch {
      update({ error: "The core could not complete this sign-in action. Check the current step and try again." });
    } finally {
      if (!closed) {
        await refresh();
        update({ busy: false });
      }
    }
  }
  return {
    act,
    close() { closed = true; listen?.(); view = initialAuth; awaitingStep = undefined; },
  };
}
