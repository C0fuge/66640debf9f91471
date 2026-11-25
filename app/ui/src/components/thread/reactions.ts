import type { AccountSummary, CoreClient, Message, OrdinaryReactionKey, PluginSummary, ReactionAvailability, ReactionContext, ReactionItem,
  ReactionSnapshot, ReactorItem, ThreadKey } from "../../core";
import { isObject } from "../../core/adapters/client";
import { outcomeUnknown } from "./actions";
import { failureCode, failureText } from "./shared";

const size = (value: string) => new TextEncoder().encode(value).length;
const opaque = (value: unknown): value is string => typeof value === "string" && value.length > 0 && size(value) <= 256;
const uint = (value: unknown): value is string => typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 18446744073709551615n;
const millis = (value: unknown): value is string => typeof value === "string" && /^-?(0|[1-9][0-9]{0,18})$/.test(value)
  && BigInt(value) >= -9223372036854775808n && BigInt(value) <= 9223372036854775807n;
const bounded = (value: unknown) => { try { return size(JSON.stringify(value)) <= 128 * 1024; } catch { return false; } };
export function isReactionKey(value: unknown): value is OrdinaryReactionKey {
  return isObject(value) && Object.keys(value).length === 2 && (value.kind === "emoji" ? typeof value.emoji === "string" && size(value.emoji) > 0 && size(value.emoji) <= 128
    : value.kind === "custom" && opaque(value.id));
}
export const reactionKeyId = (key: OrdinaryReactionKey) => JSON.stringify(key.kind === "emoji" ? [key.kind, key.emoji] : [key.kind, key.id]);
export const reactionKeyLabel = (key: OrdinaryReactionKey) => key.kind === "emoji" ? key.emoji : `Custom emoji ${key.id}`;
export function isReactionItem(value: unknown): value is ReactionItem {
  return isObject(value) && isReactionKey(value.key) && (value.count === null || uint(value.count))
    && (value.mine === null || typeof value.mine === "boolean");
}
export function isReactionSnapshot(value: unknown): value is ReactionSnapshot {
  return isObject(value) && Array.isArray(value.items) && value.items.length <= 128 && value.items.every(isReactionItem)
    && new Set(value.items.map(item => reactionKeyId(item.key))).size === value.items.length;
}
function isAvailability(value: unknown): value is ReactionAvailability {
  return isObject(value) && typeof value.allowCustom === "boolean" && (value.maxPerMessage === null || uint(value.maxPerMessage))
    && (value.keys === null || Array.isArray(value.keys) && value.keys.length <= 256 && value.keys.every(isReactionKey)
      && new Set(value.keys.map(reactionKeyId)).size === value.keys.length);
}
function isContext(value: unknown): value is ReactionContext {
  return isObject(value) && bounded(value) && opaque(value.sessionId) && (value.contextToken === null || opaque(value.contextToken))
    && (value.expiresAt === null || millis(value.expiresAt)) && (value.snapshot === null || isReactionSnapshot(value.snapshot))
    && (value.availability === null || isAvailability(value.availability)) && typeof value.canSet === "boolean";
}
function isReactor(value: unknown): value is ReactorItem {
  return isObject(value) && opaque(value.userId) && isReactionKey(value.key) && (value.name === null || typeof value.name === "string" && size(value.name) <= 256)
    && (value.at === null || millis(value.at));
}
export interface ReactionPermissions { refresh: boolean; set: boolean; who: boolean; custom: boolean; declaredMax: number | null }
export function reactionPermissions(client: CoreClient, plugin: PluginSummary | null | undefined, account: AccountSummary | null | undefined,
  thread: ThreadKey, message: Message): ReactionPermissions {
  const confirmed = !message.deleted && !message.messageId.startsWith("local:") && !["sending", "failed"].includes(message.delivery ?? "");
  const active = confirmed && account?.accountId === thread.accountId && account.enabled && account.phase === "running"
    && plugin?.pluginId === account.pluginId && plugin.phase === "running";
  const has = (key: string) => !!active && (plugin?.capabilities?.[key] === true || typeof plugin?.capabilities?.[key] === "object" && plugin.capabilities[key] !== null);
  const cap = plugin?.capabilities?.["reactions.set"];
  const max = cap === true ? 1 : typeof cap === "object" && cap !== null ? Object.hasOwn(cap, "max") ? cap.max : 1 : null;
  const validSet = Number.isSafeInteger(max) && typeof max === "number" && max >= 1;
  return { refresh: has("messages.get") && typeof client.refreshMessageReactions === "function",
    set: has("reactions.set") && validSet && typeof client.setMessageReactions === "function", who: has("reactions.who") && typeof client.listMessageReactors === "function",
    custom: has("reactions.custom"), declaredMax: validSet ? max as number : null };
}
const sameKeys = (a: OrdinaryReactionKey[], b: OrdinaryReactionKey[]) => a.length === b.length && a.every(key => b.some(other => reactionKeyId(key) === reactionKeyId(other)));
export interface ReactionPlan { generation: number; contextToken: string; keys: OrdinaryReactionKey[]; removed: OrdinaryReactionKey[] }
type Result = { ok: boolean; error: string; stale?: boolean };

// One bounded panel, no persistent proof registry. All network calls below have
// explicit user callers. Cached history/invalidations never mint a context.
export function reactionSession(client: CoreClient, source: ThreadKey, messageId: string, initial: ReactionSnapshot | null,
  live: () => ReactionPermissions, now: () => number = () => Date.now()) {
  const thread = { ...source };
  let context: ReactionContext | null = null;
  let snapshot: ReactionSnapshot | null = initial && Array.isArray(initial.items) ? { items: initial.items.filter(isReactionItem).slice(0, 8) } : null;
  let generation = 0, refreshing = false, refreshes = 0, writing = false, whoBusy = false;
  let reported = false, filter: OrdinaryReactionKey | null = null, reactors: ReactorItem[] = [], nextCursor: string | null = null, started = false;
  let attempts = 0, rowCount = 0, whoGeneration = 0;
  const pages = new Map<string, { items: ReactorItem[]; nextCursor: string | null }>();
  let seen = new Set<string>();
  const fresh = () => !!context?.contextToken && context.expiresAt !== null && BigInt(context.expiresAt) > BigInt(now());
  const canSet = () => fresh() && !!context?.canSet && live().set && context.snapshot !== null && context.availability !== null
    && context.snapshot.items.every(item => item.mine !== null);
  function resetPages() { whoGeneration++; pages.clear(); seen = new Set(); attempts = 0; rowCount = 0; reactors = []; nextCursor = null; started = false; whoBusy = false; filter = null; }
  const pageKey = () => JSON.stringify([filter === null ? null : reactionKeyId(filter), started ? nextCursor : null]);
  const canLoadMore = () => fresh() && live().who && !refreshing && !writing && !whoBusy && !(started && nextCursor === null)
    && (pages.has(pageKey()) || attempts < 8 && rowCount < 256);
  function invalidate() { generation++; context = null; refreshing = false; writing = false; reported = false; resetPages(); }
  function plan(keys: OrdinaryReactionKey[]): ReactionPlan | string {
    if (!canSet() || !context?.contextToken || !context.snapshot || !context.availability) return "Refresh reactions before replacing the complete ordinary set.";
    if (writing || refreshing || whoBusy) return "Wait for the current reaction operation.";
    if (!keys.every(isReactionKey) || new Set(keys.map(reactionKeyId)).size !== keys.length) return "Choose distinct valid ordinary reactions.";
    if (keys.length > 16) return "This panel supports at most 16 selected reactions.";
    const policy = context.availability, permissions = live();
    if (keys.length && (policy.maxPerMessage !== null && BigInt(keys.length) > BigInt(policy.maxPerMessage)
      || permissions.declaredMax !== null && keys.length > permissions.declaredMax)) return "The selection exceeds a reported reaction limit.";
    if (keys.some(key => policy.keys !== null && !policy.keys.some(offered => reactionKeyId(offered) === reactionKeyId(key)))) return "Choose only reactions in the offered catalog.";
    if (keys.some(key => key.kind === "custom" && (!permissions.custom || policy.keys === null))) return "Only listed custom emoji with the live custom capability can be selected here.";
    const current = context.snapshot.items.filter(item => item.mine === true).map(item => item.key);
    if (sameKeys(keys, current)) return "The ordinary reaction membership has not changed.";
    return { generation, contextToken: context.contextToken, keys: structuredClone(keys),
      removed: current.filter(key => !keys.some(selected => reactionKeyId(selected) === reactionKeyId(key))) };
  }
  return {
    get context() { return context; }, get snapshot() { return snapshot; }, get reported() { return reported; }, get busy() { return refreshing || writing || whoBusy; },
    get fresh() { return fresh(); }, get canSet() { return canSet(); }, get canWho() { return fresh() && live().who; }, get generation() { return generation; },
    get reactors() { return reactors; }, get filter() { return filter; }, get nextCursor() { return nextCursor; }, get started() { return started; },
    get viewLimit() { return attempts >= 8 || rowCount >= 256; }, get canLoadMore() { return canLoadMore(); }, plan, invalidate,
    async refresh(): Promise<Result> {
      if (writing || !live().refresh || refreshes >= 4) return { ok: false, error: "Reaction refresh is unavailable right now." };
      const requestGeneration = ++generation;
      context = null; reported = false; resetPages(); refreshing = true; refreshes++;
      try {
        const next = await client.refreshMessageReactions({ thread, messageId });
        if (requestGeneration !== generation) return { ok: false, error: "", stale: true };
        if (!isContext(next)) throw { code: "unavailable" };
        context = next; snapshot = next.snapshot; reported = true;
        return { ok: true, error: next.snapshot === null ? "Current ordinary reactions were not reported; this is not a known-empty selection."
          : !next.canSet || next.availability === null ? "Review the reported reactions. Replacing the set is unavailable for this view." : "Review the full ordinary set before choosing a replacement." };
      } catch (cause) {
        return requestGeneration === generation ? { ok: false, error: failureText(cause) } : { ok: false, error: "", stale: true };
      } finally { refreshes--; if (requestGeneration === generation) refreshing = false; }
    },
    async replace(selection: ReactionPlan): Promise<Result> {
      const checked = plan(selection.keys);
      if (typeof checked === "string") return { ok: false, error: checked };
      if (selection.generation !== generation || selection.contextToken !== context?.contextToken) return { ok: false, error: "The confirmation view changed. Refresh and review again." };
      const proof = context!;
      const requestGeneration = ++generation;
      context = null; reported = false; resetPages(); writing = true;
      try {
        const result = await client.setMessageReactions({ thread, messageId, sessionId: proof.sessionId, contextToken: proof.contextToken!, keys: selection.keys });
        if (requestGeneration !== generation) return { ok: false, error: "", stale: true };
        if (!isObject(result) || !bounded(result) || result.snapshot !== null && !isReactionSnapshot(result.snapshot)) throw { code: "unavailable", outcomeUnknown: true };
        const next = result.snapshot;
        if (next !== null && (next.items.some(item => item.mine === null) || !sameKeys(next.items.filter(item => item.mine).map(item => item.key), selection.keys))) throw { code: "unavailable", outcomeUnknown: true };
        snapshot = next; reported = true;
        return { ok: true, error: next === null ? "The operation returned no reaction snapshot. Refresh explicitly before another action."
          : "The reported ordinary membership is shown. Another write requires an explicit refresh." };
      } catch (cause) {
        return requestGeneration === generation ? { ok: false, error: outcomeUnknown(cause) || failureCode(cause) === undefined
          ? "The replacement may have completed. Refresh and review before another action." : `${failureText(cause)} Refresh explicitly before another action.` } : { ok: false, error: "", stale: true };
      } finally { if (requestGeneration === generation) writing = false; }
    },
    chooseFilter(key: OrdinaryReactionKey | null) {
      if (key !== null && !isReactionKey(key)) return;
      whoGeneration++; whoBusy = false; filter = key; reactors = []; nextCursor = null; started = false; seen = new Set();
    },
    async loadMore(): Promise<Result> {
      if (!fresh() || !context?.contextToken || !live().who) return { ok: false, error: "Refresh reactions before loading reactor metadata." };
      if (refreshing || writing || whoBusy) return { ok: false, error: "Wait for the current reaction operation." };
      if (started && nextCursor === null) return { ok: false, error: "No further page was reported for this traversal." };
      const proof = context, requestGeneration = generation, traversal = whoGeneration, key = filter, cursor = started ? nextCursor : null;
      const cacheKey = JSON.stringify([key === null ? null : reactionKeyId(key), cursor]);
      let page = pages.get(cacheKey);
      if (!page && (attempts >= 8 || rowCount >= 256)) return { ok: false, error: "This reactor view reached its local limit. Refresh explicitly for a fresh view." };
      whoBusy = true;
      try {
        if (!page) {
          attempts++;
          const result = await client.listMessageReactors({ thread, messageId, sessionId: proof.sessionId, contextToken: proof.contextToken!, key, cursor });
          if (requestGeneration !== generation || traversal !== whoGeneration || context !== proof || !fresh()) return { ok: false, error: "", stale: true };
          if (!isObject(result) || !bounded(result) || !Array.isArray(result.items) || result.items.length > 32 || !result.items.every(isReactor)
            || result.nextCursor !== null && (typeof result.nextCursor !== "string" || !result.nextCursor || size(result.nextCursor) > 4096)
            || rowCount + result.items.length > 256 || key !== null && result.items.some(item => reactionKeyId(item.key) !== reactionKeyId(key))) throw { code: "unavailable" };
          page = { items: result.items, nextCursor: result.nextCursor as string | null };
          pages.set(cacheKey, page); rowCount += page.items.length;
        }
        if (page.nextCursor !== null && (page.nextCursor === cursor || seen.has(page.nextCursor))) throw { code: "invalid_request" };
        if (cursor !== null) seen.add(cursor);
        reactors = [...reactors, ...page.items]; nextCursor = page.nextCursor; started = true;
        return { ok: true, error: nextCursor === null ? "No further page was reported. Reactor lists can change while you browse." : "More metadata may be available. Load the next page explicitly." };
      } catch (cause) {
        if (requestGeneration !== generation || traversal !== whoGeneration) return { ok: false, error: "", stale: true };
        invalidate(); return { ok: false, error: `${failureText(cause)} Refresh explicitly to start a fresh view.` };
      } finally { if (requestGeneration === generation && traversal === whoGeneration) whoBusy = false; }
    },
  };
}
