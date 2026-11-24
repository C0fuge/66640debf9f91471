import type { CoreClient, ThreadKey } from "../../core";
import { threadId } from "./shared";

// A lost enqueue reply may be the last notification we receive. Prompt the
// mounted history to re-open its local snapshot when the composer reconciles.
const listeners = new WeakMap<CoreClient, Set<(key: string) => void>>();

export function watchLocalHistory(client: CoreClient, thread: ThreadKey, reload: () => void) {
  const key = threadId(thread);
  let group = listeners.get(client);
  if (!group) { group = new Set(); listeners.set(client, group); }
  const listener = (changed: string) => { if (changed === key) reload(); };
  group.add(listener);
  return () => { group.delete(listener); };
}

export function notifyLocalHistory(client: CoreClient, thread: ThreadKey) {
  const key = threadId(thread);
  listeners.get(client)?.forEach(listener => listener(key));
}
