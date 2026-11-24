import type { CoreClient, Message, PluginSummary, TextEntity, ThreadKey } from "../../core";
import { failureText } from "./shared";

export type MessageAction =
  | { kind: "edit"; messageId: string; text: string; entities: TextEntity[] }
  | { kind: "delete"; messageIds: string[]; revoke: boolean }
  | { kind: "pin"; messageId: string; silent: boolean }
  | { kind: "unpin"; messageId: string };

export function messageActions(client: CoreClient, plugin: PluginSummary | null | undefined, message: Message) {
  const has = (key: string) => plugin?.phase === "running" &&
    (plugin.capabilities?.[key] === true || typeof plugin.capabilities?.[key] === "object" && plugin.capabilities[key] !== null);
  const confirmed = !message.messageId.startsWith("local:") && !["sending", "failed"].includes(message.delivery ?? "");
  return {
    showEdit: typeof client.editText === "function" && has("edit") && has("send.entities") && message.direction === "out" && message.text !== null && message.mediaKind === null && !message.deleted,
    showDelete: typeof client.deleteMessages === "function" && has("delete"),
    showPin: typeof client.pinMessage === "function" && has("pin") && !message.deleted,
    showUnpin: typeof client.unpinMessage === "function" && has("unpin") && !message.deleted,
    // Bundled UI/core versions must match; method checks also protect fixture/older clients.
    edit: typeof client.editText === "function" && has("edit") && has("send.entities") && confirmed &&
      message.direction === "out" && message.text !== null && message.mediaKind === null && !message.deleted,
    delete: typeof client.deleteMessages === "function" && has("delete") && confirmed,
    deleteForMe: has("delete.for_me"),
    pin: typeof client.pinMessage === "function" && has("pin") && confirmed && !message.deleted,
    unpin: typeof client.unpinMessage === "function" && has("unpin") && confirmed && !message.deleted,
  };
}

export function outcomeUnknown(error: unknown) {
  return typeof error === "object" && error !== null && "outcomeUnknown" in error && error.outcomeUnknown === true;
}

// One session per mounted thread, independent from composer drafts. It never replays.
export function actionSession(client: CoreClient, thread: ThreadKey) {
  let busy = false;
  let reloadRequired = false;
  return {
    get busy() { return busy; },
    get reloadRequired() { return reloadRequired; },
    async perform(action: MessageAction, topicId: string | null = thread.topicId): Promise<{ ok: boolean; error: string }> {
      if (busy || reloadRequired) return { ok: false, error: "Reload history before another action." };
      busy = true;
      const target = { ...thread, topicId };
      try {
        switch (action.kind) {
          case "edit": await client.editText({ thread: target, messageId: action.messageId, text: action.text, entities: action.entities }); break;
          case "delete": await client.deleteMessages({ thread: target, messageIds: action.messageIds, revoke: action.revoke }); break;
          case "pin": await client.pinMessage({ thread: target, messageId: action.messageId, silent: action.silent }); break;
          case "unpin": await client.unpinMessage({ thread: target, messageId: action.messageId }); break;
        }
        return { ok: true, error: "" };
      } catch (cause) {
        reloadRequired = outcomeUnknown(cause);
        return { ok: false, error: reloadRequired ? "The action may have completed. Reload history and review before trying again." : failureText(cause) };
      } finally { busy = false; }
    },
    // Only a completed read for this exact mounted thread can unlock an explicit retry.
    reloaded() { reloadRequired = false; },
  };
}
