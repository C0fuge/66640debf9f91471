import type { CoreClient, Message, PluginSummary, PollSnapshot, ThreadKey } from "../../core";
import { outcomeUnknown } from "./actions";
import { failureText } from "./shared";

export function pollPermissions(client: CoreClient, plugin: PluginSummary | null | undefined, message: Message, poll: PollSnapshot) {
  const has = (key: string) => plugin?.phase === "running" && (plugin.capabilities?.[key] === true ||
    typeof plugin.capabilities?.[key] === "object" && plugin.capabilities[key] !== null);
  const confirmed = !message.deleted && !message.messageId.startsWith("local:") && !["sending", "failed"].includes(message.delivery ?? "");
  const refresh = confirmed && has("messages.get") && typeof client.refreshPoll === "function";
  return { refresh, vote: refresh && has("polls.vote") && typeof client.votePoll === "function" && !poll.closed &&
    (!poll.quiz || poll.options.length > 0 && poll.options.every(option => option.chosen === false)),
    retract: refresh && has("polls.vote") && !poll.closed && !poll.quiz && poll.options.some(option => option.chosen === true),
    close: refresh && has("polls.close") && typeof client.closePoll === "function" && !poll.closed && message.direction === "out" };
}

// Every fresh interaction requires an explicit targeted read. Cached snapshots,
// remounts and account switches cannot release a lost-write guard. No registry is retained.
export function pollSession(client: CoreClient, thread: ThreadKey, messageId: string, initial: PollSnapshot) {
  let poll = initial;
  let busy = false;
  let needsRefresh = true;
  return {
    get poll() { return poll; }, get busy() { return busy; }, get needsRefresh() { return needsRefresh; },
    async perform(kind: "vote" | "close" | "refresh", optionIds: string[] = []) {
      if (busy || kind !== "refresh" && needsRefresh) return { ok: false, error: "Refresh this poll before another action." };
      busy = true;
      try {
        const target = { thread, messageId };
        const result = kind === "vote" ? await client.votePoll({ ...target, optionIds }) : kind === "close" ?
          await client.closePoll(target) : await client.refreshPoll(target);
        if (!result || typeof result !== "object" || !("poll" in result) ||
          result.poll !== null && (typeof result.poll !== "object" || !Array.isArray(result.poll.options))) {
          throw { code: "unavailable", ...(kind !== "refresh" ? { outcomeUnknown: true } : {}) };
        }
        if (result.poll) { poll = result.poll; needsRefresh = false; }
        else { needsRefresh = true; }
        return { ok: true, error: result.poll ? "Review the reported poll before another action." : "Current poll state was not returned. Refresh this poll." };
      } catch (cause) {
        if (kind !== "refresh" && outcomeUnknown(cause)) needsRefresh = true;
        return { ok: false, error: kind !== "refresh" && needsRefresh ? "The action may have completed. Refresh this poll and review before trying again." : failureText(cause) };
      } finally { busy = false; }
    },
  };
}
