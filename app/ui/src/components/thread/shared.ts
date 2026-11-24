import type { CoreFailure, ThreadKey } from "../../core";

export function threadId(thread: ThreadKey) {
  return JSON.stringify([thread.accountId, thread.chatId, thread.topicId]);
}

export function failureCode(error: unknown): CoreFailure["code"] | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) return;
  switch (error.code) {
    case "invalid_request": case "not_found": case "unsupported":
    case "conflict": case "unavailable": case "internal": return error.code;
  }
}

// Keep transport details and outgoing text out of visible error messages.
export function failureText(error: unknown) {
  switch (failureCode(error)) {
    case "invalid_request": return "The request is no longer valid.";
    case "not_found": return "This item is no longer available.";
    case "unsupported": return "This account does not support this action.";
    case "conflict": return "The item changed before the action completed.";
    case "unavailable": return "The connection is unavailable.";
    default: return "The core could not complete the request.";
  }
}
