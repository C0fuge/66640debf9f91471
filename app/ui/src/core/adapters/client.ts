import type { CoreChange, CoreClient, CoreFailure, CoreOperations, CoreRequest } from "../index";

export type Request = (request: CoreRequest) => Promise<unknown>;
const reasons: Record<CoreFailure["code"], string> = {
  invalid_request: "The core could not accept this request.",
  not_found: "This item is no longer available.",
  unsupported: "This account does not support that action.",
  conflict: "The item changed. Check its current state and try again.",
  unavailable: "The core is unavailable.",
  internal: "The core could not complete this request.",
};

export function failure(code: CoreFailure["code"]): CoreFailure {
  return { code, message: reasons[code] };
}

export function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function isChange(value: unknown): value is CoreChange {
  return isObject(value) && (value.kind === "reset" || value.kind === "inventory" ||
    (value.kind === "account" && typeof value.accountId === "string"));
}

function unpack(reply: unknown, mutation: boolean): unknown {
  if (isObject(reply)) {
    if (reply.ok === true && Object.hasOwn(reply, "value")) return reply.value;
    if (reply.ok === false && isObject(reply.error)) {
      const code = reply.error.code;
      if (typeof code === "string" && Object.hasOwn(reasons, code)) {
        throw { ...failure(code as CoreFailure["code"]), ...(reply.error.outcomeUnknown === true ? { outcomeUnknown: true } : {}) };
      }
    }
  }
  throw { ...failure("unavailable"), ...(mutation ? { outcomeUnknown: true } : {}) };
}

const messageMutations = new Set<keyof CoreOperations>(["editText", "deleteMessages", "pinMessage", "unpinMessage", "votePoll", "closePoll",
  "startMediaDownload", "startMediaSave", "cancelMediaJob", "setMessageReactions"]);

export function createClient(send: Request, subscribe: CoreClient["subscribe"]): CoreClient {
  function method<K extends keyof CoreOperations>(name: K) {
    return async (params: Parameters<CoreOperations[K]>[0]): Promise<Awaited<ReturnType<CoreOperations[K]>>> => {
      let reply: unknown;
      try {
        // The call site's method and parameters share K.
        reply = await send({ method: name, params } as CoreRequest);
      } catch {
        throw { ...failure("unavailable"), ...(messageMutations.has(name) ? { outcomeUnknown: true } : {}) };
      }
      return unpack(reply, messageMutations.has(name)) as Awaited<ReturnType<CoreOperations[K]>>;
    };
  }
  return {
    inventory: method("inventory"),
    listChats: method("listChats"),
    history: method("history"),
    fillGap: method("fillGap"),
    enqueueText: method("enqueueText"),
    editText: method("editText"),
    deleteMessages: method("deleteMessages"),
    pinMessage: method("pinMessage"),
    unpinMessage: method("unpinMessage"),
    votePoll: method("votePoll"),
    closePoll: method("closePoll"),
    refreshPoll: method("refreshPoll"),
    refreshMessageReactions: method("refreshMessageReactions"),
    setMessageReactions: method("setMessageReactions"),
    listMessageReactors: method("listMessageReactors"),
    startMediaDownload: method("startMediaDownload"),
    startMediaSave: method("startMediaSave"),
    listMediaJobs: method("listMediaJobs"),
    cancelMediaJob: method("cancelMediaJob"),
    listOutbox: method("listOutbox"),
    cancelOutbox: method("cancelOutbox"),
    retryOutbox: method("retryOutbox"),
    search: method("search"),
    addAccount: method("addAccount"),
    getSettings: method("getSettings"),
    saveSettings: method("saveSettings"),
    getAuthStep: method("getAuthStep"),
    beginAuth: method("beginAuth"),
    submitAuth: method("submitAuth"),
    cancelAuth: method("cancelAuth"),
    subscribe,
  };
}
