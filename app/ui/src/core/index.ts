// JSON contract shared by the desktop and development adapters. See docs/app-api.md.
export type Json = null | boolean | number | string | Json[] | JsonObject;
export interface JsonObject { [key: string]: Json }

// All identifiers are scoped, opaque strings. Millis is a signed decimal Unix timestamp.
export type Millis = string;
export interface ThreadKey {
  accountId: string;
  chatId: string;
  topicId: string | null;
}

export interface PluginSummary {
  pluginId: string;
  name: string;
  brandColor: string | null;
  phase: "unavailable" | "stopped" | "starting" | "running" | "restarting"
    | "failed" | "incompatible" | "shutting_down";
  capabilities: Record<string, boolean | JsonObject> | null;
  authMethods: string[];
  settingsSchema: JsonObject | null;
}

export interface AccountSummary {
  accountId: string;
  pluginId: string;
  label: string | null;
  enabled: boolean;
  phase: "stopped" | "starting" | "running" | "stopping";
  state: string;
  reconnecting: boolean;
}

export interface Inventory {
  plugins: PluginSummary[];
  accounts: AccountSummary[];
}

export interface ChatSummary {
  thread: ThreadKey;
  title: string | null;
  kind: string;
  archived: boolean;
  pinned: boolean;
  muted: boolean | null;
  canPost: boolean | null;
  unreadCount: number;
  markedUnread: boolean;
  lastMessageAt: Millis | null;
  lastMessagePreview: string | null;
  lastMessageKind: string | null;
}

export interface Utf16Span {
  offset: number;
  length: number;
}

export interface TextEntity extends Utf16Span {
  type: string;
  url?: string;
  userId?: string;
  language?: string;
  customEmojiId?: string;
  collapsed?: boolean;
}

export interface Attachment {
  kind: string;
  name: string | null;
  mime: string | null;
  bytes: string | null;
  width: number | null;
  height: number | null;
  durationSeconds: number | null;
  hasSpoiler: boolean;
}

export interface PollOptionSnapshot {
  id: string; text: string; votes: string | null; chosen: boolean | null; correct: boolean | null;
}
export interface PollSnapshot {
  id: string | null; question: string; options: PollOptionSnapshot[]; totalVoters: string | null;
  closed: boolean; multiple: boolean; quiz: boolean; anonymous: boolean; closeAt: Millis | null;
  solution: string | null; solutionEntities: TextEntity[];
}
export interface PollResult { poll: PollSnapshot | null }

export interface Message {
  rowKey: string;
  messageId: string;
  topicId: string | null;
  direction: "in" | "out";
  sentAt: Millis;
  senderName: string | null;
  text: string | null;
  entities: TextEntity[];
  mediaKind: string | null;
  attachment?: Attachment | null;
  poll?: PollSnapshot | null;
  edited: boolean;
  deleted: boolean;
  delivery: string | null;
  isPinned?: boolean;
}

export type HistoryItem =
  | { kind: "message"; message: Message }
  | { kind: "gap"; gapId: string };

export interface HistoryPage {
  items: HistoryItem[];
  moreOlder: boolean;
  moreNewer: boolean;
}

export type HistoryWindow =
  | { kind: "latest" }
  | { kind: "refresh" }
  | { kind: "older"; before: string | null }
  | { kind: "newer"; after: string };

export interface OutboxItem {
  id: string;
  thread: ThreadKey;
  echoRowKey: string | null;
  state: "queued" | "sending" | "sent" | "failed" | "cancelled";
  nextAttemptAt: Millis | null;
  outcomeUnknown: boolean;
}

export interface SearchHit {
  thread: ThreadKey;
  messageId: string;
  sentAt: Millis;
  snippet: string;
  snippetHighlights: Utf16Span[];
}

export interface AccountSettings {
  values: JsonObject;
  secretsSet: string[];
}

export interface AuthStep {
  flow: string;
  stepId: string;
  kind: string;
  title: string | null;
  hint: string | null;
  placeholder: string | null;
  data: string | null;
  expiresAt: Millis | null;
  error: string | null;
  minLength: number | null;
  maxLength: number | null;
  canResend: boolean;
}

export type CoreChange =
  | { kind: "reset" }
  | { kind: "inventory" }
  | { kind: "account"; accountId: string };

export interface CoreFailure {
  code: "invalid_request" | "not_found" | "unsupported" | "conflict"
    | "unavailable" | "internal";
  message: string;
  outcomeUnknown?: boolean;
}

type Account = { accountId: string };
type InThread = { thread: ThreadKey };
type OutboxTarget = InThread & { id: string };
type AuthFlow = Account & { flow: string };

export interface CoreOperations {
  inventory(params: Record<string, never>): Promise<Inventory>;
  listChats(params: Account & { archived: boolean | null }): Promise<ChatSummary[]>;
  history(params: InThread & { window: HistoryWindow; limit: number }): Promise<HistoryPage>;
  fillGap(params: InThread & { gapId: string }): Promise<{ closed: boolean }>;
  enqueueText(params: InThread & { text: string; entities?: TextEntity[] }): Promise<OutboxItem>;
  editText(params: InThread & { messageId: string; text: string; entities: TextEntity[] }): Promise<null>;
  deleteMessages(params: InThread & { messageIds: string[]; revoke: boolean }): Promise<null>;
  pinMessage(params: InThread & { messageId: string; silent: boolean }): Promise<null>;
  unpinMessage(params: InThread & { messageId: string }): Promise<null>;
  votePoll(params: InThread & { messageId: string; optionIds: string[] }): Promise<PollResult>;
  closePoll(params: InThread & { messageId: string }): Promise<PollResult>;
  refreshPoll(params: InThread & { messageId: string }): Promise<PollResult>;
  listOutbox(params: InThread): Promise<OutboxItem[]>;
  cancelOutbox(params: OutboxTarget): Promise<OutboxItem>;
  retryOutbox(params: OutboxTarget): Promise<OutboxItem>;
  search(params: InThread & { query: string; offset: number; limit: number; utcOffsetMinutes: number }):
    Promise<{ hits: SearchHit[]; nextOffset: number | null }>;
  addAccount(params: { pluginId: string; label: string | null; settings: JsonObject }): Promise<Account>;
  getSettings(params: Account): Promise<AccountSettings>;
  saveSettings(params: Account & { values: JsonObject }): Promise<null>;
  getAuthStep(params: Account): Promise<AuthStep | null>;
  beginAuth(params: Account & { method: string }): Promise<null>;
  submitAuth(params: AuthFlow & { stepId: string; value: string }): Promise<null>;
  cancelAuth(params: AuthFlow): Promise<null>;
}

export interface CoreClient extends CoreOperations {
  subscribe(onChange: (change: CoreChange) => void): Promise<() => void>;
}

export type CoreRequest = {
  [K in keyof CoreOperations]: { method: K; params: Parameters<CoreOperations[K]>[0] }
}[keyof CoreOperations];

export type CoreReply<K extends keyof CoreOperations> =
  | { ok: true; value: Awaited<ReturnType<CoreOperations[K]>> }
  | { ok: false; error: CoreFailure };

// The shell owns selection. Each pane reads its own views through the shared client.
export interface ChatListPaneProps {
  client: CoreClient;
  accountId: string | null;
  selectedThread: ThreadKey | null;
  onSelectChat: (chat: ChatSummary) => void;
}

export interface ThreadPaneProps {
  client: CoreClient;
  chat: ChatSummary | null;
  plugin?: PluginSummary | null;
}

export type ComposerProps = ThreadPaneProps;
