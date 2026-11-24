# History and local search services

`History::from_core(&core, config)` uses the runtime's shared ingest writer, plugin
host and change bus. `History::new` and `History::with_bus` accept a scripted
`PluginApi` and isolated `SharedIngest` for tests. Successful fetched message
changes publish `CoreChange::Local { service: "history", ... }` after the message
transaction, including cursor/gap bookkeeping, commits. A metadata failure rolls
back the fetched page and produces no message notification. History policy changes made through the runtime apply to
subsequent fetches.

`History::open` reads the newest stored window without contacting a plugin.
`refresh` explicitly fetches the latest page. `older`, `newer`, `jump_to_date` and
`fill_gap` fill missing history only when needed, with bounded foreground work,
per-thread serialization and cursor-based continuation. A gap id must belong to
the requested account, chat and topic before any edge resolution. Gap lookup is
read-only, including for corrupt/foreign gaps. Message anchors are topic-scoped.

Every fetched page is validated in full before any write. Whole-chat pages may
include multiple topics. Topic-scoped pages reject explicit foreign topic ids or
omitted ids that would retain a known sibling topic. A new/unknown message with
an omitted topic id inherits the requested topic context; omissions on known
matching rows keep their merged fields. Initial date jumps keep an open upper
gap when `newer_cursor` indicates later history, so scrolling can reach it.

Public window sizes default to configured
page size when zero and otherwise cap at 200.

`Search::from_core(&core)` opens a separate WAL connection for reads, so a search
need not hold the ingest writer's mutex. The lower-level `search(connection,
request)` is also available. Constructor and search calls are synchronous SQLite
work; call them off UI/async executor threads. History's stored-window operations
are synchronous too, and its plugin-facing methods perform short synchronous DB
sections between awaits.

Search supports prefix terms, quoted phrases, literal emoji, `from:`, `in:`,
`media:`/`has:`, `before:`, `after:` and `is:deleted`. Filter values for the same key
are alternatives; different keys and text terms intersect. Calendar filters use
the request's offset from UTC at midnight. Results have deterministic timestamp
and row-id ordering, bounded offset paging, UTF-16 highlights and snippets.
Deleted messages are excluded by default; scheduled messages are always excluded.
All SQL values and generated FTS phrases are bound/escaped as data. Search reads
rows, FTS highlights and snippets inside one SQLite snapshot, reusing a caller
transaction without committing it or opening a read transaction when necessary.

History gap metadata does not currently have a distinct change-bus variant.
A fill that only closes an empty gap returns that result to its caller; a
subscriber relying solely on message-change events must re-read the thread after
its requested fill finishes.

## Verification

Run `cargo test -p ch-core --test history --test search` and the complete workspace
suite with `CH_DATA_DIR` and XDG directories under a fresh temporary directory.
The history suite uses a scripted platform; the search suite uses fresh in-memory
DBs, except a runtime integration case that uses a temporary on-disk core.
No test needs live credentials or network access.

Regression coverage includes injected gap-insert, gap-close and island-split
failures, whole-page rejection, topic-context merging, foreign corrupt gaps,
timestamp ties, first date jumps with newer cursors, caller-owned transactions
and concurrent Unicode edits through a second WAL connection.
