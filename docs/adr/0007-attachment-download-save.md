# ADR 0007: attachment download and native Save-copy jobs

Status: accepted for implementation by root, 2026-11-25. The owner authorized ongoing full development; this ADR records the reviewed bounded slice.
Baseline: main `de86a48` (23 CoreOperations), current stored Attachment projection/ADR0005,
accepted poll slice/ADR0006, and the earlier private app-media proposal. Sources were
read only; no build, install, GUI, live account, legacy backend or user file was used.

## 1. Accept exactly this next batch

Explicit full-attachment Download and native Save-copy, tracked as bounded session jobs.
Keep existing metadata/spoiler rendering. Save never downloads implicitly. No open/reveal,
thumbnail fetch, asset protocol, generic filesystem/shell access, uploads or poll creation.
Plugin API/schema/SDK/database remain unchanged. The four additions bring the app to 27
operations; update Rust, both TS transports, fixtures and docs together after approval.

## 2. Exact application surface

```ts
interface MediaAccess { saveCopy: boolean } // new required Inventory.mediaAccess
interface MediaJobError {
  code: CoreFailure["code"];
  reason: "notCached" | "unsafeCache" | "changed" | "tooLarge" |
    "destinationExists" | "unsupportedDestination" | "unavailable" | "internal";
  outcomeUnknown: boolean;
}
interface MediaJob {
  id: string;
  sessionId: string;
  requestKey: string;
  thread: ThreadKey;                 // always the message's exact topic
  messageId: string;
  kind: "download" | "save";        // full attachment only
  state: "queued" | "running" | "choosing" | "committing" | "cancelling" |
    "succeeded" | "failed" | "cancelled" | "unknown";
  cancelRequested: boolean;
  doneBytes: string | null;          // unsigned decimal; null = no progress evidence
  totalBytes: string | null;
  error: MediaJobError | null;       // enums only; never raw errors/paths
}
interface MediaJobPage {
  sessionId: string;
  jobs: MediaJob[];                  // at most 16, newest creation first
  nextCursor: string | null;        // opaque, session/thread-scoped creation ordinal
  remainingRequestKeys: number;     // 0..4096; admission quota, not active-job slots
}
interface MediaStart {
  thread: ThreadKey;
  messageId: string;
  sessionId: string;                // learned from listMediaJobs
  requestKey: string;               // fresh random opaque key for one explicit user intent
}
startMediaDownload(p: MediaStart): Promise<MediaJob>;
startMediaSave(p: MediaStart): Promise<MediaJob>;
listMediaJobs(p: { thread: ThreadKey; cursor: string | null;
  requestKey: string | null }): Promise<MediaJobPage>;
cancelMediaJob(p: { thread: ThreadKey; sessionId: string; jobId: string }): Promise<MediaJob>;
```

`cursor` and `requestKey` cannot both be non-null. An exact requestKey lookup returns
zero or one job and no next cursor, for recovery after a lost start reply. Job state/error/byte fields are read snapshots, not commands. No job contains
an attachment filename, filesystem path, URL, file handle, request wire ID or file bytes.
Existing Attachment stays unchanged: no cache booleans that falsely promise existence.
Capability saveCopy means a reviewed native provider is installed; it is not proof that
a particular file/destination is usable. Browser production provider returns false;
isolated fixtures can inject a fake. Native Save initially supports Linux only.

## 3. Admission, identity and finite receipt ledger

All operations are short: local validation, reserve/update/read a job, return. Neither
600-second download nor chooser/copy runs inside the app request. Keep the existing
30-second browser timeout and 256-KiB frame limit. Pages16 stay below128KiB even with
maximal escaped IDs; enforce serialization bounds in a regression.

- New random app-session ID on App creation. Reject foreign/stale sessionId, malformed
  requestKey (ASCII letters/digits/underscore/hyphen, 1..64 bytes), unknown/foreign job ID
  and cross-thread lookup/cancel. Starts verify persisted account→plugin ownership and
  exact stored chat/topic/message; reject deleted, scheduled, local/provisional/failed
  echoes and messages without media. Whole-chat history passes the message's actual topic.
  List/cancel verify the persisted account owner and exact receipt ThreadKey, without
  requiring the old message/media/chat row still to exist: deletion must not prevent
  cancellation/recovery. Account removal internally cancels its jobs before commit;
  receipts/keys remain retained but unavailable to a removed/replaced account identity.
- Download requires enabled/running account and current media.download capability; worker
  rechecks immediately before dispatch. Save requires enabled persisted account and a
  native provider; cached saving can work while that plugin is stopped/offline.
- Reserve at most8 nonterminal jobs globally, download execution concurrency2, and only1
  native chooser/save reservation. Reject overload before accepting a new key. No
  unbounded queue/task spawn. Different new keys for the same active kind/target conflict;
  a repeated identical accepted key returns its existing receipt in any state.
- Retain every accepted key→exact parameters→job receipt for the entire app session,
  including succeeded/cancelled/failed/unknown. Maximum4096 accepted keys globally,
  finite metadata on the order of several MiB (IDs have existing byte bounds); no
  TTL/LRU eviction and no implicit session rotation.
  Same key with different parameters is invalid. At capacity fail closed for new starts;
  list/cancel/existing-key lookup still work. No accepted key can become a second save or
  download merely because a snapshot aged out. Cursor paging changes neither receipts
  nor creation order. Admission reservation and receipt insertion happen atomically.
- A lost start response is recovered by listMediaJobs with the same key. Never create a
  fresh key on timeout/reconnect. First recover/review; another copy/re-download needs a
  new explicit user action. A new process has a different session and never replays old
  jobs. No cross-crash exactly-once claim: a previously committed save may exist.

Capture stored row identity and a Rust-only Media fingerprint excluding path/thumb_path.
Recheck before attaching a download result or committing a save; a changed/deleted message
fails safely. The frozen event has no media-version token: an independently stale plugin
media.ready cannot prove which historical attachment revision it describes. Do not claim
this app batch fixes that protocol limitation or proves remote content authenticity.

## 4. Download/progress/cancel/recovery

Subscribe/register before dispatch. Construct typed MediaDownloadParams with what=full,
no caller priority; invoke existing HostHandle::call_raw_tracked with a fresh OnceLock<i64>
and owned CancellationToken. Decode MediaDownloadResult before local effects. Register
job before the supervisor can fill the wire ID; it fills the cell before writing the
request, so first progress can be matched. Match plugin, account, numeric request ID and
direction=download; supplied chat/message must match too. Never infer a match from timing
or message alone when request_id is missing. Wire IDs are private and globally increasing
within the host process. Clamp nothing into invented counts: retain monotonic reported
done, accept total only when consistent; absent/inconsistent total remains unknown.

Update bounded snapshots and coalesce progress-only invalidations to at most4Hz/account;
terminal changes notify immediately. Subscribe before reading; reread after lag/reset.
No network call from a render, invalidation or list. Do not alter history cursors/gaps.

For success, revalidate target and the returned path through the cache-only held-fd
boundary below, then apply typed Event::MediaReady through Ingest::apply and publish its
normal local effects. Do not trust generic ingest path filtering as file authorization.
Event-before/reply and reply-before/event may both record cache paths; never let an event
alone declare the tracked request successful. After a lost/error reply the job is unknown
or failed as warranted, while a later explicit Save can independently validate a cache
file already recorded by an event. A succeeded download means validated at completion,
not a permanent promise against eviction; every Save opens/checks again.

Cancel returns promptly with cancelRequested and current state. Queued cancellation
prevents dispatch; running cancellation forwards the token and remains cancelling until
the actual result settles. A verified completion may win the race. The existing host
sends $/cancelRequest but does not force an immediate reply. At20 minutes request cancel;
keep the outstanding reservation/task if a plugin keeps reporting progress/ignores it,
rather than spawning replacements or pretending it stopped. Idle registry timeout is
600 seconds and progress extends it. No automatic plugin restart is added by this batch.

Use the helper's fixed2GiB acceptance/copy cap. Reject known larger metadata, cancel if
reported progress crosses it, and reject oversized resulting files. This is NOT a hard
network/disk-write quota on a plugin: the frozen download request has no byte-limit field
and progress/cancellation are cooperative. Jobs/tasks/accepted files are bounded; a
misbehaving same-user plugin is not sandboxed by this design.

## 5. Source file authority: account cache only

Caller derives root solely from core.host().config().cache_dir(account), candidate solely
from that stored confirmed message's Media.path. Exclude sibling state/session/uploads,
other accounts, provisional upload echoes, arbitrary renderer paths and generic media_file
rows. Resolve no source from a filename or MIME. No cache file is opened/executed by MIME.

Use the separately reviewed CacheFile::open(root,candidate), metadata(), and copy_to
(&mut destination,limit,cancel predicate) helper. It holds the source descriptor and
ancestor chain, rejects lexical traversal/symlinks/nonregular/FIFO/device/hardlinks and
wrong uid, and uses O_NOFOLLOW/O_NONBLOCK/CLOEXEC plus inode/size/mode/uid/link/mtime/ctime
checks. Its64KiB copies verify fingerprints and exact length before/after. Source path
replacement or mutation aborts and discards the unpublished destination. Never reopen
source by its pathname. Current helper is Unix only, rejects symlinked absolute ancestry,
and cannot preempt a blocking syscall; those are honest deployment limits. This does not
establish that an otherwise trusted plugin could not copy sensitive bytes into its cache.

## 6. Native provider and atomic no-overwrite publication

Chosen dependency: Linux-target dependency in app/src-tauri/Cargo.toml:
`rfd = { version = "=0.17.2", default-features = false, features = ["gtk3"] }`.
The native shell injects a small Rust-only SaveDialog provider before Arc<App> is managed;
default/demo/browser provider is unsupported. Suggested internal seam: SaveDialog::choose
(sanitized_name, callback(Result<Option<PathBuf>, static_error>)); App::set_save_dialog
(&mut self, Arc<dyn SaveDialog>) is setup-only. A path exists only inside Rust callback/
worker state. No new Tauri invoke command, JS dialog package or renderer permissions.

Construct AsyncFileDialog::save_file on AppHandle::run_on_main_thread; await its future
on one bounded native worker, matching first-party Tauri Dialog's scheduling pattern.
Never block GTK/Tauri's main loop, create a second GTK loop, or call a blocking chooser
from the app RPC. Parent-window construction also belongs on the main thread. rfd's GTK
backend does not promise Linux parent support (its documented Linux parent support is
XDG-only), so do not claim true owner-modal behavior. The single main-window/provider
reservation and captured target remain authoritative regardless of window focus. Remote
native smoke must check GTK coexistence; a stub/compile alone cannot prove actual dialogs.
[First-party scheduling source](https://raw.githubusercontent.com/tauri-apps/plugins-workspace/v2/plugins/dialog/src/desktop.rs),
[pinned rfd API](https://docs.rs/rfd/0.17.2/rfd/struct.AsyncFileDialog.html),
[Tauri main-thread API](https://docs.rs/tauri/2.12.7/tauri/struct.AppHandle.html).

Hold a validated source fd across the chooser; filename is only a bounded suggestion
(strip separators/NUL/control/bidi characters, safe attachment fallback), not authority.
Show 'Save a new copy — existing files will not be replaced' before and in the chooser.
None means user cancellation, never success. A chosen existing basename returns
conflict/destinationExists; even an OS Replace prompt never authorizes overwriting in
this slice. Offer a new explicit Save-copy action with an unused name, without automatic
chooser reopen or guessed destination names. Chooser-created directories are user actions;
the backend does not create missing destination directories or synthesize extensions.

After selection, recheck job/account/message/source, reject relative/URI/traversal paths,
and open/hold destination parent ancestry with directory descriptors and O_NOFOLLOW.
Reject symlink parent/target and existing target. Publish exactly the copied descriptor,
not a temporary pathname that another writer could substitute:

1. Linux: open an unnamed O_TMPFILE|O_RDWR|O_CLOEXEC inode in the held destination directory,
   mode0600 (without O_EXCL, which would prohibit later linking). No named temp to exchange.
2. Copy held CacheFile→held temp fd with2GiB/64KiB/cancel checks; check exact source length,
   final fingerprints, destination size and fsync(temp). No whole-file buffer.
3. Atomically claim the job's commit phase against cancellation; if cancel won, discard.
   Recheck ownership/fingerprint and directory ancestry before publication. After commit
   begins a late Cancel may lose; report actual completion instead of promising removal.
4. Link that exact O_TMPFILE fd to the chosen basename via linkat's documented
   /proc/self/fd/<held-fd> + AT_SYMLINK_FOLLOW route and held destination dirfd. The proc
   name is generated privately from a held descriptor, never accepted from a plugin/UI.
   This avoids AT_EMPTY_PATH's privilege requirement. linkat never replaces an existing
   name, including a racing file or symlink; EEXIST is safe destinationExists.
5. fsync(destination dir); mark succeeded only afterwards. A publication/sync ambiguity
   is unknown/outcomeUnknown; never remove the final name or replay the save. Drop all
   descriptors. Cancellation/errors before linking leave no visible partial file.

Fail unsupportedDestination if the destination filesystem cannot support O_TMPFILE/link
publication or required procfs is unavailable. No weaker named-temp/check-then-rename
fallback is silently enabled. This initially excludes some removable/network filesystems;
expand only after reviewing an equivalent primitive. Kernel calls can block and are not
preemptible; quotas/concurrency/copy length are hard bounds, cancellation is cooperative.
[Linux held-file/open semantics](https://man7.org/linux/man-pages/man2/open.2.html),
[linkat no-overwrite and unprivileged fd publication](https://man7.org/linux/man-pages/man2/link.2.html).

Native chooser cannot be forcibly closed by the selected API. Cancellation while choosing
latches no-publish immediately, releases source bytes/FD when possible, and stays cancelling
until the chooser exits. KEEP the native provider reservation until its callback actually
runs, even after cancellation/window changes; never open a second chooser. A late selected
path after cancellation is dropped, never copied. App shutdown closes admission, revokes
callback generation and cancels workers; it must not await a native chooser on Tauri's
main Exit callback. A bounded native waiter may detach at process exit; callbacks use
weak/generation-checked job state and cannot resurrect jobs or publish after cancellation.
Do not claim guaranteed crash durability/exactly-once across a lost final callback.

## 7. Dependencies, locks, tests and split

Cargo.lock currently has tauri2.12.7, libc0.2.191, rustix1.1.6, tempfile3.27.0, GTK,
raw-window-handle and Tokio. It has neither rfd, tauri-plugin-dialog nor tauri-plugin-fs.
Use existing libc.workspace=true for narrow Linux destination syscalls/helper (new direct
edge only). Add only pinned rfd above to native shell; generate/review Cargo.lock via the
root's dedicated remote Cargo metadata job, preserving existing pins wherever compatible.
Report the actual new transitive graph/checksums from resolution, not an invented lock.
No npm lock/dependency, filesystem/opener/shell permissions, CSP or asset-scope change.

Tauri Dialog2.8.4 is a maintained first-party alternative, but its current manifest adds
rfd0.17 plus tauri-plugin-fs. Direct rfd with the cited scheduling wrapper avoids those
unused command surfaces/dependencies. If review selects the plugin instead, pin2.8.4,
features gtk3, call Rust only and grant no dialog/fs permissions. This is an explicit
alternative requiring a new lock review, not a second simultaneous backend.
[Dialog dependency manifest](https://raw.githubusercontent.com/tauri-apps/plugins-workspace/v2/plugins/dialog/Cargo.toml),
[Tauri capability boundary](https://v2.tauri.app/security/capabilities/).

Classify both start calls and cancel as side effects in TS/development worker failures;
lost replies are outcomeUnknown, list is a read. UI keeps its requestKey through recovery,
uses explicit Download/Cancel/Save-copy, reads bounded jobs on invalidation, and never
restarts work on remount/reconnect. Switching accounts discards late UI results but does
not silently cancel a user-started job. Disable/remove account cancels precommit work;
shutdown revokes admission, cancels downloads before core shutdown, and does not deadlock
waiting for a callback that requires the blocked main loop. No app/job/worker Arc cycle.

Implementation split after acceptance: (A) isolated cache helper (already under separate
review); (B) ch-app job registry/typed wire/download/progress/ingest and fake-plugin tests;
(C) native provider + destination fd publisher + reviewed lock; (D) TS transports/UI,
fixtures/browser screenshots/docs. Keep controls disabled until end-to-end pieces join.

Required isolated regressions: complete4096-key lifecycle/capacity/no eviction/replay;
pages16/frame bound/foreign cursor; lost start lookup and stale session; exact account/
chat/topic and attachment replacement; early/wrong/missing progress IDs and lag; event/
reply order; cancellation/completion and ignored cancellation preserving admission slots;
cache eviction, symlink/hardlink/state/upload/FIFO/mutation rejection; no chooser without
explicit click; delayed chooser plus cancel never opens another or publishes late;
source/parent/temp substitution; destination exists/races; exact held inode commit; source
change/disk-full/copy/sync failures leave no partial; unknown post-link failure no replay;
missing O_TMPFILE/procfs false/unsupported behavior; offline cache Save; native main-thread
stub scheduling; real-App account switch/lost reply/no automatic download; browser false
provider. All temporary HOME/XDG/data/files synthetic. Heavy compilation/native/browser
verification stays remote; actual native dialog interaction has not been exercised here.
