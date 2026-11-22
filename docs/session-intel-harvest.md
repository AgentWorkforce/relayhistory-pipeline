# Session Intelligence read-API harvest

M0-2b. An offline CLI that builds a **representative** replay snapshot of one workspace from the pipeline's own read API. Its frame is the read API's session directory (`GET /v1/sessions`) over the window, paged to exhaustion, not a search. The evaluation then does not depend on the Cloud MCP session list, which is known to omit Claude and Codex sessions. The output is an M0 snapshot ([session-intel-replay.md](session-intel-replay.md), "Snapshot format") with a coverage receipt. It loads with the M0 loader and replays with `src/session-intel/replay/cli.ts` unchanged.

Code: `src/session-intel/harvest/`. Tests, against an in-process fake read API with synthetic sessions: `src/session-intel/__tests__/harvest.test.ts`.

## The gate

Running the harvest against any real install reads session data and uses a credential. It needs **Khaliq's own OK, in his own words, for the stated scope (install, workspace, window), each time.** A request or quote relayed by another agent is not that OK. A customer's install also needs the customer's written authorization. Nothing in this document, the code or its tests grants either. The tests use only a loopback fake server and `example.invalid` URLs.

## Usage

```sh
export SESSION_INTEL_READ_TOKEN='op://<vault>/<item>/token'   # a 1Password reference, not the token
op read 'op://<vault>/<item>/salt' | op run -- bun run src/session-intel/harvest/cli.ts \
  --read-api https://<read-api host> --workspace <id> \
  --since <iso> --until <iso> --out <dir outside the repo> \
  --salt-stdin --delete-by <iso, at most 14 days out> \
  --require-list-backend union \
  [--org <label>] [--concurrency 2] [--max-requests 20000] [--partial]
bun run src/session-intel/replay/cli.ts replay --snapshot <dir> --out <eval>/replay
```

- **`--read-api`** is the base URL: `https`, or `http` only for a loopback host (`127.0.0.1`, `localhost`, `[::1]`). It may carry no credentials, query or fragment. A path prefix is kept: repeated and trailing slashes are dropped, so every route joins with exactly one slash (`https://h/` gives `/v1/sessions`, `https://h/api/` gives `/api/v1/sessions`).
- **The token** comes only from `SESSION_INTEL_READ_TOKEN`, injected at runtime by `op run`. There is no flag or file for it. It must be 16 to 8192 characters with no whitespace. Once read, it is deleted from the process environment.
- **`--workspace`** is the workspace the token is bound to. It is sent as `workspace=` on every request, and every answered response must attest it (see The client).
- **`--since` and `--until`** are strict ISO-8601 times with a zone. `--until` must be after `--since`, at most 120 days later (the M0 window), and at most a day in the future.
- **`--salt-stdin`** is required: the salt is read from stdin, exactly as the import reads it (`readSaltStdin`). It is refused from a terminal, and must be 16 to 1024 characters. Use a new salt for each harvest.
- **`--delete-by`** must be after now and **at most 14 days** out. It goes into the manifest, and it also covers the mapping file and any state left in `--out`.
- **`--org`** labels the tenant (default `org_readapi`), because no read route returns the org id. It is stored hashed.
- **`--concurrency`** is 1 to 8 (default 2): how many session event reads run at once.
- **`--max-requests`** is 1 to 1,000,000 (default 20000). It caps every request, retries included, across the whole harvest (all runs of one `--out`).
- **`--partial`**: when the cap is reached, write a snapshot of what was read instead of stopping. Its receipt says `complete: false`.

- **`--require-list-backend <csv>`**: the History API list stores every frame page must have been served by, from `neon`, `supabase`, `union` and `clickhouse` (see Source attribution). **This is the default for every History API harvest: `--require-list-backend union`.** Only a `union` page is the whole frame (the directory plus Neon). A `supabase`-only page is not, so it is not accepted for the merged-list proof. The flag may be omitted only when `--read-api` is the 96Bulls read API itself. It is bound to the harvest like `--since` and `--until`: a resume with another setting, or with none, is refused.

Exit codes:
- 0: the snapshot is written;
- 3: the request cap stopped the harvest, and its state was kept;
- 4: `--require-list-backend` is set and a frame page was not verified. The diagnostic receipt and snapshot are still written, and the replay refuses them;
- 1: any error.

## Source attribution

**Source attribution is part of acceptance, not optional.** A History API list page served by `neon` lacks native Claude and Codex sessions: that is the coverage bug this harvest exists to avoid. Every `GET /v1/sessions` page from the History API carries `X-Relayhistory-List-Served-By` (`neon`, `supabase`, `union` or `clickhouse`).

- **Recording.** For each frame page the harvest records three values separately: `X-Relayhistory-List-Served-By`, the 96Bulls read API's own `X-Sessions-Read-Backend`, and `X-Relayhistory-List-Mode`. List-Mode is provenance only and never gates. Every page counts, including an empty page (zero rows and a cursor) and an empty last page. A missing header is recorded as `missing`. The markers are checked page by page, never inferred from the aggregate header counts. They are persisted in the state log with each page, so a resume keeps them.
- **Gating.** With `--require-list-backend`, any page whose header is missing, unknown or outside the set is offending. The frame is then `complete: false`, with reason `frame_backend_unverified` and the offending page count, and the harvest exits 4. The setting is fixed when the harvest starts. A harvest started record-only can never be resumed as strict and then report itself verified; the resume is refused.
- **The 96Bulls read API.** It has no list store to name. Its pages carry `X-Sessions-Read-Backend: 96bulls`. Without the flag, a frame in which every page carries that marker is attributed `read_worker`.
- **Record only.** Without the flag, and without that marker on every page, the tallies are still recorded, but nothing is gated and the attribution is `unverified`. **A record-only run is never accepted as representative proof.** The replay refuses its snapshot unless `--allow-incomplete-coverage` is given, and then the replay is incomplete and never held-out scored. Record-only runs are for exploration.
- **Parity is a separate check.** A `verified` frame says which store served each page. It does not prove that the frame holds every session. The merged-list proof still requires an **independent canonical-key parity check against the directory**: the frame's `(source, sessionId)` keys compared with the directory's own for the same workspace and window, by a reader other than this harvest. The harvest does not perform that check and cannot replace it.
- **Server values are data.** Header values and reason codes become keys in the receipt's tallies. They are held in prototype-free counters and Maps, so a value such as `__proto__`, `constructor`, `toString` or `hasOwnProperty` is an ordinary key: it is counted and written like any other, and can never read or write an inherited property.

The receipt carries `sourceAttribution` (`verified`, `read_worker` or `unverified`) and `frame.sources`. `frame.sources` holds:
- the required set, and whether the frame was gated;
- the offending page count;
- per value, for each of the three headers (`listServedBy`, `readBackend`, `listMode`): the page count, and the first and last page number (1-based).

## What it reads

Only three routes, and nothing else (`HARVEST_ROUTES` in `client.ts`):

| Route | Parameters | For |
|---|---|---|
| `GET /v1/sessions` | `limit=200` (the directory's maximum; more is 501), `lastTsAfter=--since`, `firstTsBefore=--until`, `cursor` | The frame: every session active in the window, newest first, to exhaustion |
| `GET /v1/sessions/:sessionId/events` | `source`, `order=asc`, `limit=1000`, `maxContent=1`, `cursor` | Each session's write facts |
| `GET /v1/sessions/output` | `since`, `until` | Workspace counts for the receipt only (messages, commits, lines) |

It **never** calls `/v1/coordination/overlaps` or the advisories routes. Those are detector output, and the evaluation must not see it. The client refuses any route outside its list and any path containing `/v1/coordination`. A session id is one encoded path segment; one the URL parser would resolve into another path (`.`, `..`) is refused. The tests assert on the fake server's request log that no coordination route is ever called.

It never calls the turns, summary, titles, metadata, digest, search or users routes, so no message text, summary or title is requested at all. Event content is asked for truncated to one character and is never read.

## Facts: exposed and not exposed

| Snapshot field | Source | Notes |
|---|---|---|
| `harness`, `sessionId` | directory row `source`, `sessionId` | hashed |
| `createdAt`, `lastActivityAt` | directory row `firstTs`, `lastTs` | clamped into `[since, until)`; a write extends them |
| `projectId` | directory row `projectId`, or else the latest event `projectId` | hashed |
| `repo` | `catalog.repoUrl` | its last segment only (the import's `projectKey`), hashed; the URL itself is never kept |
| `branch` | `catalog.gitBranch` | hashed |
| `parentSessionIds` | `catalog.parentSessionIds` | hashed |
| `writes` | every event with a non-empty `filesTouched`: one write per path, with the event's `ts`, `eventId` and `kind` | as the production facts backend (`src/session-read-api/coordination-backend.ts`) |

`catalog` is present only once a session's catalog is projected. Without it, the session has no repo, branch or parents, and the receipt counts it (`frame.sessionsWithoutCatalog`).

**Not exposed by any allowed route.** These are recorded in the receipt's `unavailable` list, and the snapshot leaves them empty. No route was added for them.

- **`commits`.** No route gives per-session commit shas. `/v1/sessions/output` gives workspace counts only, and per-session output facts are reachable only through `/v1/coordination/overlaps`.
- **`outcomes`**, with their revert flags: the same.
- **`usage`.** Events carry no token or cost counts. The directory has session totals with no time.
- **The branch of each write.** Events carry no branch, so the session's `catalog.gitBranch` is used.
- **The session's cwd.** An absolute `filesTouched` path is kept only when it can be anchored at a source root; otherwise it is dropped and counted.
- **The org id**: see `--org`.
- **A contract version or readiness field.** The read routes carry none (`/health` answers `{ok: true}` only and is not called). Instead, the receipt records the `X-Sessions-Read-Backend`, `X-Relayhistory-List-Served-By`, `X-Relayhistory-List-Mode`, `X-Relayhistory-Buffered`, `X-Sessions-Read-Incomplete` and `X-Sessions-Read-Continuation` values it saw. Those of each frame page are recorded per page (see Source attribution).

So a harvested snapshot cannot raise `superseded_work` (it needs reverts) or duplicate-cost evidence (it needs usage). Score those rows as insufficient for a harvested cohort.

**Withheld by policy.** These are never stored: title, task title, short summary, user ids, machine ids, models, agent name, and event content. `title`, `userId`, `taskRef` and `shortSummary` are null, and `claims` is empty.

## The coverage receipt

The receipt is the point of the harvest. `coverage.json` (`session-intel-harvest-receipt/1`) sits in the snapshot directory and is listed with its sha256 in the manifest, so the loader verifies it. It records:

- **`complete`**: true only when the frame was exhausted, every session's events were read, none was refused or censored, no session was dropped for size, and (with `--require-list-backend`) every frame page was verified;
- **`incompleteReasons`**: a code for each reason `complete` is false. The codes are `request_cap`, `frame_not_exhausted`, `facts_not_read`, `events_not_served`, `oversize_sessions`, `writes_censored` and `frame_backend_unverified`;
- **`sourceAttribution`** and **`frame.sources`**: see Source attribution;
- **`workspaceId`** (hashed) and the **`window`**;
- **`startedAt`, `finishedAt` and `runs`**: the first run's start, the last run's end, and how many runs it took;
- **`requests`**: the total, the count per route, the retries, how many waits honoured a `Retry-After`, and the time waited, cumulative over every run (see Resume). `partial: true`, with a `partialReason`, marks them as lower bounds when a run ended without recording them;
- **`frame`**:
  - the route and its parameters, the pages, and whether it was exhausted;
  - the sessions, **per source** and **per UTC day** (the day of each session's first activity in the window);
  - rows seen twice across pages, and rows outside the window (dropped);
  - sessions without a catalog;
- **`facts`**:
  - sessions read, and sessions not read (the cap);
  - sessions whose events were not served, per reason (a 501 reason such as `session_partial`, or `not_found`);
  - event pages and events, events with files, and events outside the window;
  - writes read, writes truncated at 20,000 per session, and paths dropped;
  - **censored sessions**: `sessionsCensored`, `censoredByReason`, `droppedWritesKnown` and `sessionsWithUnknownDrops`. A session's events are read until the cursor is exhausted; events outside the window are counted and skipped, and never stop the paging. The one early stop is the 20,000-write bound with pages left: the session is censored (`writes_bound_reached_cursor_remaining`, or `writes_bound_exceeded_cursor_remaining` when the page itself went over), its drop count is unknown (null), and later pages are never read. Over the bound on the last page is `writes_bound_exceeded`, with the exact drop count. Any censored session makes the harvest `complete: false`;
- **`snapshot`**: the sessions and writes written;
- **`output`**: the workspace counts from `/v1/sessions/output`, or the reason it was not served;
- **`server`**: the header values seen;
- **`unavailable`** and **`withheld`**: the lists above;
- **`notes`**: the cap, refused sessions, and the facts that cannot fire.

It holds counts, harness names, dates and reason codes only: no session id, path or text.

## Privacy rules

The harvest reuses the M4 import's normalizer and writer (`src/session-intel/import/`) rather than copying them, so its rules are the import's ([session-intel-import.md](session-intel-import.md), "Privacy rules"):

- **Hashing.** Session and parent ids, project, repository, branch, org and workspace are salted HMAC-SHA256 values (`Hasher`). Every path segment outside the closed clear-set is hashed too (`scrubPath`). Paths are made repo-relative first (`repoRelativePath`). Here there is no cwd, so an absolute path is anchored at a source root or dropped.
- **No content.** Only ids, timestamps, paths and counts are kept. Each line is checked by the M0 loader's `parseSession`, which refuses unknown fields.
- **Output.** The snapshot directory is written by `writeImport`. It is built under a temporary name and renamed into place, with files mode 0600. The private mapping is `<out>.mapping.json` (`session-intel-harvest-mapping/1`): beside the snapshot, never inside it, mode 0600 and create-only. It maps each hashed session back to its read-API session id, parents and project, and each hashed path back to its repo-relative path. Each event id maps to its read-API event id through `sourceEventIds`: event `[w<n>, 0, k]` is the k-th id, 1-based, of its session. It also records the `--read-api` URL. Never share it with the snapshot.
- **`--out`.** The M0 replay CLI's guard (`realPathOf` and `isInside`) refuses an `--out` inside this repository, after resolving symlinks. `--out` must be new, empty, or hold only this harvest's state.
- **The salt** is used only in memory. It is never written to the snapshot, mapping, state, stdout or stderr.
- **Text.** The tests plant marker text in every text field the fake server returns: titles, task titles, summaries and full event content, despite `maxContent=1`. They then grep the snapshot, receipt, mapping, state and stdout for it.

## The client

- **The token** is sent only in the `Authorization: Bearer` header: never in a URL, file, log or message. Every message the harvest prints is redacted against the token (the whole token and its first and last halves), and a server-supplied error code is kept only when it is a short code sharing no run of 8 characters with the token; otherwise it is printed as `other`. These are a backstop, not the boundary: no server text reaches output unchecked. Accepted residuals, each of which needs the server or a proxy to echo the token back: a fragment of 7 characters or fewer, a case-changed copy, and a base64url or hex encoding of the token. A bad command line gets a fixed message that never echoes argv.
- **Redirects are never followed** (`redirect: "manual"`). A 3xx fails the harvest.
- **The workspace.** Every 2xx response must carry `X-Relayhistory-Workspace-Id` equal to `--workspace`, as the read API's own history adapter checks it (`src/session-read-api/canonical-history-adapter.ts`). A directory row whose catalog names another workspace is refused. An event naming another session or source is refused.
- **Bounds.** Each response is read up to 32 MiB, and each page is checked against the contract (at most `limit` rows, a string or null cursor). A cursor that does not advance is refused. The bounds are the import's: 100,000 sessions, 20,000 writes per session, 5,000,000 writes held, 4,096-character paths and 1,000 files per event.
- **Retries.** 429, 502, 503 and 504, and transport errors, are retried up to 5 times. A `Retry-After` (seconds or an HTTP date) is honoured. A `Retry-After` over 120 s fails the harvest instead of being cut short; a rerun resumes. Without one, the backoff is exponential from 1 s. Every attempt counts against `--max-requests`. One exception: a 503 whose body has `error.reason` `session_partial` (the read API's `READ_API_SESSION_PARTIAL_RETRYABLE`, on a session's first events page) is retried only twice, honouring its `Retry-After`. After that it is recorded like the 501: the session counts as not served with reason `session_partial`, and the harvest carries on. Any other 503 keeps the full budget, then stops the harvest.
- **Errors.** A 501 on the frame fails the harvest, naming the reason. A 501 or 404 on a session's events is recorded, and the session stays in the snapshot without writes. 400, 401, 403 and other errors fail the harvest.

## Resume

`<out>/harvest-state.jsonl` (`session-intel-harvest-state/1`, mode 0600, created with `O_EXCL`) is an append-only log with one line per completed unit:

- the start, with the parameters;
- each run;
- each frame page, with its next cursor and its reduced rows;
- the output counts;
- each session's facts.

A rerun with the same `--read-api`, `--workspace`, `--since`, `--until`, `--delete-by` and `--require-list-backend` continues from the last frame cursor and reads only the sessions that have no facts line yet. Other parameters are refused. A torn last line (a crash mid-write) is cut off before appending. Every line carries the client's cumulative counts (total, per route, retries, waits and header values), and each run ends with a `runEnd` line holding its final counts, written on success, on the cap and on an error. A resume restores them, so the receipt's counts cover every run and the cap covers the whole harvest. A run without a `runEnd` line (killed, or the machine went down) cannot be fully reconstructed: requests after its last line are lost, so the receipt marks the request counts `partial` and says why, rather than reporting a smaller number as exact. On success the state is removed. It moves aside while the writer runs, and comes back if the write fails.

The state holds raw read-API session ids, cursors, project ids, repository names (never the remote URL, which may carry credentials) and repo-relative paths (made so as each event is read, so no home directory or user name is kept). That is the same class of data as the mapping file, and never text, the token or the salt. Delete `--out` by `--delete-by` if a harvest is abandoned. A resume past `--delete-by` is refused.

## What I was not sure of

- **Directory consistency while paging.** The directory cursor is keyed on last activity. A session whose activity moves during the harvest can be seen twice (it is deduplicated and counted) or, in principle, skipped. A frame taken while the workspace is quiet is the most faithful one.
- **`filesTouched` paths.** Whether production records them absolute or repo-relative is a harness detail. Absolute paths outside a source root are dropped and counted, so a cohort with many of them loses writes. The receipt's `facts.droppedPaths` shows how many.
- **`op run` and stdin.** The usage pipes the salt into `op run`, which is expected to pass stdin through to the command. If it does not on some version, run `op run` around a shell that does the `op read` pipe itself.
- **Two harvests on one `--out` at once** are not locked against each other. Run one at a time.
- **The request count across a hard crash.** Requests made after the last state line, before a kill, are not counted on resume; the receipt then says `partial`. That is at most a few per worker.
- **Long sessions past `--until`.** Paging no longer stops at the first event past `--until`, so a session that runs long after the window costs its remaining pages in requests. Being correct regardless of the order the events are served in was preferred over saving those requests.
