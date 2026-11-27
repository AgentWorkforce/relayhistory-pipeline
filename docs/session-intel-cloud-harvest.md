# Session Intelligence cloud harvest

Reads one workspace's sessions over a time window from Agent Relay Cloud and reduces every coding session's events to **derived facts**: paths, refs, branch names, shas and short closed-vocabulary codes. No transcript text leaves the reducer. The output feeds the overlap benchmark ([session-intel-overlap-bench.md](session-intel-overlap-bench.md)).

Code: `src/session-intel/cloud-harvest/`. Tests: `src/session-intel/__tests__/cloud-harvest.test.ts`.

## Running it

```
CLOUD_HARVEST_TOKEN='op://<vault>/<item>/<field>' op run -- \
  bun run src/session-intel/cloud-harvest/cli.ts --cloud-api https://<host>/cloud/api/v1 --workspace <uuid> \
    --since <iso> --until <iso> --out <dir outside the repo> --delete-by <iso, at most 14 days out> \
    [--concurrency 3] [--max-requests 20000] [--max-rows 25000] [--via cloud|reader-mcp] [--accept-client-side-window]
```

- **Authorization.** Reading session data needs the workspace owner's OK for the stated scope, each time: workspace, window, request ceiling, retained fields, storage and delete-by.
- **Credential.** The bearer comes only from `CLOUD_HARVEST_TOKEN`, injected by `op run`, and is removed from the environment once read. Nothing credential-like is written to disk.
- **Workspace.** Cloud's routes carry the workspace in their path. The shared-sessions list tool takes no workspace and reads the credential's own, so with `--via reader-mcp` the harvest first asks `describe_capabilities` which workspace the credential is bound to, and refuses before any read unless it is the requested one. Each transcript read also names the workspace, and every response (each list page and each transcript page) must name the requested workspace, or the harvest stops before projecting it.
- **Lock contents.** A lock held by another run is verified (a private regular file) and only a bounded pid is reported; its text is never echoed. Links anywhere in a path's ancestors are refused, except system links owned by root (such as macOS's `/var`).
- **Window.** The bounds are admitted only as canonical UTC instants of at most millisecond precision (sub-millisecond digits are refused, not dropped). Every event is checked against them at its own full precision (up to nanoseconds, `Z` or an offset) before it is reduced: an event outside the window, or with an unparseable time, is counted (`outOfWindow`) and dropped, never derived or stored. A kept event's time is stored at that same full precision in one fixed width, `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ` (`canonicalTs`), so string order is time order. Paging stops once events pass the end of the window. Neither reader can bound event times on the server: both page a session's whole history, so earlier events of a long-lived session are still received over the wire. The CLI therefore refuses both readers unless `--accept-client-side-window` records (in the manifest) that the approved scope allows this. A bulk reader must filter by event time on the server.
- **Limits.** `--max-requests`, `--max-rows` and `--concurrency` (at most 5) are fixed in the manifest when the harvest is created; a resume can neither raise nor reset them. One process holds a harvest at a time (`harvest.lock`, created exclusively). A lock left by a process that died is not taken over automatically: the run is refused with the pid, and the operator removes the lock after checking. The request ledger counts every request, retries included, across every resume, and each request is recorded (temporary file, then rename) before it is sent. An interrupted run can over-count by its requests in flight but never under-count. A harvest from before the ledger cannot be resumed, because its spent budget is unknown and is never reset to zero. `--concurrency` is enforced by one limiter in the client, at the request boundary. Every request, from directory slices and session workers alike, waits for a slot and holds it until its body is read. Directory rows are **reserved before each list request** against `--max-rows`. The request asks for at most the rows left, the reservation is persisted at once, and the unused part is returned when the page arrives. A page bringing more rows than it was granted is refused before projection, and once nothing is left no request is sent. A reserved list request is sent exactly once, with no retry: a lost answer may still have been served, so a retry could draw the rows twice against one reservation. The listing stops instead, resumably, and the reservation stays spent. Every row received counts, filtered and duplicate rows included, cumulatively across resumes. Cloud's list route takes no page size, so it cannot honour a ceiling and the CLI refuses it (`READERS.cloud.boundedListPages`). Its earlier harvest predates the ceiling. Restored counters and limits are validated before any request.
- **Interrupted runs.** An append cut off mid-record leaves a final line without its newline. Under the lock, that partial tail (only what follows the last newline) is cut away, and the session is read again. A malformed line anywhere else is corruption and stops the run.
- **Output.** `--out` must be outside this repository, checked before the directory is created and again on its real path. Every directory and file the harvest reads or writes (output, resumed inputs, manifest, lock) must be a real path (no symbolic link), owned by this user, and closed to group and others. Anything else is refused, never repaired. Files are written to an exclusively created temporary file (mode 600) and renamed. `--delete-by` is required, at most 14 days out, and recorded in the manifest.
- **Read-only.** The client sends only GETs to two Cloud routes and read-only tool calls to the shared-sessions MCP. Redirects are never followed. 429 and 5xx answers are retried with `retry-after`.
- **Resumable.** Output is append-only, so a rerun skips sessions already read. Only the manifest's ledger and `coverage.json` are rewritten.

## Directory and events

The directory is listed over the window. The backend fails a whole page when one row in it cannot be served, so on a failing cursor the harvest finds the nearest cursor below that pages again (a growing step, then a binary search). The rows it cannot reach are recorded as a **gap**, never silently skipped.

Sessions from the harnesses in `CODING_SOURCES` have their events read, at up to 300 pages of 1,000 events each. Beyond that cap a session is marked `censored`. Other sessions stay in the directory only.

## Facts

`features.ts` is the privacy boundary. A raw event goes in and only these come out:

- `files`: the event's `filesTouched`, where the harness records it;
- `touched`: paths recovered from commands and their output, tagged by how they were seen;
- git and gh operation codes, branch names, PR refs, commit shas, and the relay envelope's sender and recipient. A `gh` operation is one the call executes (`commands.ts`): `gh` as the first word of a shell segment, after assignments and wrappers such as `sudo`, inside `bash -c` scripts, or in a JSON or code-mode `cmd`. Echoed, quoted or prose mentions of `gh` are not operations.

The schema is closed and every kept string is validated: identifiers and codes must be identifier characters (no whitespace, so no prose), times must parse, and a path must be path characters only with a separator or an extension. Free text where a path is expected (a patch header, a redirect target) is dropped, not kept. No content string is ever returned. The receipt lists the withheld fields.

Directory rows go through the same kind of projection (`directoryProjection`): each field is named, validated and rebuilt, and nested objects such as the project and the pull request are rebuilt from validated leaves, never copied. A row without a valid session id, source and time is dropped.

`touched` exists because most harnesses do not fill `filesTouched`. On the Jul 28 → Nov 5 harvest, only 450 of the 2,148 Codex sessions with edits had it, and 1,519 of the 3,914 Claude ones. Recovery found about 11 times more distinct paths than `filesTouched` alone: 98.5k against 6.3k for Codex, and 70k against 8.4k for Claude. Most recovered path events are `status` or `stat` observations, not writes (see below).

Each `touched` path is tagged by how it was seen:

| Tag | Seen in |
|---|---|
| `patch` | `apply_patch` headers |
| `add` | `git add` |
| `shell` | `sed -i`, `cat >` and `tee` |
| `status` | `git status --short` output |
| `stat` | `git diff --stat` and `git show --stat` output |

`status` and `stat` are observations, not writes. The bench decides how to treat them (overlap bench, "Model").

## Coverage receipt

`coverage.json` (`session-intel-cloud-harvest-receipt/1`) records:

- the source (`via`) and the window;
- the frame's pages and gaps;
- per source: sessions, sessions read, events, informative events, sessions with edit evidence, censored sessions, errors, sessions without a published transcript, truncated events, and events dropped for falling outside the window;
- the request counts by route.

A harvest is `complete` only when it is not capped, the frame has no gaps, and every coding session was read with no errors and nothing censored. A session without a published transcript is counted, not failed.

## Sessions after the storage move

Since 2026-11-05, coding sessions are stored by the 96Bulls Reader (a Supabase directory with ClickHouse events). Cloud's `/sessions` list and `/reflex/sessions/{id}/events` routes still read the old store, so they never list those sessions.

`--via reader-mcp` reads them through the hosted shared-sessions MCP (`<base>/mcp/shared-sessions`), after attesting the credential's workspace (see "Running it"). It pages `list_session_summaries` and `get_shared_session` and maps their rows onto the same shapes, so the same reducer and privacy boundary apply.

Compared with the Cloud routes, the MCP does not carry:

- `createdAt`, `parentSessionIds` or `machineIds` on directory rows;
- `filesTouched` on events.

Event content is also capped at 4,000 characters, and each session records how many of its events were truncated. A session's `last_active` is its latest activity, so the directory filter applies only the lower bound of the window.

The MCP serves at most 50 events per call, so a full workspace takes many hours. Bulk read access to the Reader, filtering by event time on the server, is the better path if it is granted. `mergeHarvests` in the bench joins a Cloud harvest and a Reader harvest of one workspace: per session it keeps the richer directory row, every owner, and the union of events by event id.
