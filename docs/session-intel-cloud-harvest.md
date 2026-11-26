# Session Intelligence cloud harvest

Reads one workspace's sessions over a time window from Agent Relay Cloud and reduces every coding session's events to **derived facts**: paths, refs, branch names, shas and short closed-vocabulary codes. No transcript text leaves the reducer. The output feeds the overlap benchmark ([session-intel-overlap-bench.md](session-intel-overlap-bench.md)).

Code: `src/session-intel/cloud-harvest/`. Tests: `src/session-intel/__tests__/cloud-harvest.test.ts`.

## Running it

```
CLOUD_HARVEST_TOKEN='op://<vault>/<item>/<field>' op run -- \
  bun run src/session-intel/cloud-harvest/cli.ts --cloud-api https://<host>/cloud/api/v1 --workspace <uuid> \
    --since <iso> --until <iso> --out <dir outside the repo> --delete-by <iso, at most 14 days out> \
    [--concurrency 3] [--max-requests 20000] [--via cloud|reader-mcp]
```

- **Authorization.** Reading session data needs the workspace owner's OK for the stated scope, each time.
- **Credential.** The bearer comes only from `CLOUD_HARVEST_TOKEN`, injected by `op run`, and is removed from the environment once read. Nothing credential-like is written to disk.
- **Output.** `--out` must be outside this repository. Every file is written with mode 600. `--delete-by` is required and at most 14 days out, and it is recorded in the manifest.
- **Read-only.** The client sends only GETs to two Cloud routes, and read-only tool calls to the shared-sessions MCP. Redirects are never followed. 429 and 5xx answers are retried with `retry-after`. `--max-requests` caps the run, and a capped run is marked incomplete.
- **Resumable.** Output is append-only, so a rerun skips sessions already read. Only `coverage.json` is rewritten.

## Directory and events

The directory is listed over the window. The backend fails a whole page when one row in it cannot be served, so on a failing cursor the harvest finds the nearest cursor below that pages again (a growing step, then a binary search). The rows it cannot reach are recorded as a **gap**, never silently skipped.

Sessions from the harnesses in `CODING_SOURCES` have their events read, at up to 300 pages of 1,000 events each. Beyond that cap a session is marked `censored`. Other sessions stay in the directory only.

## Facts

`features.ts` is the privacy boundary. A raw event goes in and only these come out:

- `files`: the event's `filesTouched`, where the harness records it;
- `touched`: paths recovered from commands and their output, tagged by how they were seen;
- git and gh operation codes, branch names, PR refs, commit shas, and the relay envelope's sender and recipient.

No content string, nor any substring of one longer than an identifier, is ever returned. The receipt lists the withheld fields.

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
- per source: sessions, sessions read, events, informative events, sessions with edit evidence, censored sessions, errors, sessions without a published transcript, and truncated events;
- the request counts by route.

A harvest is `complete` only when it is not capped, the frame has no gaps, and every coding session was read with no errors and nothing censored. A session without a published transcript is counted, not failed.

## Sessions after the storage move

Since 2026-11-05, coding sessions are stored by the 96Bulls Reader (a Supabase directory with ClickHouse events). Cloud's `/sessions` list and `/reflex/sessions/{id}/events` routes still read the old store, so they never list those sessions.

`--via reader-mcp` reads them through the hosted shared-sessions MCP (`<base>/mcp/shared-sessions`). It pages `list_session_summaries` and `get_shared_session` and maps their rows onto the same shapes, so the same reducer and privacy boundary apply.

Compared with the Cloud routes, the MCP does not carry:

- `createdAt`, `parentSessionIds` or `machineIds` on directory rows;
- `filesTouched` on events.

Event content is also capped at 4,000 characters, and each session records how many of its events were truncated. A session's `last_active` is its latest activity, so the directory filter applies only the lower bound of the window.

The MCP serves at most 50 events per call, so a full workspace takes many hours. Bulk read access to the Reader is the better path when it is granted. `mergeHarvests` in the bench joins a Cloud harvest and a Reader harvest of one workspace: per session it keeps the richer directory row, every owner, and the union of events by event id.
