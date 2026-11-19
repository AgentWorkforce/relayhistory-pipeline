# Session Intelligence trace import

The offline first slice of the enterprise batch path ([session-intelligence.md](proposals/session-intelligence.md), "Enterprise batch path", ticket M4-1). A customer hands us existing agent traces. The converter turns them into a replay snapshot in the M0 format ([session-intel-replay.md](session-intel-replay.md), "Snapshot format"), and the replay harness then runs the same detectors over it unchanged.

It runs offline and stays contained. It uses no network, database or credentials, and reads no environment. It does not touch ingest, the upload path, the read API, migrations or flags. Wiring imports into canonical sessions through the upload path, with relayscrubber at ingest, comes in a later slice. Code: `src/session-intel/import/`. Tests and the synthetic fixtures: `src/session-intel/__tests__/import-*.test.ts` and `src/session-intel/__tests__/fixtures/import/`.

**Authorization.** Running the converter on a customer's traces needs Khaliq's explicit OK and the customer's written authorization, each time. Fixtures, tests, docs and commit text hold only synthetic data. They never hold a real trace, not even a short excerpt.

## Usage

```sh
bun run src/session-intel/import/cli.ts --format claude-code|codex|generic --in <file-or-dir> --out <dir> \
  --salt <secret> --delete-by <iso> [--org <id> --workspace <id>]
bun run src/session-intel/replay/cli.ts replay --snapshot <dir> --out <eval>/replay
```

- **`--format`** picks one adapter for every `*.jsonl` file under `--in`. To combine harnesses, import each one with the same `--salt`; their projects and repositories hash alike (see Projects below).
- **`--in`** is a file, or a directory walked recursively. The walk skips dot entries, never follows a symlink, and reads `*.jsonl` regular files only.
- **`--out`** must not exist, or must be an empty directory. It must sit outside this repository, and must neither contain `--in` nor sit inside it. The check runs on the real path: the nearest existing ancestor is realpath'd, so a symlink into the repository is refused. A path counts as outside only when its path relative to the repository is `..`, starts with `..` plus a separator, or is absolute, so a directory named `..cache` inside the repository is still inside it.
- **`--salt`** (16 to 1024 characters) keys every id hash. Keep it private, and use a new salt for each customer and each import. The salt is never written anywhere. It does appear in the process list and the shell history.
- **`--delete-by`** is required: a strict ISO-8601 time with a zone, after now and at most 30 days out. It goes into the manifest, and the M0 loader refuses the snapshot after it.
- **`--org` and `--workspace`** set the tenant fields (defaults `org_import` and `ws_import`). They must match `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`.

On stdout the CLI prints counts only: files, lines, malformed and oversize lines, untimed records, dropped paths, truncated facts, sessions, and facts by kind. It prints no ids, paths or text.

## Output

- **`<out>/manifest.json` and `<out>/sessions.jsonl`** are the M0 snapshot. `files` holds the sha256 of `sessions.jsonl`. The window runs from the earliest session start to two minutes after the latest activity (one replay tick, so the last fact is visible), and is refused past 120 days. The directory is built under a temporary sibling name and renamed into place, so a reader never sees a partial snapshot. Files are mode 0600.
- **`<out>.mapping.json`**, beside the snapshot and never inside it, is **private to the customer**. It maps each hashed session id to its source session id, source parent ids and source project name, and each event id to its input file (relative to `--in`) and line. With it the customer can find the trace behind an alert, so it is never shared with the snapshot. It holds no message text.

## Privacy rules

1. **No content.** The snapshot holds only ids, timestamps, repo-relative paths, shas and token counts. Message text, tool inputs (patch bodies, commands, commit messages) and tool outputs are read only to find a path or a sha, and are dropped there. `title`, `userId`, `taskRef` and `shortSummary` are null, and `claims` is empty; M1 fills claims later. Each line is built from explicit fields alone and checked by the M0 loader's own `parseSession`, which refuses unknown fields.
2. **Opaque ids.** Session ids and parent ids are salted HMAC-SHA256 values with a per-kind domain: `s_` plus 32 hex. Project, repository and branch names are hashed the same way: `p_` and `r_` plus 24 hex, `b_` plus 16 hex. A branch is first put into git's spelling (`refs/heads/` and `origin/` dropped, lowercased), so the detectors' branch comparisons still hold. Event ids are per-session counters (`w1`, `c1`, `o1`, `u1`) and carry nothing from the source. A parent outside the import is still hashed, so its children stay one fleet.
3. **Repo-relative paths.** Backslashes become `/` and repeated slashes collapse. Then:
   - A path with a `..` or `~` segment is dropped.
   - An absolute path under the session's cwd loses that prefix.
   - Any other absolute path is anchored at its first source-root segment, the same set as `decisionTopic` in `src/session-analysis/decision-records.ts` (`src`, `packages`, `apps`, `lib`, `libs`, `services`, `crates`, `cmd`, `internal`, `scripts`, `docs`, `supabase`, `flows`, `test`, `tests`, `e2e`, `infra`), or dropped.
   - A result that starts with a drive letter, `Users`, `home` or `root` is dropped.
   - A result with any segment equal to the user name of a home directory in the path or the cwd (`/Users/<u>`, `/home/<u>`, `C:/Users/<u>`) is dropped.
   - A path with control characters is dropped, and so is one longer than 4,096 characters.

   No home directory or user name survives. Dropped paths are counted.
4. **Projects.** The project key is the repository name: the last segment of the remote URL with `.git` dropped, or else the cwd's basename, lowercased. A home directory itself is never a key. `projectId` and `repo` are hashes of that key, so a Claude Code session in a checkout named `acme-api` and a Codex session whose remote ends in `acme-api` line up. In the generic format an explicit `projectId` becomes the project key, and `repo` the repository key.
5. **Bounds.** These are the M0 loader's limits:
   - 100,000 sessions;
   - 20,000 facts per list per session (more are truncated and counted);
   - 64 parents;
   - 512-character ids, 4,096-character paths;
   - 4 MiB per snapshot line (a larger session is dropped and counted);
   - 256 MiB for `sessions.jsonl`;
   - a 120-day window.

   On input: 100,000 files, 2 GiB per file, a 32-level walk, 16 MiB per line, and 5,000,000 facts held across the import. Each file is opened with `O_NOFOLLOW` and checked on the open handle, so a file swapped for a symlink after the walk is refused. An oversize line is skipped without being buffered. A line that is not JSON, or that breaks the format, is skipped and counted. No error message carries line content.
6. **Times.** Every timestamp must be strict ISO-8601 with a zone, or the record is skipped. `createdAt` and `lastActivityAt` are the earliest and latest timestamped record of the session, so every fact lies between them, as the M0 loader requires.

## Formats

### Claude Code JSONL

Transcripts are `<sessionId>.jsonl` files, with subagent transcripts at `<sessionId>/subagents/agent-<id>.jsonl`. Each line is a record `{type, uuid, parentUuid, sessionId, timestamp, cwd, gitBranch, isSidechain, agentId?, message: {id, role, content[], usage}, toolUseResult?}`.

- **Session.** The session is `sessionId`, or the file name when that is missing. A record with `isSidechain: true` and an `agentId` (or a file named `agent-<id>.jsonl`) is its own session, `<sessionId>#agent-<id>`, with `sessionId` as its parent.
- **Writes.** These come from `tool_use` blocks in assistant content: `Edit`, `Write` and `MultiEdit` use `input.file_path`, and `NotebookEdit` uses `input.notebook_path` (or `file_path`). Paths are relativized against the record's `cwd`. Each write carries the record's `gitBranch`. Tool-use ids are deduplicated.
- **Commits.** A `Bash` `tool_use` whose `input.command` runs `git commit` gives a commit. Its sha comes from git's summary line (`[branch 1a2b3c4] ...`) in the matching `tool_result` content, or else from `toolUseResult.stdout`. Without one, the sha is `unknown`, which the detectors ignore.
- **Reverts.** `git revert <sha>` gives an outcome `{key: <sha>, reverted: true}`. With no sha, as in `git revert HEAD`, the key is `unknown`.
- **Usage.** It comes from `message.usage` on assistant records: `input_tokens + output_tokens + cache_creation_input_tokens + cache_read_input_tokens`, counted once per `message.id`, because Claude Code repeats a message's usage on each content block's record.

### Codex rollout JSONL

Rollouts are `rollout-<time>-<uuid>.jsonl` files. Each line is `{timestamp, type, payload}`.

- **`session_meta`** gives the session id (`payload.id`, or the uuid in the file name), `cwd`, `git.repository_url` and `git.branch`.
- **`turn_context`** updates the `cwd`.
- **`response_item` writes.** These come from `apply_patch`, whether a `custom_tool_call` with the patch as `input`, a `function_call` with `{input}`, or a shell call whose command is `["apply_patch", patch]` or a heredoc. The paths are the patch's `*** Add File:`, `*** Update File:`, `*** Delete File:` and `*** Move to:` headers, relative to the call's `workdir` or the session cwd.
- **`response_item` git commands.** Shell calls (`function_call` named `shell`, `container.exec`, `shell_command`, `exec_command` or `local_shell`, and `local_shell_call`) are scanned for `git commit` and `git revert`. A commit's sha comes from the matching `*_output` item, either the `output` string or the `output` inside its JSON.
- **`event_msg` usage.** `token_count` gives `info.last_token_usage.total_tokens`, or else the increase in `info.total_token_usage.total_tokens`.

### Generic JSONL

This is the format customers can export to. Each line is one event:

```json
{"sessionId": "run-17", "ts": "2026-11-01T09:05:00Z", "kind": "write", "files": ["src/a.ts"], "commit": "1a2b3c4",
 "outcome": {"key": "1a2b3c4", "reverted": true}, "tokens": 1200, "parentSessionId": "run-1",
 "projectId": "billing", "repo": "acme/billing", "branch": "feat/x", "cwd": "/work/billing"}
```

- **Required fields.**
  - `sessionId`: a string of at most 512 characters.
  - `ts`: a strict ISO-8601 time with a zone.
  - `kind`: `[A-Za-z0-9_.-]{1,64}`. Any value is allowed (for example `start`, `write`, `commit`, `outcome` or `usage`). It is validated but never copied: every generic write has the kind `write`.
- **Optional fields.**
  - `files`: up to 1,000 paths.
  - `commit`: a sha.
  - `outcome`: `{key, reverted}`, where a revert's key is the reverted sha.
  - `tokens`: a non-negative integer.
  - `parentSessionId`, `projectId`, `repo` and `branch`.
  - `cwd`: an extension that lets absolute paths in `files` be relativized.
- **Everything else** is ignored and never copied. Put no message text in the export.

Every event extends the session's time range. A line with a wrong type in any listed field is skipped and counted.

## What I was not sure of

The adapters were written from public documentation and general knowledge of the formats, and tested only on synthetic fixtures. No real transcript was read. These points are unconfirmed:

- **Claude Code.**
  - Older versions keep subagent (sidechain) records in the parent's file, with no `agentId`. Those records then count as part of the parent session.
  - The `toolUseResult` shape for Bash (`{stdout, stderr}`) may differ between versions.
  - The usage total includes cache reads. A cost-weighted total may need to exclude them.
- **Codex.**
  - The subagent parent link is read from `session_meta.payload.source.subagent.thread_spawn.parent_thread_id`. That field path is not confirmed.
  - Older rollouts had no `{type, payload}` envelope and no per-line timestamp. Such lines take the last timestamp seen (the session metadata's), so their facts can bunch at the start.
  - The shell tool names (`shell`, `shell_command`, `exec_command`, `container.exec`) have changed across versions. The `function_call_output` body is sometimes a JSON string `{output, metadata}` and sometimes plain text; both are handled.
- **The repository root** is not in either trace format; only the cwd is. A session started in a subdirectory of the repository (`<repo>/packages/x`) gets paths relative to that subdirectory and the project key `x`, so it may not line up with a session started at the root.
- **Git parsing** is a rough tokenizer. A `git commit` quoted inside another command's argument can be miscounted. Commits made outside the agent's shell are not seen.

## Follow-ups

- **OpenTelemetry GenAI spans** (`gen_ai.*` semantic conventions) as a fourth adapter, out of scope for this slice.
- **The upload path.** Import into canonical sessions through the upload path, scrubbed by relayscrubber at ingest, and make re-imports idempotent there (M4-1 in full).
- **M4-2.** A batch runner: backfill the analysis and claims, then replay, then the report.
