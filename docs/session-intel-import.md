# Session Intelligence trace import

The offline first slice of the enterprise batch path ([session-intelligence.md](proposals/session-intelligence.md), "Enterprise batch path", ticket M4-1). A customer hands us existing agent traces. The converter turns them into a replay snapshot in the M0 format ([session-intel-replay.md](session-intel-replay.md), "Snapshot format"), and the replay harness then runs the same detectors over it unchanged.

It runs offline and stays contained. It uses no network, database or credentials, and reads no environment. It does not touch ingest, the upload path, the read API, migrations or flags. Wiring imports into canonical sessions through the upload path, with relayscrubber at ingest, comes in a later slice. Code: `src/session-intel/import/`. Tests and the synthetic fixtures: `src/session-intel/__tests__/import-*.test.ts` and `src/session-intel/__tests__/fixtures/import/`.

**Authorization.** Running the converter on a customer's traces needs Khaliq's explicit OK and the customer's written authorization, each time. Fixtures, tests, docs and commit text hold only synthetic data. They never hold a real trace, not even a short excerpt.

## Usage

```sh
op read 'op://<vault>/<item>/salt' | bun run src/session-intel/import/cli.ts \
  --format claude-code|codex|generic --in <file-or-dir> --out <dir> \
  --salt-stdin --delete-by <iso> [--org <id> --workspace <id>]
bun run src/session-intel/replay/cli.ts replay --snapshot <dir> --out <eval>/replay
```

- **`--format`** picks one adapter for every `*.jsonl` file under `--in`. To combine harnesses, import each one with the same salt; their projects and repositories hash alike (see Projects below).
- **`--in`** is a file, or a directory walked recursively. The walk skips dot entries, never follows a symlink, and reads `*.jsonl` regular files only.
- **`--out`** must not exist, or must be an empty directory. It must sit outside this repository, and must neither contain `--in` nor sit inside it. The check is the M0 replay CLI's own guard (`realPathOf` and `isInside` in `src/session-intel/replay/cli.ts`). It runs on the real path: the nearest existing ancestor is realpath'd, so a symlink into the repository is refused. A path counts as outside only when its path relative to the repository is `..`, starts with `..` plus a separator, or is absolute, so a directory named `..cache` inside the repository is still inside it.
- **The salt** keys every id hash. With the salt, anyone holding a candidate customer id can confirm which hash it is, so the salt is what de-anonymizes the ids. It is customer key material, and 1Password is its system of record. It is never taken from argv, where it would land in the process list and the shell history, and the documented path never puts it on disk. There is no `--salt` flag. Exactly one of these two is required:
  - **`--salt-stdin`** (the documented path) reads the salt from stdin, piped straight from `op read` as above. It is refused when stdin is a terminal. Input is capped at 4,096 bytes, and the trimmed salt must be 16 to 1024 characters.
  - **`--salt-file <path>`** is a fallback for a salt that already sits in a file. It is not the way to run an import: AGENTS.md says nothing credential-like on disk, so do not write a salt to a file for a run. The file is refused when:
    - it is a symlink (it is opened with `O_NOFOLLOW`) or not a regular file;
    - it is readable or writable by group or others (`mode & 0o077` is not 0);
    - it sits inside this repository;
    - its content, trimmed, is not 16 to 1024 characters (at most 4,096 bytes are read).

  The salt is never written to the snapshot, the mapping file, stdout, stderr or an error message. Use a new salt for each customer and each import.
- **`--delete-by`** is required: a strict ISO-8601 time with a zone, after now and at most 30 days out. It goes into the manifest, and the M0 loader refuses the snapshot after it. **It covers the mapping file too.** The loader cannot enforce that, so the operator deletes both the snapshot directory and `<out>.mapping.json` by that date.
- **`--org` and `--workspace`** name the tenant (defaults `org_import` and `ws_import`). They must match `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`. The snapshot stores them hashed (see Privacy rules); the raw values are only in the mapping file.

What the CLI prints:
- **stdout:**
  - a line of counts: files, lines, malformed and oversize lines, untimed records, future timestamps, dropped paths, truncated facts, sessions and facts dropped outside the window, sessions, and facts by kind;
  - a line with the two output paths, the snapshot directory and the mapping file. These are the resolved real paths: `--out` after its symlinks are resolved, which can differ from what was typed.
- **stderr:** a warning when outliers were dropped (see Times below).

It prints no session ids, source paths, salt or text.
- A bad command line gets a fixed message that never echoes argv, so a salt pasted as an argument by mistake is not printed.
- A failure prints the import's own message, which names flags, bounds and error codes only. Any other error, such as a Node filesystem error whose message would carry a full source path, prints its code alone (for example `failed (EACCES)`).

## Output

- **`<out>/manifest.json` and `<out>/sessions.jsonl`** are the M0 snapshot. `files` holds the sha256 of `sessions.jsonl`. The window runs from the earliest session start to two minutes after the latest activity (one replay tick, so the last fact is visible), and is refused past 120 days. The directory is built under a temporary sibling name and renamed into place, so a reader never sees a partial snapshot. Files are mode 0600.
- **`<out>.mapping.json`**, beside the snapshot and never inside it, is **private to the customer**. It maps:
  - each hashed session id to its source session id, source parent ids and source project name;
  - each hashed path to its repo-relative source path;
  - each event id to its input file (relative to `--in`) and line;
  - the hashed tenant back to the raw `--org` and `--workspace`.

  It is written first, with mode 0600, and is create-only: a temporary file hard-linked into place, so it never replaces an earlier reverse map. An import beside an existing mapping file is refused. If the snapshot write then fails, the new mapping is removed, so a snapshot never exists without its map. A crash between the two writes leaves a 0600 mapping with no snapshot. The rerun then fails closed on it, and the operator deletes the mapping before retrying. With it the customer can find the trace behind an alert, so it is never shared with the snapshot. It holds no message text.

## Privacy rules

1. **No content.** The snapshot holds only ids, timestamps, repo-relative paths, shas and token counts. Message text, tool inputs (patch bodies, commands, commit messages) and tool outputs are read only to find a path or a sha, and are dropped there. `title`, `userId`, `taskRef` and `shortSummary` are null, and `claims` is empty; M1 fills claims later. Each line is built from explicit fields alone and checked by the M0 loader's own `parseSession`, which refuses unknown fields.
2. **Opaque ids.** Session ids and parent ids are salted HMAC-SHA256 values with a per-kind domain: `s_` plus 32 hex. Project, repository and branch names are hashed the same way: `p_` and `r_` plus 24 hex, `b_` plus 16 hex. So are the tenant ids from `--org` and `--workspace`: `o_` and `w_` plus 24 hex. A branch is first put into git's spelling (`refs/heads/` and `origin/` dropped, lowercased), so the detectors' branch comparisons still hold. Event ids are per-session counters (`w1`, `c1`, `o1`, `u1`) and carry nothing from the source. A parent outside the import is still hashed, so its children stay one fleet.
3. **Repo-relative paths.** Backslashes become `/` and repeated slashes collapse. Then:
   - A path containing `://` is a URL, not a file, and may carry credentials: it is dropped.
   - A path with a `..` or `~` segment is dropped.
   - **Home directories.** These spellings are recognized: `/Users/<u>`, `/home/<u>`, `/var/home/<u>`, `/export/home/<u>`, `/mnt/<drive>/Users/<u>`, `C:/Users/<u>` (also written `C:\Users\<u>`), and `/root`.
   - **When the cwd is a home directory itself,** it is not a checkout. Stripping it would keep home-relative paths (`Documents/`, `Desktop/`, dot-directories). So every path, relative or absolute, must be anchored at a source root, or is dropped.
   - **Otherwise,** an absolute path under the session's cwd loses that prefix. Any other absolute path is anchored at its first source-root segment, or dropped. The source roots are the same set as `decisionTopic` in `src/session-analysis/decision-records.ts`: `src`, `packages`, `apps`, `lib`, `libs`, `services`, `crates`, `cmd`, `internal`, `scripts`, `docs`, `supabase`, `flows`, `test`, `tests`, `e2e`, `infra`.
   - A result that starts with a drive letter, `Users`, `home` or `root` is dropped.
   - A result with any segment equal to a home directory's user name, from the path or the cwd, is dropped.
   - A path with control characters is dropped, and so is one longer than 4,096 characters.

   No home directory or user name survives. Dropped paths are counted.

   **Every segment is then hashed** with the salt (HMAC-SHA256, 12 hex, after lowercasing, since the detectors compare paths lowercased). Only a closed set stays in clear, so equal paths still match and the detectors still work:
   - the source roots above, and the migration directory names `migrations`, `migration`, `db` and `migrate`, as directory segments;
   - the hot-path file names, by exact name, plus `package.json`. These are exactly the names `KNOWN_HOT_PATH` in `packages/coordination-detect/src/index.ts` matches: `bun.lock`, `package-lock.json`, `npm-shrinkwrap.json`, `pnpm-lock.yaml`, `yarn.lock`, `cargo.lock`, `poetry.lock`, `composer.lock`, `gemfile.lock`, `_journal.json`, `generated-manifest.json` and `manifest.generated.json`. A test parses `KNOWN_HOT_PATH` and fails if the lists drift. Kept in clear, the detector still skips these files, so a replay raises no false duplicate-work alert on them;
   - `KNOWN_HOT_PATH`'s other form, `generated/*manifest*.json`. The directory `generated` stays in clear only right above a `*manifest*.json` file. That file becomes `<12 hex>.manifest.json`, so it still matches. Anywhere else, `generated` is hashed;
   - a file's extension, only when it is on this fixed allow-list (after lowercasing): `ts` `tsx` `js` `jsx` `mjs` `cjs` `json` `jsonc` `jsonl` `md` `mdx` `rst` `sql` `py` `rb` `go` `rs` `java` `kt` `kts` `scala` `swift` `c` `h` `cc` `cpp` `hpp` `cs` `php` `lua` `dart` `ex` `exs` `sh` `bash` `zsh` `ps1` `yml` `yaml` `toml` `ini` `cfg` `lock` `txt` `html` `css` `scss` `sass` `less` `vue` `svelte` `proto` `graphql` `gql` `tf` `hcl` `dockerfile` `xml` `csv` `ipynb` `svg` `png` `jpg` `gif` `gradle` `mod` `sum`. Any other extension (`.initech`, `.production`, `notes.this is a sentence`) is hashed with the rest of the name;
   - in a migration directory, an all-digit file name of at most 14 digits (`0042.sql`), or a number prefix of 3 to 14 digits with its `_` or `-` (`0042_`, `20261105062150_`, a `YYYYMMDDHHMMSS` stamp). The rest of the name is hashed. So is a longer digit run, and so is a "prefix" with a non-digit in it.

   Depth is kept. A sentence used as a file name, a key-shaped name, a name with whitespace or `@`, and a patch header holding a sentence all come out hashed. For example, `supabase/migrations/0042_add_index.sql` becomes `supabase/migrations/0042_<12 hex>.sql`, so the duplicate-migration detector still keys on `0042`. The private mapping file maps each hashed path back.
4. **Projects.** The project key is the repository name from the remote (`repoNameFromRemote`, shared with the harvester), or else the cwd's basename, lowercased. A home directory itself is never a key. The remote rule never keeps credentials:
   - a `scheme://` URL keeps only its path (the host and any `user[:password]@` are dropped); `[user@]host:path` needs a `/` in its path; anything else is a local path;
   - the query, fragment and `;params` are cut; the last segment, with `.git` dropped, is the name;
   - a remote with whitespace, a control character or a `%` escape, an `@` left in its path, or a name outside `[a-z0-9._-]` gives no name, and the cwd decides.

   So some legitimate remotes now fall back to the cwd, and their key can change from earlier imports: an scp-form remote with no `/` in its path (`myserver:project.git`), a repository name with a space, `+` or a percent escape (Azure DevOps `_git/My%20Repo`), a credentialed URL whose password contains `%`, and a remote ending `/.git`. `projectId` and `repo` are hashes of that key, so a Claude Code session in a checkout named `acme-api` and a Codex session whose remote ends in `acme-api` line up. In the generic format an explicit `projectId` becomes the project key, and `repo` the repository key.
5. **Bounds.** These are the M0 loader's limits:
   - 100,000 sessions;
   - 20,000 facts per list per session (more are truncated and counted);
   - 64 parents;
   - 512-character ids, 4,096-character paths;
   - 4 MiB per snapshot line (a larger session is dropped and counted);
   - 256 MiB for `sessions.jsonl`;
   - a 120-day window.

   On input: 100,000 files, 2 GiB per file, a 32-level walk, 16 MiB per line, and 5,000,000 facts held across the import. Each file is opened with `O_NOFOLLOW | O_NONBLOCK` and checked on the open handle. A file swapped for a symlink after the walk is refused, and a FIFO is refused at once instead of blocking the import. The walk skips FIFOs and symlinks. An oversize line is skipped without being buffered. A line that is not JSON, or that breaks the format, is skipped and counted. No error message carries line content.
6. **Times.** `createdAt` and `lastActivityAt` are the earliest and latest timestamped record of the session, so every fact lies between them, as the M0 loader requires. Outlier timestamps are dropped and counted rather than aborting the import:
   - A timestamp that is not strict ISO-8601 with a zone means the record is skipped.
   - A record stamped more than a day after the import's clock is dropped.
   - When the sessions span more than the 120-day window, the import keeps the latest 120 days in which the most sessions end. A session ending outside them is dropped. A session that started before them loses its earlier facts, and its `createdAt` moves to the window start.

   Each of these is counted, and a warning on stderr names every count: records without a strict ISO-8601 timestamp, records in the future, sessions and facts dropped outside the window, and sessions clamped to its start.

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
- **Code-mode writes.** An outer `exec` call can contain `tools.apply_patch(...)` rather than a separately recorded patch call. Both custom-tool input and function-call `{input}` use the existing production projection's bounded static-patch parser. Literal patch strings and static variables are supported; JavaScript is never executed. Comments, quoted code, interpolated templates and inputs over the projection's 1 MiB cap produce no paths. An explicitly failed call produces no paths. These are extracted tool-call facts, not independent confirmation that every patch reached the filesystem. Calls hidden behind dynamic code, and nested shell commit/revert commands, remain outside this adapter's coverage.
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

The adapters were initially written from public documentation and general knowledge of the formats. A later authorized local native-format audit confirmed the Codex parent field and copied-context boundary below. Tests and committed fixtures remain synthetic; no real transcript is committed. Other format/version limits remain:

- **Claude Code.**
  - Older versions keep subagent (sidechain) records in the parent's file, with no `agentId`. Those records then count as part of the parent session.
  - The `toolUseResult` shape for Bash (`{stdout, stderr}`) may differ between versions.
  - The usage total includes cache reads. A cost-weighted total may need to exclude them.
- **Codex.**
  - The subagent parent link is read from `session_meta.payload.source.subagent.thread_spawn.parent_thread_id`, confirmed in the audited format. A modern child starts with its own metadata, followed by copied parent history whose timestamps have been rewritten to the child's start. When that metadata supplies `agent_path`, facts are suppressed until `thread_settings_applied` names the child's exact thread id and a subsequent native `agent_message` addressed to that exact path carries the `Message Type: NEW_TASK` header. A parent task addressed elsewhere or ordinary message text cannot open the boundary; the encrypted task body is never read. This also keeps copied metadata from replacing the child's cwd. `codexPrefixRecordsDropped` counts discarded pre-task records (including setup), not independently verified duplicate actions.
  - A child explicitly carrying copied-history metadata but lacking a usable path or matching task boundary contributes no facts after its metadata; `codexChildrenWithoutTaskBoundary` and a CLI warning expose that loss. Legacy children without either marker retain their existing behavior, so this does not establish provenance for every older format.
  - Older rollouts had no `{type, payload}` envelope and no per-line timestamp. Such lines take the last timestamp seen (the session metadata's), so their facts can bunch at the start.
  - The shell tool names (`shell`, `shell_command`, `exec_command`, `container.exec`) have changed across versions. The `function_call_output` body is sometimes a JSON string `{output, metadata}` and sometimes plain text; both are handled.
- **The repository root** is not in either trace format; only the cwd is. A session started in a subdirectory of the repository (`<repo>/packages/x`) gets paths relative to that subdirectory and the project key `x`, so it may not line up with a session started at the root.
- **Git parsing** is a rough tokenizer. A `git commit` quoted inside another command's argument can be miscounted. Commits made outside the agent's shell are not seen.

## Known limits

- **`O_NOFOLLOW`** guards only the last component of a path: of each input file, and of `--salt-file`. A symlinked parent directory is followed. The repository checks on `--out` and `--salt-file` resolve the real path, so a symlink cannot point either into the repository. A symlink under `--in` is skipped by the walk.
- **`--salt-stdin`** reads until stdin closes. A pipe whose writer never closes it blocks the import. `op read` closes it.
- **Directories in the path.** Kept-in-clear directory names come only from the fixed lists above. A project's own top-level directory, such as `server/`, is hashed: consistent across sessions, but no longer readable without the mapping file.
- **Segment hash collisions.** The 12-hex segment hash has no collision check. At a million distinct segments, the chance of one silent false match is about 0.2%.
- **Case variants.** The mapping's `paths` keeps only the last raw spelling seen for paths that differ only in case, since those hash alike.

## Follow-ups

- **OpenTelemetry GenAI spans** (`gen_ai.*` semantic conventions) as a fourth adapter, out of scope for this slice.
- **The upload path.** Import into canonical sessions through the upload path, scrubbed by relayscrubber at ingest, and make re-imports idempotent there (M4-1 in full).
- **M4-2.** A batch runner: backfill the analysis and claims, then replay, then the report.
