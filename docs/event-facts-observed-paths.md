# Observed paths on event facts

A tool result that is exactly a `git status --short` listing names the paths its session **saw** as changed in its checkout. Those are observations, not writes, so they are kept in their own field, `observed_paths`, never mixed into `files_touched`.

This field is **dormant**. The schema is ready, but no runtime derives it yet, and no reader or detector reads it. Turning it on, and applying the migration to any real database, each need Khaliq's separate, explicit OK.

## Why

The overlap benchmark (branch `review/claude-overlap-benchmark`, `docs/session-intel-overlap-bench.md` there, "For production") reran its frozen combined ranker with different evidence sources on one workspace's dev split.

| Lane evidence | Dev incidents caught within the alert budget |
|---|---|
| `files_touched`, patch paths, shell writes and `git add` | 0 of 21 |
| The same, plus `git status` and `--stat` listings | 5 of 21 |

Paths seen in status listings are the only evidence that lifted the ranker within its alert budget. Two lines of work listing the same dirty paths **suggests** a shared checkout, or one line picking up another's uncommitted work. It does not establish either without independent checkout or lineage evidence. The 5 hits are exploratory: this field exists so the signal can be measured on live data before anything uses it.

The benchmark's 5 of 21 was measured with `git status` and `--stat` listings together. This field reads `git status` only (see below). How a status-only lane performs is unmeasured, and it is not bounded by that figure either way: at a fixed alert budget, removing evidence can displace negatives as well as positives. It has to be measured on its own.

## Derivation

`src/session-pipeline/observed-paths.ts` defines `observedPaths(content)`. It is pure and bounded: at most 64 paths, each at most 512 characters, from at most 256 KiB of text. A tool result is read only when its whole text is a `git status --short` listing:

- **`git status --short`** (porcelain v1). Every non-empty line is `XY path` or `XY old -> new`, optionally after one `## branch` header. `XY` must be a code git prints: `??`, `!!`, an unmerged pair, or index and worktree states of which at least one is set (two spaces never occur, so indented text is not a listing). The `old -> new` form appears exactly when `XY` holds `R` or `C`. Git quotes any path containing whitespace, a double quote, a backslash or a control character. A quoted path must be well-formed git quoting (printable ASCII and git's own escapes), or the listing is refused. It is not decoded: a line with any quoted operand, a rename's source included, stores nothing, and the rest of the listing still counts. A listing naming an absolute path (`/…` or a drive letter, on either side of a rename or copy, quoted or not) is refused: git prints names relative to the working directory, even for an absolute pathspec. An unquoted path is read only when it is printable ASCII without space, `"` or `\`, which is exactly what default git prints unquoted. Under `core.quotePath=false` git prints other characters raw (a trailing U+00A0, say), which cannot be delimited safely, so such a listing is refused. Lines are never trimmed, so the bytes validated are the bytes stored. A rename contributes its new path.

`git diff --stat` (and `git show --stat`) is not read. It prints names verbatim and pads them with spaces, so a name cannot be recovered from it: trees holding `tail` and `tail ` print byte-identical blocks, and `a => b` may be a rename or a file so named.

Anything else yields nothing: prose (indented or not), a log that merely contains a status-shaped line, or a `--stat` block. The parser reads the result's text alone. It does not match the result to its `git` call, since the staged path sees results and calls in different batches, so the whole-text shape is what keeps it conservative.

`src/session-pipeline/output-facts.ts` builds `EventFactRow.observed_paths` for `tool_result` events, but only when the builders are given `{ observedPaths: true }`. That option is accepted by `outputFactsFromScope`, `outputFactsBatchStep` and `stageOutputFactsChunk`. No runtime passes it yet.

With the option off (the default), rows carry no `observed_paths` key and are byte-identical to before. With it on, empty paths are left out of the fingerprint, so enabling it rewrites only the rows that gain paths. The ClickHouse schema probe (`outputFactsSchema`) requires the `observed_paths` column only when the option is on.

## Storage

- **Postgres:** `supabase/migrations/20261105062400_event_facts_observed_paths.sql`, registry slot 062400.
  - It appends `sessions.session_event_facts.observed_paths text[] NOT NULL DEFAULT '{}'`. A constant default adds the column without rewriting the table.
  - The 64-entry bound is a `CHECK ... NOT VALID`. Validating it would scan every row under the step's exclusive lock, and every existing row already holds `'{}'`. It is enforced for every row inserted or updated afterwards.
  - It replaces `output_facts_reconcile` (045040) and `staged_facts_reconcile_v1` (062140) with identical signatures and ACLs. Their bodies change only to carry the column through. A row without the key stores `'{}'`, and a row whose paths change is updated like any other fingerprint change. A row whose stored paths differ from the incoming ones is also updated when the fingerprint is unchanged: the rollback drops the column but keeps the fingerprints computed with it, so after a down and a re-apply the next facts pass repairs the paths.
  - Lane: `scripts/sessions-function-boundary/lanes/event-facts-observed-paths.ts`.
  - Sentinel: `src/hoopdb/migration-sentinels.ts`, a replacement step.
  - Rollback: `supabase/rollbacks/20261105062400_event_facts_observed_paths.down.sql`, generated by the lane. It restores the exact 045040 and 062140 bodies and drops the column. The values are derived, so a facts pass rebuilds them.
- **ClickHouse:** `output-facts-schema.ts` schema v5 adds `observed_paths Array(String) DEFAULT []` to the event facts table (CREATE and upgrade ALTER). The DDL is owner-applied, as before.

## Not yet

- **Wiring the option to a runtime flag** (the self-host consumer and the search-index pass).
- **Reading the field:** the coordination readers (`coordination-backend.ts`, `postgres/coordination-query.ts`) and an optional `LaneFacts.observed` that no detector reads at first.
- **A detector or ranker input,** once live data has been measured against the benchmark's numbers.

Each lands separately through review.
