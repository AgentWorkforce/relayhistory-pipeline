# Legacy canonical evidence (conversation_turns, session_links, session_outcomes)

These three relayhistory tables exist only in Neon. They are exported once, read-only, into canonical S3 objects by the one-shot runner `bun run migration:legacy-export` (`src/migration/legacy-export/`). The operator steps are in [runbooks/legacy-export.md](../runbooks/legacy-export.md).

The old continuous-exporter opt-in `S3X_INCLUDE_LEGACY_SOURCES` was never enabled, and it is now **retired**: `neon-s3-sync` refuses to start with it set. It mixed legacy rows into ordinary delivery manifests, and that caused gates 1-3 and 8.

## Layout and records

- **One manifest per scope.** Each (org, workspace, source, session) scope gets one manifest on the dedicated machine id `rh-legacy-tables-v1` (`RELAYHISTORY_LEGACY_MACHINE_ID`). The key is `96bulls/{org}/v1/ws=…/m=rh-legacy-tables-v1/h={source}/d=YYYY/MM/DD/s=…/manifest.json`.
  - That is the existing canonical layout, which is already org- and date-scoped. A per-plan retention TTL can be a prefix/date rule later without a new layout.
  - `d=` is the start date of the scope's ordinary unit when one exists. Otherwise it is the date of its oldest legacy row.
  - Ordinary manifests are never touched.
- **Exporter-owned.** The manifest carries the normal export marker, raw owner `migration:{grant}` and a single `initial` epoch.
- **Pointer.** The existing S3-event session-pipeline Lambda indexes the manifest into `sessions.session_index` (harness = source, the scope's session id), as it does every export. Readers find it only through that pointer.
- **Records.**
  - `legacy_conversation_turn`, origin `legacy:conversation_turns`, record `legacy:conversation_turns:{uuid}`. The payload is the Neon row (`to_jsonb`), with `source` = the attributed public source and `legacy_stored_source` = the stored column.
  - `legacy_session_link`, origin `legacy:session_links`, record `legacy:session_links:{bigint id as text}`.
  - `legacy_session_outcome`, origin `legacy:session_outcomes`, record `legacy:session_outcomes:{commit_sha}`.
  - One `legacy_scope` coverage marker (origin `legacy:scope`, record `legacy:scope:coverage`, `tables` = all three). An empty table is still covered. Without the marker, readers report coverage unknown.
- **Strings.** Long strings are bounded with the `[neon-s3-sync: truncated …]` note, then scrubbed by `sealRawChunk`, like any export.
- **Turn attribution.** Turns use relayhistory's public rule (`src/shared/legacy-turn-source.ts`, parity-tested against 0074 `selected_session_turn_ids`):
  - events are counted in the turn's own workspace. That is relayhistory's workspace-scoped read rule, and canonical reads are always workspace scoped.
  - "unbound" turns have no attribution and no event source at all. They are counted and listed (checkpoint table `unbound`).
  - An unbound turn is still exported inside any attributed scope of the same session whose public replay includes it, exactly as relayhistory's `?source=` read does.

### Live turns (PR 1)

Live relay-session turns are separate records:
- kind `conversation_turn`;
- origin `relayhistory:conversation_turns:v1`;
- `srv_` machines from the buffered ingest (`src/session-ingest/turns.ts`).

They never contend with legacy records: the kinds differ, and the legacy fold keys include the kind.

The read-side merge (`src/session-read-api/canonical-turns.ts`) works per Neon row key (workspace, stored source, turn_index):
- a live record wins over a legacy row;
- among live records, the highest revision wins.

This is exact only while the writer never goes back from dual/forward to Neon-only after the export. If it does, re-run the export with `--refresh`, and re-verify.

## Activation gates (adversarial review of #176, 2026-11-05): status

| Gate | Problem | Fix | Test |
|---|---|---|---|
| 1 | One unattributable turn threw `legacy_source_unbound` for a whole page or group. That stopped discovery for every org and crash-looped the sweep. | Discovery is per table and per org. An unattributable turn is counted as `unboundTurns` and recorded in the checkpoint; it never throws. | `canonical-legacy-source.test.ts` "gate 1" |
| 2 | Legacy kinds failed the seed proof. Legacy lines were appended into ordinary manifests and `includeLegacyUnits` moved `firstMs`, so ordinary keys changed. | Legacy evidence goes into its own legacy-machine manifest; ordinary units and keys are unchanged. `seed-proof.ts` acknowledges (does not seed) legacy kinds in exported manifests only. | "gate 2" (both files), `seed-proof-legacy.test.ts` |
| 3 | `forceChanged` re-exported every legacy group each sweep, and discovery re-evaluated a global three-table UNION per page. | The continuous path is retired. The one-shot runner pages each table on its own unique-key prefix, inside one org, with a `(table, org)` keyset cursor. A scope whose Neon fingerprint (count, key md5, whole-row md5, max time) is unchanged is skipped without streaming or writing. | "gate 3" |
| 4 | Legacy records skipped acceptance contention and owner binding. A same-scope forged or stale copy could replace content or silently empty the rows. | `canonical-projection.ts` trusts legacy records only from exactly one manifest per scope that is: exporter-owned (marker grant = raw owner `migration:{grant}`), a single `initial` epoch, on `rh-legacy-tables-v1`, and holding only legacy records. Anything else sets `legacy_provenance` and refuses the read (`canonical_projection_incomplete`). | "gate 4", `canonical.test.ts` "provider refuses legacy records carried by a live delivery manifest" |
| 5 | The attribution helper hard-coded event workspace = turn workspace, unlike the oracle's `p_event_workspace`. | `EventWorkspace` mode: `"turn"` (default; RHC workspace-scoped), `"any"` (RHC unscoped), or `"$n"` (exactly the oracle's `COALESCE(e.workspace_id,'') = $n`). | `legacy-turn-source.test.ts` "gate 5" (oracle parity with differing workspaces) |
| 6 | Schema-legal values (empty `link_ref`, negative `turn_index`) threw a plain Error and failed the whole read (500). | Any int4 `turn_index` and an empty `link_ref` are accepted. A row the schema could not hold is counted in `rejected`, and coverage becomes unknown (null). A scope mismatch is a `CanonicalReadError` (a fallback, not a 500). Live turns use the same error class. | `canonical-legacy.test.ts` "gate 6" ×3 |
| 7 | The eraser counted legacy rows unconditionally. | Fixed earlier: `S3X_ERASER_RECOGNIZES_LEGACY`, default false. | existing |
| 8 | Legacy-only manifests had user `unknown` and machine `legacy:records`. | Machine `rh-legacy-tables-v1`. The user is, in order: the scope's delivery user (`convergence_events.user_id`), the outcome `user_id`, then a turn `session_owner` that is a user rather than a harness label. Scopes with none are counted as `userUnknown`. | "gate 8" |
| 9 | The tiebreak was byte order versus Postgres collation, and the read worker ignored `order=desc` for legacy tables. | `textOrder` is UTF-8 byte (= code-point = `COLLATE "C"` = Neon C.UTF-8) order, proven against Postgres. The worker sorts legacy tables in Neon key order (`sortLegacyRows`: turns by the public tuple, links by bigint id, outcomes by commit_sha) and reverses them for `desc`. | `legacy-turn-source.test.ts` "gate 9" ×2 |

### Still open (not code; operator or follow-up)

- **Pointer proof.** Confirm on staging, or after the first canary org, that each legacy manifest got its `session_index` pointer (verification query in the runbook).
- **Erasure after export.** The `neon-s3-erasure` job only erases prefixes in the continuous exporter's own state, so it does not see legacy manifests. A session erased after its legacy export needs its legacy prefix erased too: either a follow-up that teaches the eraser the legacy checkpoint, or a manual erasure run per fenced scope. The runner never exports a scope that is fenced at export time.
- **Turns written after the export.** Until relayhistory's turn writer is forward-only (PR 1), Neon keeps receiving turns. Re-run `--export --refresh` (cheap: unchanged scopes are skipped by fingerprint) after the writer flips, then `--verify`.
- **Phantom directory rows.** A turn-only relay session becomes a `session_index` row. The directory list should exclude legacy-only (`rh-legacy-tables-v1`-only) sessions before `LIST_BACKEND=supabase`.
