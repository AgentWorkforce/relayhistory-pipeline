# Session analysis: when canonical S3 evidence is complete

The session analysis job (`src/session-analysis`, `loadSessionSource` in `evidence.ts`) reads one
canonical S3 snapshot of a session and never reads Neon. A session whose turns or events were
written to Neon may have rows that the canonical snapshot lacks. Analyzing it would publish an
analysis built from partial evidence. So unless one of the conditions below holds, the job fails
with the non-retryable `session_evidence_incomplete` ("legacy evidence coverage is unknown").

| | Condition | Scope | Default |
|---|---|---|---|
| (a) | The snapshot carries the legacy export's coverage marker (`canonical-legacy.ts` `legacy_scope`): Neon's turns, links and outcomes for the scope were exported into S3. | per session | on |
| (b) | `SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS=true`: the operator asserts that no session has Neon-only evidence. | global | off (the Neon-exit lead rejected it as a production bypass) |
| (c) | The never-in-Neon proof: no manifest of the snapshot is Neon-derived, and `sessions.session_analysis_neon_free` (046000) holds. That needs the sealed frozen copy, the sealed Neon census, every pointer created after the seal, and no frozen or census row for the session. | per session | on, and false for every session until the census is sealed |
| (d) | The scoped cutover proof below, enabled by `SESSION_ANALYSIS_CUTOVER_PROOF=on`. | per org, then per session | off |

The conditions are evaluated in order. If any one holds, the session is analyzed. (d) is
consulted only when (a), (b) and (c) have all failed and the switch is on. With the switch off, or
set to anything other than exactly `on`, behaviour is identical to before 062120.

## (d) The scoped cutover proof

The Neon-exit lead's rule: a session is Neon-free when every one of its turns and events is in the
canonical S3 snapshot, and its first-seen time is after its org's recorded S3 cutover. Sessions
older than the cutover stay blocked until the census seal exists, which is condition (c).

**Activation blockers.** The switch stays off until all of these are true:

1. `20261105062120` and `20261105062130` are both applied. 062130 puts the proof's database inputs
   into the source revision (below). Without it a verdict flip is not seen by reuse or publication.
2. The pointer-move fix has landed and is deployed in the session pipeline. A move (the start date
   changed, so the `d=` key changed) now deletes the old row and inserts the new one inside ONE
   transaction: the publication transaction, or `inTransaction` without one (`src/hoopdb/index.ts`
   `rekeyFrom`, called from `src/session-pipeline/pointer.ts`). The new row keeps
   `least(old, new)` `created_at`, and a failure in between leaves the old row and its time intact.
   **The recorded `cutover_at` must not be earlier than this fix's deploy time in that pipeline.**
   Moves before the fix may have raised first-seen. The deploy time is **operator-attested**: the
   recorder requires `--pointer-fix-deployed-at <iso>` and refuses (`cutover_before_pointer_fix`) an
   effective cutover before it. It is stored in `org_s3_cutover.pointer_fix_deployed_at`, and a
   CHECK (`org_s3_cutover_after_pointer_fix`) refuses any row or correction whose effective
   cutover precedes it. A move whose re-key is skipped (manifest gone, publication retired,
   operator-erased) throws the retryable `pointer_move_skipped` instead of reporting `moved`.
3. The worker that passes `legacyDroppedReason` through, and fails (d) closed on it, has landed and
   is deployed.
4. The operator index `supabase/pending/20261105062120_session_index_org_canonical.sql`
   (`CREATE INDEX CONCURRENTLY`, no slot) has been built. It serves first-seen's canonical-id
   lookup over live and tombstoned rows; si_session serves the physical-id lookup. Without it the
   lookup reads the org's pointers: slower, never different.
5. Khaliq has given his explicit OK, in his own words, for each apply, each recorded cutover, the
   index and the switch.

**The cutover is explicit.** `sessions.org_s3_cutover` (migration `20261105062120`) holds one
row per org, changed only through the audited correction path. An operator records it. It is never inferred from the fence-authority
marker or from any other state. Neon still receives writes for every org that is not forwarded:
the cloud turns route writes `conversation_turns` to Neon unless the org's `TURNS_WRITE_PATH` is
`forward`. So the row attests both Neon write paths closed, each with its own time:

- `uploads_cutover_at`: the org's fence-authority marker is written and its Worker HTTP admission
  has stopped, both at or before this time;
- `turns_cutover_at`: the org's turns are no longer written to Neon (turns forwarding is on), at
  or before this time.

`cutover_at` is generated as the later of the two. It is the only time the proof compares
against. An org without turns forwarding cannot be recorded.

**The snapshot half (worker).** Before it asks the database, the worker requires all of the
following:

- the snapshot exists, and its projection is complete (the producer snapshot already refuses
  `canonical_projection_incomplete` and a pointer set that moved during the read);
- `neonDerived === false`: no manifest carries a Neon export marker or a backfill epoch;
- `legacyRowsMerged === false`: no exported Neon row was merged in (an absent flag refuses);
- no `legacyDroppedReason`: if invalid legacy evidence was dropped from the snapshot, (d) refuses;
- every turn and event has a parseable time. Their earliest time is sent to the database as
  `earliestEvidenceAt`.

The canonical snapshot has no separate "every turn is here" or EOF marker. These checks are the
completeness signal it exposes: an entirely canonical scope that was read whole and did not move
during the read.

**The database half.** `sessions.session_analysis_cutover_free(org, workspace, harness, session,
earliest_evidence_at)` is SECURITY DEFINER. It returns true only when all of these hold:

1. the org has a cutover row;
2. the scope (org, workspace, harness, and the session matched as the read API matches it:
   canonical id, else raw id) has a live pointer;
3. the session's first-seen time is strictly after `cutover_at + interval '1 hour'`;
4. `earliest_evidence_at` is not null and is strictly after `cutover_at + interval '1 hour'`.

The 1 hour is a fixed clock-skew margin in the function itself. Both clocks are the client's: **the
proof rests on client clocks being within that margin.** A client clock running more than an hour
ahead could place an older session after the cutover. Exactly `cutover_at + 1 hour` is false;
`cutover_at + 1 hour + 1 microsecond` is true. Operators add their own margin on top when they
choose the recorded times (see the runbook).

Any doubt makes it false. It reads only sessions-pipeline tables, never Neon.

**First-seen.** First-seen (`sessions.session_analysis_first_seen_v1`) is the least
`session_index.created_at` over every pointer anywhere in the org (any workspace, harness or
machine, live and tombstoned) whose physical id OR canonical id is one of the session's ids. The
session's ids are the scope's physical and canonical ids plus the requested id. It runs as two
lookups: by physical id (served by si_session) and by canonical id (served by the operator index in
blocker 4).
`created_at` is the session's first activity from its first manifest. Every upsert keeps it with
`least()`. A pointer *move* (the start date changed, so the `d=` key changed) deletes the row and
re-inserts it from the new manifest, in one transaction that carries `least(old, new)` (blocker 2). First-seen can still change: it FALLS when an older pointer of the session
appears (in any workspace), and it can rise if an erase hard-deletes a row. Either change moves the
source revision (062130).

First activity is the right clock, rather than the pointer's insert time, for this reason. Neon
receives a session's turns when the client uploads them, and that is at or after their activity.
If a session's first activity is after both cutovers, every upload, including every turn write,
happened after both Neon write paths were closed for its org. A pointer's insert time cannot
establish that: an old Neon session whose first S3 pointer lands after the cutover would pass.
The snapshot's earliest evidence time is checked as a second, independent clock. Evidence older
than the cutover plus the margin refuses even when the pointer disagrees.

Accepted by the Neon-exit lead: first-seen as the minimum over the session's ids, tombstoned
pointers included, bounded by the snapshot's evidence time plus the margin; and `org_id` as text,
as `session_index.org_id` is.

**The verdict can flip, so the source revision binds it (062130).** The verdict is not monotone. It
goes from true to false when an older pointer of the session appears in another workspace, or when
a correction moves `cutover_at` later. `20261105062130` replaces `session_analysis_source_revision`
(046000). For an org with a cutover row it appends `cutover_at`, first-seen and the database
verdict to the revision tuple. Any change that could flip the verdict therefore moves the revision.
An analysis made before the change is then neither reused (`analysis_request_v1`) nor published
(reserve and complete compare the revision). For an org without a row the revision is
byte-identical to 046000's, so applying 062130 invalidates no existing analysis. Recording an org's
cutover moves the revision of every session in that org once. The evidence half (the snapshot's
earliest time) is bound through the manifest etags the tuple already holds.

Nothing is retracted from storage. A result that was already published stays readable. It is
simply not reused for the next request once the revision has moved.

**Guards on the row.** A trigger refuses a plain UPDATE, a DELETE and a TRUNCATE, so a row changes
only through the audited correction path. A test pins this. `recorded_at`, and a history row's
`superseded_at`, are always the database clock: a BEFORE INSERT trigger overwrites any value the
writer sends.

**The switch.** `SESSION_ANALYSIS_CUTOVER_PROOF=on` is read by `analysisConfig`
(`src/session-analysis/worker.ts`). It is not rendered by the stack, so it stays off until an
operator sets it, in a reviewed change, on the session-pipeline Lambda.

If the switch is on while the migration is missing (for example after a rollback), the proof's SQL
errors. The job fails with the retryable `analysis_processing_failed`. It is retried, never
completed without the proof.

**Jobs that failed before the cutover row existed are not retried.** A job that failed
`session_evidence_incomplete` (non-retryable) stays failed after the org's row lands or the switch is
turned on. Re-request the analysis after the cutover row lands: the new request admits a new job,
and that job evaluates (d).

## Recording a cutover (runbook)

Khaliq-gated. Applying `20261105062120` or `20261105062130` to a real database, recording or
correcting a cutover for any real org, and turning `SESSION_ANALYSIS_CUTOVER_PROOF` on each need his
separate, explicit OK in his own words. **Even a dry run connects to the pinned production database
as the migration owner** (BEGIN, counts, ROLLBACK), so any run, dry or not, needs his own words.
Org ids belong in the private ops repository, not here.

Preconditions, all of which must be true and verified before recording:

1. The org's fence-authority marker is written, and its Worker HTTP admission has stopped.
   `--uploads-cutover-at` must be at or before both of those times.
2. The org's turns are forwarded (`TURNS_WRITE_PATH` `forward` for the org on the cloud turns
   route), so no turn for the org is written to Neon. `--turns-cutover-at` must be at or before the
   time forwarding took effect. **Without turns forwarding the org cannot get a cutover row.**
3. Both times are in the past, and are written as explicit ISO times with `Z` or an offset.
4. Operator margin: record the latest time you can prove for each condition, never an estimate.
   This is in addition to the function's fixed 1 hour margin.
5. `--pointer-fix-deployed-at` is the operator-attested time the pointer-move fix (blocker 2) was
   deployed in the org's session pipeline. Verify it from the deploy record, not from memory. It is
   stored with the row, and the effective cutover must be at or after it.

Run from a checkout of this repository. The owner URL comes from 1Password into the environment
only, never into a file:

```sh
SUPABASE_MIGRATION_OWNER_URL=... bun scripts/record-org-s3-cutover.ts \
  --org <org id> \
  --uploads-cutover-at 2026-11-20T10:00:00Z \
  --turns-cutover-at 2026-11-20T11:00:00Z \
  --recorded-by <operator> \
  --pointer-fix-deployed-at 2026-11-19T10:00:00Z \
  --attest-uploads-fenced --attest-turns-forwarded \
  [--note "<ticket or reason>"]
```

The default is a dry-run: it writes nothing and prints counts only, for example
`{"mode":"dry-run","existing":0,"inserted":0,"sessionsAfter":12,"sessionsAtOrBefore":340}`.
`sessionsAfter` counts the org's sessions whose first-seen time is after the effective cutover plus
the 1 hour margin (they become eligible). The counts group pointers by physical session id only, so
they are approximate: the proof itself also matches canonical ids. `sessionsAtOrBefore` counts the sessions that stay blocked until the census
seal. Review the counts, then repeat the command with `--execute` to insert the one row. The script
connects as the pinned migration owner (`requireReleaseOwner`), because the table is owner-only.
It never prints an org, workspace or session id. On error it prints only a fixed code, such as
`cutover_turns_not_attested`, `cutover_time_future` or `cutover_already_recorded`.

**Corrections.** A trigger refuses DELETE and TRUNCATE for every role, the owner included, and
refuses every UPDATE except one made through the owner-only `sessions.org_s3_cutover_correct_v1`.
That function copies the prior row into the append-only `sessions.org_s3_cutover_history`, then
writes the new times, `recorded_by`, `recorded_at` and note. The script runs it with `--correct`:

```sh
SUPABASE_MIGRATION_OWNER_URL=... bun scripts/record-org-s3-cutover.ts --correct \
  --org <org id> --uploads-cutover-at <iso> --turns-cutover-at <iso> --recorded-by <operator> \
  --pointer-fix-deployed-at <iso> --attest-uploads-fenced --attest-turns-forwarded [--note "<reason>"] [--execute]
```

- **Later (stricter) is the only routine correction.** The script computes the new `cutover_at` (the
  later of the two new times) and reports `direction` (`later`, `unchanged` or `lowered`). A dry-run
  writes nothing. A correction must invalidate what was published on the old verdict. With 062130
  applied this is automatic: `cutover_at` is in the source revision, so every analysis of the org
  made before the correction stops being reused or published. Without 062130, do not correct.
- **Earlier needs Khaliq's explicit approval.** A lowering is refused (`cutover_lowering_not_approved`)
  by the script, and the function and trigger refuse it too, unless `--khaliq-approved-lowering` is
  passed. With the flag, the script prints a loud warning, and the history row records
  `lowering_approved = true`. `--khaliq-approved-lowering` is a flag name, not an approval. Any real
  use needs Khaliq's own words for exactly that org and those times.
- **Deleting a row is not in the script.** It is a runbook-only owner action that needs Khaliq's
  explicit approval: in one reviewed owner transaction, disable `org_s3_cutover_immutable`, delete
  the row, and re-enable the trigger. That leaves a DDL trail. Export the row and its history
  first.

**Rollback.** Turn the switch off first. Roll back `20261105062130` first: it restores 046000's
revision body exactly. The `20261105062120` rollback refuses while 062130 is applied, and refuses
while any cutover or history row exists.
