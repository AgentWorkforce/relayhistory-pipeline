# Wave 1 (two orgs): hold lifecycle plan

Code and design only. Nothing here is approved to run. Installing a hold, dispatching any execute against
production, and enabling `marker-execute` each need Khaliq's explicit OK in his own words. The org names and ids are
not recorded here; the run names them only through the `BACKLOG_OPS_ORGS` secret.

Branch: `review/0005-hold-lifecycle`, stacked on `1bce290b` (the frozen minors head). Tooling: `hold-dry`,
`hold-install-execute`, `hold-seal-execute`, `hold-rollback-execute` (`src/session-ingest/hold-lifecycle.ts`,
`scripts/backlog-hold.ts`). Every execute is held in code (`BACKLOG_OPS_HOLD_EXECUTE_ENABLED = false`).

## Sequence per org

1. `hold-dry` (read only). It shows that no hold is live, nothing is pending and no canonical claim is open.
2. `hold-install-execute` (binds that dry). `_install` and the open-canonical-claim check run in one transaction
   under the org key; an open claim rolls the install back. **From here the org's admission is refused (RH042).**
3. The FULL `backfill-execute`, dispatched **after** the install committed.
4. `hold-dry` again (after the FULL), then `hold-seal-execute` (binds that dry, the FULL and the WAVE id over the
   held epochs). `_seal` and 062340's `_bind` commit together, with the digest over the FULL receipt's exact bytes and
   the FULL's start. **From here publication for the org is refused too.**
5. The catch-up and the marker chain, as `docs/backlog-ops-workflow.md` "Marker publication" describes. This is not
   allowed live until the release gate below is met.
6. One of:
   - `hold-rollback-execute`, before any marker attempt. Admission and publication resume.
   - `bridge-execute` (`bridge_complete`) after the attempt, which is held and blocked (see the release gate).

## The ordering conflict

062340 (`_bind`, lines 360-363) requires `installed_at < FULL start <= sealed_at`. The two-phase cutover recorded
earlier ran the FULL while the org was still live and stopped admission only for the catch-up. That FULL can never
bind to a hold. So under this plan the FULL runs with the org's admission already refused.

Admission is refused from step 2 to the release: the whole FULL (3-5 hours for a scoped run), the seal, the
catch-up, the marker chain and the bridge. This needs owner disposition before any live install.

## How the outage is measured

- **Per org, exactly:** `released_at - installed_at` in `sessions.ingest_org_admission_holds`. The hold history is
  immutable, so the record can't be edited afterwards. The synthetic run asserts it
  (`hold-lifecycle.pglite.test.ts`, "synthetic wave 1").
- **Refused attempts:** each refused upload logs `route: ingest.admission_hold_alert` with
  `error_code: ledger_org_admission_held` (the Worker's structured log). Count them per org over the window. The
  synthetic run counts them directly (every upload during the hold gets RH042 and writes nothing).
- **Resumption:** the first accepted upload after the release, and the publication check
  (`ingest_org_admission_hold_check(org, 'publication')`) passing again. Records from before the hold and after the
  release are both present.

## Alarms and deadlines (unchanged, explicit)

- **Sealed over 60 min:** alarm to `BACKLOG_OPS_HOLD_ALARM_OWNER`. Publication is refused while sealed, so the org's
  search stalls.
- **Marker attempted over 24 h without `bridge_complete`:** alarm.
- **The catch-up a marker binds:** under 3 hours old. Its FULL: under 24 hours.
- **A per-org retry's previous run:** under 3 hours.

## Release gate (a hard precondition before any live marker attempt)

After a marker attempt the only release is `bridge_complete`, and an empty initial erase set does not guarantee it
later: an erase arriving during the hold makes the empty-set release refuse (proven in
`hold-lifecycle.pglite.test.ts`). Releasing with a non-empty set needs both of these:

| Dependency | Owner | State |
| --- | --- | --- |
| Durable, request-bound session erase manifest (slot 20261105062360) | hard-delete-owner (design) | not built; every session erase replays to `session_manifest_missing` |
| User-erase generation boundary proof (`bytes_verified` alone does not close it) | no confirmed producer (artifact lane) | open; every user erase replays to `erasure_generation_unproven` |
| The bridge consumer (`bridge-execute`) | sessions-backlog-integration | held in code |

So without those, a live marker attempt can leave an org held, with admission and publication stopped,
indefinitely. layered-sessions-pipeline does not accept that as an implicit outcome. Wave 1 runs end to end only
after a demonstrated release for both non-empty erase cases.

## Rollback criteria (before any marker attempt)

Roll back (`hold-rollback-execute`) if any of these happens:
- the FULL or the catch-up fails, or its retry does not complete within the freshness windows;
- the seal alarm fires and the marker chain is not ready;
- any org's evidence or attestation refuses at `marker-dry`;
- the owner calls it.

The rollback refuses after a marker attempt, in the same transaction that reads the hold (the SQL refuses too).

## Proven in code (synthetic, no production)

- **PGlite with every migration** (`hold-lifecycle.pglite.test.ts`):
  - two orgs with independent outcomes: A is rolled back, and B goes through attempt then `bridge_complete` on an
    empty set;
  - measured downtime; uploads and publication resumed; old and new records present;
  - lost-response retries of the install (same epoch) and of seal+bind (same binding);
  - a seal+bind interrupted at the bind rolls back whole and its retry succeeds;
  - a FULL started before the install is refused;
  - an erase during the hold refuses the empty-set release;
  - an open canonical claim or a pending batch refuses the install.
- **Real PostgreSQL, two connections** (`hold-lifecycle.postgres.test.ts`, in pg-proof and the CI flow's pg-proof
  lane):
  - an install and an open canonical claim serialize on the org key, in both orders;
  - a rollback racing a marker attempt: whichever commits first wins and the other refuses.
