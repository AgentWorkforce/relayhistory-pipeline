# Wave 1 (two orgs): hold lifecycle plan

Code and design only. Nothing here is approved to run. Installing a hold, dispatching any execute against
production, and enabling `marker-execute` each need Khaliq's explicit OK in his own words. The org names and ids are
not recorded here; the run names them only through the `BACKLOG_OPS_ORGS` secret.

Branch: `review/0005-hold-lifecycle`, stacked on `1bce290b` (the frozen minors head). Tooling: `hold-dry`,
`hold-install-execute`, `hold-seal-execute`, `hold-rollback-execute` (`src/session-ingest/hold-lifecycle.ts`,
`scripts/backlog-hold.ts`). Every execute is held in code (`BACKLOG_OPS_HOLD_EXECUTE_ENABLED = false`).

## Sequence per org

1. `hold-dry` (read only). It shows that no hold is live and nothing is pending, plus the fan-out state (unproven),
   the canonical history and the active seed captures.
2. `hold-install-execute` (binds that dry). `_install` and the open-canonical-claim check run in one transaction
   under the org key. Unproven fan-out, canonical history or an active seed capture refuses or rolls it back. **From
   here the org's admission is refused (RH042).**
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

**Wave 1 (two orgs), as Pipeline relays owner decision 252297350480269312 (A′).** Approval provenance is clarified in
252300211022802944. Wave 1 may qualify through the **empty-erasure-window** release only:
- the 15-source `erasure_window_empty` proof bound to the org, the epoch and the `_bind` FULL;
- the in-flight pre-install erase check;
- the final proof read and `_release` in one READ COMMITTED transaction, with the org key taken EXCLUSIVE first;
- the release-versus-erase races in both commit orders.

That release transaction and its race tests belong to **sessions-backlog-integration**. They are not built here.
Wave 1 also needs three more things: a reviewed two-org scope (an allow-list of two org HASHES, inside the run's
secret scope, never raw ids) before any mutation; a timed representative FULL; and real reads of old and new events
plus search resumption. This lane authors no enable without Khaliq's words reaching it directly or delegate-marked
(AGENTS.md "Relayed approvals").

**Wave 2 needs the full path.** After a marker attempt the only release is `bridge_complete`, and an empty initial
erase set does not guarantee it later: an erase arriving during the hold makes the empty-set release refuse (proven in
`hold-lifecycle.pglite.test.ts`). A non-empty release needs:

| Dependency | Owner |
| --- | --- |
| Durable, request-bound session erase manifest (slot 20261105062360) | sessions-erase-manifest-062360 (design: hard-delete-owner) |
| User-erase generation boundary proof (`bytes_verified` alone does not close it) | sessions-erase-generation-proof |
| The bridge's release transaction | sessions-backlog-integration |

## Install preconditions still open (the install stays held)

- **B: canonical fan-out.** There is no SQL record of outstanding fan-out. A staged canonical batch commits with
  records `[]`, and its fan-out runs from FANOUT_QUEUE messages. Pipeline accepts neither queue depth alone, nor
  buffered-only-since-a-cutoff, nor leaving work outstanding. `hold-dry` reports `fanout: unproven`, and every install
  refuses `hold_fanout_unproven` before `_install`.
  - As a read-only diagnostic, `hold-dry` reports the 062320 owner's superset: committed canonical batches, ever. A
    non-zero count also refuses the install, under the key. The count never admits an install.
  - A proof needs either a reconciliation of every accepted staged batch to its revision effects (the only
    distinguishing artifact is the `live-batches/v1` objects, whose retention infra must prove), or a fan-out
    obligation ledger (a new slot) plus a historical proof.
  - Owners: the 062320 owner (sessions-authority-evidence) and Pipeline.
- **C: seed-capture issuance.** `ingest_seed_scope_capture` takes no org key, so the install cannot exclude a racing
  capture (characterized in `hold-lifecycle.postgres.test.ts` case 3).
  - The seeder is operator-run only. Nothing schedules a retry, and a refused run records nothing.
  - The install still refuses on any committed, unexpired capture, under the key.
  - The fix proposed by the 062320 owner is a new slot that puts the admission check on capture issuance. Pipeline
    assigns the slot.

## Rollback criteria (before any marker attempt)

Roll back (`hold-rollback-execute`) if any of these happens:
- the FULL or the catch-up fails, or its retry does not complete within the freshness windows;
- the seal alarm fires and the marker chain is not ready;
- any org's evidence or attestation refuses at `marker-dry`;
- the owner calls it.

The rollback refuses after a marker attempt, in the same transaction that reads the hold (the SQL refuses too).

## Proven in code (synthetic, no production)

- **PGlite with every migration** (`hold-lifecycle.pglite.test.ts`):
  - two orgs with independent outcomes: A is rolled back, and B goes through attempt then a DIRECT `bridge_complete`
    on an empty set (a fixture, not the A′ release transaction);
  - measured downtime; uploads and publication resumed; stage intents from before and after present. The real
    readbacks of old and new events are PENDING;
  - these fixtures inject a SYNTHETIC fan-out proof; the production path, with no proof, refuses before `_install`;
  - lost-response retries of the install (same epoch) and of seal+bind (same binding);
  - a seal+bind interrupted at the bind rolls back whole and its retry succeeds;
  - a FULL started before the install is refused;
  - an erase during the hold refuses the empty-set release;
  - canonical history or an active seed capture refuses the install (rolled back), and an expired capture does not
    count;
  - an install retry after the hold has advanced (sealed, marker_attempted) reports the actual state and refuses
    `hold_epoch_advanced`.
- **Real PostgreSQL, two connections** (`hold-lifecycle.postgres.test.ts`, in pg-proof and the CI flow's pg-proof
  lane):
  - an install and a canonical commit serialize on the org key, both orders;
  - a rollback and a marker attempt, both orders: whichever commits first wins and the other refuses;
  - gap C characterized: a seed capture is not blocked by the install's key.
  - Each ordering has its own migrated database.
