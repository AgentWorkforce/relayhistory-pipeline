# Synthetic per-org cutover qualification (decision 0005)

`e2e/cutover/` drives decision 0005's per-org cutover end to end on one synthetic org, in the corrected order, and
reports a PASS, FAIL, BLOCKED or SKIP per step, each step's duration, and the admission downtime. It is the
qualification run for the moment the release-path heads land on main. Until then it **fails closed**: it stops at
the first step whose head is missing and exits 1.

```sh
TMPDIR=$HOME/.agent-tmp bun run e2e/cutover/run.ts                       # CI fixture
TMPDIR=$HOME/.agent-tmp bun run e2e/cutover/run.ts --size representative # the reported fixture
TMPDIR=$HOME/.agent-tmp bun run e2e/cutover/run.ts --simulate-pending    # timings only, never QUALIFIED
TMPDIR=$HOME/.agent-tmp bun run e2e/cutover/run.ts --erasures empty      # no erasure during the hold (empty-window release)
# PostgreSQL 17 + MinIO (disposable containers; the runner fails on a skip):
TMPDIR=$HOME/.agent-tmp bun src/session-ingest/__tests__/hold-harness/run.ts --pg 17 -- src/session-ingest/__tests__/synthetic-cutover.postgres.test.ts
CUTOVER_SIZE=representative CUTOVER_REPORT_DIR=$HOME/.agent-tmp/cutover-pg <the same>   # the reported run, with reports
bun test src/session-ingest/__tests__/synthetic-cutover.pglite.test.ts   # the harness and its fail-closed verdict
```

The run writes `report.json` and `report.md` to `--out`, or by default `$RELAYFLOW_STATE_DIR/synthetic-cutover/`
or `.relayflow/synthetic-cutover/`. It refuses a `TMPDIR` under `/tmp`, because the fixture's object store lives
there.

## Backend and fixture

- **Backends** (`e2e/cutover/backend.ts`), both local, with no network beyond loopback containers, no credentials
  and synthetic ids only:
  - `pglite+fs`: PGlite with every migration in the checkout (062320 hold and 062340 erasure history included),
    and a filesystem object store under `$TMPDIR`. Used in CI.
  - `pg<major>+minio`: a scratch database on a real PostgreSQL, migrated by the real migrator as the owner role,
    and a versioned MinIO scratch bucket. The hold-harness runner starts both as disposable containers.
- **Connections on PostgreSQL.** The lifecycle and erase entry points and the backlog tools' reads run as the
  owner. The SQL ledger runs as `bulls96_session_ingest`. The superuser is used only for the seeding listed below.
- **Erasure scenarios** (`--erasures`):
  - `injected`, the default, plants the erasures below during the hold.
  - `empty` plants none, so the release is the empty-window path. It is the genuinely empty control.
  - `direct` models an org that already accepts direct uploads before the install. See below.
- **One org on the buffered SQL ledger.** Its backlog is admitted through the real buffered path: `deliverBatch`
  over `sqlLedger` with a custody witness, under the lazy-current policy.
- **Fixture sizes:**

  | Preset | Users | Sessions per user | Batches per session | Records per batch | Payload | Batches | Records |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | `ci` | 8 | 2 | 2 | 5 | 256 B | 32 | 160 |
  | `representative` | 32 | 12 | 4 | 20 | 1 KiB | 1,536 | 30,720 |

  Each user has its own two device origins. A shared origin refuses the operator user erase (55000).
- **Erasures before the hold:**
  - an operator user erase, SQL side, through `operator_user_erase_begin`, `_step` and `_complete`;
  - a user's own session deletion;
  - a session erasure fence.

  The FULL covers all three.
- **Erasures during the hold:**
  - a user session deletion while the hold is `held` (controls still proceed then);
  - an operator user erase and a session erasure fence while it is `sealed`. Erasures are never refused.

  The bridge must account for all three.
- **Live uploads:** two users upload a new session at every step boundary.
  - Before the marker they use the SQL path, and after it the S3 path (the 0005 consumer, fence authority required).
  - Every refused upload is queued and retried after the release.
- **Seeded below the real entry points**, and why:
  - `owner_buffered_parity` and the active privacy authority. They are the export's per-user gate, which
    production gets from the parity job.
  - The SQL row of a user's session deletion. On a lazy-policy deployment, SQL serves a selection only as an
    ordered control with Neon custody, and the fixture has no Neon. Its S3 fence goes through the real S3 ledger.
  - `session_erasure_fences`. `ingest_operator_erase_session` refuses a buffered org (RH011). The 062360 manifest
    lane owns the real path.
  - On PostgreSQL, the role-validation row. The migrator writes it with the real schema generation, and the
    fixture re-points it to its synthetic generation, which its lazy policy uses.

## The steps

| Step | What runs | Integration |
| --- | --- | --- |
| `seed` | the backlog and the pre-hold erasures | main |
| `rearm.list` | the org's pending re-arm markers and a dry `sweepRearmMarkers` pass | main |
| `hold.install` | `_install`; live SQL uploads refused RH042; a held user deletion proceeds | stand-in for hold-install-execute |
| `full` | `backfillUploadFences` execute, started strictly after the install | main |
| `seal.bind` | `_seal` (sha256 of the FULL receipt), `_bind` (FULL start in (install, seal]); publication refused; in-hold erasures | stand-in for hold-seal-execute + `_bind` |
| `catchup` | the catch-up, revisions since FULL start minus 15 min | main |
| `reshard.verify` | `reshardRevisions` verify over the org's admission prefix | main |
| `marker` | `_marker_attempt` (sha256 of the catch-up receipt), then a create-only per-org marker PUT, read back | stand-in for marker-execute |
| `marker.verify` | hold `marker_attempted`, marker valid, rollback refused, SQL admission still RH042, S3 closed | main; S3 hold pending |
| `drain` | export, then drain, within 3 h of the catch-up; every non-erased backlog session has exactly its canonical manifest and no erased one does; S3 closed | main |
| `release` | per-erasure coverage; S3 closed before the replays; each in-hold erasure replayed; `_release bridge_complete` with an S3 upload in flight | pending: generation proof, 062360 manifest; stand-in for bridge-execute |
| `admission` | every upload refused during the hold retried, then new uploads (S3 ledger) | main |
| `publication` | the 062320 publication gate opens; then a real publication | gate on main; publication pending |
| `reads` | old and new sessions read exactly | main |

**S3 closed** means that, from the install to the COMMITTED `bridge_complete`, every S3 upload of the org is refused
with a code the S3-side hold names (`s3HoldRefusal`) and leaves no durable write: no batch record, no owner stub and
no canonical object of its session. The probes run:
- after the marker;
- at the drain;
- before the replays;
- for one upload already in flight when the release commits.

**The in-flight probe is ordered by a barrier, not by clocks.** The request's view of the store parks it at its first
object-store call. That call is inside admission and comes before any claim or write. The harness waits until the
request is parked, commits the release, then lets the request continue. Its outcome and its durable state are checked
after it settles. The receipt records the barrier in `inflight_barrier`. Its `store_ops_after_barrier_open` is null
when the release never committed.

This proves request-start ordering only. A request parked at its first store call has not yet claimed or passed its
last authority check. The other interleaving is listed in `uncovered` until the real S3-hold adapter exercises it:
after the last authority check and before the durable PUT of the claim, a chunk or the manifest, with readable chunks
and manifests inspected.

There is no S3-side hold yet, so the named code set is empty and any S3 refusal also fails. Each failure has its own
code:

| Code | When |
| --- | --- |
| `s3_admits_before_bridge_complete` | the upload was accepted |
| `s3_refusal_unspecified` | refused with a code the hold does not name |
| `s3_refusal_with_effects` | refused, but a durable write was found |

**Erasure coverage** is checked per erasure, not per kind. The 062340 history keeps no row identity, so the harness
attributes each history row to the erasure it planted, by reading the history immediately before and after each
write. At release, every row since the fence must belong to a planted erasure that was replayed. Otherwise the
release fails with `erasure_unaccounted`, even when another erasure of the same kind was replayed. The test proves
this case with `unreplayedErasure`.

**Exact reads.** Each session's single manifest is found under its exact prefix: org, workspace, the live machine of
(origin, user), harness and `s=<session>`. Its records are read with the read API's canonical reader
(`readManifestRecords`), which fetches every chunk and verifies its digest. They are then compared with what was
uploaded: the same record ids, the operation, the expected revision and the exact payload text. Erased content must
be absent in two ways:
- no manifest under the org names an erased session or a session of an erased user;
- no surviving manifest holds an erased session's record ids.

Otherwise the step fails with `erased_records_present`. HTTP reads through session-read-api are not covered yet.

**Timeline.** Each boundary is recorded on its own:
- the install commit and the release commit, both `installed_at` / `released_at` on the database clock;
- the first successful S3 admission after the install, with the client's request start and answer;
- the first admission after the release.

Two durations come from these:
- `observed_upper_bound_ms`, from the install commit to the first successful S3 answer. It is an observed upper bound
  on the downtime, not a measurement of when admission reopened.
- `through_release_ms`, from the install commit to the release commit. A blocked release has no release commit and
  no resumption.

**Receipts** also carry:
- `first_failure`: the first step that did not pass, with its first failing assertion;
- `uncovered`: every pending adapter and every check not built yet, such as HTTP serving reads and search rows.

**Verdict.** The run is QUALIFIED only when all of these hold:
- every step passes;
- every adapter in `e2e/cutover/heads.ts` is `landed`;
- the backend is `pg17+minio`;
- the run is not simulated.

## The direct-upload scenario (`--erasures direct`)

This is the already-direct org from layered-sessions-pipeline's approved workspace routing. It admits through the S3
ledger with a valid per-org marker in place before the install. Every check of the other scenarios also runs.

- `direct.enroll` writes a valid per-org marker before the install, proves one direct upload is admitted, and parks
  a second direct writer.
  - The writer is parked at its first durable write, after its authority reads, before the install.
  - It is carried through the whole cutover and let go at the end. Its outcome, its durable writes and its manifest
    are then inspected (`direct.parked_writer`).
- At `seal.bind`, the user reset goes through the real mirrored control (`withSqlControlMirror`, S3 first, then
  SQL). An S3-only reset is the negative control. For each, the harness records whether S3 applied it and how many
  062340 history rows it produced.
- `marker` preserves the existing marker. The stand-in writes no publication identity, so the marker cannot be
  adopted and the step fails with `marker_conflict`. It never overwrites the marker.
- `release` tries the empty-window release (`_release ... 'erasure_window_empty', 0`) inside a transaction that is
  rolled back. It then refuses fail-closed with `direct_erasure_unrecorded` when a direct control applied in S3 has
  no history row.

On main (pinned by both suites):
- The pre-install upload is admitted. The writer parked before the install completes during the hold: its batch
  record, owner stub and canonical manifest are written.
- **The mirrored reset:**
  - S3 applies it.
  - Its SQL half is refused with RH042 while the hold is sealed, because the fixture has no mirror-role member.
  - Outside the seal, the same call is refused with 22023: a lazy-policy SQL reset needs an ordered control with
    Neon custody.
  - It produces no 062340 row, and neither does the S3-only reset.
- The empty-window release would therefore commit, so `release` fails with `direct_erasure_unrecorded`. No
  authoritative history source records the direct path's controls.
- The drain leaves the reset users' stages inside the 5-minute reset margin. They stay undrained until a rerun, and
  the step fails with `drain_reset_margin_pending`.

## Integration points (`e2e/cutover/heads.ts`)

Each head is one adapter. Integrating a head means replacing the adapter's `run` with the head's entry point and
setting `integration: "landed"`.

- `holdInstall`, `holdSealBind`, `markerExecute`, `bridgeRelease`: sessions-wave-tooling's held modes. Today each is
  a stand-in that keeps to the documented contract over the SQL functions on main.
- `userEraseReplay`: sessions-erase-generation-proof. Pending today: it refuses `erasure_generation_unproven`.
- `sessionEraseReplay`: hard-delete-owner's 062360 manifest. Pending today: it refuses `session_manifest_missing`.
- `selectionReplay`: checks that a user deletion made during the hold is in the S3 fence (controlMirror on).
  `hold-bridge` `ERASURE_SET_SQL` reads only operator user erases and session fences, while 062340 also counts
  `upload_deletions` (the wave 2 mismatch).
- `s3HoldRefusal`: the S3-side hold, so that S3 admission stays fenced through the committed `bridge_complete`.
  Pending, with an empty code set.
- `publication`: a real publication of drained sessions. Pending today: it refuses `publication_not_exercised`.

## What main does today (pinned by both suites, PGlite and PostgreSQL)

- **S3 admission is open from the marker** through the committed `bridge_complete`. Every S3 probe is accepted,
  including the in-flight one. The S3 consumer serves an org as soon as its marker exists and never reads the SQL
  hold, so `marker.verify`, `drain` and `release` fail with `s3_admits_before_bridge_complete`.
- **The drain leaves the backlog unmaterialized** (`drain_not_materialized`).
  - The FULL's revisions section puts each owned lazy native slot into the S3 index with its `current_digest`, so
    the drain counts every buffered line as `already_stored` and writes none.
  - The same drain after a FULL without the revisions section writes every line.
  - Layered's ruling: a lazy slot's revision or digest is not proof of canonical bytes. A materialization path, or
    verified existing canonical bytes, is required.
- **Injected erasures:** `release` is BLOCKED with `erasure_generation_unproven`. There is no release commit and no
  resumption.
- **Empty window:** the release commits through the stand-in bridge. `publication` fails with
  `publication_not_exercised`, and `reads` fails with `old_events_not_canonical`.
- **The session erasure fence** is a table insert. It covers the history recorder only and is not the 062360
  producer: the operator entry refuses a buffered org with RH011.

## Not covered yet (TODO in the code)

- **The pg-proof CI lane:** the PostgreSQL suite runs under the hold-harness runner on 17. It is not yet in
  `flows/lib/lane.sh` pg-proof.
- **The read API:** `/v1/sessions/:id/events` on the compose stack.
- **A real publication** and its `search_index` rows: the `publication` adapter.
- **The sweeper-pass attestation** (`freshSweeperPass`).
