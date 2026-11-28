# Synthetic per-org cutover qualification (decision 0005)

`e2e/cutover/` drives decision 0005's per-org cutover end to end on one synthetic org, in the corrected order, and
reports a PASS, FAIL, BLOCKED or SKIP per step, each step's duration, and the admission downtime. It is the
qualification run for the moment the release-path heads land on main. Until then it **fails closed**: it stops at
the first step whose head is missing and exits 1.

```sh
TMPDIR=$HOME/.agent-tmp bun run e2e/cutover/run.ts                       # CI fixture
TMPDIR=$HOME/.agent-tmp bun run e2e/cutover/run.ts --size representative # the reported fixture
TMPDIR=$HOME/.agent-tmp bun run e2e/cutover/run.ts --simulate-pending    # timings only, never QUALIFIED
bun test src/session-ingest/__tests__/synthetic-cutover.pglite.test.ts   # the harness and its fail-closed verdict
```

The run writes `report.json` and `report.md` to `--out`, or by default `$RELAYFLOW_STATE_DIR/synthetic-cutover/`
or `.relayflow/synthetic-cutover/`. It refuses a `TMPDIR` under `/tmp`, because the fixture's object store lives
there.

## Backend and fixture

- **Backend:** local only. PGlite with every migration in the checkout (062320 hold and 062340 erasure history
  included). The object store is a filesystem store under `$TMPDIR`. There is no network and there are no
  credentials, and every id is synthetic.
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
| `marker.verify` | hold `marker_attempted`, marker valid, rollback refused, SQL admission still RH042, **S3 admission still closed** | main |
| `drain` | `exportAcceptedStages`, then `drainBufferedBatches`, within 3 h of the catch-up's created_at | main |
| `release` | each in-hold erasure replayed, then `_release bridge_complete` with the history count | pending: generation proof, 062360 manifest; stand-in for bridge-execute |
| `admission` | queued uploads retried, new uploads admitted (S3 ledger) | main |
| `publication` | the 062320 publication gate opens for the org | main |
| `reads` | old (drained backlog) and new (S3-admitted) sessions: canonical manifest and every named chunk | main |

The admission downtime runs from the install to resumed admission. The report also gives the time to the first S3
admission after the install.

**Verdict.** The run is QUALIFIED only when every step passes and every adapter in `e2e/cutover/heads.ts` is
`landed`. Anything else is NOT_QUALIFIED, and the run exits 1:
- a step that fails or is BLOCKED;
- an adapter that is a `standin` or `pending`;
- a run with `--simulate-pending`.

## Integration points (`e2e/cutover/heads.ts`)

Each head is one adapter. Integrating a head means replacing the adapter's `run` with the head's entry point and
setting `integration: "landed"`.

- `holdInstall`, `holdSealBind`, `markerExecute`, `bridgeRelease`: sessions-wave-tooling's held modes. Today each is
  a stand-in that keeps to the documented contract over the SQL functions on main.
- `userEraseReplay`: sessions-erase-generation-proof. Pending today: it refuses `erasure_generation_unproven`.
- `sessionEraseReplay`: hard-delete-owner's 062360 manifest. Pending today: it refuses `session_manifest_missing`.
- `selectionReplay`: checks that a user deletion made during the hold is in the S3 fence (controlMirror on).
  `hold-bridge` `ERASURE_SET_SQL` reads only operator user erases and session fences, while 062340 also counts
  `upload_deletions`.

## What main does today (CI fixture, pinned by the test)

- Every step through `drain` passes, except `marker.verify`, which fails with `s3_admits_before_bridge_complete`.
  - The S3 consumer serves an org as soon as its marker exists, and it never reads the SQL hold.
  - So S3 admission for the org reopens at the marker PUT, before the drain and before `bridge_complete`.
- `release` is BLOCKED with `erasure_generation_unproven`. Nothing after it runs, and no downtime is reported.
- With `--simulate-pending`, release and resumed admission run and the downtime is measured.
  - `reads` then fails with `old_events_not_canonical`: the drain writes no canonical line for the backlog.
  - The FULL's revisions section puts each owned lazy native slot into the S3 index with its `current_digest`.
  - The drain therefore counts every buffered line as `already_stored`.
  - The same drain after a FULL without the revisions section writes every line.
  - Whether that is intended (canonical produced by another path) is an open question for the lane owners.

## Not covered yet (TODO in the code)

- **The pg-proof backend:** PostgreSQL 15, 16 and 17 with MinIO. The steps take an owner SQL client and an
  `ObjectStore`, so this is a backend switch.
- **The read API:** `/v1/sessions/:id/events` on the compose stack.
- **A real publication claim** and its `search_index` rows. Today the step checks only the gate.
- **The sweeper-pass attestation** (`freshSweeperPass`).
