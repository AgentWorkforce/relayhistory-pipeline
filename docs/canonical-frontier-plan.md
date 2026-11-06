# Canonical publication frontier plan

Status: read-only evidence evaluator, production-reviewable capture runner, and offline rehearsal. No production capture was invoked by this change. It does not insert `sessions.ingest_publication_frontiers`, claim a stage, publish an object, enable a worker, or authorize rollout.

## Contract

`src/session-ingest/frontier-plan.ts` consumes `canonical-publication-frontier-capture-v1` and returns a discriminated `FrontierReadiness`:

- `status: "hold"` has stable reason codes and JSON evidence paths. A hold is the only answer for sampled, stale, unknown, changing, cross-tenant, unversioned, or incomplete evidence.
- `status: "ready"` has one exact `CanonicalFrontierRow` per fully covered `(org, workspace, user, origin)`, plus `evidenceSha256` and `planSha256`. These are candidate plan rows, not insert authority.

The row mapping is exact:

| Capture fact | `20261104060000` column |
|---|---|
| Exact org/workspace/user/origin | `org_id`, `workspace_id`, `user_id`, `origin_id` |
| Closed source authority, bounded scan provenance, immutable export roots | `source_frontier_sha256` |
| Complete record maxima and first-ACK decisions | `record_maxima_sha256` |
| Capture time plus the fixed 300-second lifetime | `valid_until` |
| Reviewed destination | `target_bucket`, `target_region`, `key_root` |
| Exact derived schema generation | `schema_generation` |

The publication worker must require `status: "ready"` and exact equality between its trusted stage ticket and the plan's org/workspace/user/origin, target, and schema generation before claim. It must not convert a hold into a retry that silently widens scope.

## Evidence capture protocol

`src/session-ingest/frontier-capture-source.ts` and `scripts/capture-canonical-frontier.ts` implement the reviewed read-only capture path. The runner accepts credentials only in capped stdin JSON. Database URLs and object credentials are rejected on argv, are never written to the capture, and are not printed on error. Its cache and output parent must be owner-only directories; output is create-only mode `0600`. The S3 principal must be limited to object reads/listing and bucket-versioning reads, and the runner wraps its store in a port whose write/delete methods always refuse. There is no owner SQL, insert, apply, claim, or publication mode.

The source connection must be a dedicated `NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS` role with only `USAGE` on the source schema and `SELECT`/`EXECUTE` on the enumerated relations, sequences, and functions. Capture uses `REPEATABLE READ READ ONLY`, bounded statement/lock timeouts, UTC, fixed parameterized queries, and a validated schema identifier. The capture order is:

1. Read the complete origin inventory with a fixed keyset query and an explicit empty terminal page. Record every actual page count, reviewed query SHA, transaction snapshot SHA, page-chain SHA, configured bounds, item/byte counts, and terminal `null` cursor. Any ownerless origin, reset-only user, or control-only user refuses capture instead of being hidden.
2. Read the opening source authority: database/role/collation facts, exact relation and sequence OIDs, hashes of the five critical source functions, ordered control heads, cutover epoch, privacy baseline, and transaction snapshot. `sourceHead` is the first 40 hex characters of this source-authority digest; it is deliberately not represented as a deployment Git commit. If a deployment commit is an operational gate, verify it independently.
3. For each exact user/origin, read every current record maximum and its projection/catalog acceptance sequence with a bounded ordered keyset scan and explicit empty terminal page. An absent destination revision row is not evidence of an empty legacy source. `sourceRecordCount` must equal the maxima count.
4. Use `captureSeedScopeExportEvidence` from `frontier-plan-source.ts` for export evidence. It issues only the fixed preview RPC, takes two authoritative scope captures, runs every captured pointer through `SeedProofIndex`, derives the target origin's root counts from the complete sealed evidence enumeration (independent of import ACK progress), and records the manifest version returned by the exact GET whose bytes were verified. Explicit record, evidence-byte, and elapsed-time bounds refuse before materialization can become unbounded. `createS3FrontierVersioningReader` performs bounded signed `GetBucketVersioning` reads before and after the capture; production uses the pinned AWS HTTPS host, redirects refuse, and endpoint overrides are explicit literal-loopback fixtures only. The runner repeats this full proof for every required scope and requires the two passes to be byte-for-byte identical before taking the closing legacy-source scan; the final scan provenance binds both seed snapshots and signed versioning-read digests. The adapter refuses source changes, absent or literal `null` versions, non-delivery/unseeded records, foreign roots, and disabled/unknown/changing versioning. It has no caller-supplied completeness, count, version-ID, or versioning booleans.
5. Read all ordered deletion/reset rows and bind each user’s exact control-head version. Every production attestation also requires a complete receipt-seat scan, including proof that it is empty when no receipt seats are in scope; a legacy maximum proved directly from immutable seed bytes never licenses an invented empty scan. A losing equal-revision receipt candidate stays auditable; two accepted bodies for one native record hold.
6. After all seed and S3 proof work, repeat the complete bounded source inventory, maxima, privacy, and authority capture in new read-only transactions. Reconciliation requires exact ordered content and page-count equality, binds both transaction snapshots and both page chains, and takes each origin’s closing privacy version from the second capture. Any source, generation, control, privacy, record, or scan-state change refuses the run. Re-run from the opening inventory; do not patch a stale capture.

Each category has its own fixed adapter version. A complete scan requires `complete=true`, `terminalCursor=null`, a final zero in `pageItemCounts`, exact item-count closure, a valid page-chain hash, and matching opening/closing state hashes. Export composition requires a unique byte-proved record with the exact native record ID, revision, kind, source, session, and operation; revision ID, delivery digest, and acceptance sequences remain bound to the source maximum and its raw page-chain evidence because the scrubbed export stream deliberately does not retain those fields. All item hashes are recomputed by the planner using canonical key-sorted JSON. The whole capture is content addressed by `captureId`; mutation after capture holds.

### Reviewed live-capable runner

The operator supplies a single stdin JSON document with protocol `canonical-frontier-readonly-capture-runner-v1`, `sourceDatabaseUrl`, `seedDatabaseUrl`, exact `scope`, reviewed `target`, source-object coordinates/credentials, and numeric `pageLimit`, `maxPages`, `maxScopes`, `maxOrigins`, `maxRecords`, `maxBytes`, and `maxDurationMs` bounds. The source and target key-root names must match. The only argv values are absolute local `--output` and `--cache-dir` paths. Generate the document directly from the approved credential broker into the process; do not save it or place secrets in environment files or shell history:

```sh
approved-credential-broker emit canonical-frontier-readonly-capture-runner-v1 |
  bun scripts/capture-canonical-frontier.ts \
    --output /private/review/frontier-capture.json \
    --cache-dir /private/review/frontier-cache
```

Exit `0` means the captured evidence evaluated `ready`, exit `2` means a durable hold was written, and exit `1` means capture refused. The runner logs only protocol, status, capture ID, and output path. The present runner intentionally emits no receipt scan: the seeder role is explicitly revoked from `ingest_receipt_seats_for_stage` and the private receipt/stage tables. It therefore remains `hold/scan_incomplete` for production rather than claiming that no receipt records exist. Closing that last gate requires a separately reviewed restricted read API/role or trusted publication-claim evidence; owner credentials and an asserted empty scan are not acceptable. A production invocation remains a separately reviewed operator action.

## Offline dry run

The CLI accepts only a local JSON evidence file and an explicit clock. It has no implicit current time, network adapter, database option, `--execute`, or `--apply`:

```sh
bun scripts/canonical-frontier-plan.ts \
  --evidence /absolute/path/to/reviewed-capture.json \
  --now 2026-11-06T04:00:05.000Z
```

Exit `0` is a deterministic ready plan, exit `2` is an actionable hold, and exit `1` is malformed CLI/input. Evidence is capped at 64 MiB and emitted without payload logging.

## Hold actions

| Reason | Required action |
|---|---|
| `capture_stale`, `scan_stale` | Re-run the entire bounded capture; do not extend timestamps. |
| `inventory_incomplete`, `origin_coverage_incomplete`, `scan_incomplete` | Resume from the durable cursor chain and prove the final empty page. |
| `unknown_legacy_fact` | Obtain authoritative source metadata; do not synthesize revisions, digests, versions, or epochs. |
| `source_race`, `privacy_changed` | Discard the capture and restart after the source/control state stabilizes. |
| `tenant_mismatch` | Correct the scoped adapter or source ownership evidence; never borrow another workspace/user's root. |
| `export_not_immutable` | Prove versioning and exact object version/content identity through the existing seed/manifest path. |
| `record_coverage_incomplete` | Reconcile complete source maxima to immutable roots; sampled counts and absent rows are insufficient. |
| `record_conflict`, `receipt_decision_conflict` | Resolve native identity/first-ACK authority; no last-writer or timestamp fallback is permitted. |

## Offline evidence

Focused Bun coverage includes missing/stale/incomplete evidence, explicit final-page closure, source and privacy races, wrong tenant, immutable-version refusal, record coverage, competing equal-revision bodies, full coverage, CLI refusal of `--apply`, stdin-only capture configuration, and idempotency. Source-adapter coverage additionally verifies two preview captures, full byte proof, exact native record/revision lineage, derived root counts, same-response versions, signed versioning reads, malformed/oversized S3 responses, source races, absent versions, and bucket-status changes.

The disposable proof uses the owned loopback PostgreSQL 17 and versioned MinIO fixtures. It creates and drops an owned synthetic source schema and a dedicated restricted read role, writes only a unique synthetic S3 prefix using the actual manifest writer, runs the source adapter over verified manifest/chunk bytes and a fixed in-memory seed-capture response, reads bucket versioning through the signed S3 adapter, records the non-null same-response manifest version, and performs the full fixed-query capture with bounded `REPEATABLE READ READ ONLY` scans and terminal empty pages:

```sh
FRONTIER_DISPOSABLE=1 \
FRONTIER_PROOF_OUT=/home/khaliqgant/relay-briefs/canonical-frontier-plan-20261106-0530 \
bun scripts/proof-canonical-frontier-plan.ts
```

The recorded result artifact reports the exact PostgreSQL version, commit/dirty state, test count, assertions, and this limitation. It proves actual PostgreSQL transaction behavior, actual manifest/chunk bytes, and actual signed versioned-MinIO reads, but does **not** execute the real migrated `sessions.ingest_seed_scope_capture` RPC or production source schema. A fixture `READY` is only internally consistent protocol coverage, not authoritative production readiness. It also does not prove the current production source inventory, source freshness, AWS bucket versioning/KMS/IAM, S3 object coverage, production privacy closure, destination readiness, worker deployment, or consumer visibility.

## Remaining production gates

The lead/operator still needs a separately reviewed invocation of the provided bounded read-only runner and exact production evidence for:

- the complete one-org origin inventory (the measured 469 origins and 10,001 sentinels are samples, not closure);
- authoritative legacy source identity/generation and every record maximum; independently verify the deployed source commit if that is a release gate;
- strict `seed-s3` byte proof for every export root, including non-null immutable AWS version IDs;
- a disposable proof that applies the real migrations and calls the actual managed-role `sessions.ingest_seed_scope_capture` RPC; the current fixture deliberately does not claim this;
- opening/closing deletion, reset, control-epoch, privacy-baseline, receipt-seat, and source-state equality;
- an actual complete receipt-seat/stage-root adapter using a least-privilege read capability (or reviewed publication-claim evidence); the current seeder capability cannot provide it;
- exact schema generation and destination bucket/region/key root;
- independent review of the resulting local capture and candidate rows;
- a separately authorized owner-write mechanism, if one is later approved. This change intentionally provides none.

No new migration slot is required by this read-only planner. If production capture cannot express an authoritative fact with the existing schema/protocol, stop and report the missing contract rather than adding a migration implicitly.
