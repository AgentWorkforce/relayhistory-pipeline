# Canonical publication frontier plan

Status: read-only evidence evaluator and offline rehearsal. It does not scan production, insert `sessions.ingest_publication_frontiers`, claim a stage, publish an object, enable a worker, or authorize rollout.

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

A reviewed read-only probe should construct the JSON capture; this repository does not include a database URL, owner SQL, credential flag, or live scan entry point. The capture order is:

1. Read the complete origin inventory with a fixed keyset query and one page past the final row. Record the reviewed query SHA, source snapshot SHA, page/cursor-chain SHA, configured bounds, item/byte counts, and terminal `null` cursor.
2. For each exact user/origin, read the opening source identity, generation, source commit, control epoch, privacy baseline, and privacy version. Unknown legacy values remain `null`, which produces `unknown_legacy_fact`.
3. Read every current record maximum with a bounded resumable keyset scan. An absent destination revision row is not evidence of an empty legacy source. `sourceRecordCount` must equal the maxima count.
4. Use `captureSeedScopeExportEvidence` from `frontier-plan-source.ts` for export evidence. It issues only the fixed preview RPC, takes two authoritative scope captures, runs every captured pointer through `SeedProofIndex`, derives the target origin's root counts from the sealed per-record index, and records the manifest version returned by the exact GET whose bytes were verified. `createS3FrontierVersioningReader` performs bounded signed `GetBucketVersioning` reads before and after the capture. The adapter refuses source changes, absent versions, non-delivery/unseeded records, foreign roots, and disabled/unknown/changing versioning. It has no caller-supplied completeness, count, version-ID, or versioning booleans.
5. Read ordered deletion/reset control state and the complete first-ACK receipt-seat decisions. A losing equal-revision candidate stays auditable; two accepted bodies for one native record hold.
6. Repeat the authority read after all PostgreSQL and S3 work. Any source, generation, control, privacy, or scan-state change invalidates the entire origin. Re-run from the opening inventory; do not patch a stale capture.

Each category has its own fixed adapter version. A complete scan requires `complete=true`, `terminalCursor=null`, a valid page-chain hash, and matching opening/closing state hashes. All item hashes are recomputed by the planner using canonical key-sorted JSON. The whole capture is content addressed by `captureId`; mutation after capture holds.

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

Focused Bun coverage includes missing/stale/incomplete evidence, source and privacy races, wrong tenant, immutable-version refusal, record coverage, competing equal-revision bodies, full coverage, CLI refusal of `--apply`, and idempotency. Source-adapter coverage additionally verifies two preview captures, full byte proof, derived root counts, same-response versions, signed versioning reads, malformed/oversized S3 responses, source races, absent versions, and bucket-status changes.

The disposable proof uses the owned loopback PostgreSQL 17 and versioned MinIO fixtures. It creates and drops an owned synthetic schema, writes only a unique synthetic S3 prefix using the actual manifest writer, runs the source adapter over verified manifest/chunk bytes and two preview captures, reads bucket versioning through the signed S3 adapter, records the non-null same-response manifest version, and performs a bounded `REPEATABLE READ READ ONLY` record scan:

```sh
FRONTIER_DISPOSABLE=1 \
FRONTIER_PROOF_OUT=/home/khaliqgant/relay-briefs/canonical-frontier-plan-20261106-0530 \
bun scripts/proof-canonical-frontier-plan.ts
```

The recorded result artifact reports the exact PostgreSQL version, commit/dirty state, test count, and assertions. This proves the offline protocol path only. It does not prove the current production source inventory, source freshness, AWS bucket versioning/KMS/IAM, S3 object coverage, production privacy closure, destination readiness, worker deployment, or consumer visibility.

## Remaining production gates

The lead/operator still needs a separately reviewed bounded read-only probe and exact production evidence for:

- the complete one-org origin inventory (the measured 469 origins and 10,001 sentinels are samples, not closure);
- authoritative legacy source identity/generation/commit and every record maximum;
- strict `seed-s3` byte proof for every export root, including non-null immutable AWS version IDs;
- opening/closing deletion, reset, control-epoch, privacy-baseline, receipt-seat, and source-state equality;
- exact schema generation and destination bucket/region/key root;
- independent review of the resulting local capture and candidate rows;
- a separately authorized owner-write mechanism, if one is later approved. This change intentionally provides none.

No new migration slot is required by this read-only planner. If production capture cannot express an authoritative fact with the existing schema/protocol, stop and report the missing contract rather than adding a migration implicitly.
