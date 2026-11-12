# R2 control-head contract v5.3 — CURRENT (2026-11-08 ~01:56Z)

**v5.2 is WITHDRAWN** (it removed rule 4; Khaliq reversed that at 01:50Z — rule 4 stays).

v5.3 = v5 semantics + v5.1 wire freeze:
- **Wire (producer as built, frozen):** relayhistory-cloud PR #328 head `a06a58f9fee3957f8f758ab5793bd8e9420eadbe`, named entrypoint `SessionReadPrivacyControls`, `GET /v1/session-read/control-head`, identity headers `x-relayhistory-org-id` / `-workspace-id` / `-user-id` set only from the verified token, 2 s deadline. The exact 200 key set, identifier limits and error shape are **whatever a06a58f9 emits** (including its `cutover {epoch, status:"active", mode:"ingest"}` object); any missing/extra/unknown key, non-200, timeout or tenant-echo mismatch ⇒ not visible.
- **Visibility rule (all required, evaluated after every S3 fetch, before any emit, on list/metadata/turns):**
  1. exact shape + tenant echo; `cutover.status == "active"`, `mode == "ingest"`;
  2. `pending == false`;
  3. row's original admission epoch + version == `controlEpoch`/`sourceControlEpoch` + `sourceControlVersion` (decimal BigInt); NULL witness ⇒ hidden;
  4. **mandatory per-user G2 parity row** (`sessions.owner_buffered_parity`, migration 043700) for the exact (org, workspace, user), bound to controlEpoch, privacyBaselineSha256, head tuple or explicit no-head, full deletion/reset set digests, `target_lineage_digest` over contiguous `ingest_control_receipts` 1..V, and target schema generation; missing/stale/mismatch ⇒ hidden; default no rows;
  5. defense in depth: hidden if in `ingest_frozen_source_deletions`, current `upload_deletions`, or behind `upload_resets`.
- **Owners:** producer relayhistory-cloud-owner; consumer + 043700 layered-sessions-pipeline; G1 census + G2 writer sessions-g1g2-census; fixtures (26 causal v3 `25bbbdfa…` + 16 parity `f1a31a65…`) in `fixtures/`.
