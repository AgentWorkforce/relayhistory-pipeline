# Worker compatibility delta: 6440bbf2 -> f9409ccb

Disposition: **HOLD for the requested assertion that every new path is default-off.** That assertion is false for the unconditional turns endpoint and active shared changes. This is a source/compatibility finding, not a report of an authentication bypass and not authorization to deploy. Pipeline-lead owns disposition of the new endpoint and the final runtime selection.

## Exact evidence baseline

Base 6440bbf2e7b1c05e5dd7312a2f3179e861680881; candidate f9409ccb9e46c3f100cf3017847d8ed62bc716c7. Source files for the Worker, app, SQL adapter, delivery path, shared S3 transport and the Cloudflare declaration were compared. The existing writer, staging, replica, delivery parser/preparer/protocol files are unchanged in that diff. GC/publication/claim-records-archive code is separate from the Worker entrypoint and is not made active by merely bundling the Worker.

Retained live-to-source association at Nov7 03:56:20Z identifies Worker version 334a7683-82fc-4a54-afd6-b65aa6bfc9a2 / deployment 040ff479-abe3-4dbf-a50e-c34117fed6ec, JS SHA256 d5d3d8e1a85268d66a79f0e8b9de9c6e4cb7225f3559546e782c2ae11a822707, associated with 6440. This review did not perform another provider query. A fresh native plan/live association remains a deployment precondition.

The retained 20 provider bindings contain 15 application bindings: buffered acceptance, inline0, replica off, expected generation G1 78c7..., canonical96bulls key root, max per-user4/org32, transcript bucket/KMS, Hyperdrive0fcc0265..., fanout queue and three Secrets Store handles. No custody-source, privacy-authority, retention-routing, subrequest-budget, Neon-exit or inbox-authority setting is present. No secret value was read; secret handles were metadata only. A later Hyperdrive limit adjustment is not proof that these Worker bindings remain unchanged.

## Path-by-path disposition

| Candidate path | Gate / default under retained config | Finding |
|---|---|---|
| Supabase privacy authority C3 | worker.ts:225 defaults INGEST_PRIVACY_AUTHORITY to neon; supabase requires frozen-supabase; app.ts:594/611 refuse authority RPCs unless selected. Legacy mirror routes remain available under neon. | New authority mutations inactive with the retained missing flag. This does not prove source/native data migration readiness. |
| Frozen/shadow custody | INGEST_CUSTODY_SOURCE defaults relayhistory; the frozen/shadow setting path already existed in6440. | Not a new automatic source switch. No frozen native-source producer or attestation is synthesized. |
| Babysitter retention registration/PUT/stub/write-obligation | worker.ts:184 requires trimmed case-insensitive on; deliver.ts:95 additionally requires replica off, proven subrequest cap and generation; deliver.ts:117 returns before budget routing when inactive; :299 builds no routing plan. | Inactive with retained absent routing/cap. Existing non-Babysitter grouping/writes remain the selected path. |
| Neon-exit startup guard | SESSIONS_NEON_EXIT unset/false => guard off, true requires frozen-supabase/buffered/no-replica. Invalid values now refuse startup. | Off under retained absent setting. Not an automatic Neon exit. |
| POST /v1/turns/batches | app.ts:443 registers unconditionally and uses the same acceptBatch path. No Worker turns-enable variable exists. | **New callable route active under existing settings.** Signed tenant/rth:sync/private custody validation remains required; defaults do not turn this endpoint off. Caller-side TURNS_WRITE_PATH may suppress traffic but is not a Worker admission gate or established current caller-config proof. |
| DB error classification/log fields | Ledger adapter snapshots guarded properties, carries closed diagnosis to route/client logs. | Active on existing delivery error paths, intentionally diagnostic; response/status/retry remain covered by tests. Not default-off. |
| Shared S3 transport | Successful PUT now returns optional x-amz-version-id; optional AbortSignal passes through only when supplied; existing Worker writer/staging callers are unchanged and do not add a signal. | Active compatible metadata addition; not a default-off feature. Version IDs are transport metadata, not a new receipt/source authority. New callers relying on it still need their own reviewed protocol. |
| Managed inbox-authority declaration | sessionIngest.authority is optional and all-or-nothing; no object => no new authority env bindings. | Retained inputs do not select it. In f940 the ingest Worker does not consume INGEST_AUTHORITY_MODE; declaration presence is not proof of a live authority endpoint. |
| Session-ingest code refactor | ingestApp extracted from fetch; same per-request Hyperdrive client and buffered queue refusal; new settings-invalid log helper. | Unflagged shared wiring; not a feature-flag activation. |

The lazy custody policy/native-slot protocol already belongs to the deployed G1 schema/source, not a wholly new Worker flag in this delta. Applying 040000-043000 changes underlying SQL/ACL/read surfaces and is covered by the separate full21 schema rehearsal; a Worker-only source review cannot prove every effect of those migrations dormant. 041000's authority activation is separately guarded; 042000's routing flag does not imply every SQL surface or strict eraser change is gated.

## Material finding requiring lead disposition

The requested worker.compatibilityDeltaReview slot cannot be marked PASS_ALL_NEW_PATHS_DEFAULT_OFF. At least the turns route is an additive write admission surface. Options for the lead are an explicitly accepted, tested additive endpoint with qualified caller/config evidence, or a reviewed server-side default-off gate/narrower runtime source followed by fresh artifact/native capture bindings. Do not silently mark an upstream caller's default as a server fence. This review changes no source or configuration.

## Validation and limitations

On the owned clean f940 sf-frame checkout with its own frozen dependencies, through the Mac shared aw-heavy queue:

- buffered.test.ts, neon-exit.test.ts, turns.pglite.test.ts, babysitter-retention.test.ts: **65 pass / 0 fail / 333 assertions**, 6.31s. This includes a real PGlite/HTTP turns flow in relayhistory custody mode accepting a valid witness with no new turns flag, and the same default mode refusing a missing witness. It therefore supports the unconditional-route finding rather than proving the endpoint inactive.
- packages/object-store/test/unit.test.ts plus db-error-class.test.ts: **41 pass / 0 fail / 521 assertions**, 83ms. Includes optional transport signal/version metadata and closed diagnostic/error-path behavior.

These are offline synthetic tests, not current Cloudflare bindings, a real deployed Worker canary, an actual S3 write or canonical readback. Existing full21 PG17 proof covers schema/rebind/SQL-ledger custody ACK at this target generation separately. No new broad build/typecheck was necessary for unchanged reviewed source; no code edit was made. Test logs and their digests are included.

## Owner review

Adversarial: checked source gates at their mutating callers, not just defaults in comments; surfaced the unconditional route and active shared changes; distinguished caller suppression from admission enforcement, metadata IDs from authority, old runtime receipt from current preimage, separate E3 from Worker wiring, and schema effects from Worker flags. No blanket compatibility signoff inferred from passing tests.

Contextual: completes the lead's requested broad delta inventory against the actual retained config and reports the assertion that cannot be met. Preserves current privacy/custody behavior by default, identifies the explicit decision needed without inventing flags or a deployment, and keeps G2 generation/source/capture bindings separate. AWS target evidence is retained-only and does not qualify a publisher host.
