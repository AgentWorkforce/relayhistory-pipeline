# Full session model work — HELD source proposal

This branch implements bounded all-content Bob work. It is not activation-ready.
No production model, key, secret, schema, role, IAM, schedule or flag is changed
by the source/tests. Initial buffered-upload release artifacts are unchanged.

The actual 620 foundation is still HELD: full custody/dependency creation,
carrier publication/reconciliation and erasure must close independently. Manually
seeded READY fixture metadata does not prove that creator. Desktop/RHC encrypted
summary route/key wiring is not implemented here. Logical Referee closure facts
are not established: model state/reason stays encrypted as a suggestion and this
runtime publishes no authoritative state. These are full-switch RED gates.

## Data and authority

The existing official canonical fold applies the complete 620 winner mask before
graph publication. Models consume that registered, complete, masked graph through
the existing Node raw-row codec, including arbitrary record/tool fields, legacy
tables and additional declared tables. They do not use the reduced replay view,
sample ten prompts, truncate content, parse another raw format, or fall back to
Neon. Missing body/dependency/graph/source evidence refuses.

A job binds generation, identity, source epoch/fingerprint, authority ID, mask
SHA, graph generation/index key/SHA/bytes and model/prompt/key protocol. Original
source and graph must still be current under the SourceScope lock for every
claim/checkpoint and both final pointers. SourceScope precedes ModelWork locks.
A final wall-clock lease check after callback writes rolls back pointers,
checkpoint and completion together. Model/network/object IO never holds SQL
locks. A job cannot adopt another lease or resume onto a new source.

630 owns coalesced complete-graph wakes, durable ModelWork leases/checkpoints,
bounded retry and terminal failure. Wakes clear only with matching-source
enqueue. A model failure publishes no fake empty result. Retryable failures
back off; at most twelve consecutive attempts without progress are admitted.
Nonretryable failure is durable. The creator-only retry RPC revokes old nonces
and admits only the same still-current source/graph/config.

## Bounded model and object work

Each encoded input is at most 48 KiB, assembled losslessly from official
projected rows, with row ordinals/UTF16 offsets/full-row SHA. One step makes at
most one provider request. An encrypted binary reduction frontier retains
O(log input units) compact UUID/hash/range references, not repeated long object
keys or plaintext in SQL. A row above the existing 64 MiB Node codec bound
refuses. Metadata/ciphertext/request/response caps also refuse explicitly.
These are bounds, not proof that every maximum fits a particular Lambda heap.

For L nonempty inputs, the tree needs at most L map + L−1 combine + one final
request. All short/long text and intermediate notes are authenticated encrypted
objects registered before immutable PUT. Source invalidation retires their
known intents via 620. Failed/crashed PUTs are known orphans, never untracked
keys. A crash after a provider response but before checkpoint may repeat that
external request; model execution is not exactly once. Counters/usage attached
to committed work are not a universal billing ledger.

Responses uses an explicit configured model, bounded strict structured output,
store:false, no redirects, actual usage/model validation and abortable IO.
Off/extractive is not a full-summary model. No model is chosen or called by
default. Provider quality, production latency and total corpus cost are unproved.
The contract follows the [official Structured Outputs guide](https://developers.openai.com/api/docs/guides/structured-outputs).

## Managed runtime and release prerequisites

Source entries are full-model-main.ts (Lambda or --once) and full-model-job.ts.
The source-only stacks/aws/full-model.template.json declares a permanent
default-OFF minute schedule, Node24 Lambda, immutable version-addressed code,
reserved concurrency one, scoped execution role and error alarm. It is not
deployed. Reviewed resource-policy wiring and actual packaged Node/PG17/
versioned-MinIO proof remain required. Temporary migration EC2 is not that runtime.

The proposed dedicated bulls96_session_model login gets exactly eight SD RPCs:
capture/replay descriptor/object registration, model discover/enqueue/claim/
heartbeat/checkpoint. It gets no table, column, sequence, schema CREATE or
private-helper privileges. Bare bootstrap, reviewed LOGIN timeout profile and
pure effective-privilege verifier are separate. A schema owner without
CREATEROLE must not be reused or promoted to create the prerequisite. The
migrator/live gate/detached build share one authoritative verifier inventory;
this proposal adds the fifth verifier.

The managed job refuses owner/service-role/ingest URLs or alternate project/
pooler/TLS options. It requires the actual dedicated login, exact compiled
generation and temporary execution-role credentials. A dedicated managed secret
has exactly hoopdb_url, openai_api_key and the existing summary_master_keys;
nothing writes or prints those values. No master key is invented or rotated.

Required IAM/resource-plan review: model-role S3 GET for qualified graphs and
its encrypted slices, immutable PUT only for derived/bob-slice, summary-short
and summary-long, one secret GetSecretValue, scoped logs, and the actual S3 CMK
Decrypt/GenerateDataKey permission. Identity policy alone does not prove the
existing CMK resource policy delegates those operations. No raw/canonical PUT,
delete/list/presign, IAM/admin or secret-write authority is needed. The actual
registered-object eraser/resource-policy and retention SLO remain a separate
620/full-switch gate.

Use the existing protected owner/migrator release path with a fresh exact-head
plan/capture/generation, current CI and independent review. Model settings,
key changes and deployment overlap need a controlled quiescent upgrade; this
proposal does not claim automatic mixed-configuration rollout.

## Evidence and limits

Component tests exercise complete rows after ten, multiMiB text/tool arrays,
lone surrogates, encrypted source/key domains, interrupted writes, nonce/version
replay, final expiry rollback, explicit failure/retry, graph pinning and
default-off/target refusal. Actual 620/630 integration uses the official export,
fold, masked graph, restricted roles, durable discovery/work and both encrypted
pointers. A source change during final read withholds the still-readable old
ciphertext. This is an offline provider fixture, not a live quality/spend proof.

The dedicated-role policy has separate actual owned PostgreSQL 15/16/17
controls; the first proof covered the seven-capability predecessor, and must be
rerun against the final eight-capability source before signoff. Full corpus,
cloud KMS/IAM, real provider, persistent runtime and actual Desktop caller
acceptance are not inferred from those results.
