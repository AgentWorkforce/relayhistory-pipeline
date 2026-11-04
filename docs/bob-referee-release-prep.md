# Bob Costas and Referee release preparation

Status: **DRAFT / activation held**. This is source and disposable-fixture work
from `9582ff8fe2566841a3e0ed294ebbff3e64029eb6`, not a deployed release. No
production flags, resources, permissions, databases, keys, or workloads changed.

## Verified runtime inventory (2026-11-04, approximately 06:25–06:35Z)

- AWS account `941433935742`, `us-east-1`, verified using the existing
  sessions-prod SSO configuration. The unrelated Mini `sessions-dev` profile
  resolves to `500268966557` and was not used for production diagnostics.
- `96bulls-aws-sessions-prod-session-pipeline`: Active / Successful,
  Node 24, arm64, 3008 MB, 300 seconds; modified 2026-11-02T19:22:46Z.
  `PIPELINE_FANOUT=off`, `SEARCH_INDEX_ENABLED=0`,
  `SESSION_BRIEF_ENABLED=false`; derived-worker switch absent.
- Regional Lambda inventory contains the pipeline and read API, with no
  separate Bob/Referee functions. This does not establish absence from every
  other possible service/platform. Production Bob/keyring health is unproved.
- Existing reconcile rule enabled every 30 minutes; brief rule disabled.
  Session-events SQS snapshot: 0 visible / 12 not visible. This is a snapshot,
  not a queue-throughput or consumer-health proof.
- Pipeline role: `96bulls-aws-session-pipeline-aws-sessions-prod-nsvhn3beskmuvvon`.
  Its resource policies authorize the existing one-secret read, S3 list/read,
  SQS consumption/send, and S3-mediated KMS Decrypt. No summary S3 PutObject or
  KMS GenerateDataKey grant was found in its identity/resource policies.
- SF KIND namespace `96bulls-e2e`: Bob/Referee each 1/1 on
  `96bulls-services:summary-4386bee`; Bob has a projected summary-key Secret.
  These are older **test** deployments, not production acceptance.
- Read-only SF chart checkout: `helm-charts` head
  `d200cc5cdb560e646d16ad603da400a031f9eb58`, branch
  `feat/relaycast-portable-core`. Its default workers are enabled; Bob's
  default 384 MiB limit leaves little or no margin against observed native peaks.
  No chart files or existing workloads were edited.

## Source package

`bob-costas/raw-digest.ts` retains one bounded native chunk and a bounded
digest instead of a session-sized array/string/spread call. Native Claude and
Codex prompt/message records are recognized. It retains at most ten 2000-char
prompt samples, 64 bounded model names, and 128 bounded tool names, while
counting all turns. Tool dictionaries are safe for names such as `__proto__`.

Canonical RelayHistory envelopes use the existing `readManifestRecords`,
`normalizeRelayhistoryRecords`, and `projectRelayhistorySession` implementations.
No separate revision/delete fold or JSON decoder is introduced. Canonical
folding is capped at 64 MiB / 100,000 records; each raw chunk is capped at
16 MiB. Refusal is permanent and visible through the existing pipeline DLQ.
The finite-duration consumer additionally caps native jobs at 64 MiB. This is
safe bounded refusal, not full support for the large canonical population.

Both workers verify tenant/session bindings and require the exact live index
key, revision and content hash. Bob rechecks under the publication transaction's
index row lock; Referee checks and writes its verdict/state in one transaction.
Short-summary writes and clears, including retry/error paths, recheck the source
and expected summary ciphertext hash under that lock. Publication transactions
have 5-second lock and 30-second statement timeouts. Both current summary kinds
are authenticated on replay; native stored/raw digests are checked when declared.

The sweep now detects changed revisions/hashes and moved ready-object locations.
Pending jobs and unresolved DLQs suppress work only for the matching harness.

SQL text projection defaults off. Only explicit
`BOB_COSTAS_DIRECTORY_SUMMARY_ENABLED=true` or an injected test directory
opts into the legacy short-summary copy. Existing columns/data are not removed.
`BOB_COSTAS_MODEL=off` is extractive; other model names remain unimplemented.

Managed `upload-generations/` / `publications/` jobs are explicitly refused
until Native's shared authority contract is composed. Legacy index-row fencing
does not replace managed-generation authority. No managed canonical image is
rewritten or given a guessed sidecar path by this draft.

`prepareSummaryObject` now seals a supplied immutable key entirely in memory and
returns the exact ciphertext, SHA-256, byte count and current key ID for Native's
registration step. It performs no storage or authority operation. Generate the
artifact UUID before encryption through Native's helper; register before the
conditional immutable PUT. A retry under the same key must reuse the prepared
bytes; re-encryption needs a new UUID/key. Envelope v1, key derivation and AAD
remain compatible. Reads enforce the 1 MiB actual-stream bound even when object
metadata lies, and oversize writes fail permanently.

## Default-off durable-consumer preparation

`session-pipeline/derived-tick.ts` consumes at most one job per stage from the
existing `sessions.pipeline_jobs`, using the real framework, SQL event sink,
retry/DLQ paths and a fresh schema/role gate before every claim. Without a
keyring it consumes Referee work only and leaves summary jobs untouched.
It starts no new claim with less than 60 seconds remaining. An optional
historical sweep is independently gated and limited to ten rows per kind.

The Lambda accepts `{ "derived": true }` only when
`SESSION_DERIVED_ENABLED=true`. Unset/false returns before secret or DB access.
`SESSION_DERIVED_SWEEP_ENABLED` is separately default-off. The existing managed
secret may eventually carry `summary_master_keys` using the existing master
format; values are imported into RAM, never logged or written by this code.
Existing master IDs and decrypt keys must be preserved. No key was fetched,
created in 1Password, rotated, or installed during this work.

**Packaged schema contract:** the Alchemy function build now embeds the exact
migration generation and both authoritative verifier SQL files through
`derived-schema-build.ts`. Source/container runs load those same files normally.
The shared live gate retains its read-only transaction, both verifiers,
function-boundary check and final marker read. Empty assets close the gate.
Embedded inputs are frozen and registered as build/watch dependencies. The
compiled generation comes from source, never from the live database marker;
the final composed release must rebuild this capture from its own migration set.
This older baseline has two verifier files. The newer release composition has
four, including ingest and seeder; Auth owns that adaptation. This checkpoint
does not prove the newer four-verifier composition.

The detached Node 24 proof uses the actual function-props build plugin and
Alchemy/Rolldown bundler. Only the resulting `.mjs` file is in its temporary
runtime directory: no SQL or migration files. It performs real PG/S3 queue and
summary work, and claims nothing when the role marker is pending. This closes
the schema-asset packaging gap in source; production arm64/live validation and
the other authority/permission gates below remain held.

After that and the managed-authority dependencies pass, the reviewable deployment
changes are: a disabled EventBridge rule targeting this same function with
`{"derived":true}`, its account-scoped invoke permission, exact-image/schema
pinning, managed keyring injection, and separately reviewed runtime/IAM flags.
Do not use the auto-terminating migration EC2 runner as a persistent service.

Necessary AWS policy scope is S3 PutObject only on approved publication-qualified
encrypted summary sidecars (plus existing read/list) and KMS GenerateDataKey via
S3 with the existing account/service/encryption-context restrictions. No delete,
old-version read, broad bucket write, new key, or broader credential fallback is
needed for Bob. Exact managed sidecar resources depend on Native's shared helper;
do not invent an alternative layout or apply a wildcard grant in this draft.

## Actual proof and validation

Run on Linux with Node 24 on PATH, in an isolated checkout with its own frozen Bun install:

```sh
bun run typecheck
bun test src/bob-costas src/referee src/session-pipeline/__tests__/derived-lambda.test.ts src/session-pipeline/__tests__/pipeline.test.ts
bun test src/hoopdb/__tests__/session-directory-short-summary.test.ts --test-name-pattern 'short_summary end to end'
bun run src/proof/bob-runtime.ts
bun run src/proof/bob-capacity.ts
```

The runtime proof creates uniquely named Docker fixtures and removes only its
own containers/volumes. PG17 runs the full real migrator and validates a dedicated
non-superuser runtime role. MinIO is versioned; generated fixture credentials
enter through a RAM FIFO, not Docker Config.Env, arguments, or saved files.
Summary master keys exist only in process memory. ClickHouse 26.8 is real.

Validation: 107 build/gate/worker regression tests and 11 legacy projection cases
passed; the full-flow proof passed 13 checks. Three scoped erasure tests and
eleven migration/sentinel/ACL cases also passed. Root and workspace typechecks
passed. The bundled Lambda
returned safely for the default-off event under Node 24 from outside the checkout.
The services Dockerfile built locally as `96bulls/pipe-bob-prep:20261104`,
image ID `sha256:6a9a8d8be8e231ab8748e64fbfda80ec102f317bf72eef51597bf32ffe8aacf7`
(amd64). Worker imports and missing-summary-key startup refusal passed inside it.
This image was built at the original `c98a532` checkpoint, not rebuilt for every
later source change. It was not pushed; production arm64 packaging is unverified.

The proof starts actual indexer/Referee/Bob processes, sends a signed HTTP upload
through the real SQL ledger and Technical Foul path, then verifies live index
and verdict rows, both encrypted and decryptable summary artifacts, current
pointers, absence of planted secrets, canonical source reads, and actual
S3-to-ClickHouse source search rows. It proves stable replay hashes, regeneration
after update, wrong-tenant permanent refusal/DLQ visibility, finite-duration
queue consumption, absent-key behavior, deadline gating, and all-version S3
raw/manifest/summary erasure plus ClickHouse erasure. Worker logs exclude the
planted secret and source marker.

Barrier regressions explicitly let a newer job commit before releasing an older
summary, nonready clear, or retry read-error job. The newer pointer/object survives.
Legacy projection opt-in tests continue to cover provenance, integrity failure,
rotation/decrypt compatibility and retry repair.

Native capacity: 1,610,637,312 raw bytes, 192 chunks, 192 counted turns,
ten retained prompt samples; final observed max RSS 386,464 KiB (about 377 MiB).
An earlier run peaked at 394,880 KiB (about 386 MiB).
This is synthetic native JSONL, not a measured canonical 1.53 GB acceptance run.
Do not retain the old 384 MiB chart limit or infer canonical support from it.

## Remaining acceptance dependencies

1. Native 040600's authoritative generation-qualified SQL final-write and clear
   CAS, immutable image recognition, and publication/attempt-qualified sidecar
   helpers. Exercise both-job barriers against that exact contract for Bob and
   Referee; legacy tests do not prove managed-image safety.
2. Compose the canonical foundation and consume its authoritative published
   pages (`canonicalArtifactPage`, replay view), with pinned publication
   generation/currentness, instead of refolding >64 MiB canonical sessions.
   Reuse the authoritative large fold chooser/stream from the seed/replay source
   where required. Run Bob against the actual largest-cohort proof afterward.
3. Compose and prove managed reset-generation safety with Native/GC. Migration
   `0005_derived_pointer_erasure.sql` now atomically removes summary pointers and
   verdicts for an authorized physical index tombstone/delete, including scoped
   bulk deletes. Installation changes no existing row; siblings and rollback are
   tested. It does not replace managed generation/ABA fencing or erase preexisting
   orphan metadata through an unreviewed initial purge.
4. Review the exact arm64 artifact, permissions, secrets, schema and disabled
   schedule as one release. The armed schema-gate bundle is now proved locally.
5. Add the authenticated Bob output reader and approved search projection.
   In this source, only Bob retry and the smoke/proof utilities read
   `session_summaries` / decrypt its objects. Neither the read API nor the read
   Worker exposes those artifacts. Searches in the current Desktop Rust/TS
   checkout found no Bob pointer consumer. Existing CH indexing consumes
   canonical source records, not Bob summaries. Preserve the existing four
   search pools; do not add a fifth pool or reuse model-brief state.
6. Perform authorized live upload-to-Desktop/search acceptance, backlog/error
   observation and population checks. No production readiness claim follows
   from these local fixtures or an HTTP health response.
