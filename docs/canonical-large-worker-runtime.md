# Large canonical Node worker

This is dormant preparation. Neither building the image nor rendering a chart authorizes a production rollout, schema apply, privilege grant or flag change.

Build `docker/large-read-worker.Dockerfile` with `SOURCE_REVISION` equal to the checked-out commit. It uses Bun 1.4.2 only to install and bundle, and Node 24.21.0 to execute. The bundle is produced by `scripts/build-large-read-worker.ts` with the same derived-schema plugin as the Lambda build, so the migration generation and verifier SQL are embedded and the runtime image contains neither `supabase/` nor `node_modules`. The separate services image does not contain this entry point. The image CMD is `node --max-old-space-size=49152 /app/large-read-main.mjs`; run it under the independently measured capacity envelope; no public ingress is needed. Disabled startup reads no credentials, opens no sockets and makes no database, S3 or network calls; it only loads the bundle (Node reads its own OpenSSL/resolver configuration) and writes one JSON status line to stdout.

Required environment when explicitly enabled:

- `SESSION_LARGE_READ_WORKER_ENABLED=1`; `SESSION_LARGE_READ_MAX_JOBS=1` by default, valid 1–10.
- `SESSION_PIPELINE_SECRET_JSON` from the existing secret contract (Supabase URL and ClickHouse client configuration), injected through a secret reference.
- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, from an approved credential reference. This runtime does not implement ambient federation exchange.
- `PIPELINE_RAW_S3_BUCKET`, `PIPELINE_KEY_PREFIX`, `PIPELINE_ORG_FROM_PREFIX=true`.
- Optional `PIPELINE_RAW_S3_SSE=AES256` or `aws:kms`; `PIPELINE_RAW_S3_KMS_KEY_ID` requires `aws:kms`.

No secret values belong in chart values, logs, source or proof artifacts. The runtime needs existing backend RPC execution, ClickHouse ingestion, current raw S3 Get and normalized/read-projection Put/Get, plus the applicable KMS permissions. It does not need Neon access, raw overwrite, object deletion or version access. Missing runtime identities and permissions remain an operator dependency; none are granted here.

The process advances a persistent 128-scope keyset cursor before consuming jobs, with at most 16 pages per invocation. The cursor rotates across the whole eligible catalog, so an interrupted reconcile does not repeatedly strand cohorts after the first page. Unknown or legacy zero counts use conservative large-worker routing. A source is not acknowledged on enqueue alone: the existing nonce-bound job must publish a current source epoch/fingerprint and complete under the same valid lease. Repeated scans coalesce unchanged jobs. Five failed attempts poison a job for explicit repair; errors retain bounded codes only.

The worker renews every 30 seconds against a 120-second lease. Wall-clock expiry is checked after source capture and after potentially blocked publication; losing expiry rolls back provisional publication. A 30-minute process timer and SIGTERM/SIGINT prevent a new publication after shutdown is requested. Abrupt process loss leaves the lease reclaimable. Every output object is verified before the final source CAS. A shared 64 MiB hydrated-row bound refuses generations the reader cannot serve; no row is silently truncated.

Measured synthetic capacity, not live parity: 1,550,924,671 raw union bytes; 901,653 unique ClickHouse rows; 21,254 verified output objects totaling 5,482,859,417 bytes; 600.972 seconds. Peak RSS 8,672,305,152 bytes and cgroup peak 12,389,949,440 bytes in a 64 GiB / 8 CPU container. Four actual workerd HTTP proofs preserve exact windows/cursors, tenant/revision fences and full 2,360,000-character content with 10,000 tools. The standard profile refuses the same source before raw reads. These results do not establish live backfill completeness, production latency, every admitted extreme, or a production identity/resource plan.

Before activation: independent review of this runtime/discovery composition, final image revision/digest, chart-owned default-off execution plan, exact least-privilege operator plan, applied SQL/ACL, full current publication, all search on ClickHouse, all selected desktop reads on Supabase/S3, erasure parity, fresh gates and rollback. No 48-hour canary wait is required; the accepted post-switch observation still applies.
