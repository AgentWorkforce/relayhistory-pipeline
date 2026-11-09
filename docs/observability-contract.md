# Observability contract (plan section 12, as built)

**Ids.** `session_uuid`: UUIDv7, minted once by the client (courier) when a
session first appears, immutable, carried in the manifest's `extensions["96bulls"]`,
the manifest object's `x-amz-meta-session-uuid`, `StageMessage.ctx.session_uuid`,
`sessions.pipeline_jobs.session_uuid` (trigger), `sessions.session_index_ext`,
`session_summaries`, `session_verdicts`, `pipeline_dead_letters`, and every span
as an attribute. Never a metric label. If a client sends none, the indexer
derives one once and records `session_uuid_source = derived`.

`trace_id`: the pipeline trace id (UUIDv7 in the manifest, upstream's contract:
one per session, revisions carried separately) is also the W3C trace id
(32 hex, no dashes). `traceparent` = `00-<trace_id hex>-<span>-01`. Propagation:
courier HTTP headers `traceparent` + `X-Relay-Trace-Id`; ingest span joins it;
`ctx.traceparent` on every queue message; `pipeline_jobs.traceparent`
(trigger, synthesised from `trace_id` when absent); each consumer continues it.
Dead letters keep `session_uuid` + original `traceparent`; replay links.

`tenant_id` = org id, from auth, in `ctx.tenant_id` and the jobs column.
`user_id` on the index comes from the authenticated ingest request
(`ctx.user_id`), never from the manifest body; a reconcile without a request
keeps the value an authenticated upload recorded.

**Telemetry carries ids, codes and counts only.** The allowlist is in
`src/shared/log.ts` and applies to log lines and span attributes. Strings are
scrubbed again on emit as a backstop; the collector drops `prompt`, `summary`,
`message`, `path`, `body` attributes and redacts secret shapes. The smoke test
scans spans and every container log for the planted secrets, the content
marker and prompt text.

**Fields** (allowlisted): service, deployment, deployment_id, stage, status,
outcome, attempt, trace_id, span_id, traceparent, session_uuid,
session_uuid_source, tenant_id, org_id, workspace_id, machine_id, harness,
session_id, revision, work_key, error_code, retryable, duration_ms,
scrub_ruleset, scrub_version, redaction_count, object_sha256, bytes_raw,
bytes_stored, chunk_count, model_id, tokens_in, tokens_out, state, rule, kind.

The AWS EKS stage runs the pinned contrib collector in-cluster and archives
lossless OTLP/JSON trace batches under the Locker bucket's encrypted
`telemetry/traces/` prefix through a dedicated least-privilege EKS Pod
Identity. The collector can append there but cannot read or delete either
telemetry or session objects. This preserves the canonical UUIDv7 trace id;
X-Ray cannot, because it interprets the first 32 bits as epoch seconds.
Structured stage events and DLQs remain in HoopDb; service logs remain
structured stdout.

**Product analytics is a separate stream** (helm-charts
`docs/analytics-adapter-contract.md`, `docs/analytics-collector-interface.md`).
Business logic emits typed events through `AnalyticsPort.capture`
(`src/shared/analytics.ts`); a per-kind allowlist projects each into the
versioned envelope (closed classes, latency buckets and clamped counts; verified
org/workspace ids for workspace events; never content, paths, session/user ids,
error text or tokens). `ANALYTICS_SINK` selects the destination
(`src/shared/analytics-config.ts`) and defaults to noop. Self-host services accept
only `collector` (POST to the in-cluster collector's `/v1/events` with a
per-producer bearer); the hosted Workers accept only `posthog`
(`https://i.agentrelay.com`). There is no fallback between them. Export is
bounded (`src/shared/analytics-export.ts`): a queue that drops the newest event
when full, batch and byte limits, a total delivery deadline, bounded retries, a
shutdown flush, and drop/failure counters reported as `analytics export degraded`
log lines. A failing sink never fails a request. Events: `session_ingest.batch_completed`,
`session_read.request_completed` and `session_pipeline.index_tick_completed`
(installation-scoped). Their registration in the producer registry is pending owner review.

**Not yet built:** metrics and SLO alerts (stage lag, DLQ > 0, scrub refusal),
tail sampling, per-tenant retention/deletion in the telemetry backend, and a
durable hosted log exporter. Logs are unsampled; errors are never sampled out.
