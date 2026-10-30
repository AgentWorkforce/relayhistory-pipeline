# Cutover observability: metrics contract and T0 dashboard

Status: contract and runbook. The code is in #47. Authorizes no live action.
Applying its migration, running the sampler against a real database, creating
the sampler's login or alert token, and every T0 step still need Khaliq's
explicit OK. The T0 steps are those of
`docs/relayhistory-switch-first-dag.md`.

**Thresholds are proposals.** None has been measured against production
traffic. Calibrate them at the fallback and rollback rehearsals (pre-flip
readiness 3 and 4), and record the pre-T0 baseline of each counter on the day.

## What exists and what doesn't

| Part | State |
| --- | --- |
| Stage lag, queue depth, DLQ depth, stage outcomes | #47: `bun run cutover:metrics` |
| Ingest status by route, scrub refusals | #47: log lines from `alleyoop-ingest` |
| Gate phase, HoldLog, held-write replay | Specified here, **implemented in relayhistory-cloud #224** |
| Read fallback | Specified here, implemented with **G3, which is unowned** |
| Export coverage | Specified here, implemented with **E1 (96BullSessions #40)** |
| Neon write fence, Neon load | Operator SQL below, run by hand through a read-only role. Not run against Neon yet |
| Alerts | #47 sends four rules through nightcto, see "Alert transport". **Unproven: nothing has been delivered** |
| Time-series backend, dashboard product | **Not built, and not needed for T0.** Panels are the sampler's log lines |

Follow the sampler on a terminal. Its login is a member of
`cutover_metrics_reader`; see "Sampler access".

```sh
read -r -s CUTOVER_METRICS_DB_URL < <(op read "op://96Bulls-prod/<cutover metrics login>/url")
export CUTOVER_METRICS_DB_URL
bun run cutover:metrics --interval=15 --window=300 2>&1 | jq -c 'select(.metric or .level == "error")'
```

## Alert transport

Alerts go through the existing transport, not a new backend: **nightcto's
evidence webhook**, `POST /webhooks/cloud-runtime`, schema
`cloud-runtime-evidence/2`. See `docs/alerting-contract.md` and
`packages/relay-entrypoint/src/cloud-runtime-webhook.ts` in `nightcto`. It runs
on GCP Cloud Run, so it shares no failure domain with Supabase or Neon. nightcto
persists the evidence, dedupes on `incidentFingerprint`, suppresses repeats for
15 minutes, delivers to Slack and records the outcome. The sampler only decides
what is wrong.

- **Config:** `NIGHTCTO_EVIDENCE_URL` and `NIGHTCTO_EVIDENCE_TOKEN`, from
  1Password. Neither set means alerts are off, and
  `cutover.alerts.configured = 0` says so. One set, a placeholder, or any URL
  that isn't the https webhook stops the sampler.
- **Dedupe scope:** nightcto isolates evidence with no workspace per occurrence,
  which would page on every sample. Evidence therefore carries
  `workspace.relayId = 96bulls-platform`. That is a fixed scope, not a real Relay
  workspace, so **nightcto's owner should confirm it**.
- **Recovery:** `outcome: "ok"` on the same fingerprint resolves the incident.

| Rule | Fires when | Severity | Sustained |
| --- | --- | --- | --- |
| `metrics_blind` | A source can't be read, including `CM001` | 7 | First sample |
| `dlq_depth` | `dead` plus `pending` > 0 on any stage | 8 | 2 samples |
| `stage_lag` | `object-landed` or `session-indexed` lag ≥ 60 s | 6 | 2 samples |
| `stage_stalled` | Ready jobs on those stages and no `ok` in the window | 8 | 2 samples |

Under the nightcto contract, delivery is **unproven until a person has seen one
real message** (rule 9). Send one from a rehearsal before relying on it. Payloads
pass nightcto's validator, run once by hand; nothing pins that in CI.

**NOT COVERED:** these reach no one unless a person is watching them.

- **A dead sampler.** It sends nothing, and nightcto doesn't detect an absent
  sender, so silence reads as health. During T0, someone must watch for
  `cutover metric` lines stopping.
- **Ingest 5xx and scrub refusals,** including `chunk_stored_unscrubbed`. These
  are log lines.
- **Gate, HoldLog, fallback and export metrics,** until their owners send evidence
  under this same transport. Use `service` of the emitter and the same scope.
- **Neon load and fence.** These are hand-run SQL.

Every other **Alert** cell below is therefore a condition a person on the T0
bridge checks, not a page. The `dlq_depth` rule has no baseline yet: a stage
quarantining `stage_not_configured` will fire it.

## Conventions

- **Emission:** one JSON log line per point through `src/shared/log.ts` (or
  relayhistory-cloud's logger, which must carry the same field names):
  `metric`, `value`, `unit`, plus labels.
- **Labels are allowlisted fields only.** `session_uuid`, `session_id`,
  `machine_id` and `trace_id` are never labels. Each label's values are a closed
  set or pipeline codes. `route` is a registered template such as
  `/v1/pipeline/traces/:traceId`, never a URL. `pipeline.stage.errors` reports
  any `error_code` that isn't code-shaped as `other`, because clients report
  some of these rows.
- **Zero means zero.** A source that can't be read emits no points and
  `cutover.metrics.read_ok{kind} = 0`. **No data is an alert**, never healthy.
- **Units:** `1` (count), `s`, `ms`. `pipeline.stage.events` and
  `pipeline.stage.errors` are counts over the trailing
  `cutover.metrics.window_seconds`, not cumulative counters.

## Metrics

### Sampler health

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `cutover.metrics.read_ok` | `cutover:metrics` | 1 | `kind` = jobs, dead_letters, events, errors | 1 | 0 for 2 samples, or no line for 2 intervals | Treat that panel as blind. During T0 a blind jobs or dead_letters panel stops the flip until restored. `error_code` 57014 on events: raise `--interval`, see "Sampler cost". `CM001`: RLS would hide the rows, see "Sampler access" |
| `cutover.metrics.window_seconds` | `cutover:metrics` | s | none | n/a | n/a | Reads the window of the counts |
| `cutover.alerts.configured` | `cutover:metrics` | 1 | none | 1 | 0 during T0 | Alerts are off; the transport isn't configured. Log lines `cutover alert not delivered` carry the HTTP status of a refused send |

### Gate and hold (relayhistory-cloud #224 emits these)

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `cutover.gate.phase` | `CutoverGate` DO, on every transition and every 15 s | 1 (always 1) | `state` = neon, holding, supabase; `epoch` = gate generation | One `state` at a time; never backward outside an abort | Two states within one `epoch`, or a phase no operator requested | Stop. Read `GET /v1/internal/cutover/phase`; don't advance until explained |
| `cutover.gate.phase_age_seconds` | `CutoverGate` DO | s | `state` | `holding` < 300 | `holding` ≥ 240 warn; ≥ 300 abort decision | See abort criteria |
| `cutover.gate.in_flight_writes` | `CutoverGate` DO | 1 | none | 0 before the fence | > 0 after 125 s in `holding` | A statement should have hit the ceiling by now. Don't fence; investigate |
| `cutover.hold.depth` | `HoldLog` DOs | 1 | `kind` = route class, buffer or refuse | Drains to 0 after the flip | Rising for 5 min after the flip | Check replay outcomes |
| `cutover.hold.oldest_age_seconds` | `HoldLog` DOs | s | none | < 300 + drain target | ≥ `HOLD_WAIT_MS` means clients are seeing 503 with `Retry-After`; ≥ 600 is an incident | Abort before the flip; after it, see replay |
| `cutover.hold.replay` | Replay loop | 1 | `outcome` = ok, duplicate, retrying, failed; `kind` = pipeline, neon; `error_code` | `failed` = 0 | Any `failed` | I3 is at risk. Replay is ordered per org, so stop and inspect that org's HoldLog. Never drop an entry |
| `cutover.hold.refused` | Gate | 1 | `route`; `kind` = route class | Only the refuse class | A buffer-class route refused | Hold is not behaving as designed; abort before the flip |

HoldLog is one DO per org. `org_id` is allowlisted, but aggregate these metrics
across orgs and put `org_id` only on `failed` replay log lines.

### Neon write fence

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `cutover.neon_fence.writable_objects` | SQL 1, read-only | 1 | `kind` = table, function | 0 from step 2 onward, or after a `.restore` the pre-fence count | > 0 after the fence | I2 is not enforced. Don't take the marker; re-apply the fence |
| `cutover.neon_fence.denied_writes` | Worker and Cloud logs, `error_code` 42501 | 1 | `service` | 0 | Any after the flip | Some route still reaches Neon. It failed closed, so no data moved; find the stale route |

### Ingest and scrub

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `pipeline.ingest.requests` | Count `request completed` lines from `alleyoop-ingest` | 1 | `route`, `status` (group as 2xx, 4xx, 5xx) | 5xx < 0.1% over 5 min | 5xx ≥ 1% for 2 min, or 401/403 above the pipeline baseline | Before the flip this is canary traffic, so don't flip. After it, see abort criteria |
| `pipeline.ingest.duration_ms` | Same line | ms | `route` | p95 < 2,000 on chunks and manifests | p95 ≥ 5,000 for 5 min | Check object storage and HoopDb |
| `pipeline.scrub.refusals` | Count `scrub refused` lines (`kind = scrub_refusal`) | 1 | `error_code` = chunk_not_utf8, chunk_unsegmentable, chunk_string_too_long, chunk_stored_unscrubbed | 0 beyond baseline | Any `chunk_stored_unscrubbed`; other codes above baseline | `chunk_stored_unscrubbed` means an object at that key carries no scrub metadata: page the owner. The others mean a client's chunk isn't stored, so check the courier's retries |

The request line has no tenant label, and neither does the refusal line. Refused
chunks are stored nowhere and were not previously logged: the route returned 400
with no event, so `pipeline_events` can't show them.

### Stages and DLQ (`bun run cutover:metrics`)

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `pipeline.stage.queue_depth` | `pipeline_jobs` | 1 | `stage` | Returns to baseline | Rising for 10 min | Check workers for that stage |
| `pipeline.stage.ready_depth` | `pipeline_jobs` | 1 | `stage` | Near 0 | With `lag_seconds` below | Same |
| `pipeline.stage.leased_depth` | `pipeline_jobs` | 1 | `stage` | ≤ workers × batch | n/a | Diagnostic |
| `pipeline.stage.lag_seconds` | `pipeline_jobs`: now minus the oldest ready `visible_at` | s | `stage` | `object-landed`, `session-indexed` < 30 | ≥ 60 for 2 samples; ≥ 300 abort input after the flip | Scale or restart the stage's workers |
| `pipeline.stage.oldest_job_age_seconds` | `pipeline_jobs`: now minus the oldest `enqueued_at` | s | `stage` | < 900 | ≥ 3,600 | Catches what lag can't: a job in backoff or held by a dead consumer. The lease is 300 s |
| `pipeline.stage.events` | `pipeline_events`, folded by `idempotency_key` | 1 | `stage`, `status` | Follows traffic | `ready_depth` > 0 and no `ok` in the window | Workers are stalled |
| `pipeline.stage.duration_p95_ms` | `pipeline_events.duration_ms` | ms | `stage`, `status` | Baseline × 2 | Baseline × 5 | Check the stage's dependency |
| `pipeline.stage.last_event_age_seconds` | `pipeline_events` | s | `stage`, `status` | n/a | Only points inside the window appear; absent means none | Diagnostic |
| `pipeline.stage.errors` | `pipeline_events`, failure statuses, top 50 | 1 | `stage`, `status`, `error_code` | 0 `dead_lettered` | Any `dead_lettered` on a stage with a worker | Read the code first; `stage_not_configured` means no handler is deployed |
| `pipeline.dlq.depth` | `state` = dead, replaying from `pipeline_dead_letters`; pending from `pipeline_jobs.dlq` | 1 | `stage`, `state` | 0 above the pre-T0 baseline | **> 0 above baseline** | `GET /v1/internal/pipeline/dead-letters?stage=`; fix the cause, then dry-run replay. Don't replay during a hold |
| `pipeline.dlq.oldest_age_seconds` | `pipeline_dead_letters` | s | `stage` | < 86,400 | ≥ 86,400 | Unattended dead letters; assign an owner |

The baseline matters because stages that are registered but have no worker
quarantine `stage_not_configured` by design, so a plain `> 0` may already fire
before T0. `lag` covers the Postgres queue only: on a Cloudflare Queues
deployment `pipeline_jobs` is empty and these read 0, so the Queues dashboard is
the source there.

### Read fallback (implemented with G3)

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `fallback.reads` | Worker read path | 1 | `route`; `outcome` = pipeline, fallback, refused | Hit rate falls as export proceeds; 0 before A9 | Fallback share above the expected unexported share | Check `session_index` writes and the export |
| `fallback.duration_ms` | Same | ms | `route` | p95 < 2,000 | p95 ≥ 5,000 for 5 min | Check the branch compute |
| `fallback.breaker_trips` | Circuit breaker | 1 | `state` = open, half_open, closed | 0 | Any `open` | Pre-export reads are failing, so I5 is degraded. Size up the branch compute; never point at Neon production |
| `fallback.limit_hits` | Row and time limits | 1 | `route`, `reason` = rows, time | Low | Rising | Limits may be truncating a real session; check before raising them |

Fallback reads hit the **T0 child branch**. A read on the production compute
breaks I6, so `fallback.reads` carries no target label to choose from: it is
branch only by construction, and Neon load below proves it.

### Export coverage (implemented with E1)

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `export.sessions_total` | Child branch at the marker | 1 | none | Fixed after T0 | Changes | The branch moved; stop the export |
| `export.sessions_exported` | Exporter plus `session_index` | 1 | none | Monotonic | Flat for 30 min during a run | Check the exporter |
| `export.sessions_missing` | `total` minus exported | 1 | `reason` = pending, tombstoned, failed | 0 before A9, tombstoned excluded | Any `failed` not retried | Retry. A9 needs missing 0 and a fallback hit rate of 0 |

### Neon load (SQL 2 to 4, read-only)

`pg_stat_statements` counters are cumulative since reset, so a panel is the
delta between two samples. Label by `queryid` only: **never export the query
text**, which holds literals. `query_id` must be added to the allowlist before
these are emitted as log lines.

| Metric | Source | Unit | Labels | SLO | Alert | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| `neon.statements.top_total_time_ms` | SQL 2 | ms | `query_id` | n/a | One statement over 50% of the delta | Identify it before T0 |
| `neon.statements.near_ceiling` | SQL 2: `max_exec_time` ≥ 120,000 | 1 | none | 0 during the drain | Any new one during the hold | The drain may reach its 125 s bound |
| `neon.statements.cancelled` | Worker and Cloud logs, `error_code` 57014 | 1 | `service` | Baseline | Above baseline | See below |
| `neon.cache_hit_ratio` | SQL 3 | ratio | none | ≥ 0.91, the 2026-10-30 reading | < 0.85 for 5 min | Stop discretionary load; never start bulk reads on production |
| `neon.temp_spill_bytes` | SQL 3 delta | bytes | none | Baseline | Sustained rise after T0 | Production should be near idle by then; find the reader |
| `neon.sessions_activity` | SQL 4 | 1 | `state` | 0 after T0 | Any active `sessions` read or write after the flip | I6 is broken. Find the caller |

**Limit:** `pg_stat_statements` records only statements that finish. A statement
killed by the 125 s ceiling never appears there, so "calls hitting the ceiling"
can't be read from it. `near_ceiling` shows the ones that just made it, and
`neon.statements.cancelled` counts the killed ones from the callers' logs.

### Operator SQL

Not yet run on Neon. Read-only; run through the read-only Neon role.

```sql
-- 1. Fence: objects the runtime role can still write ($1 = role).
SELECT 'table' AS kind, count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
   AND has_table_privilege($1, c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE')
UNION ALL
SELECT 'function', count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'sessions' AND p.prokind = 'f' AND p.provolatile = 'v'
   AND has_function_privilege($1, p.oid, 'EXECUTE');

-- 2. Top statements, by id. Never select `query`.
SELECT queryid, calls, total_exec_time, max_exec_time, temp_blks_written
  FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 20;

-- 3. Cache hit ratio and temp spill.
SELECT blks_hit::float / nullif(blks_hit + blks_read, 0) AS cache_hit_ratio, temp_bytes
  FROM pg_stat_database WHERE datname = current_database();

-- 4. Activity by the runtime roles ($1 = role array). Never select `query`.
SELECT usename, state, wait_event_type, count(*), max(now() - query_start) AS oldest
  FROM pg_stat_activity WHERE usename = ANY($1) AND state <> 'idle'
 GROUP BY 1, 2, 3;
```

SQL 4 needs `pg_read_all_stats` on the read-only role. Without it,
`pg_stat_activity` hides other roles' `state` as NULL, the filter drops those
rows, and the drain looks finished when it isn't. Confirm that a known active
session is visible before relying on it.

`has_table_privilege` with a list is true if the role holds **any** of them,
which is what the fence needs. SQL 4 is by role, and the Worker shares its role
with Cloud, which keeps writing `public`. Read `neon.sessions_activity` together
with `denied_writes`, not as proof by itself.

### Sampler cost

`pipeline_jobs` and `pipeline_dead_letters` reads use small tables. The `events`
read filters on `at`, which has no index of its own, so it scans the partitions
in the window: at most two months' worth. The `errors` read has the partial
failures index. Each read runs in its own `READ ONLY` transaction with a 5 s
statement timeout, one connection, and at least 15 s between samples. If
`events` times out, its `read_ok` goes to 0 and the jobs and DLQ panels keep
working. If that happens at real volume, add an index on `at` as a reviewed
migration rather than lengthening the timeout.

### Sampler access

The sampler never bypasses RLS. The three tables have RLS on and no policy, so it
reads through SECURITY DEFINER aggregates, `sessions.cutover_metrics_jobs()`,
`_dead_letters()`, `_events(window)` and `_errors(window)`, as a login that is a
member of `cutover_metrics_reader`. That role is NOLOGIN, has no BYPASSRLS and no
table privileges. The functions return aggregates and closed labels only.

A role reading these tables without access would get no rows, and every panel
would show a healthy zero. Three things stop that:

- Each function raises `CM001` if RLS would filter it. That means `read_ok = 0`
  and an immediate `metrics_blind` alert, never a zero.
- The sampler refuses to start on a role with BYPASSRLS or superuser.
- `anon` and `authenticated` can't execute these functions.

## T0 dashboard and runbook

Row 1, always on: gate phase and age, `read_ok`, HoldLog depth and oldest age,
replay outcomes, ingest 5xx. Then, per step:

| T0 step | Watch | Proceed when | Abort when |
| --- | --- | --- | --- |
| Before (phase `neon`) | Every `read_ok`; DLQ baseline; `neon.cache_hit_ratio`; `near_ceiling` | All `read_ok` 1, baselines written down, canary indexed | Any blind panel, or Neon already under alert |
| 1. Hold (`neon → holding`) | Phase; `cutover.hold.depth`, `oldest_age_seconds`; `cutover.hold.refused`; reads still on Neon | Depth grows only for buffer-class routes; no replay; reads healthy | A buffer-class route refused, a HoldLog write fails, or read errors rise |
| 2. Drain and fence | `in_flight_writes`; SQL 4; `near_ceiling`; SQL 1 | In-flight 0, no active `sessions` write, then SQL 1 reads 0 for both kinds | Not drained by 125 s, or SQL 1 > 0 after the fence |
| 3. Cutover marker | Phase age; `denied_writes`; marker read back from S3 and Supabase | Both copies agree, and any `denied_writes` are explained | The copies disagree, or a write landed after the fence |
| 4. Child branch | Phase age against 240 s and 300 s; branch checks of step 4; `fallback.duration_ms` from a probe | Marker, counts and ledger match; branch compute answers | Any mismatch, or the branch can't be ready by 300 s |
| 5. Flip (`holding → supabase`) | `cutover.hold.replay{kind=pipeline}`; `pipeline.ingest.requests`; `pipeline.stage.lag_seconds`; `pipeline.dlq.depth`; `pipeline.scrub.refusals`; `fallback.*`; `neon.sessions_activity` | HoldLogs empty, 5xx at baseline, lag under 60 s, DLQ at baseline, a pre-T0 session reads through the fallback | See "After the flip" |

### Abort, steps 1 to 4

Abort on any cell above, on `holding` reaching 300 s without a clear finish, or
on a blind jobs, dead_letters or gate panel at the moment of a decision. Apply
the fence's `.restore`, move the gate `holding → neon`, watch
`cutover.hold.replay{kind=neon}` drain with `failed` = 0, confirm SQL 1 returns
to the pre-fence count, and discard the child branch. An abort after step 3
leaves the marker as written, since it is never edited. A new attempt records
its own marker under a new gate generation.

### After the flip

There is no repointing. These stop the rollout and call Khaliq; the rollback is a
new hold plus a reverse copy, with its own approval:

- any `cutover.hold.replay{outcome=failed}` that doesn't clear on retry (I3);
- ingest 5xx ≥ 5% for 5 min;
- `object-landed` or `session-indexed` lag ≥ 300 s with no `ok` in the window;
- `pipeline.dlq.depth` above baseline on either of those stages and still rising;
- any `chunk_stored_unscrubbed`;
- `fallback.breaker_trips{state=open}` that doesn't close within 5 min (I5);
- any `sessions` write activity on Neon production (I2, I6).
