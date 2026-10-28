# MOVE_NOTES: pipeline framework from relayhistory-cloud PR #210

Source: https://github.com/AgentWorkforce/relayhistory-cloud/pull/210
(branch `sp/pipeline-framework`, head `b453db5571a716b22480074bee7233c3caac60f3`, 7 commits).

Only the PR diff was moved (33 files). Nothing outside the diff was copied or
reimplemented. The 21 files the PR adds are byte-identical to their blobs at the
head sha (checked with `cmp`). The 12 files the PR changes exist only on
relayhistory-cloud `main`, so their hunks are kept as unapplied patches.

Status: inactive. The migration has not been applied anywhere. Nothing here is
wired into a worker, a queue binding or a deployment.

## Path mapping

| relayhistory-cloud (PR #210) | 96BullSessions |
| --- | --- |
| `packages/relayhistory/src/lib/pipeline/*.ts` (12 files) | `packages/pipeline/src/lib/pipeline/*.ts` |
| `packages/relayhistory/src/routes/pipeline.ts` | `packages/pipeline/src/routes/pipeline.ts` |
| `packages/relayhistory/test/pipeline-*.test.ts` (3 files) | `packages/pipeline/test/pipeline-*.test.ts` |
| `packages/relayhistory/test/support/pipeline-queues.ts` | `packages/pipeline/test/support/pipeline-queues.ts` |
| `docs/pipeline-observability.md` | `docs/pipeline-observability.md` |
| `scripts/pipeline-dlq-replay.mjs` | `scripts/pipeline-dlq-replay.mjs` |
| `supabase/migrations/20261025000600_pipeline_queue.sql` | `supabase/migrations/20261025000600_pipeline_queue.sql` |
| `supabase/tests/pipeline_queue_test.sql` | `supabase/tests/pipeline_queue_test.sql` |
| modified: `AGENTS.md`, `CLAUDE.md` | `integration/relayhistory-cloud/{AGENTS,CLAUDE}.md.patch` |
| modified: `packages/relayhistory/src/{api-worker,app,env}.ts` | `integration/relayhistory-cloud/packages/relayhistory/src/*.ts.patch` |
| modified: `packages/relayhistory/src/lib/logger.ts` | `integration/relayhistory-cloud/packages/relayhistory/src/lib/logger.ts.patch` |
| modified: `packages/relayhistory/src/middleware/observability.ts` | `integration/relayhistory-cloud/packages/relayhistory/src/middleware/observability.ts.patch` |
| modified: `packages/relayhistory/src/routes/{delivery,ingest}.ts` | `integration/relayhistory-cloud/packages/relayhistory/src/routes/*.ts.patch` |
| modified: `supabase/README.md`, `supabase/tests/{behavior_test.sql,run.sh}` | `integration/relayhistory-cloud/supabase/...patch` |

The `src/lib/pipeline` and `src/routes` structure is kept on purpose. Every
relative import between files in the diff still resolves without edits, and each
import that does not resolve now points at the path where the missing module
would have to live (`packages/pipeline/src/env.ts`, `src/lib/logger.ts`, and so on).
The tests reach the SQL through `new URL("../../../supabase/", import.meta.url)`,
which still lands on the repo-root `supabase/` at the new depth.

## Unresolved imports (modules not in the diff)

Each one is a relayhistory-cloud `main` module under `packages/relayhistory/src/`.

### `src/env.ts` (needs `packages/pipeline/src/env.ts`)

| File | Import | What it needs |
| --- | --- | --- |
| `src/lib/pipeline/index.ts:9` | `import type { Bindings } from "../../env.js"` | Worker `Bindings` with `ENVIRONMENT` and the PR #210 additions (`integration/.../env.ts.patch`): `PIPELINE_EVENTS_URL/_TOKEN/_SCHEMA`, `PIPELINE_ERROR_TRACKER`, `OTEL_EXPORTER_OTLP_ENDPOINT/_HEADERS`, `SENTRY_DSN`, `PIPELINE_OPS_SECRET`, `PIPELINE_TELEMETRY_ID_KEY`, `PIPELINE_FILE_NORMALIZE_ENABLED`, and seven `Queue<StageMessage>` bindings `PIPELINE_OBJECT_LANDED ... PIPELINE_FILE_MAINTENANCE` |
| `src/routes/pipeline.ts:2` | `import type { AppContext, AppEnv, Bindings } from "../env.js"` | Hono `AppEnv` (`{ Bindings; Variables }`), with `Variables.traceId?` (added by the patch), `requestId`, `correlationId`, `auth?`, plus `AppContext` = `Context<AppEnv>` |
| `test/pipeline-observability.test.ts:4` | `import type { AppEnv, AuthContext, Bindings } from "../src/env.js"` | as above, plus `AuthContext` `{ userId, orgId, workspaceId, tokenSubject, scopes, claims }` |
| `test/pipeline-queue.test.ts:20` | `import type { Bindings } from "../src/env.js"` | `Bindings` (only used as `{} as Bindings`) |

### `src/lib/logger.ts` (needs `packages/pipeline/src/lib/logger.ts`)

| File | Import | What it needs |
| --- | --- | --- |
| `src/lib/pipeline/dlq.ts:22` | `import type { LoggerEnv } from "../logger.js"` | `LoggerEnv` `{ environment; posthogApiKey?; posthogHost?; telemetryIdKey? }` (`telemetryIdKey` comes from `logger.ts.patch`) |
| `src/lib/pipeline/events.ts:15-20` | `import { createLogger, type LogFields, type Logger, type LoggerEnv } from "../logger.js"` | `createLogger(env, component)` returning a `Logger` with `info`/`warn`/`error`/`flush()`, plus the `LogFields` type |
| `src/lib/pipeline/index.ts:10` | `import { loggerEnvFromBindings } from "../logger.js"` | `loggerEnvFromBindings(env: Bindings): LoggerEnv`, which per the patch also sets `telemetryIdKey: telemetryIdKey(env.PIPELINE_TELEMETRY_ID_KEY)` |
| `src/routes/pipeline.ts:4` | `import { loggerEnvFromBindings } from "../lib/logger.js"` | same |

### `src/lib/scrub.ts` (needs `packages/pipeline/src/lib/scrub.ts`)

| File | Import | What it needs |
| --- | --- | --- |
| `src/lib/pipeline/trace.ts:16` | `import { scrubText } from "../scrub.js"` | `scrubText(value: string): string`, relayhistory's secret/PII scrubber. Used as `scrubText(value) === value` to reject unsafe identifiers |

### `src/middleware/auth.ts` (needs `packages/pipeline/src/middleware/auth.ts`)

| File | Import | What it needs |
| --- | --- | --- |
| `src/routes/pipeline.ts:3` | `import { getAuth, requireScope } from "../middleware/auth.js"` | `getAuth(c): AuthContext` (reads `orgId`, `workspaceId`) and the `requireScope("rth:read" \| "rth:sync")` Hono middleware |

### `src/app.ts` (needs `packages/pipeline/src/app.ts`)

| File | Import | What it needs |
| --- | --- | --- |
| `test/pipeline-observability.test.ts:3` | `import { createApp } from "../src/app.js"` | The full relayhistory Hono app with the `app.ts.patch` wiring: trace-id middleware echoing `X-Relay-Trace-Id`, `/health`, and `pipelineRoutes` / `pipelineOpsRoutes` mounted under `/v1` |

### Ambient types and packages (no manifest moved)

- Cloudflare Workers globals `Queue<T>` (`queue.ts:71`, `index.ts:107-113`) and
  `Message<T>` (`dlq.ts:297`) need `@cloudflare/workers-types`. The patches also
  use `MessageBatch`, `ExecutionContext` and `ScheduledController`.
- npm: `hono` (routes, tests), `vitest` (tests), `@electric-sql/pglite` (queue and
  schema tests). `node:crypto`, `node:fs`, `node:url` and `node:util` need
  Node built-ins or `nodejs_compat`.
- There is no `package.json` or `tsconfig.json` for `packages/pipeline`, because
  none was in the diff.

### Integration patches (the call sites, not applied)

Under `integration/relayhistory-cloud/`. These are kept as the hunks that wire
the framework into the relayhistory worker. Each one targets a file that does
not exist here:

- `api-worker.ts.patch`: queue dispatch via `PIPELINE_QUEUE_PREFIX` /
  `handleStageQueue` / `pipelineQueueDeps`, and `maintainPipelineEvents` on cron.
  It also uses `./lib/session-brief-search.js`, `./lib/session-analysis/worker.js`
  and `handleSessionQueue` / `SessionWake` from main.
- `app.ts.patch`: trace-id middleware and route mounting. It needs the rest of
  relayhistory's routes (`auth`, `memory`, `delivery`, `onboarding`, `pair`,
  `recall`, `rollups`, `reflex`, ...).
- `delivery.ts.patch` / `ingest.ts.patch`: `recordRequestStage(c, "deliver.put", ...)`
  call sites. They also use `../lib/request-body.js`, `../lib/request-stats.js`,
  `../lib/telemetry.js` (`exportableCorrelationId`, `captureServerEvent`),
  `../lib/embed.js`, `../lib/ingest.js`, `../lib/types.js` and `createDb`.
- `env.ts.patch`, `logger.ts.patch`, `observability.ts.patch`: see above.
- `AGENTS.md.patch` / `CLAUDE.md.patch`: relayhistory-cloud policy text (the
  "approved exception" for Supabase REST and "PR 170 must not merge"). That text
  is specific to relayhistory-cloud. Do not adopt it here as-is.
- `supabase/README.md.patch`, `supabase/tests/run.sh.patch`,
  `supabase/tests/behavior_test.sql.patch`: docs and test-runner hunks for files
  that exist only on relayhistory-cloud main.

## Migration dependency

`supabase/migrations/20261025000600_pipeline_queue.sql` cannot run on its own.
It runs `ALTER TABLE sessions.pipeline_events` / `sessions.pipeline_dead_letters`,
drops their constraints `pipeline_events_stage_check` /
`pipeline_dead_letters_stage_check`, and creates `sessions.pipeline_jobs` in the
`sessions` schema. All of these are created by
**`20261025000000_sessions_index.sql`** on relayhistory-cloud `main`, merged via
PR #171 (merge commit `51c313fda3c4288cf75f9927c4004fb39b6cd2ee`). That file is
not part of the #210 diff and is not here.

On relayhistory-cloud main the migration also sorts after `20261025000100_repo_identity.sql`,
`20261025000200_scrub_quarantine.sql` and `20261025000300_upsert_keep_on_null.sql`.
The TS tests apply every file in `supabase/migrations/` in order, after
`supabase/tests/supabase_shim.sql`, which is also not in the diff. The SQL tests
`supabase/tests/pipeline_queue_test.sql` and the `behavior_test.sql`/`run.sh`
patches need the same base schema, and `dblink` for the concurrent-claim case.

The migration's header comment still names `packages/relayhistory/src/lib/pipeline/registry.ts`.
Here that file is `packages/pipeline/src/lib/pipeline/registry.ts`. The comment
was left untouched so the file stays byte-identical.
