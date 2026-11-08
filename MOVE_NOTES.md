# MOVE_NOTES: pipeline framework from relayhistory-cloud PRs #210 and #211

The #210 notes come first; the #211 port (storage adapter and object store)
is at the end.

# PR #210

Source: https://github.com/AgentWorkforce/relayhistory-cloud/pull/210
(branch `sp/pipeline-framework`, head `f544f6de80106f7782b48993bb269e0cb152831e`, 11 commits).

Only the PR diff was moved (33 files). Nothing outside the diff was copied or
reimplemented. The 21 files the PR adds are byte-identical to their blobs at the
head sha (checked with `cmp`). The 12 files the PR changes exist only on
relayhistory-cloud `main`, so their hunks are kept as unapplied patches.

Status: inactive. No migration has been applied to any database. The tests use
in-memory PGlite only. Nothing here is wired into a worker, a queue binding or a
deployment.

## Which #210 head

The moved files were taken from head `f544f6d`. A later check was asked to use
`f544f6de80106f7782b48993bb269e0cb152831e`, believed to be the current head on
GitHub. Comparing the two (no network; `f544f6d` is not in any local clone, so
the comparison is the moved files against the `f544f6d` blobs):

- Identical: `dlq.ts`, `errors.ts`, `registry.ts`, `stages.ts`,
  `test/support/pipeline-queues.ts`, `scripts/pipeline-dlq-replay.mjs`.
- Different: `events.ts`, `index.ts`, `postgres(t)-dead-letters.ts`, `queue.ts`,
  `trace.ts`, `traces.ts`, `routes/pipeline.ts`, the three tests,
  `docs/pipeline-observability.md`, `20261025000600_pipeline_queue.sql` and
  `pipeline_queue_test.sql`.
- Not in `f544f6d` at all: `telemetry-ids.ts`, and the `logger.ts` and
  `PIPELINE_TELEMETRY_ID_KEY` hunks.

Every difference is an addition on the `f544f6d` side: keyed telemetry ids,
`session_id`/`machine_id` on jobs and dead letters, attempts counted when a job
starts rather than when it is claimed, and tests for each (for example the
observability test is 2243 lines against 1985). The local
`origin/sp/pipeline-framework` ref was fetched at 15:59 PDT on 2026-10-28 and the
move commit was made at 16:39, so `f544f6d` is most likely a later push than
`f544f6d`, which is a stale local ref. **The moved files were kept as they are
(`f544f6d`).** Replacing them with `f544f6d` would drop those fixes. Someone with
network access should confirm `f544f6d` is the PR head with
`gh pr view 210 -R AgentWorkforce/relayhistory-cloud --json headRefOid`.

The supporting files added later (stand-in modules, vendored migrations) come
from `f544f6d`. Those blobs are the same at the #211 head, apart from `env.ts`,
which only gets the field names here anyway.

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

## Self-contained package (what was added)

`packages/pipeline` is now a TypeScript package (`@96bulls/pipeline`) that
type-checks and runs its tests on its own. Nothing in the moved files was
edited to get there.

| Added | What it is |
| --- | --- |
| `package.json` (root) | private, `workspaces: ["packages/*"]`, `typecheck` and `test` scripts (bun) |
| `packages/pipeline/package.json` | `@96bulls/pipeline`, pinned: hono 4.12.26, vitest 4.1.9, @electric-sql/pglite 0.5.3, @cloudflare/workers-types 4.20260621.1, typescript 5.9.3, @types/node 25.9.4 (what relayhistory-cloud's lockfile resolves at `f544f6d`) |
| `packages/pipeline/tsconfig.json` | upstream `packages/relayhistory/tsconfig.json` minus the cloud-client paths, plus `node` in `types` for the `node:` imports |
| `packages/pipeline/vitest.config.ts` | upstream config minus the cloud-client alias (30 s hook timeout for PGlite) |
| `src/env.ts` | Types only: `Bindings` with `ENVIRONMENT`, `POSTHOG_*` and the `env.ts.patch` additions copied verbatim (including the seven `Queue<StageMessage>` bindings and `PIPELINE_TELEMETRY_ID_KEY`), `Variables` (with `traceId`), `AppEnv`, `AppContext`, `AuthContext`. The rest of the relayhistory Worker's bindings are left out |
| `src/lib/logger.ts` | Upstream file at `f544f6d` with `logger.ts.patch` applied |
| `src/lib/scrub.ts` | Verbatim upstream file at `f544f6d` (last changed in `cdc55a6`, #179). Since the #211 port it carries no header (see below). Byte-identical with relayhistory-cloud PR #269 at `ad36081b` (`packages/relayhistory/src/lib/scrub.ts`, scrub rule set 3; open upstream as of 2026-11-05) until the #162 adversarial-review fixes (stacked PR #232, on `fix/scrub-quoted-keys-cloudflare`: entropy fallback off `/`, `.`, `-`; unquoted value after a quoted key only when generated; then, on #162, literal-prefix rules no longer start after a `-` inside a token, with a glued `word-sk-…` taken whole after the full-shape rules, and the SendGrid rule ahead of them, so a SendGrid key holding `-sk-…` no longer leaks up to 45 characters). This repo (PR #162, which includes the stacked fix PR #232) merges first; relayhistory-cloud PR #269 follows and must mirror the same diff (`scrub.ts`, `test/fixtures/scrub-secret-corpus.json`, `CORPUS_SHA256`) before #269 merges. Pins of the merged state: `scrub.ts` sha256 `e912c396277d383ef07a77b5b3dbd54d9ad83e59d3d5d1bafdab62b2b619daa1`, scrub rule set 3 digest `e7607073a3afca3db6b015945f0b5fab256202f355b740f515987ec162bf3076`, corpus sha256 `9a80b081d2ab1a2be7a9f36e73bcf6f27861b7096418a372ce12b567be52a860`; #269 at `ad36081b` still carries the pre-fix `scrub.ts` (`d8c84848…`) and corpus (`4b0ffeae…`). No longer byte-identical with `f544f6d` |
| `src/middleware/auth.ts` | `getAuth` and `requireScope` copied unchanged, and a re-export of `AuthContext`. No token verification: the host app authenticates and sets `auth` on the context |
| `src/app.ts` | Minimal `createApp({ auth? })`: the `app.ts.patch` id/trace-id middleware echoing `X-Relay-Trace-Id`, one request log line carrying `trace_id` (the `observability.ts.patch` hunk, without upstream's request stats), `/health`, `pipelineOpsRoutes` under `/v1` outside auth, and `pipelineRoutes` under `/v1` behind `auth`. The default `auth` answers 401 unless `auth` is already set |
| `supabase/migrations/20261025000{000,100,200,300}_*.sql`, `supabase/tests/supabase_shim.sql` | Verbatim from `f544f6d`. See `supabase/README.md` |

Every stand-in file has a header that names the upstream file it replaces and
says why it exists, except `src/lib/scrub.ts`, which must stay byte-identical
(see the #211 section).

## Commands

```sh
bun install              # from the repo root
bun run typecheck        # tsc --noEmit in packages/pipeline
bun run test             # vitest run in packages/pipeline (PGlite in process)
```

Or run the same from `packages/pipeline`: `npx tsc --noEmit` and `npx vitest run`.

Results on 2026-10-28 (2026-10-29 UTC):

- typecheck: exit 0, no errors.
- test: 3 files, 114 tests, all pass (vitest, PGlite in process).

## Skipped and failing tests

None. An earlier head of #210 had a date-dependent partition test (it added
four months with `setUTCMonth`, which rolls over on the 29th to 31st); the
current head f544f6d carries the upstream fix ("date-safe partition test"),
and the moved copy is that version.

## Differences from #210

None in the moved files. All 21 files added from #210 match the commit
`7d7893e` byte for byte, and the 12 `.patch` files under
`integration/relayhistory-cloud/` are kept as they were, as the record of the
upstream call sites. This includes `20261025000600_pipeline_queue.sql`, whose
header still names `packages/relayhistory/...`. Everything else is new
supporting code, listed above.

## Original import inventory (now resolved by the stand-ins above)

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
- `packages/pipeline/package.json` and `tsconfig.json` were not in the diff and
  have been added (see above).

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
not part of the #210 diff. It is now vendored verbatim, together with `000100` to
`000300` and `supabase_shim.sql` (see `supabase/README.md`).

On relayhistory-cloud main the migration also sorts after `20261025000100_repo_identity.sql`,
`20261025000200_scrub_quarantine.sql` and `20261025000300_upsert_keep_on_null.sql`.
The TS tests apply every file in `supabase/migrations/` in order, after
`supabase/tests/supabase_shim.sql`, which was also not in the diff and is now vendored. The SQL tests
`supabase/tests/pipeline_queue_test.sql` and the `behavior_test.sql`/`run.sh`
patches need the same base schema, and `dblink` for the concurrent-claim case.

The migration's header comment still names `packages/relayhistory/src/lib/pipeline/registry.ts`.
Here that file is `packages/pipeline/src/lib/pipeline/registry.ts`. The comment
was left untouched so the file stays byte-identical.

# PR #211: storage adapter, chunk hop, manifest reconcile, object store

Source: https://github.com/AgentWorkforce/relayhistory-cloud/pull/211
(branch `sp/rebuild-storage-adapter`, head
`d9f2ba2bedf8d8e73f3b406f93383d598cb5956f`, checked with
`gh pr view 211 -R AgentWorkforce/relayhistory-cloud --json headRefOid` on
2026-10-28; state OPEN). #211 is stacked on #210 (`f544f6d`); its diff against
`f544f6d` is 17 files. Every one of them is either copied byte-identical here
or kept as a patch.

Status: inactive, the same as #210. Nothing is wired into a worker, a queue
binding, an R2 event notification, a cron or a deployment. No migration was
applied to any database (#211 adds none). Tests use in-process PGlite and an
in-memory/local-directory object store; the optional MinIO block ran against a
throwaway local container that was removed afterwards.

## Mapping

| relayhistory-cloud (#211 head `d9f2ba2`) | 96BullSessions | How |
| --- | --- | --- |
| `packages/object-store/**` (32 files, incl. `s3.ts` and `test/unit.test.ts` changed by #211) | `packages/object-store/**` | whole package, byte-identical (on relayhistory-cloud `main` since #174/#205, unchanged by #210) |
| `packages/relayhistory/src/lib/pipeline/{adapter,object-layout,raw-chunks,reconcile}.ts` (added) | `packages/pipeline/src/lib/pipeline/` | byte-identical at the move. `raw-chunks.ts` has since diverged here: scrub rule set 2 (#108) segments strings past the scrub bound. Rule set 3 (#162) is also carried here (`SCRUB_RULESET_VERSION` 3), and rule set 4 (`SCRUB_RULESET_VERSION` 4): a value after a quoted key whose matching closing quote is not within the 4096-unit scan bound, counted across line breaks (a short first line does not keep it within), is redacted whole, to that closing quote or the end of input; no line break (CR, LF, CRLF, U+2028, U+2029, U+0085) ends it (the line-confined narrowing and the key-line-only decision were rejected by relayscrubber-lead and withdrawn). The segmenter refuses every cut strictly between such a key's start and its value's end: `longCut` wherever the key sits relative to the segment bound (it scans 4096 + 256 units past the bound; also for a generated unquoted value of 4096+ units), `safeCut` at line boundaries, and a run that ends inside such a value, or inside a secret value not yet decided, keeps its next record or key line (each search scans its window as a prefix and takes an open value as open to the end). A string can now be rejected (`chunk_string_too_long`, or `chunk_unsegmentable` for a run; non-retryable, no content) where rule set 3 cut it; the ingest replaces it with the redaction marker. Unquoted keys, >16 spaces before the colon, `\u` escapes, keys >64 characters and NBSP separators are left to a later rule set. Rule set 5 (`SCRUB_RULESET_VERSION` 5) classifies over-cap quoted values on the original text before any token rule can shrink them, and keeps key context on the record path (`scrubJson`/`scrubRecord` and the record-line tokenizer redact a non-empty string under a secret key at any depth); the `<scrub-segmenter>` block now also encloses `scrubRecordLine`. |
| `packages/relayhistory/src/lib/pipeline/{events,telemetry-ids,trace}.ts` (changed) | `packages/pipeline/src/lib/pipeline/` | overwritten, byte-identical |
| `packages/relayhistory/src/routes/pipeline.ts` (changed) | `packages/pipeline/src/routes/pipeline.ts` | overwritten, byte-identical |
| `packages/relayhistory/test/{pipeline-adapter,scrub-ruleset-version}.test.ts` (added) | `packages/pipeline/test/` | `pipeline-adapter.test.ts` byte-identical. `scrub-ruleset-version.test.ts` has diverged: it pins the v3 rule-set digest `e7607073a3afca3db6b015945f0b5fab256202f355b740f515987ec162bf3076`, which upstream's v3 (`scrub.ts` only) does not share; to be mirrored with #269 |
| `packages/relayhistory/test/pipeline-observability.test.ts` (changed) | `packages/pipeline/test/` | overwritten, byte-identical |
| `docs/pipeline-storage-adapter.md` (added), `docs/pipeline-observability.md` (changed) | `docs/` | byte-identical |
| `packages/relayhistory/src/api-worker.ts` (changed) | `integration/relayhistory-cloud/pr-211/packages/relayhistory/src/api-worker.ts.patch` | patch, not applied |
| `packages/relayhistory/src/env.ts` (changed) | `integration/relayhistory-cloud/pr-211/packages/relayhistory/src/env.ts.patch`, and the fields in the `src/env.ts` stand-in | patch + stand-in |

After the port, every file under `packages/pipeline/src/lib/pipeline/`,
`src/routes/pipeline.ts` and `test/` (including `test/support/`) matches its
blob at `d9f2ba2` (checked with `cmp`), and so did `src/lib/scrub.ts` (until rule set 3, #162; see its row above).

The moved files import the object store by relative path
(`../../../../object-store/src/index.js` from `lib/pipeline/`,
`../../object-store/src/...` from `test/`). With `packages/object-store` next
to `packages/pipeline` those paths resolve exactly as upstream, so no alias was
needed for the moved files. `packages/pipeline/package.json` also declares
`"@relayhistory/object-store": "workspace:*"`, so code in this repo can import
it by package name (`packages/pipeline/node_modules/@relayhistory/object-store`
is a link to `packages/object-store`). That package's `exports` point at
`dist/`, so a by-name import needs `bun run build` first; the build output is
gitignored.

## Stand-in changes

- `packages/pipeline/src/env.ts`: added the twelve #211 bindings verbatim
  (`PIPELINE_RAW_PROVIDER`, `PIPELINE_RAW_S3_{BUCKET,ENDPOINT,REGION,ACCESS_KEY_ID,SECRET_ACCESS_KEY,SSE}`,
  `PIPELINE_ORG_ID`, `PIPELINE_ORG_FROM_PREFIX`, `PIPELINE_KEY_PREFIX`,
  `PIPELINE_RECONCILE_ENABLED`, `PIPELINE_RECONCILE_LOOKBACK_MINUTES`) and
  mentioned them in the header.
- `packages/pipeline/src/lib/scrub.ts`: the six-line provenance header was
  removed, so the file is now byte-identical to upstream `lib/scrub.ts` at
  `f544f6d` and `d9f2ba2` (blob unchanged since `cdc55a6`, #179). At the port; #162
  has since moved it to relayhistory-cloud #269 `ad36081b` (rule set 3).
  `test/scrub-ruleset-version.test.ts` pins `SCRUB_RULESET_VERSION = 1` to the
  sha256 of that file (`db9d65c7ab230d67488900203bb78ca8bd3ad16949a0a51b5d3d117d0cff49f8`),
  so any header fails the test. Provenance is recorded here instead.
- `src/app.ts`, `src/lib/logger.ts`, `src/middleware/auth.ts`: unchanged. #211
  does not touch upstream `app.ts`, `logger.ts`, `auth.ts` or
  `observability.ts`.

## Workspace and package wiring

| Changed | What |
| --- | --- |
| root `package.json` | `build` = `bun run --filter @relayhistory/object-store build`; `typecheck` and `test` also run the object-store package |
| `packages/pipeline/package.json` | `dependencies: { "@relayhistory/object-store": "workspace:*" }` |
| `bun.lock` | the object-store workspace and its deps (zod 4.6.7, esbuild 0.28.2, prettier 3.9.10; vitest 4.1.9 and typescript 5.9.3 shared). `packages/object-store/package.json` keeps upstream's caret ranges verbatim; the lockfile pins them |

## Integration patches

The twelve #210 patches under `integration/relayhistory-cloud/` are unchanged:
each still reproduces exactly from `git diff 248fe67 f544f6d -- <file>`, and
for ten of the twelve files `git diff f544f6d d9f2ba2` is empty. The two files
#211 changes get stacked patches, generated with
`git diff f544f6d d9f2ba2 -- <file>`:

- `integration/relayhistory-cloud/pr-211/packages/relayhistory/src/api-worker.ts.patch`:
  dispatches `OBJECT_EVENTS_QUEUE_PREFIX` batches to `handleObjectEventQueue`
  (with `r2EventSource`, `manifestReaderFromEnv`, `orgResolverFromEnv`,
  `prefixAllowed`, `pipelineQueueFromEnv`), and runs `runManifestReconcile` from
  the scheduled handler.
- `integration/relayhistory-cloud/pr-211/packages/relayhistory/src/env.ts.patch`:
  the twelve bindings above.

To reproduce upstream's call sites at the #211 head, apply the #210 patch for a
file and then the `pr-211/` patch for it.

## Commands

```sh
bun install
bun run build                                       # object-store: tsc -p tsconfig.build.json -> packages/object-store/dist
bun run --filter @96bulls/pipeline typecheck        # tsc --noEmit
bun run --filter @relayhistory/object-store typecheck
bun run --filter @96bulls/pipeline test             # vitest, PGlite in process
bun run --filter @relayhistory/object-store test    # vitest

# optional MinIO block of test/pipeline-adapter.test.ts (and the object-store S3 conformance)
docker run -d --rm --name p211-minio -p 127.0.0.1:29110:9000 \
  -e MINIO_ROOT_USER=rh-conformance -e MINIO_ROOT_PASSWORD=rh-conformance-secret \
  cgr.dev/chainguard/minio:latest server /data
RH_OBJSTORE_S3_ENDPOINT=http://127.0.0.1:29110 RH_OBJSTORE_S3_ACCESS_KEY_ID=rh-conformance \
  RH_OBJSTORE_S3_SECRET_ACCESS_KEY=rh-conformance-secret npx vitest run   # in either package
docker rm -f p211-minio
```

`packages/object-store/scripts/emulators.sh` is included verbatim but was not
used: it names its containers `rh-objstore-*`, which are reserved on this
machine. The commands above start the same MinIO image with the same
credentials under another name and port.

## Results (2026-10-28)

- `@96bulls/pipeline` typecheck: exit 0.
- `@relayhistory/object-store` typecheck: exit 0. Build: exit 0.
- `@96bulls/pipeline` test: 5 files, 161 passed, 1 skipped (162). Per file:
  `pipeline-adapter` 44 passed + 1 skipped (the MinIO block, no
  `RH_OBJSTORE_S3_ENDPOINT`), `pipeline-observability` 80, `pipeline-queue` 26,
  `pipeline-supabase-schema` 10, `scrub-ruleset-version` 1.
- With the throwaway MinIO: `pipeline-adapter.test.ts` 45 passed (45).
- `@relayhistory/object-store` test: 6 files, 51 passed, 3 skipped (54). The
  three skips are the S3, GCS and Azure conformance runs with no emulator. With
  the throwaway MinIO: 52 passed, 2 skipped (GCS and Azure; no emulators
  started for them).

Root `bun run typecheck` and `bun run test` start with the stack's own
`tsc --noEmit` and `bun test` over `src/`. In this worktree those fail before
reaching the packages, because the `vendor/relayhistory-cloud` submodule is not
checked out (every error is `Cannot find module '@rhc/...'`). That is
independent of this port; `src/` was not changed.

## For the stack's import switch (not done here)

Everything `src/shared/vendor.ts` imports from `@rhc/lib/pipeline/*`,
`@rhc/routes/pipeline.ts`, `@rhc/lib/scrub.ts`, `@rhc/env.ts` and
`@rhc-object-store` now exists under `packages/pipeline/src` and
`packages/object-store/src` with the same names. Three imports have no
counterpart here, because they are relayhistory-cloud `main` modules outside
both PR diffs: `healthRoutes` (`routes/health.ts`), `requireAuth`
(`middleware/auth.ts`; the stand-in only has `getAuth`/`requireScope`) and
`observability` (`middleware/observability.ts`).

The submodule is pinned at `6f6d568`, not at the current #211 head `d9f2ba2`.
`6f6d568` is not an ancestor of `d9f2ba2` (the branch was rebased), and the
two differ in 25 files, including the review fixes to `raw-chunks.ts`,
`telemetry-ids.ts`, `routes/pipeline.ts` and `queue.ts`. Switching the stack to
these packages also moves it to `d9f2ba2`.

### Not moved: `packages/object-store/scripts/emulators.sh`

Dropped from the port on review (96BullSessions #6). The upstream script
generates a throwaway GCS signing key into `gcs-sa.pem`/`gcs-sa.json` on disk
and leaves them after `down`, which conflicts with this repository's rule that
no credential-like file lives on disk. The S3/GCS/Azure conformance tests still
run against emulators started by hand (the MinIO block was exercised that way);
a cleaner emulator helper can come later if needed.

## Stack switch to the in-repo packages (feat/stack-on-packages)

The services under `src/` now import the pipeline framework and object store
from `packages/pipeline` and `packages/object-store` (tsconfig paths `@rhc/*`
and `@rhc-object-store`), and the `vendor/relayhistory-cloud` submodule is
removed. `vendor/relayhistory` stays: it provides the `ai-hist` Rust CLI and
the transcript fixtures Concourse is seeded from.

New stand-ins the stack needs, beyond the #210/#211 diffs:

| File | Source | Notes |
| --- | --- | --- |
| `packages/pipeline/src/auth/relayauth.ts` | relayhistory-cloud `main` @ 6f6d568 | verbatim plus a one-line provenance header; RS256 JWT verification against `RELAYAUTH_JWKS_URL` |
| `packages/pipeline/src/auth/analytics-identity.ts` | same | verbatim plus provenance header (relayauth.ts depends on it) |
| `packages/pipeline/src/middleware/auth.ts` `requireAuth` | written here | the RelayAuth JWT path only; upstream's opaque `rth_at_`/`rth_st_` path needs Neon and is refused with `unsupported_token`. With `RELAYHISTORY_ORIGIN` set and the interim bearer off, it admits RelayAuth's origin-bound History assertion directly (`verifyOriginBoundAssertion` in `auth/relayauth.ts`, a 96BullSessions addition): upstream admits the same token at `/v1/auth/cli/login` and exchanges it for an `rth_` session, which this repo cannot store |
| `packages/pipeline/src/env.ts` | upstream `env.ts` | adds the `DEFAULT_RELAYAUTH_*` constants, the RelayAuth bindings, and `historyOrigin`/`sessionId`/`analyticsIdentity` on `AuthContext` |

Not ported, on purpose: upstream `middleware/observability.ts` (needs the
Worker's request stats and PostHog plumbing) and `routes/health.ts` (needs
session-analysis). The ingest has its own allowlisted request log, `/health`,
`/healthz` and `/readyz`.

HoopDb: the runner now reads `supabase/migrations` and records them as
`upstream:<file>`. `20261025000600_pipeline_queue.sql` changed upstream after the
old vendored pin, so a database built from that pin stops with
`legacy_schema` and an instruction to recreate it (`96bulls down <stage> --purge`).
