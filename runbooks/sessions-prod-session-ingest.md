# Deploying the session ingest Worker from CI

The ingest Worker `96bulls-session-ingest` (`src/session-ingest/worker.ts`, stack `stacks/cloudflare/session-ingest.ts`) is deployed by the manual workflow `.github/workflows/deploy-session-ingest.yml`. The workflow is the only CI path that changes it. It deploys main's code with a configuration copied from the live Worker; only the knobs below may differ from live, and changing any of them is Khaliq's own go.

Nothing in this file names an account, a version, a queue, a Hyperdrive config or a credential. The workflow reads them from Cloudflare at run time.

## Where it runs

- `workflow_dispatch` only, and only from `refs/heads/main`. A dispatch from any other ref fails in the `refuse-non-main` job; the deploy job does not start.
- GitHub environment `session-ingest`, approval-gated with Khaliq as the required reviewer. Every run waits for that approval, dry runs included, because the plan reads live with the environment's secrets.
- Environment secrets, by name: `CLOUDFLARE_API_TOKEN` (Workers Scripts edit for this account) and `CLOUDFLARE_ACCOUNT_ID`. The workflow checks they exist and never prints them. Neither lives in the repository or on any disk; 1Password is the record.
- wrangler is the lockfile pin (`4.156.1`, `@96bulls/session-read-worker/wrangler` in `bun.lock`); the job refuses another version.
- One run at a time (`concurrency: session-ingest-production`, never cancelled mid-run).

## What it owns and what it preserves

Owned (may differ from live, under the precedence below):

| variable | values (first = what absence means) | committed profile | input |
|---|---|---|---|
| `INGEST_ACCEPTANCE_MODE` | `canonical`, `buffered` | `buffered` | none: profile only |
| `INGEST_ASYNC_ADMISSION` | `off`, `drain`, `on` | `drain` | `ingest_async_admission` |
| `INGEST_CLAIM_LOCK_BUSY` | `off`, `on` | `on` | `ingest_claim_lock_busy` |
| `INGEST_CLAIM_TIMEOUT_BUSY` | `off`, `on` | `off` | `ingest_claim_timeout_busy` |
| `INGEST_SYNC_ADMISSION_FROZEN_RECEIPT` | `off`, `on` | not in the profile | `ingest_sync_admission_frozen_receipt` (empty = preserve) |

The profile is `deploy/session-ingest/production-profile.json`. Its parser is strict: a JSON object with exactly the four profile keys, canonical string values, no duplicates, no BOM. Changing it is a reviewed commit on main.

Preserved from the live active version, exactly: every other plain-text variable (in live order), every Worker secret (wrangler keeps secrets on deploy), the Hyperdrive, queue-producer, Secrets Store and service bindings, the compatibility date and flags (which must also equal the stack's constants), CPU limits, smart placement, observability, logpush and tail consumers. `workers_dev` and preview URLs stay off. The render never declares routes, queue consumers or anything else the live version does not carry.

Not touched at all: queue consumers and their settings, DLQs, delivery pause, retention, and anything else that is an Alchemy resource rather than part of a Worker version.

There is no per-org admission input. Per-org opt-in is relayhistory's signed `admission: "queued-v1"` request field (`src/session-ingest/app.ts`), not Worker configuration.

## Precedence

explicit input > committed profile > live.

| input | live variable | result |
|---|---|---|
| explicit value (`off`/`drain`/`on`, or `off`/`on`) | anything | the input. The diff shows it with source `explicit`. |
| `profile` | absent | the profile value fills it. If that changes the effective value, the run fails closed: dispatch the input explicitly. If it is what absence already means (for example `INGEST_CLAIM_TIMEOUT_BUSY=off`), nothing is rendered and the diff stays empty. |
| `profile` | set, same effective value as the profile | live's literal is kept. |
| `profile` | set, differs from the profile | refused: `live X is A but the production profile says B; refusing in profile mode`. Dispatch explicitly, or fix the profile in a reviewed commit. |
| empty (frozen receipt only) | anything | live is kept. |

An explicit value that equals live keeps live's literal. A target of the "absent" value renders the variable as absent, as the stack does.

Cross-knob rules are checked on the render and refuse the run:
- buffered acceptance needs `INGEST_EXPECT_GENERATION` (64 hex) on the live Worker;
- async `drain`/`on` needs buffered acceptance and the live `INGEST_ADMISSION` queue binding (the workflow never adds bindings);
- async goes `off` -> `drain` -> `on`, never `off` -> `on` in one run;
- frozen receipt `on` needs buffered acceptance and `INGEST_CUSTODY_SOURCE=frozen-supabase` on the live Worker.

Setting async admission to `off` explicitly keeps the `INGEST_ADMISSION` binding; nothing is ever dropped.

## The guard

After rendering, the guard compares the render with the live version and refuses (no deploy) when:
- a live binding would be dropped, retyped, or changed, other than a knob variable changed by its own decision;
- a binding would be added, other than a knob variable the decision fills;
- a binding is declared twice or would shadow a live secret;
- the render declares a key outside the allowlist (routes, queue consumers, triggers) or turns on workers.dev or preview URLs;
- the compatibility settings, placement, observability, logpush or tail consumers differ from live;
- the live Worker has a gradual deployment (not exactly one version at 100%), an unknown binding type, or a field the parser does not understand.

The live reads are GET only, with redirects refused, a 10 s timeout and a size cap.

## Dry run (the default)

`dry_run` defaults to `true`. A dry run stops after resolve, render and guard and prints:
- the live version id (the rollback handle; not masked);
- each knob's live value, rendered value and source;
- a sanitized diff vs live: knob names and their `on`/`off`/`drain`/`buffered`/`canonical` values only. Other variable values, Hyperdrive ids and store ids are masked in the log.

With default inputs the job prints `Sanitized diff vs live: EMPTY. The render equals the live configuration.` A non-empty diff in profile mode fails the run; a diff exists only for inputs dispatched explicitly.

## First run: a no-op

1. Khaliq approves the environment run. Dispatch from main with every input at its default (`dry_run` true, knobs `profile`, frozen receipt empty).
2. Expect `EMPTY`. If the plan refuses because live differs from the profile, stop: either live moved (find out why) or the profile is stale (fix it in a reviewed commit). Do not dispatch an explicit value to make the error go away.
3. Record the live version id from the log.
4. Only with Khaliq's go: dispatch again with `dry_run` false and the same defaults. The job re-reads live right before deploying and refuses if the active version or the render changed since the plan; then wrangler deploys; then it verifies the new active version is new and carries exactly the rendered bindings (secrets as they were).
5. The resulting deploy changes code to main's bundle and nothing else. Watch ingest health as for any ingest release.

## Changing a knob

Every flip is Khaliq's own go, in his words, for that value. Dispatch with the explicit input and `dry_run` true first; check the diff lists exactly that knob; then the same inputs with `dry_run` false. To make the change the new norm, update the profile in a reviewed commit afterwards so profile-mode runs stay empty.

## Rollback

- Code: redeploy the previous good commit through the same workflow with the same inputs (revert on main with a normal revert commit, then dispatch). Never rewrite main.
- A knob: dispatch the same workflow with the previous value given explicitly. That is a flag flip, so it is Khaliq's own go.
- Emergency only, with Khaliq's OK: `wrangler rollback <previous version id>` using the id the run recorded. A later workflow run plans against whatever is then live.

## Known gaps

- The workflow deploys main's code as bundled by wrangler, not by Alchemy. The bundles may differ in layout; behaviour comes from the same source.
- After a wrangler deploy, Alchemy's state no longer matches the live Worker. Run an Alchemy deploy of this stack only after reconciling it.
- The Cloudflare API response shapes the parser expects were written from the documented API and fixtures, not checked against the live Worker. The first dry run is that check; a parse refusal there is safe and means the parser needs a reviewed fix.
