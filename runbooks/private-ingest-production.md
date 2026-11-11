# Private ingest production release

Production `96bulls-session-ingest` deploys run only through
`.github/workflows/deploy-private-ingest-worker.yml`. Do not run Wrangler, SST, Alchemy, or the
retained-plan operator locally against production.

## Authority and desired state

The complete nonsecret feature profile is
`config/cloudflare/private-ingest-production.json`; the protected environment supplies the KMS
target. Workflow inputs cannot override either source. The release contract recognizes only these
ordered rollout profiles, plus the baseline rollback target:

0. `baseline-drain` (rollback only): no frozen custody source or synchronous receipt bridge;
   async admission remains `drain`.

1. `step-1-custody`: `custodySource=frozen-supabase`, async admission remains `drain`, the
   synchronous frozen-receipt bridge is absent.
2. `step-2-frozen-receipt`: the same profile plus
   `syncAdmissionFrozenReceipt=on`; async admission remains `drain`.
3. `step-4-async-on`: the same profile with `asyncAdmission=on`.

Each transition is a reviewed commit to the tracked profile followed by its own plan and deploy.
Do not combine steps. Step 4 is followed by one relayhistory-cloud per-org canary; widening is a
separate decision owned by relayhistory-cloud.

Khaliq's production authorization is bound into the deploy capture by
`src/session-ingest/private-ingest-release.ts`. The GitHub environment still requires a real
human approval for every plan or deploy run.

## One-time GitHub environment

Create `private-ingest-production` with:

- exactly one deployment branch policy: branch `main`;
- no unsupported reviewer rule; instead the workflow requires both the dispatch actor and
  triggering actor to be exactly `khaliqgant` (GitHub user ID `1724137`);
- environment variable `PRIVATE_INGEST_STATE_URL`, the HTTPS
  `alchemy-state-store-96bulls.<account-subdomain>.workers.dev` origin;
- environment variable `PRIVATE_INGEST_KMS_KEY_ARN`, the production transcripts key ARN;
- environment secret `PRIVATE_INGEST_STATE_TOKEN`;
- environment secret `CLOUDFLARE_API_TOKEN`, scoped to the production account and only the
  Worker/Queue/Hyperdrive/Secrets Store reads and writes used by this stack.

Secrets stay in the GitHub environment and the system of record. They are exposed only to the
plan/deploy/readback steps after dependency installation and environment approval. Do not copy a
Wrangler OAuth credential into GitHub Actions.

The prerequisite job refuses a missing, unprotected, or non-main environment before entering it.
The KMS target stays in the protected environment rather than engine source.

The source contract retains Khaliq's direct rollout quote and date. Every run must be dispatched and
triggered by his exact GitHub user; the capture records that run's creation date, so the retained-plan
freshness check applies to the actual authorized run rather than expiring the workflow itself. A
missing or stale authorization is a safety stop, not a reason to bypass the workflow.

## Plan

Wait for the exact `main` SHA's `ci.yml` run to finish green. Dispatch **deploy private-ingest
worker** with:

- `mode=plan`
- `sha=<exact current main SHA>`
- empty `plan_run` and `confirmation`

The run reads live state without provider writes, enforces the native resource and binding
inventory, and uploads one `private-ingest-plan` artifact for 24 hours. Review both compact and
detailed plans. Only creates for an authorized addition, or in-place updates/noops, are allowed;
replacement, deletion, adoption, foreign resources, credential literals, and unexpected binding
changes refuse.

## Deploy

Dispatch the same workflow from the same still-current SHA with:

- `mode=deploy`
- `sha=<same exact main SHA>`
- `plan_run=<successful plan run id>`
- `confirmation=DEPLOY 96bulls-session-ingest <sha> PLAN <plan_run>`

After environment approval, the run downloads the exact artifact, binds the recorded production
authorization to its hashes, regenerates the live plan, refuses any drift, and applies once. It
then requires the current deployment to have exactly one Worker version at 100% and checks every expected `INGEST_*`
value plus Hyperdrive, both Queue bindings, and the three Secrets Store bindings. The sanitized
readback is retained as `private-ingest-readback` for 30 days.

## Rollback

- Step 1: revert the tracked profile to `baseline-drain` by a normal commit, then plan and deploy
  through the same workflow. Step 2 rolls back to `step-1-custody`. Never rewrite `main`.
- Step 4: first turn the relayhistory per-org opt-in off. Change the tracked ingest profile from
  `on` back to `drain`, then plan and deploy through CI. Do not go directly `on -> off` while
  messages may be in flight. Wait for the admission queue and DLQ to empty before a later `off`
  change.

No local deploy is an emergency rollback path.
