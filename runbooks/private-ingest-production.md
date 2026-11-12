# Private ingest production release

Production `96bulls-session-ingest` deploys run only through
`.github/workflows/deploy-private-ingest-worker.yml`. Do not run Wrangler, SST, Alchemy, or the
retained-plan operator locally against production.

## Authority and desired state

The complete nonsecret feature profile is
`config/cloudflare/private-ingest-production.json`; the protected environment supplies the KMS
target. Workflow inputs cannot override either source. The release contract recognizes only these
rollout profiles, a custody shadow preparation profile, and the baseline rollback target:

0. `baseline-drain` (rollback only): no frozen custody source or synchronous receipt bridge;
   async admission remains `drain`.
   `step-0-shadow`: `custodySource=shadow`, async admission remains `drain`, and the
   synchronous frozen-receipt bridge is absent. This makes the P1 runbook's shadow evidence
   possible through the same CI gates. Capture it only after the custody copy/seal is qualified.

1. `step-1-custody`: `custodySource=frozen-supabase`, async admission remains `drain`, the
   synchronous frozen-receipt bridge is absent.
2. `step-2-frozen-receipt`: the same profile plus
   `syncAdmissionFrozenReceipt=on`; async admission remains `drain`.
3. `step-4-async-on`: the same profile with `asyncAdmission=on`.

Each transition is a reviewed commit to the tracked profile followed by its own plan and deploy.
Do not combine steps. Step 4 is followed by one relayhistory-cloud per-org canary; widening is a
separate decision owned by relayhistory-cloud.

Adding shadow support does not change the tracked production profile or dispatch a deployment.
For the P1 flow, change that profile to `custodySource=shadow` in its own reviewed commit, plan
and deploy, then retain genuine `custody.shadow` match evidence on batches and controls before
the separate frozen profile commit. A mode readback alone is not a parity receipt. The live
transition gate requires `baseline-drain <-> step-0-shadow <-> step-1-custody`; direct
baseline-to-custody promotion and direct custody-to-baseline rollback both refuse. Already-live
custody deployments remain recognizable and may be redeployed without a phase change. Neither
an adjacent transition nor a mode readback waives P1 copy/seal/mirror/shadow evidence. Shadow
cannot enable the receipt bridge or async admission, and cannot skip directly to/from the
advanced phases.

The first `step-0-shadow -> step-1-custody` promotion also requires
`REVIEWED_CUSTODY_SHADOW_MATCH` in `src/session-ingest/private-ingest-custody-evidence.ts`.
It is deliberately `null` until genuine evidence is retained and its exact metadata/digest is
reviewed into source. A mode-only deployment cannot populate it. The closed receipt schema keeps
only generation, exact shadow deployment/version, UTC observation window, aggregate batch/control
match counts, zero mismatch/error counts, and copy/seal plus mirror receipt digests. It permits
no tenant identifiers, bodies, headers or raw log errors. The receipt hash is SHA-256 over compact
JSON in the schema's declared field order, without a newline.

Require at least ten minutes of observations after that deployment began, at least one matching
batch and control, no errors or mismatches, and an end time within the preceding 24 hours. The
generation and both live shadow IDs must still match at plan and deploy. Missing, stale, future,
wrong-generation or non-shadow predecessor evidence refuses. Later idempotent custody redeploys
and adjacent advanced-phase changes retain their phase guards; they cannot stand in for the first
promotion's evidence.

Khaliq's recorded production authorization is bound into the deploy capture by
`src/session-ingest/private-ingest-release.ts`; every run must remain inside its authorized
scope. The workflow requires his exact authenticated dispatch and triggering identity plus
the protected environment gates below. It does not configure a separate reviewer-approval rule.

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
plan/deploy/readback steps after dependency installation and environment gates. Do not copy a
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
detailed plans and `cf-sessions-prod.custody-evidence.json`. That evidence capsule must match the
exact reviewed source bytes; deploy checks it again after downloading the plan. Only creates for
an authorized addition, or in-place updates/noops, are allowed;
replacement, deletion, adoption, foreign resources, credential literals, and unexpected binding
changes refuse.

## Deploy

Dispatch the same workflow from the same still-current SHA with:

- `mode=deploy`
- `sha=<same exact main SHA>`
- `plan_run=<successful plan run id>`
- `confirmation=DEPLOY 96bulls-session-ingest <sha> PLAN <plan_run>`

For the first frozen promotion append ` SHADOW <reviewed receipt sha256>` to that confirmation.
The digest must be the source-pinned receipt, not an operator-supplied replacement. Other phase
changes use the base confirmation without the suffix.

After those gates, the run downloads the exact artifact, binds the recorded production
authorization to its hashes, repeats the live custody preflight immediately before the operator,
regenerates the live plan, refuses any drift, and applies once. It
then requires the current deployment to have exactly one Worker version at 100% and checks every expected `INGEST_*`
value plus Hyperdrive, both Queue bindings, and the three Secrets Store bindings. The sanitized
readback is retained as `private-ingest-readback` for 30 days.

## Rollback

- Step 1: first return relayhistory to its Neon witness mode through its CI path and verify that
  readback, respecting the independent one-way privacy/auth boundaries. Hold the RHC writer lane
  until the Worker rollback finishes. Then change the tracked profile to `step-0-shadow` by a normal commit, then plan and deploy
  through the same workflow. If a return to `baseline-drain` is required, use a second profile
  commit and its own plan/deploy after the shadow readback passes. A direct step-1-to-baseline
  revert refuses. Step 2 rolls back to `step-1-custody`. Never rewrite `main`.
- The Worker rollback preflight reads the sole current RHC version at 100% and requires a complete
  binding response with `DELIVERY_CUSTODY_SOURCE_MODE` absent, empty, or exactly `relayhistory`.
  Its existing `NEON_APP_DATABASE_URL` secret binding must still be present; its value is never read.
  Frozen/Supabase-authority, unknown/opaque modes, missing metadata and split deployments refuse;
  no credential or permission fallback is allowed. Its read-only RHC check is repeated immediately
  before apply. Once RHC has crossed the one-way Supabase privacy boundary, this rollback is unavailable.
- Shadow may return to baseline through the same CI path; step 1 may return to shadow for
  diagnosis. Refuse a custody-binding deletion from an unknown mode or with a receipt bridge
  still enabled. RHC privacy/auth authority boundaries still apply independently.
- Step 4: first turn the relayhistory per-org opt-in off. Change the tracked ingest profile from
  `on` back to `drain`, then plan and deploy through CI. Do not go directly `on -> off` while
  messages may be in flight. Wait for the admission queue and DLQ to empty before a later `off`
  change.

No local deploy is an emergency rollback path.
