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

## Generation overlap (G -> G+H -> H)

Dormant by default: `PRIVATE_INGEST_OVERLAP` in `src/session-ingest/private-ingest-release.ts` is
`null`, so a tracked `overlapGeneration` and a live `INGEST_ACCEPT_GENERATIONS` both refuse. The
protocol is `docs/design/zero-503-schema-windows.md` section 3.2. Every step is a reviewed commit
plus CI; nobody issues SQL by hand.

1. **Review the pair.** By a normal commit, set `PRIVATE_INGEST_OVERLAP = { from: G, to: H }` while
   `expectedGeneration` is still G. Add `"overlapGeneration": "<H>"` to the tracked profile's
   `sessionIngest`. The rollout phase stays the same; the planner names it `<phase>-overlap`.
2. **Worker to G+H.** Plan, then deploy as above. The confirmation is the usual
   `DEPLOY 96bulls-session-ingest <sha> PLAN <plan_run>` followed by ` OVERLAP <H>`. The readback
   requires `INGEST_ACCEPT_GENERATIONS=G,H`, and the transition gate allows only G -> G+H.
3. **Open the DB window.** Run this immediately before the schema apply. Dispatch
   **ingest generation overlap window** with:
   - `mode=open`
   - `sha=<exact main SHA>`
   - `ttl_seconds` from 60 to 14400; cover the apply, the runtime replacements and the rebind
   - `confirmation=OPEN-OVERLAP <G> <H> TTL <ttl_seconds> SHA <sha>`

   It uses the `session-schema-production` environment and its owner secret. It refuses unless all of
   these hold:
   - readiness is PASS at G
   - the custody policy is at G
   - the reviewed contract is still at G
   - no foreign unexpired window is open

   `admitDuringApply` is always off. `mode=readback` (empty TTL and confirmation) is read-only and
   reports `none`, `open` or `expired`.
4. **Apply, then rebind.** Use the normal schema release and the rebind. The rebind closes the
   window in its own transaction. `mode=readback` should then report `none`.
5. **Worker to H.** By a normal commit, set `expectedGeneration` to H in both the source constant and
   the tracked profile. Remove `overlapGeneration` from the profile and keep the pair. Plan and
   deploy with the plain confirmation. The gate allows only G+H -> H.
6. **Dormant again.** Once the live Worker reads back exactly H, set `PRIVATE_INGEST_OVERLAP`
   back to `null` by a normal commit.

While the live Worker is at G+H the rollout phase is frozen: a profile that changes the
phase and the generation set together, or the phase alone, refuses with
`private_ingest_live_transition`. Finish or roll back the overlap first. H -> G+H, G -> H and
H -> G are always refused. A live accept set the tracked profile does not declare refuses with
`private_ingest_live_undeclared_overlap`. A live state that matches no permitted phase refuses with
`private_ingest_live_phase`.

## Rollback

- Step 1: revert the tracked profile to `baseline-drain` by a normal commit, then plan and deploy
  through the same workflow. Step 2 rolls back to `step-1-custody`. Never rewrite `main`.
- Step 4: first turn the relayhistory per-org opt-in off. Change the tracked ingest profile from
  `on` back to `drain`, then plan and deploy through CI. Do not go directly `on -> off` while
  messages may be in flight. Wait for the admission queue and DLQ to empty before a later `off`
  change.
- Generation overlap, before the rebind (G+H -> G): dispatch **ingest generation overlap window** with
  `mode=close` and `confirmation=CLOSE-OVERLAP <G> <H> SHA <sha>` (it is idempotent if nothing is open).
  Then remove `overlapGeneration` from the tracked profile by a normal commit and keep the pair. Plan
  and deploy; the gate allows G+H -> G. Then set `PRIVATE_INGEST_OVERLAP` back to `null`. Close the
  window first only if the marker is still at G. If the apply already moved the marker to H, do not
  roll the generation back: finish the rebind and go forward to H. After the rebind, G is retired, and
  the database refuses both a G Worker and a reopened window. An expired window admits nothing and
  fails closed, which brings back today's outage, never a widening.
- The deploy refuses `private_ingest_live_phase` or `private_ingest_live_transition` whenever the live
  Worker is not in a state from which the tracked profile is a permitted move. Fix the profile; never
  bypass the readback.

No local deploy is an emergency rollback path.
