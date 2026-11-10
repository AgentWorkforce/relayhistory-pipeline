# Cutover ceremony runner (lazy-current custody)

`cutover-ceremony.ts` runs steps 2–6 of relayhistory-cloud `docs/buffered-delivery-cutover.md` for the
lazy-current target (sessions-pipeline 055000). It imports the reviewed exporter, the custody-authority
SQL and the generation from the pinned worktrees on sf-frame and never copies them.

Run from the sessions-pipeline worktree (its `node_modules` serve both imports):

```sh
cd /home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104
B=/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun
# read-only preflight, any time:
<stdin> | $B ~/relay-briefs/bulls-resume-20261104/cutover-ceremony.ts --dry-run [--runtime-host <host>]
# the window:
<stdin> | $B ~/relay-briefs/bulls-resume-20261104/cutover-ceremony.ts --execute --retention-sha <ARMED sha> --runtime-host <host>
# after the switch, for orgs created later (idempotent):
<stdin> | $B ~/relay-briefs/bulls-resume-20261104/cutover-ceremony.ts --admit-new-orgs
```

The runner and its `mini/` wrappers take the production Neon identifiers from the environment and refuse to run
without them: `NEON_ENDPOINT` (endpoint id, `<neon-endpoint>`), `NEON_PROJECT_ID`, `NEON_BRANCH_ID`, and for the
wrappers that build the runtime URL `NEON_RUNTIME_HOST` (the pooled hostname). None of these values are committed.

stdin is ONE JSON line, RAM only:
`{"neonOwnerUrl":"postgresql://neondb_owner:…@<neon-endpoint>…/neondb","neonRuntimeUrl":"postgresql://relayhistory_app_runtime:…@<host>/neondb"}`

## Inputs and credentials

| input | source | 1Password? |
|---|---|---|
| Neon owner URL (`neondb_owner`, production endpoint `<neon-endpoint>`) | `neonctl connection-string <neon-branch-id> --project-id <neon-project-id> --role-name neondb_owner --database-name neondb` | no |
| Neon runtime URL (`relayhistory_app_runtime`) | password in 1P item `oxvgx7xbu2d4j62th6iiudt4oe` field `app_password` | **yes** |
| `--runtime-host` | EXACTLY the hostname that will be in `NEON_APP_DATABASE_URL` (the custody identity hashes it) | no |
| `--retention-sha` | the GC lane's retention contract SHA once it is `ARMED_HEALTHY` (never the DORMANT one) | via the GC arm (needs 1P) |
| Supabase owner | existing Supabase CLI temporary login on sf-frame (`cli_login_postgres` + `SET ROLE postgres`) | no |

No seeder role or credential is used. Lazy mode refuses to start once any `ingest_fence_owners` row
exists, and the per-user `ingest_seed_fence_baseline` import creates exactly those. So that import
(doc step 4) must NOT run before the lazy policy.

## What it does

- **S0 preflight.** It checks:
  - the pinned RHC sources are byte-identical to main `e6bf926`, and the generation is `d2569f34`;
  - the Neon owner identity, phase `legacy`, and the source identity on the restricted runtime connection;
  - Supabase readiness is PASS, and the target holds no authority;
  - the org registry.
- **S1:** `owner_delivery_cutover_v1('seeding', E, NULL)`, then a readback. The Neon DB guard holds legacy writes from here on.
- **S2:** the reviewed `exportPrivacyBaseline(..., {page:500, captureAcceptanceFloor:true})`. It also enforces the lazy invariant of at most 512 KiB and 5000 entries per user packet.
- **S3:** the capture artifact. Its SHA is `sourceProofSha256`.
- **S4:** one Supabase transaction: admission rows for (export orgs ∪ `public.organizations`), then `ingest_lazy_policy_set`, then `ingest_receipt_seat_initialize`, then a guard check.
- **S5:** `owner_delivery_cutover_v1('active', E, D)`, then a readback.
- **S6:** writes `ceremony/rhc-ingest-config.json` with `DELIVERY_CUTOVER`, `DELIVERY_CUSTODY_SOURCE` and `DELIVERY_FORWARD`.

Every step records its evidence (epoch, descriptor, packet SHA, floor, capture SHA, policy and seat results,
admitted-org SHA) in `ceremony/state.json` (0600). On any refusal it prints `STOPPED {step, why}`.
After fixing the cause, rerun the SAME command; completed steps are skipped.

## Caller-owned Cloudflare steps (not in this runner)

1. Before S1, pause `relayhistory-neon-replica` delivery. Seeding refuses replica writes, and unpaused retries would dead-letter. Optionally also activate a pre-uploaded HOLD Worker version so clients get a clean 503 and not the DB-held 5xx.
2. After S6, deploy the ingest Worker version with the printed `DELIVERY_*`, `DELIVERY_INGEST_SIGNING_KEY`, the `DELIVERY_INGEST` binding and `NEON_APP_DATABASE_URL` set to the runtime role at `--runtime-host`.

## Rollback (doc "Fail-closed rollback"; manual, never automatic)

1. Set `DELIVERY_CUTOVER` to hold.
2. As the Supabase owner, `UPDATE sessions.ingest_buffer_admission SET valid_until = clock_timestamp() - interval '1 second'`. Never DELETE.
3. Run `SELECT sessions.owner_delivery_cutover_v1('halted', E, NULL)`.
4. Keep every acknowledged S3, outbox and receipt object. There is no automatic return to `legacy`.

## Rehearsal

`--rehearsal <branch-endpoint> <rehearsal_target_N>` points the Neon side at a Neon BRANCH and the target at
the local docker PG17 fixture. It refuses the production endpoint.
