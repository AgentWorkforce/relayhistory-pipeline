# Buffered upload cutover, 2026-11-05 (as run)

This is the record of the production switch of `POST /v1/delivery/batches` off Neon, onto the private
96Bulls session-ingest Worker in buffered mode. The scripts that ran are in `scripts/ops/cutover-20261105/`.
Everything below is what actually happened. The reviewed design is relayhistory-cloud
`docs/buffered-delivery-cutover.md` and `docs/delivery-source-runtime-role.md`.

Operator: bulls-migrator (Claude, sf-mac-mini), on Khaliq Gant's standing approval in that session:
"everything is pre approved" and "relax that relayhistory-cloud rule and u just do it all YOU HAVE PERMISSION",
then "ok GO" for the hold.

## Live state after the switch

| Piece | Value |
|---|---|
| relayhistory-api | relayhistory-cloud main `e6bf926` (#299), runtime binding `relayhistory_app_runtime` (pooled host `ep-floral-pond-apkvat38-pooler…`), `DELIVERY_INGEST` service binding to `96bulls-session-ingest` |
| `DELIVERY_CUTOVER` | `{"version":1,"mode":"ingest","epoch":"17e73957…1a86","privacyBaselineSha256":"c2d68710…083f"}` |
| `DELIVERY_FORWARD` | `{"mode":"on","percent":100,"onUnreachable":"503"}` |
| `DELIVERY_CUSTODY_SOURCE` | hostname as above, `sourceIdentitySha256` `7891c819…acc4` |
| Ingest Worker | `96bulls-session-ingest` from this repo `5b1c449`: buffered, expected generation `d2569f34`, inline 0, replica off, Hyperdrive `0fcc0265…` (session pooler 5432) |
| Neon (`divine-snow-84812652` / `br-damp-mud-apbr6ifp`) | 0078 installed; cutover phase `active` |
| Supabase (`pyamkrhoyszgzirqqffq`) | readiness PASS for `d2569f34`; lazy-current policy; 965 orgs admitted, `valid_until` 2026-12-05T05:49:28Z |
| Buffered GC | stack `SessionsBufferedGc` armed and healthy; retention contract `7b2b3b82…31fe` |

## Timeline (UTC)

| Time | Step |
|---|---|
| 00:49–00:50 | 462 owner apply: 13 migrations committed; verify lost a deadlock to autovacuum (40P01) |
| 01:18 | Resume apply at 8fa809b (ledger pin `de0e2fa0`): 0 applied, 33 skipped, readiness PASS |
| 01:21–01:22 | `bulls96_session_ingest` set to LOGIN NOINHERIT, both verifiers pass (`ingest-login.ts`) |
| 01:24 | Hyperdrive `96bulls-session-ingest` created (caching off, verify-full, Supabase CA) |
| 01:33 | Private ingest Worker deployed (`private-ingest-operator.ts deploy` at 5b1c449; 4 creates) |
| 01:35 | Neon `relayhistory_app_runtime` created, plus the reviewed `runtime_grants` block only (`neon-role.ts`) |
| 01:56:41 | 0078 applied through a 42 s writer drain (`drain-apply-0078.mjs`): replica paused; EXECUTE revoked from PUBLIC on accept/selection/reset; wait for 0 writers; canonical `apply-neon-migration.mjs` with `DATABASE_URL` role = the new runtime role; restore. An earlier attempt with `app_blue` refused on the runtime-grants audit and rolled back. |
| 04:45–04:46 | Credentials set from 1Password (via kjg-lap): GC secret, `NEON_APP_DATABASE_URL`, `DELIVERY_INGEST_SIGNING_KEY`, custody capture (`after-1password-unlock.py`) |
| 05:12 | Dormant redeploy SUCCESS (run 41970774658). An earlier attempt was refused because `DELIVERY_CUSTODY_SOURCE` was set without `DELIVERY_CUTOVER`. |
| 05:33:45 | HOLD live (run 41973949317; uploads 503 with Retry-After 5); replica delivery paused |
| 05:37 | Drain proof W1–W5 PASS (`legacy-drain-proof.mjs`, report `2ca83c67…`) |
| 05:41:17 | Ceremony S1: Neon `seeding`, epoch `17e73957…` (`cutover-ceremony.ts --execute`) |
| 05:49:28 | S2 export complete (8 min 11 s): 221 scopes, 8 deletions, 0 resets, baseline `c2d68710…` |
| 05:50:55 | S4 Supabase authority (965 admitted orgs, lazy policy, seat clock); S5 Neon `active` |
| 05:51:24 | Flip: `cf-vars.py` settings PATCH of the three `DELIVERY_*` vars (all other bindings inherited, 56 → 58), then the same values set as GitHub repo variables, so the next CI deploy reproduces it |

Upload downtime was about 17.5 minutes (05:33:45–05:51:24). Afterwards about 93% of batches return 200 in 0–4 s (before: 13–85 s on Neon).
The remaining ~7% are 503s, mostly a 5 s bounded custody-witness statement on Neon; that fix is tracked separately.

## Operational jobs

- **New-org admission.** Orgs created after the switch are refused by the lazy guard until admitted. `mini/admit-new-orgs.sh`
  runs `cutover-ceremony.ts --admit-new-orgs` every 120 s from the launchd agent `mini/launchd/dev.agentworkforce.bulls-admit-new-orgs.plist`
  on sf-mac-mini. It copies the live policy's epoch, baseline, retention and generation, and never changes existing rows. Move it into the cloud.
- **Admission expiry.** Every admission row expires 2026-12-05T05:49:28Z. Renew it before then (owner UPDATE of `valid_until`, not a re-insert), or every org closes.
- **Replica queue.** `relayhistory-neon-replica` is PAUSED with about 12.9k pre-switch s3_first ACKs whose Neon copies never landed.
  Messages expire after 4 days (oldest around 2026-11-08 00:00Z). They must be recovered into the new read path; do not resume them into Neon
  (`active` refuses those writes). The DLQ holds 247.
- **Note:** relayhistory's Deploy Production pins the replica consumer to `max_concurrency` 4.

## Rollback (fail-closed, manual)

1. Set `DELIVERY_CUTOVER` to `{"version":1,"mode":"hold"}`: `cf-vars.py relayhistory-api '{"DELIVERY_CUTOVER":"{\"version\":1,\"mode\":\"hold\"}"}'` and the GitHub variable.
2. As the Supabase owner: `UPDATE sessions.ingest_buffer_admission SET valid_until = clock_timestamp() - interval '1 second'`. Never DELETE.
3. As the Neon owner: `SELECT sessions.owner_delivery_cutover_v1('halted', '<epoch>', NULL)`.
4. Keep every acknowledged S3, outbox and receipt object. There is no automatic return to `legacy`, and an old binary or `app_blue` binding must not be restored without a reviewed plan.

## Credential handling

Every credential came from 1Password (on sf-mac-mini, or on kjg-lap via `mini/op-shim.sh`), neonctl, the Supabase CLI temporary login,
or SSO. Each one was passed over stdin in RAM only: never in files, argv or logs. Role passwords went to Supabase as client-computed SCRAM
verifiers. Neon only accepts plaintext, so there they were sent as a `quote_literal` statement built in RAM.
