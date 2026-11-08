# Neon dependency inventory (History / hosted Cloud) — 2026-11-08

Read-only source inventory, produced by pipeline-lead. Sources: relayhistory-cloud origin/main `c7de2163`, sessions-pipeline origin/main `fc10bb03`, Cloud origin/main `c75d94e2`. Production flag values (`DELIVERY_FORWARD`, `DELIVERY_CUTOVER`, `LIST_BACKEND`, `SEARCH_BACKEND`, `AUTH_BACKEND`, `ONBOARDING_BACKEND`, `SESSION_READ_BACKEND`, `SESSIONS_READ_96BULLS_ROUTES/_WORKSPACES`) are deployment secrets/vars and were not observed, so "A" means "code is off Neon once its flag is on".

Classes: **A** replaced (flag) · **B** replacement exists, Cloud not switched · **C** no replacement yet · **D** can be dropped.

| # | Item | Where (relayhistory-cloud `packages/relayhistory/src/` unless noted) | R/W | Replacement | Class |
|---|---|---|---|---|---|
| 1 | Neon client / `DATABASE_URL` | db/client.ts, env.ts | R/W | — | D (last) |
| 2 | Neon migrations in prod deploy | db/migrations, scripts/apply-neon-migration.mjs | W | relayhistory-cloud PR #323 | D |
| 3 | `/health/ready` requires `DATABASE_URL` | routes/health.ts:11 | – | small fix | C |
| 4 | Legacy delivery accept | lib/delivery.ts:892 | W | ingest via DELIVERY_FORWARD/CUTOVER | B |
| 5 | Forwarded delivery still checks Neon cutover/custody | lib/delivery-forward.ts | R/W | frozen-supabase custody + buffered | B |
| 6 | Write-path producer to Neon replica | lib/write-path/index.ts:651 | W | — | D |
| 7 | `relayhistory-neon-replica` consumer | write-path/neon-replica.ts | W | NEON_REPLICA_TARGET=archive | D (after reads move) |
| 8 | Replica ingest recovery | write-path/neon-replica-ingest-recovery.ts | R | ingest /v1/delivery/recovery | D |
| 9 | Upload selection (deletion consent) | lib/delivery.ts:1359 | W | ingest selection + Supabase privacy authority (rhc PR #324, draft) | **B — blocker** |
| 10 | Session reset | lib/delivery.ts:1397 | W | ingest reset + #324 | **B — blocker** |
| 11 | Turn writes | lib/turns.ts:61 | W | ingest /v1/turns/batches; rhc PR #316 | B |
| 12 | Legacy `/v1/ingest` (Cloud syncs) | lib/ingest.ts:52 | W | Cloud HISTORY_SYNC_PROTOCOL=delivery | B |
| 13 | Onboarding heartbeat | routes/onboarding.ts:125 | W | ONBOARDING_BACKEND=ingest | A |
| 14 | Onboarding status counts | routes/onboarding.ts:188 | R | partial; rhc PR #320 | C |
| 15 | Auth tokens (login/mint/refresh/revoke, per-request lookup) | routes/auth.ts, auth/store.ts | R/W | AUTH_BACKEND=supabase; rhc PR #310 | B |
| 16 | Service tokens | routes/service-tokens.ts | R/W | same | B |
| 17 | Session replay events | lib/recall.ts:1116 | R | SESSION_READ_BACKEND=canonical; read API | B |
| 18 | Events within session | lib/recall.ts:847 | R | read API /v1/events | B |
| 19 | Transcript search | search-backend.ts:661 | R | SEARCH_BACKEND=clickhouse_only; older history falls back to Neon | **B — blocker (old history)** |
| 20 | Session list | list-backend.ts:201 | R | LIST_BACKEND=supabase (falls back to Neon for some filters) + read API; Option C adds own buffered sessions | B |
| 21 | Session list text search | recall.ts:609 | R | none fully off Neon; rhc #275/#292 | C |
| 22 | Hybrid/semantic search | session-search.ts:1104 | R | rhc #290/#292; pipeline semantic copy reads Neon | C |
| 23 | Session-state enrichment (org lists) | session-evaluation.ts:154 | R/W | none | D/C |
| 24 | List titles | lib/session-list-titles.ts | R | read API /v1/sessions/titles | B |
| 25 | Activity by day | session-activity.ts:64 | R | read API /v1/sessions/activity | B |
| 26 | Session output | session-output.ts:23 | R | read API 501; design only | **C — blocker** |
| 27 | Daily digest + cache | lib/digest.ts:683, narrative.ts | R/W | read API 501; rhc #325 | **C — blocker** |
| 28 | Timing | session-timing.ts:12 | R/W | read API /:id/timing | B |
| 29 | Turns read | lib/turns.ts:114 | R | read API /:id/turns (some 501 fallbacks; dashboard replay sends no workspace) | B |
| 30 | Metadata | lib/turns.ts:159 | R | read API /:id/metadata | B |
| 31 | Desktop catalog | session-catalog.ts:297 | R | Supabase RPC; no read-API route; rhc #288 | B |
| 32 | Session thread | session-links.ts:119 | R | rhc #284 | C |
| 33 | Brief read | session-analysis service.ts:140 | R/W | pipeline brief worker; rhc #278 | B |
| 34 | Analysis read/request | routes/session-analysis.ts | R/W | rhc #286 only | C |
| 35 | Model summary | routes/session-model-summary.ts | – | canonical already | A |
| 36 | Delivery records (Babysitter) | lib/delivery.ts:1131 | R | read API /v1/delivery/records; rhc #320 | B |
| 37 | Delivery-limits role diagnostic | delivery-source-role.ts:90 | R | — | D |
| 38 | Cron expireBabysitterEvidence | lib/delivery.ts:1082 | W | pipeline babysitter retention (042000) | B → D |
| 39 | Cron database health | database-health.ts:158 | R | — | D |
| 40 | Cron brief embeddings | session-brief-search.ts:65 | R/W | none (tied to #22) | C |
| 41 | Cron analysis/brief tick | session-analysis/worker.ts:79 | R/W | pipeline brief worker (briefs); none for analysis | B/C |
| 42 | Outbox / feeds publish | session-analysis/events.ts | R/W | pipeline canonical project events; rhc #248/#249 | B |
| 43 | SESSION_EVENTS queue wakeups | events.ts:563 | R/W | pipeline brief wakeups | B |
| 44 | Search index tail Neon→ClickHouse | search-index/tail.ts:23 | R | pipeline search-features consumer | D |
| 45 | Erasure authority + turn purge | migrations 0038/0074; pipeline neon-s3-erasure (`S3X_PRIVACY_AUTHORITY=neon` default) | R/W | `S3X/INGEST_PRIVACY_AUTHORITY=supabase` (C3 runbook); rhc #324 | **B — blocker** |
| 46 | Pipeline ingest custody witness (canonical mode) | pipeline src/session-ingest/app.ts | R | frozen-supabase + buffered only | B |
| 47 | Pipeline analysis cache | pipeline src/session-pipeline | R | none | C |
| 48 | Pipeline semantic copy | pipeline src/search-features/semantic-copy-* | R | none | C |
| 49 | Pipeline migration tooling | pipeline src/migration/* | R | — | D |
| 50 | rhc rollout/backfill scripts + 2 workflows | scripts/, .github/workflows | R/W | — | D |
| 51 | Self-host Node runtime | node/* | R/W | — | D hosted / keep self-host |
| 52 | Unreachable code | memory-pack.ts, pair.ts, reflex/learn.ts, coverage.ts | – | rhc #244 | D |

No Neon use found for: relayhistory (local SQLite only), Hyperdrive in relayhistory-cloud, Cloud public schema from relayhistory-cloud, sharing/billing/exports (shared-session access goes through the private read binding).

## Top blockers to turning Neon off
1. **Privacy/deletion authority** (#9, #10, #45) still on Neon; Supabase replacement (rhc #324) is a do-not-merge draft.
2. **Output + digest** (#26, #27) have no replacement store.
3. **Search** (#19 old history, #21, #22, #40, #48) depends on Neon pgvector / hydration.
4. **Brief/analysis jobs + feeds** (#33, #34, #41–43) live in Neon tables; analysis and thread (#32) have no merged replacement.
5. **Fallbacks** (#20 list filters, read-API 501s, workspaces outside allowlist; no Cloud route for catalog/brief/analysis/thread/delivery records).
6. **In-flight writes** (#11 turns, #12 legacy ingest, #5 forwarded custody, #7 replica consumer must run until reads move).
7. **Small items** (#3 health, #15 auth flag confirmation, #14 onboarding counts, #2 deploy migrations).
