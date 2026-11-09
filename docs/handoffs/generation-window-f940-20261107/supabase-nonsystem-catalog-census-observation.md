# Supabase non-system catalog census — one read-only snapshot

Requested by pipeline-lead message `244931989581475840` for cloud-migration-lead's table map. Query once on the existing sf-frame owner adapter route; no write/retry or application-data scan.

- Target: Supabase project `pyamkrhoyszgzirqqffq`, session `cli_login_postgres`, effective role `postgres`, database `postgres`, pinned TLS session pooler. Source checkout `f9409ccb9e46c3f100cf3017847d8ed62bc716c7`.
- SQL SHA-256: `6eb326424b3737f3a90f321c8f6e21fcc217bb07d0f34d1eaa476796843b5943`.
- Helper SHA-256: `28970c259ec051c134fcb51faa8a66d3de9fdc7ab7f36ca1ecbdd62044cb0534`.
- Census: `2026-11-07T21:14:24.212Z` through `2026-11-07T21:14:24.581Z`.
- Same-transaction owner/login/database/schema-owner/read-only preflight passed. The supplied SQL's ledger SELECT and no-RLS-filter guards passed. `BEGIN READ ONLY`, `SET LOCAL ROLE postgres`, `statement_timeout=3s`, `lock_timeout=250ms`, `ROLLBACK`. No nested transaction; the supplied wrapper was parsed and its body executed inside the guarded transaction.
- Complete result: **456 catalog relations**, **35 stored migration-ledger entries**. JSON is 172798 bytes, SHA-256 **`94613369617d66de97064b6817d4fd656e648795bb80e2a0d9aec2420f88b219`**. Result file preserves the entire returned row and metadata values; no truncation or pagination.

## Derived object counts

These counts were derived offline from the saved result, with no second query. Kinds: `r` ordinary table (including any table partitions), `p` partitioned table, `i` index, `I` partitioned index, `S` sequence, `v` view, `c` composite relation.

| Namespace | r | p | i | I | S | v | c |
|---|---:|---:|---:|---:|---:|---:|---:|
| auth | 27 | 0 | 111 | 0 | 1 | 0 | 0 |
| extensions | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| history | 3 | 0 | 4 | 0 | 0 | 0 | 0 |
| realtime | 2 | 1 | 4 | 2 | 1 | 0 | 3 |
| sessions | 83 | 1 | 166 | 2 | 3 | 0 | 0 |
| storage | 10 | 0 | 26 | 0 | 0 | 0 | 0 |
| vault | 1 | 0 | 2 | 0 | 0 | 1 | 0 |

## Interpretation limits

The SQL covers every `pg_class` kind in non-system namespaces, excluding `information_schema` and names beginning `pg_`. It is not just a base-table list. Empty namespaces have no `pg_class` rows and therefore are not enumerated. Foreign or other absent kinds are not inferred to exist.

`approximate_rows` is `pg_class.reltuples`, a planner estimate; `-1` means unknown. No application `COUNT(*)`, ANALYZE, data values, defaults, trigger/policy/function definitions or credentials were requested. The only non-catalog values read are the explicitly requested migration ledger names and stored SHA-256 strings. Those strings are not fresh hashes of installed SQL bodies or a complete drift verification.

This snapshot qualifies this one sessions Supabase project. It does not prove every Cloud/public/support/raw SQL object moved from Neon or describe another proposed Cloud target. It does not qualify live writers/consumers, publication, the reporting customer readback, current role-bootstrap readiness or deployment approval. Existing schema release plan/capture expiry and execution holds remain unchanged.

Files: `supabase-nonsystem-catalog-census-result.json` (complete returned row/receipt), `supabase-nonsystem-catalog-census-summary.json` (derived counts), `supabase-nonsystem-catalog-census.sql.txt` (exact supplied SQL), and `supabase-nonsystem-catalog-census-readonly.ts.txt` (guarded transport).
