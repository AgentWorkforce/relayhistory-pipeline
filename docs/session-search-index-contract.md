# Logical session directory and search contract

Production uses S3 for canonical session content and Supabase for a rebuildable
metadata index (decision 0002). `sessions.session_index` remains one row per
physical machine manifest and holds the object pointer. The additive
`sessions.session_directory` is one row per logical session:

`(org_id, workspace_id, harness, session_id)`.

That split is required for stable pagination. A session resumed on three machines
has three manifest pointers but must occur once on a session list. The future
projector merges the physical rows into the directory; this schema PR does not
implement or run that projector.

Cloud must treat agent lookup as a future RelayHistory-advertised metadata
capability. It must not bind directly to this table or assume an `agent_name` API
field until that capability is implemented. The scalar stored here is the delivered
root agent identity; child-agent relationship labels are not session identities.

User and machine filters use `session_directory_users` and
`session_directory_machines`. The arrays on `session_directory` are response
conveniences, not indexed filter inputs. This avoids GIN-plus-sort plans for
recent-session pages.

## Indexed remote query shapes

| Filter or lookup | Index strategy |
| --- | --- |
| workspace or org recent | partial ordered btree with a total logical-session cursor |
| harness/source | workspace + harness + activity cursor |
| exact project or no project | separate partial activity indexes |
| agent exact or prefix | normalized name with `text_pattern_ops`, then activity cursor |
| created range | workspace + `created_at` cursor |
| state | workspace + state + activity cursor |
| user or machine | normalized membership table + activity cursor |
| session ID without workspace/harness | org + session ID lookup |
| project substring | trigram GIN over `project_id` |
| general metadata substring | trigram GIN over bounded `search_text` |

The executable definitions live in
`src/hoopdb/search-index-contract.ts`. `bun run audit:search-indexes` is
read-only: it checks catalog structure and fails until the singleton
`session_directory_rollout.completed_at` is set by the future projector/backfill
lane. It never creates indexes or marks rollout complete.

## Metadata-only search boundary

`search_text` may contain only title, task title, agent name, project, repository,
branch, and model metadata. It must never contain transcript text, event content,
tool payloads, prompts, or responses. Canonical content remains in S3.

Consequently `search_shared_sessions`, `/v1/events?q=`, and dashboard transcript
search cannot be implemented by silently querying `session_directory`. They remain
unavailable on this data plane until ClickHouse/ObsessionDB is ready. A product may
label a separate metadata search, but it must not claim transcript-search parity.
Do not add a temporary Supabase transcript/tsvector projection based on a claim that
deleting an S3 or per-workspace KMS key crypto-shreds it: PostgreSQL heap pages,
indexes, WAL, replicas, and backups have independent persistence. Transcript storage
requires a recorded, technically viable encryption, deletion, RLS, and backup design
before it can enter this contract.

The legacy relayhistory-cloud `0067_session_catalog_agent_lookup.sql` projection
and its `session_catalog_agents` index are not the production contract. Its
normalization and ordered-prefix lessons remain valid; its event-triggered catalog
projection and legacy rollout marker do not.

## Agent prefix and benchmark gate

Exact lookup accepts a complete normalized name. Prefix lookup requires at least
three Unicode characters after trim/lowercase normalization. Shorter prefixes are
rejected rather than allowed to scan a large tenant directory.

Before a rollout can be marked complete, run exact and prefix queries against a
production-shaped population and record p50/p95 latency plus `EXPLAIN (ANALYZE,
BUFFERS, FORMAT JSON)`. The deterministic gate is:

- page size: 50;
- exact and representative selective prefixes must use
  `session_directory_agent_recent_idx` to constrain the name;
- at least exact, representative three-character, and deliberately broad
  three-character prefixes are measured;
- for a deliberately broad prefix PostgreSQL may instead walk
  `session_directory_workspace_recent_idx` or `session_directory_org_recent_idx`
  and filter in recency order; actual index rows plus `Rows Removed by Filter`,
  divided by returned rows, must be at most 100.

If a real three-character prefix fails that gate, do not relax the limit or hide it
with a faster test host. Raise the minimum prefix, add a proven projection, or move
agent discovery to a dedicated names table. Timing thresholds are environment
specific and belong in the release evidence; the row-amplification bound is the
portable correctness/performance gate.

## Projector decision still required

Before the projector/backfill lane can mark rollout complete, Khaliq must approve
the precedence rules for a logical session whose machine manifests disagree on
title, task title, agent name, project, model, or state. The schema can hold the
result, but it intentionally does not guess whether newest activity, a designated
owner machine, or another source wins. Usage counters also need an explicit
sum-versus-deduplicate rule. Until those rules are recorded, the rollout marker
stays incomplete and the audit fails closed.

## Dimensions that remain local

Relay Desktop also filters local path/directory, upload state, relay presence,
reasoning, and live active/inactive state. Those are local catalog/runtime
dimensions and do not belong in Supabase. Model is searchable metadata, but there
is no remote ordered model-filter contract yet. Adding an API filter requires a
query-plan test before adding another write-amplifying index.
