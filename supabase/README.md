# supabase/migrations

Nothing in this directory has been applied to any database. The only place these
files run is the in-process PGlite inside `packages/pipeline` vitest.

## Provenance

| File | Source |
| --- | --- |
| `20261025000600_pipeline_queue.sql` | relayhistory-cloud PR #210 (the framework this repo moved in) |
| `20261025000000_sessions_index.sql` | relayhistory-cloud PR #171, merge commit `51c313fda3c4288cf75f9927c4004fb39b6cd2ee` |
| `20261025000100_repo_identity.sql` | relayhistory-cloud PR #177 (`7f49063`) |
| `20261025000200_scrub_quarantine.sql` | relayhistory-cloud PR #185 (`fe89fc3`) |
| `20261025000300_upsert_keep_on_null.sql` | relayhistory-cloud PR #185 (`fe89fc3`) |
| `../tests/supabase_shim.sql` | relayhistory-cloud PR #171, merge commit `51c313fda3c4288cf75f9927c4004fb39b6cd2ee` |

Repository: https://github.com/AgentWorkforce/relayhistory-cloud. The `000000` to
`000300` files and `supabase_shim.sql` are verbatim copies of the blobs at
`8603c141bf7dfcd67687db1e5f02e48c1eb2ef61` (the `sp/pipeline-framework` branch of
PR #210, which sits on main `248fe67`, after #171, #177 and #185). They are byte
identical at the PR #211 head too. Do not edit them here; change them upstream.

## Why they are here

`20261025000600_pipeline_queue.sql` alters `sessions.pipeline_events` and
`sessions.pipeline_dead_letters` and creates `sessions.pipeline_jobs` in the
`sessions` schema. All of that comes from `20261025000000_sessions_index.sql`. The
`000100` to `000300` files come before `000600` on relayhistory-cloud main, and the
TypeScript tests apply every file here in name order after
`supabase/tests/supabase_shim.sql` (which stubs the Supabase roles and `auth`
functions). So these files are dependencies of `000600`, vendored so the tests run.
