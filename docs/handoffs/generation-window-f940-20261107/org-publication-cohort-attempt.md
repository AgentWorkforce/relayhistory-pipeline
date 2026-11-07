# Org publication cohort: scope resolution refused

One bounded read-only attempt on sf-frame2026-11-07T20:12:23.033Z..20:12:24.138Z. Qualified SQL hashbc431aa6f55eefc755126f5d7a8536c4b72e1f7ede9ff9c40ee65bd6c39481fd. Actual cohort SELECT was NOT executed; no organization parameter was bound because exact scope resolution did not qualify. No retry, role grant, alternate principal/endpoint, payload collection or production write occurred.

The existing CLI owner route used one REPEATABLE READ READ ONLY transaction, SET LOCAL ROLE postgres, statement_timeout3s/lock_timeout250ms, and ROLLBACK. Expected owner/login/database/schema/read-only and SELECT/inactive-RLS checks for the four cohort tables plus ingest_fence_owners passed. Supplied workspace/user values entered the helper over RAM stdin, were bound to a fixed approved-pair digest, and were never included in output. Org lookup used parameterized SELECT DISTINCT org_id FROM sessions.ingest_fence_owners WHERE workspace_id=$1 AND user_id=$2 ORDER BY org_id LIMIT2. This is durable owner-bound ingest scope, not external organization-directory freshness proof.

The only match fact emitted is exactlyOneOrgMatched=false. Do not reinterpret that Boolean as a particular zero/multiple count, proof the workspace/user is invalid, proof Brian did not upload, or proof of no published data. No org ID was emitted, persisted or placed in argv. The cohort SQL received no guessed/fallback org. The source helper and exact cohort SQL are included as text evidence, without supplied user/workspace identifiers.

Closed result:
~~~json
{"orgResolution":{"exactlyOneOrgMatched":false},"preflightRows":[{"expected_owner_route":true,"tables_visible":true,"any_rls_active":false}],"status":"org_resolution_refused","cohortExecuted":false,"result":"UNKNOWN"}
~~~

Raw receipt org-publication-cohort-result.json SHA2564193c08c328b0d43674c15e0df62a5b15507889957169f2194c3c6291240a658. Lead244917106160033792 and release owner244917112420069376 were sent the exact closed result. Need an authoritative scope resolver/binding qualification before this org cohort can run. A separately reviewed auth/workspace directory path might supply it, but none was queried or assumed here. The earlier global publisher zero-activity snapshot remains only its own timestamped observation, not a scoped answer to this cohort or a canonical readback proof.


Privacy wording clarification: the no-argv guarantee is specifically for the resolved organization ID, as required. The supplied workspace/user coordinates originated in the authorized request and control invocation and were fed to the helper through stdin; they were not retrieved as database output or included in the receipt. The79fbf764 commit message used the broader shorthand no IDs in argv; it should be read with this narrower, accurate scope. No organization value was emitted or guessed, and the org cohort did not execute.
