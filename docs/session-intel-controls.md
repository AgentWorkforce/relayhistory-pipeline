# Optional session intelligence controls

M3-1 adds migration `20261105062160_session_intel_controls.sql` and the
`session_intel_controls_v1(org, workspace)` getter. Nothing is enabled by applying
it. Every feature requires both an enabled global row and an enabled row for
the exact organization and workspace. Missing rows mean off. Environment flags
remain the outer gate.

The advisor reads controls before each workspace scan. `advisor=false` skips
facts reads and advisory writes; `dm=false` leaves new HIGH rows pending for
pull. Existing advisories remain readable and ackable. The getter also returns
`conflicts`, `claims` and `regen` for their owning workers to integrate separately;
this change does not wire those workers or put controls in the summary path.

The runtime can execute the SECURITY DEFINER getter but cannot read or write
the table. An operator writes settings through its database-owner connection.
Use an operator label in `updated_by`, never a person's identity or a credential.
The following SQL uses psql variables `org` and `workspace`:

```sql
-- Enable the global ceiling. This alone enables no workspace.
INSERT INTO sessions.session_intel_controls(scope,feature,enabled,updated_by)
VALUES ('global','advisor',true,'local-test')
ON CONFLICT (scope,org_id,workspace_id,feature)
DO UPDATE SET enabled=excluded.enabled,updated_at=now(),updated_by=excluded.updated_by;

-- Opt in one synthetic workspace.
INSERT INTO sessions.session_intel_controls(scope,org_id,workspace_id,feature,enabled,updated_by)
VALUES ('workspace',:'org',:'workspace','advisor',true,'local-test')
ON CONFLICT (scope,org_id,workspace_id,feature)
DO UPDATE SET enabled=excluded.enabled,updated_at=now(),updated_by=excluded.updated_by;

-- Stop its advisor without a restart. Repeat for feature='dm' to stop DMs only.
UPDATE sessions.session_intel_controls
SET enabled=false,updated_at=now(),updated_by='local-test'
WHERE scope='workspace' AND org_id=:'org' AND workspace_id=:'workspace' AND feature='advisor';

-- Emergency global stop, including workspaces that opted in.
UPDATE sessions.session_intel_controls
SET enabled=false,updated_at=now(),updated_by='operator'
WHERE scope='global' AND feature='advisor';
```

Settings are not cached between scans. The client times out after two seconds;
an error, missing function, timeout or malformed row set returns all features
off and logs a fixed reason with `count=1`, without the database's error text.
Only one control query can be outstanding per reader, so an unresponsive
database cannot accumulate queries on successive ticks. A flip takes effect
when the next scan reads the workspace controls; it does not cancel a scan
already in progress. Ingest, summaries and the read API do not read this gate.

The pipeline e2e's `coordination.advisories` step opts in its synthetic workspace,
proves a disabled scan writes nothing, and flips it on while the worker remains
running. Schema tests cover missing rows, global/workspace conjunction, tenant
isolation, grants and invalid scopes; client tests cover failures and timeouts.

The checksum-pinned down migration removes the controls and getter, after
refusing a later applied migration. It discards operator settings. The client
then keeps intelligence off; existing claims, summaries and advisories are
preserved. Reapply seeds no settings. Deployment, applying migrations and
changing flags outside an explicitly authorized local test need Khaliq's own
approval, as described in AGENTS.md.
