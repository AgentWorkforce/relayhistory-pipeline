# Four missing NOLOGIN roles — created and verified

Direct user approval in this chat: “create the 4 missing NOLOGIN roles”; “schema window approved”. The latter is recorded for the coordinated window; it is not a claim that the schema has already been applied.

One execution of `supabase/bootstrap/session_ingest_roles.sql` at source f9409ccb, exact SHA `c957ae416d707d471b576bea93dee07f2035fb8b930bacaff4207d63ffc0b478`, committed at **2026-11-07T21:59:23.150Z**, on the existing sf-frame Supabase owner route. Fresh preflight confirmed the exact four missing roles; post-inventory at **2026-11-07T21:59:55.442Z** verified:

- `bulls96_babysitter_retention`
- `bulls96_canonical_feature_producer`
- `bulls96_privacy_eraser`
- `bulls96_project_events`

All four are NOLOGIN, NOINHERIT, NOSUPERUSER, NOCREATEROLE, NOCREATEDB, NOREPLICATION and NOBYPASSRLS; no outgoing memberships or role/database settings. Only Supabase's expected administrative creator edge was added automatically (postgres member, supabase_admin grantor, ADMIN true / INHERIT false / SET false). No explicit GRANT, existing-role ALTER or other mutation SQL was run.

All **13 prerequisite roles are present**, with no catalog-shape violations reported. The existing nine role records (attributes, memberships and settings) are exactly unchanged from the fresh before-inventory. This clears the four-missing-role prerequisite; it is not full effective-privilege, phase-2 or whole-window readiness proof.

- Bootstrap receipt `c344b597b2bc31b3f79a7ae183bf985a93ba41e07449adbe59d4f337e14e850e`.
- Post-inventory `d52d5723bb4be7919ea561bb931edf1a196402f104e998e8a6bb83b764acb85d`.
- Verification `15b01a1d79e4723fe8e636aef5b54111010f0636afd68a581174652fafc3b9a1`.

No migration, generation rebind, runtime deploy or feature activation has been performed by this role operation. The expired schema plan/capture is being refreshed; current coordinated runtime prerequisites are requested from pipeline-lead. The user approval is retained; no new approval question has been sent.
