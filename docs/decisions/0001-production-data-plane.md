# 0001: Production data plane is Supabase plus S3

**Status:** locked (Khaliq, 2026-10-30)
**Supersedes:** the open "HoopDb (RDS Postgres 17 + pgvector, or Supabase)"
choice in `docs/aws-stage.md`, for hosted production only.

**Source:** Khaliq gave this decision in the helm-chart-chief session on
2026-10-30. helm-chart-chief relayed it to the migration owner
(`96-no-bull-migration`), and Khaliq then confirmed the migration scope,
including this decision, in that session.

## Decision

- **Production database:** Supabase. HoopDb in hosted production is a Supabase
  project in the org the 96Bulls team owns (`docs/supabase-access.md`).
- **Production AWS:** S3 only. Locker (object storage) is the only AWS service
  in the hosted production data plane.
- **EKS, Aurora and every other AWS resource** exist only to prove the
  self-hosted deployment (`docs/aws-stage.md`, `stacks/aws/`). They are not
  the hosted production data plane, and no production data moves to them.

## Consequences

- The RelayHistory/Neon migration targets Supabase (index, queue, events) and
  S3 (objects). It does not target Aurora or RDS.
- Schema and index parity are measured against the Supabase migrations in
  `supabase/migrations/`, not against an AWS database.
- Self-host proof work on EKS/Aurora may continue, but it does not block the
  production migration, and a production cutover does not depend on it.

## Still needs Khaliq's explicit OK for each exact action

This record is not that approval. Each of these needs it separately:

- enabling Neon logical replication;
- moving any production data;
- changing secrets, IAM or DNS;
- deploying;
- merging migration PRs that change production behaviour;
- cutover and rollback.

## Amendment 1: ObsessionDB as the intended transcript search store (DRAFT, CONDITIONAL)

**Status: conditional.** Khaliq approved ObsessionDB on 2026-10-31 as the
**intended** production transcript store. It is **confirmed only after
capability and security evidence**: gate G-T0E in
`docs/relayhistory-supabase-migration-plan.md`. That covers:
- full-text and substring performance at scale;
- tenant isolation: app-layer primary plus a DB row-policy backstop (K-44);
- erasure and delete semantics, meeting the 48 h physical-purge SLA (K-45);
- IP allowlisting and private networking;
- SOC 2 and security posture;
- `us-east-1` availability;
- backup and rebuild from S3.

Until G-T0E passes and Khaliq confirms in his own words, this amendment
**does not** add ObsessionDB to the production data plane. If ObsessionDB
fails, the store choice returns to Khaliq.

The decisions are K-28 ("Yes, add ObsessionDB", restated as conditional), K-33
(region), K-34 ("Include it in the T0 gate") and K-36 ("No embeddings
pre-T0", which supersedes K-34 as written). helm-chart-chief records
relayed rulings as PROPOSED until Khaliq confirms them in its session. This text is a draft in
96BullSessions #90 and takes effect when that PR merges.
**Amends:** "Decision" and "Consequences" above.

### Decision

- **Production data plane, once confirmed:** Supabase (database) + AWS S3
  (objects) + **ObsessionDB (transcript history search store)**.
- **Off the hot path (K-39, "ObsessionDB off the hot path"):**
  - It is a derived, rebuildable index, fed asynchronously from S3.
    Ingest never waits on it.
  - The search interface reaches it through a **store-agnostic** adapter
    (K-39, K-40). The candidates are ObsessionDB (intended), ClickHouse
    Cloud and Amazon OpenSearch Service. All are chosen on the same
    benchmark and evidence gate, so a failed candidate changes the driver,
    not the design.
  - **It is primary for all transcript search (K-47).** Supabase keeps a
    48 h complete-text safety-net window (0002 Amendment 1), plus metadata
    and summary search. If the store is unavailable, search returns those
    results as a **silent partial**: no user-facing label, with internal
    coverage metadata, metrics and an alarm (K-46).
- **Isolation (K-44):** application-layer scoping is primary, with DB row
  policies on scoped users as a backstop. Both are required, and the
  vendor must enable row policies and user creation.
- **Erasure (K-45):** a logical delete immediately, and physical purge
  within 48 h, including ObsessionDB's 24 h time travel.
- **ObsessionDB's role:**
  - lexical transcript search for the MCP tools (`search_shared_sessions`
    excerpts, the `get_shared_session_context` window);
  - the dashboard's ranked search: keyword ranking over complete
    transcripts, with no vector pools before T0 (K-36);
  - the memory-pack lexical and `files_touched` pools.

  It is a derived projection, loaded from the pipeline's normalized S3
  objects. S3 stays the system of record, so ObsessionDB can be rebuilt by
  replay.
- **Region:** the production ObsessionDB service runs in AWS `us-east-1`,
  next to Neon production and Supabase `sessions`. The proof service
  `relay-transcript-proof` (`us-west-2`) is evidence only and never holds
  production data.
- **Supabase still holds no transcript text and no embeddings**, as 0002
  requires. If embeddings are added after T0 (a separate decision), they
  live with the ObsessionDB projection.

### Consequences

- The production migration's T0 write switch waits until new sessions are
  transcript-searchable on ObsessionDB, including keyword-ranked
  dashboard search (K-27, K-34 and K-36; `docs/relayhistory-supabase-migration-plan.md` gate G-T).
- Pre-T0 history keeps legacy transcript and hybrid search on the frozen
  Neon T0 child branch until it is loaded into ObsessionDB (K-6).
- ClickHouse Cloud is trialled with synthetic data only, with a $50 hard
  cap (K-37), as an engine comparison for the evidence gate. The ObsessionDB
  free dev service (eu-central) is used for functional checks only. Neither
  trial is production provisioning.
- relayhistory-cloud #233 should be updated to match.
- The "AWS is S3 only" rule above still applies to resources we operate,
  with one exception: the ObsessionDB service in `us-east-1`. Its hosting
  model and account are open (plan Q21).

### Still needs Khaliq's explicit OK for each exact action

- confirming the history store (ObsessionDB, or another candidate that passes G-T0E);
- provisioning ObsessionDB (production and staging) and its credentials;
- enabling the feed into it;
- any later embedding stage, including the model, provider and budget
  (post-T0 follow-up, K-35);
- every step in the plan's approval list.

This amendment is not that approval.
