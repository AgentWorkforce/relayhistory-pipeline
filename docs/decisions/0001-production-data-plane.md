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
