# RelayHistory migration control board

Updated: 2026-10-30 (America/Los_Angeles). This board records evidence. It does
not authorize cloud or production changes. Nothing on it may happen without
Khaliq's direct approval for that exact action: no deploy, merge, data move,
IAM or secret change, or cutover.

> **Target changed.** Production is **Supabase plus AWS S3**
> (`docs/decisions/0001-production-data-plane.md`,
> `docs/decisions/0002-relayhistory-onto-the-pipeline.md`).
>
> **Current plan:** `docs/relayhistory-supabase-migration-plan.md`. It
> covers the switch-first T0 flip, per-tenant export waves, and the
> benchmark, drill, security and approval gates.
>
> The Aurora, EKS and logical-replication rows below are kept for history
> and marked **superseded**. They are not on the production path. EKS on
> AWS remains self-host proof infrastructure only. The uncommitted
> `docs/relayhistory-cutover.md` (linuyc) is superseded in full.

## Production path (Supabase plus S3)

Section numbers refer to the plan.

| Gate | Owner | State | Evidence required to close |
| --- | --- | --- | --- |
| SEC-01 fixed in hosted RelayAuth (S1) | cloud lane | cloud #5886 merged to `trunk`; promotion to `main` and the deploy are unverified | Deployed SHA verified; negative cross-org mint probe (K-23) |
| Production S3 bucket, per-workspace KMS keys, Worker S3 identity (S5, S6, P9) | unassigned | not created | Reviewed plan; Khaliq-approved apply (K-2) |
| Supabase schema: index, directory, short_summary, token pins (#38, #48, #72) | 96Bulls lanes | drafts | Merged; applied (K-3); `audit:search-indexes` green |
| Directory projector (G-P) | **unowned** | missing | Projector built and tested; #38 merge precedence decided (K-4) |
| Hosted search issuer (relayauth #116 plus G-H key creation) | relayauth lane | draft; no hosted creation path | Released and deployed; key created (K-9); #73 suite green on hosted Supabase |
| Cutover gate, hold, driver, terminal reads, index activation (relayhistory-cloud #223, #224, #225, #226, #227, #231) | relayhistory-cloud lanes | drafts / open | Independent PASS; merged; deployed dark in phase `neon` (K-13) |
| Cloud consumers and post-T0 write gating (cloud #5850) | helm-chart-chief | open, `merge-train:hold` | Deployed with the flag off (K-14) |
| Revision feed to Cloud after T0 (G-R1) | **unowned** | missing | A pipeline producer for `relayhistory-project-events` |
| Reverse path from the pipeline to Neon (G-V), and the reverse-data decision | **unowned** | missing | Built and rehearsed (R1), or forward-only accepted (R2) (K-22) |
| Transcript search across T0 | Khaliq | decision needed | T1, T2 or T3 chosen (K-6) |
| Rehearsal snapshot, parity and benchmark (phase 2) | migration lane | not started | P-1 to P-7 exact; section 10 thresholds (K-5) green |
| Backup and restore drill (section 12) | migration lane | not started | RTO/RPO report under `docs/evidence/` |
| Observability and SLO alarms (section 13) | unassigned | backend undecided | Alarms live before phase 3 |
| T0 flip, export waves W0 to W3, per-wave fallback removal | migration lane | blocked on everything above | K-17, K-18, K-19 per wave |

## Superseded (Aurora on EKS; kept for history)

| Gate | Owner | State | Evidence required to close |
| --- | --- | --- | --- |
| sessions-prod identity and deployment path | AWS lane | **superseded**: production AWS is S3 only (0001) | — |
| private VPC, EKS and Aurora | AWS lane | **superseded**: Aurora is not the production database | — |
| Aurora logical-replication capacity | CDC lane | **superseded**: no logical replication (plan, section 5) | — |
| source publication readiness | CDC lane | **superseded** | — |
| scoped schema restore | CDC lane | **superseded**: only the legacy state copy set moves into Supabase | — |
| initial 485.6 GiB copy and WAL stream | CDC lane | **superseded**: content is exported to S3; derived tables are re-derived (plan, section 4) | — |
| compatibility API over TCP PostgreSQL | `helm-helper` | **superseded**: the existing Worker gets a TCP seam (relayhistory-cloud #225) | — |
| background workers and Cloudflare Queue replacement | compatibility lane | **superseded**: queues and crons stay on Cloudflare | — |
| durable write hold and source fence | cutover lane | **moved**: relayhistory-cloud #224 HoldLog plus `migration:neon-fence` | see the production table |
| schema/data reconciliation | verification lane | **moved**: parity checks P-1 to P-7 (plan, section 9) | see the production table |
| database search benchmark | benchmark lane | **moved**: plan, section 10, against Supabase, not Aurora | see the production table |
| end-to-end API benchmark | benchmark lane | **moved**: plan, section 10 | see the production table |
| backup, restore and failover | AWS lane | **moved**: plan, section 12, for Supabase and S3 | see the production table |
| stable traffic switch and rollback | cutover lane | **moved**: plan, sections 8 and 11 | see the production table |
| AWS acceptance | `helm-chart-chief` | **re-scoped**: self-host proof only; it does not gate production | `aws-proof` checkpoint notes |
| adapter commit and GCP proof | GCP lane | parked | Unchanged; not on the production path |

## Current local proof (unchanged from 2026-10-29)

- The database and HTTP benchmark harnesses, the schema-superset migrator
  and the read-only verifier compile and pass their focused tests.
- The Aurora, EKS and GCP declarations are local code only. This work changed
  no cloud resources.
- The existing Worker remains on Neon HTTP by default.
- SQLite write-hold code is a local fixture only. It is not an acceptable
  production durability mechanism.
