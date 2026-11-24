# Session Intelligence: spend attribution (design proposal, no code)

Status: proposal. Nothing here is built, wired or enabled. It extends the operator view
([workspace synthesis](../session-intel-workspace-synthesis.md)) with the question it cannot answer today: where did the
spend go, and did it pay off?

## The gap

`workspace-synthesis/1` and `/2` report usage *coverage* (observed or not, cost known or not) but never sums. Three
things are missing:

1. **Totals.** No token or cost sums per scope.
2. **A join.** Nothing maps a session to a person, a project, or a team.
3. **De-duplication.** A parent session and the children it delegates to, the same work seen by two harnesses, or one
   session shared by several users would each be counted more than once by a naive sum.

## Sources (read-only map, 2026-11-25)

| Link | Source | Owner | Notes |
|---|---|---|---|
| Session to tokens and cost | F1 `metrics.usage` (`SessionUsageSummary`: `observed`, `inputTokens`, `outputTokens`, `cachedInputTokens`, `totalTokens`, `costUSD`) | pipeline (F1) | `costUSD` null means unknown, never 0. A price engine (relayburn) is being scoped to fill it, with optional price provenance. |
| Session to person | the session's `userIds`, from the uploader's introspected token (`sub`) | pipeline | The join key is the user id. Host names are display text and are never joined on. |
| Person to organization | Control `organization_memberships` | Control | Authoritative. No per-workspace member table. |
| Session to project | `projectId`, `catalog.repoUrl`, `gitBranch` (free strings) | pipeline | Control attaches no meaning to them. |
| Person to team | none | unowned | No team table, IdP groups claim or SCIM. |

## First delivery (proposed)

Attribute observed spend by **organization, workspace, person and project/repo**. No team dimension.

Rules:

- **Observed only.** Sum only `observed` usage. Sum `costUSD` only where it is known, and report the known-cost
  coverage next to every sum. Nothing is imputed or extrapolated.
- **Once per session.** A session's usage counts once, never once per user. A session with one user is attributed to
  that user. A session with several users goes to a `shared` bucket for its project, with the user count. It is never
  split by guesswork.
- **Parent and child.** A delegated child's usage counts under the child only. The parent's own usage excludes it,
  provided the producer reports them separately. If a harness reports a rolled-up parent total, the parent is marked
  `includesChildren` and its children are excluded from the sum. The producer must state which applies, per source.
  This is the open question below.
- **Cross-harness duplicates.** One logical piece of work seen by two harnesses is not merged by text or time. Without
  a shared canonical id it counts twice and is labelled as such. Merging needs an explicit producer-side link.
- **Bounds and privacy.** The same window and session caps as Stage 1. Person ids are the Control user ids the reader
  already scopes by. No names or e-mail addresses enter the artifact.

Output: an optional `spend` block in a later contract version (not part of `/1` or `/2`):

- per scope: `{sessions, observedSessions, inputTokens, outputTokens, cachedInputTokens, knownCostSessions, costUSD}`;
- `shared` buckets;
- the dedupe basis used per source.

## Open questions

1. **Producer accounting (pipeline / F1 owner).** How are `userIds` and `projectId` written? Which key identifies a
   session uniquely across uploads and harnesses? For each source, does the parent's usage include its children?
2. **Price provenance (relayburn scope).** The engine, the pricing snapshot and the provider, recorded per session so a
   sum can say which price table it used.
3. **Teams (Khaliq's product decision).** Either a team table managed in Control or IdP groups. Both are new Control
   work. layered-control has offered to own it once decided. It is not part of the first delivery.

## Not in scope

Billing, invoices, per-model price tables (owned by the price engine), forecasting, and any production enablement (that
needs Khaliq's own OK, as every Session Intelligence flag does).
