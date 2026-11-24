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

## Sources (read-only map, 2026-11-24)

| Link | Source | Owner | Notes |
|---|---|---|---|
| Session to tokens and cost | F1 `metrics.usage` (`SessionUsageSummary`: `observed`, `inputTokens`, `outputTokens`, `cachedInputTokens`, `totalTokens`, `costUSD`) | pipeline (F1) | `costUSD` null means unknown, never 0. A price engine (relayburn) is being scoped to fill it, with optional price provenance. |
| Session to person | the session's `userIds`, from the uploader's introspected token (`sub`) | pipeline | The join key is the user id. Host names are display text and are never joined on. |
| Person to organization | Control `organization_memberships` | Control | Authoritative. No per-workspace member table. Used only to resolve a person INSIDE the session's own organization, never to choose the organization. |
| Session to project | `projectId`, `catalog.repoUrl`, `gitBranch` (free strings) | pipeline | Control attaches no meaning to them. |
| Person to team | none | unowned | No team table, IdP groups claim or SCIM. |

## First delivery (proposed)

Attribute observed spend by **organization, workspace, person and project/repo**. No team dimension.

Rules:

- **Observed only, on two independent axes.** Token sums cover sessions with `observed` usage. Cost sums cover
  sessions with a known `costUSD`. Each sum carries its own denominator (`observedSessions` over `sessions`;
  `knownCostSessions` over `sessions`), because a session can have observed tokens and an unknown cost. When
  `knownCostSessions` is 0, `costUSD` is null, never 0. Nothing is imputed or extrapolated.
- **What the window means.** F1 usage is a session total, not event-time spend. The window SELECTS sessions (by the
  same rule as Stage 1: analyzed and active within `[since, until)`), and each selected session contributes its whole
  recorded total once. The result is labelled "recorded usage of sessions active in the window", never "spend in that
  week". Successive windows must not be summed, because a long session can appear in several. Splitting by event time
  needs per-event usage, which the producer does not report today.
- **Token semantics.** `inputTokens`, `outputTokens`, `cachedInputTokens` and `totalTokens` are summed as the source
  reports them, never recombined (a source's `totalTokens` may or may not include cached input). F1's list of models
  used is shown as which models were used; cost is never split per model.
- **Scope comes from the session.** A session's organization and workspace are the ones on the session row itself. A
  user can belong to several organizations, so a membership never moves a session to another organization.
- **Former members.** Membership rows are deleted when someone is removed, so an older session's user id can match no
  current member. Such sessions go to a `former member` bucket, keyed by user id, so per-person sums still add up to the
  workspace total.
- **Once per session.** A session's usage counts once, never once per user. A session with one user is attributed to
  that user. A session with several users goes to a `shared` bucket for its project, with the user count. It is never
  split by guesswork.
- **Parent and child.** A delegated child's usage counts under the child only. The parent's own usage excludes it,
  provided the producer reports them separately. If a harness reports a rolled-up parent total, the parent is marked
  `includesChildren` and its children are excluded from the sum. The producer must state which applies, per source.
  This is the open question below.
- **Cross-harness duplicates.** One logical piece of work seen by two harnesses is not merged by text or time. Without
  a shared canonical id, sums are labelled "observed recorded usage, duplication possible", never "deduplicated spend".
  Each sum carries counts of unresolved and excluded sessions (`unresolvedLinks`, `excludedSessions` with reasons).
  Merging needs an explicit producer-side link.
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
