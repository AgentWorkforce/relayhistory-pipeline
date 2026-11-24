# Session Intelligence: unfinished agent work (design proposal, no code)

Status: proposal. Nothing here is built, wired or enabled.

Requirement (Khaliq, 2026-11-24): "One thing the pipeline should also surface is unfinished agent work. Maybe it's a
branch that an agent didn't push or a pr that is unmerged. Or maybe something that is shipped default off and the user
should be aware of. Those type of things need to be flagged to the user."

## What counts as unfinished

| Kind | Meaning | First delivery |
|---|---|---|
| `unpushed` | A session committed to a branch and no push of that branch is recorded afterwards, by any session of the same person and repository. | yes |
| `open_pr` | A session opened a pull request (or pushed a branch named for one) that is still open: unmerged, and either stale or waiting on review. | yes, with forge read |
| `default_off` | A session shipped code behind a flag documented as default-off, and the flag has not been turned on. | partial: the shipment is detected, the "not turned on" part needs a flag source |
| `stated_todo` | The session itself says the work is unfinished ("follow-up", "not done", "left for later") in an F1 outcome or blocker. | yes, from F1 |

Each signal is a **candidate**, never a verdict. The user decides whether it is really unfinished.

## Evidence, per kind

1. **`unpushed`.**
   - The import already records `commit` actions with repository provenance (`src/session-intel/import/provenance.ts`,
     kinds `write`, `commit` and `outcome`). This slice adds two action kinds, `push` and `pr_create`, recognized from
     shell tool calls (`git push`, `gh pr create`) with the same literal-only parsing rules as `gitDirectory`. Dynamic
     or unparseable commands are recorded as `unknown`, never guessed.
   - A candidate is a `commit` by a session on repository R and branch B (from the action's working directory and the
     catalog's `gitBranch`) with no `push` of B to R by any session of the same person after it, and older than a grace
     period (default 24 h).
   - Evidence: the commit action's event id, plus the absence of a push over the same person's sessions in the window.
     Absence is only claimed over sessions the pipeline actually holds, and the coverage says so.
2. **`open_pr`.**
   - Source: a `pr_create` action (or the forge URL printed in its tool result), joined to the forge's current PR
     state.
   - Reading GitHub is a credential and data step. It needs a read-only token for the repositories in scope, which is
     Khaliq's decision, and it runs as an offline job that writes only PR number, state, merged flag, updatedAt and
     review state. No PR bodies.
   - Stale means open and not updated for N days (default 3), or approved but unmerged for N days.
   - Without forge access, the signal degrades to "PR opened by a session; current state unknown".
3. **`default_off`.**
   - The shipment side comes from the session: F1 outcomes and decisions that name a flag, commit messages and
     CHANGELOG or doc lines added by the session that say "default off" or "behind <FLAG>", restricted to an exact flag
     token pattern (`[A-Z][A-Z0-9_]{3,}_ENABLED` and the like).
   - The "not turned on" side needs a flag source per deployment (deploy config, wrangler vars, Helm values or a flag
     service). No such source is readable by the pipeline today.
   - First delivery shows "shipped default-off; enablement state unknown", with the flag name and where it shipped.
     Reading live configuration is an infrastructure decision.
4. **`stated_todo`.** F1 `outcomes` with a non-complete status and F1 `blockers` that the session left open, with their
   evidence ids. This is already in the analysis contract; nothing new is extracted.

## Attribution, without double counting

- **Scope** comes from the session row: organization and workspace. A person is the session's user set, resolved
  inside that organization, as in the [spend design](session-intel-spend-attribution.md), with the same `shared`,
  `unattributed` and `former member` buckets.
- **One item per artifact, not per session.** Items are keyed by the artifact:
  - `(repository, branch)` for `unpushed`;
  - `(repository, prNumber)` for `open_pr`;
  - `(repository, flag)` for `default_off`;
  - `(session, outcome id)` for `stated_todo`.

  Several sessions that touched the same branch or PR produce one item listing all of them. A later session that
  pushes or merges it resolves it for everyone.
- **Project** is the repository from action provenance when known, else the declared `projectId`, labelled "as
  declared".

## How it is shown

- A section of the operator view, "Unfinished work". Each item shows its kind, artifact, age, owner bucket, the
  sessions involved, and the evidence that cites it (event ids, and PR links when forge state is known).
- Counts per person and project, with each item counted once per artifact.
- For the agent: optionally, a LOW advisory (pull path only, never urgent) to a live session of the same person in
  the same repository, saying "branch B has commits not pushed since <time>". This reuses the advisories rubric. It is a
  separate decision; it is off by default.

## False positives and dismissal

- **Intentional work.** A user can dismiss an item ("intentionally abandoned", "parked", "not mine") through a
  dismissal record keyed by the artifact. A dismissal is not reopened by the same evidence, but a new commit or push
  on that artifact reopens it.
- **Known benign patterns,** excluded by rule:
  - scratch or WIP branch prefixes (configurable);
  - the default branch when the session's own policy says it pushes directly;
  - branches that were later deleted (when forge state is known);
  - forks;
  - commits that later appear on the remote under another branch, matched by commit sha when the forge is
    readable.
- **Coverage honesty.** "Not pushed" is only ever "no push recorded in the sessions we hold". Harnesses whose tool calls
  are not captured, and sessions outside the window, are listed in coverage, not silently counted as unpushed.
- **Evaluation.** Before any notification, the same bar as the alerts: blind-labelled precision on held-out data, with
  the row SILENT (dashboard only) until it passes. The quality study's labelling kit gets an "unfinished" label.

## Contract sketch

An optional `unfinished` block in a later `workspace-synthesis` version, or its own read route:

```
{ items: [{ id, kind, artifact: {repository, branch?, prNumber?, flag?}, state: "candidate"|"dismissed"|"resolved",
            since, ageMs, owner: <user-set bucket>, sessions: [{source, sessionId}], evidence: [{source, sessionId, eventId}],
            forge?: {state, merged, updatedAt, review}|null, enablement: "unknown"|"off"|"on" }],
  counts: { byKind, byOwner, byProject }, coverage: { sessions, withToolCalls, forgeRead, flagSource }, bounds }
```

## Decisions for Khaliq

1. **Forge read.** A read-only GitHub token for the repositories in scope, used by an offline job that stores PR state
   only. Without it, `open_pr` is "state unknown".
2. **Flag source.** Which system holds whether a default-off flag is on, per deployment, and whether the pipeline may
   read it. Without it, `default_off` is "enablement unknown".
3. **Agent nudges.** Whether to send the LOW pull-path advisory to the person's live sessions, or show items in the
   dashboard only.

Everything else (the `push` and `pr_create` action kinds, artifact-keyed items, dismissals, the operator section) is
code within the existing lanes.

## Coordination

- layered-control: the owner buckets follow relay-control#323 (teams group the user-set buckets; items carry the user
  set).
- burn-cost-scope: no overlap; spend on unfinished work can be shown by joining the item's sessions to the spend
  block, never double-counting a session that appears in several items.
