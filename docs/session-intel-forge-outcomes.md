# Session Intelligence forge outcomes

Ground truth for the overlap benchmark ([session-intel-overlap-bench.md](session-intel-overlap-bench.md)) comes from what happened on the forge, not from what any detector said. This module fetches a repository's pull requests and reads its commits, then emits **candidates** for adjudication. A candidate is never a label: a person (or a blind judge, checked by a person) decides each one.

Code: `src/session-intel/forge/`. Tests: `src/session-intel/__tests__/forge-signals.test.ts`.

## Fetch

```
bun run src/session-intel/forge/cli.ts fetch --repo <owner/name> --since <iso> --until <iso> --out <file.json>
```

`fetch.ts` lists PRs through the operator's own `gh` login in weekly slices, so no slice reaches the search API's 1,000-result cap (a slice that does is an error, not a silent truncation). It adds every issue comment and the files of each PR. No token is read or stored here. `--out` must be outside this repository, because the output holds real identifiers and PR text.

`git.ts` reads commits from a local bare mirror with `git log --all --no-renames --name-status`. `--no-renames` keeps a blobless mirror from fetching blobs. The parser is pure and `readCommits` is the only I/O.

## Signals

```
bun run src/session-intel/forge/cli.ts mine --repo <owner/name> --git <mirror.git> --prs <file.json> --since <iso> --until <iso> --out <candidates.jsonl>
```

Each signal in `signals.ts` is pure and deterministic and reads outcomes only.

| Signal | What it finds | Typical false candidate |
|---|---|---|
| `migration_number_collision` | Two different migration files claiming one number in one migrations directory | A landing race between unrelated work |
| `cross_identity_revert` | A revert of another identity's commit; `repeated` when two reverts hit one commit | Merge-train bisect or review-finding reverts, tagged `process` |
| `superseded_pr` | A PR closed unmerged with supersede language, linked to the merged PRs it names | A planned replacement by the same line |
| `collision_note` | A PR body or comment reporting that two lines collided | Rare; the phrase list is narrow on purpose |

Merge-train promotion PRs (`trunk-train/`, `merge-train/` heads) are closed by design and are skipped.

Identities on commits and PR logins name machines and bots, not people. People come from the session directory, through the PR ownership rule in the bench ("Model").

## Phrase list

`COLLISION` was tuned on the AgentWorkforce PR threads from 2026-07 to 2026-11. Generic review language ("contradicts the docstring", "could duplicate work") is deliberately absent: on that corpus it was 80% merge-bot noise. Each phrase reports a collision that already happened ("independently implemented", "landed first", "beat me to it", "another agent was already working").

## From candidates to labels

Candidates are adjudicated into incidents (`duplicate_work`, `superseded_work`, `intent_conflict`) and hard negatives (a landing race, self-iteration, process). Each incident names its two lines (`pr:<n>` or `commit:<sha>`), its harm time and its workstream. The labels are frozen with a sha256, split into dev and held-out **by workstream**, so no workstream informs both. Labels hold real identifiers and stay outside the repository.
