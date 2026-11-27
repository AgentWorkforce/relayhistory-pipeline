# Relayfacts as living feature records

Proposal, 2026-11-25. Requested by Khaliq Gant. Design and an offline prototype only: nothing in this
document is applied, deployed or turned on. Activation steps are listed at the end and each needs
Khaliq's own OK.

## The idea

A relayfact is a record of one **feature**, meaning one body of work. It is not a record of one
decision. It records:

1. **What the human asked for and wanted**, in their own words, with a pointer to where they said it.
2. **What the agents actually did**: changes, commits, pull requests and flags, taken from evidence
   rather than from an agent's own claims.
3. **Asked against built**: where the work met the request, where it diverged, and what is still open.
4. **The current state** of the feature, derived from evidence.

Every newly analysed session can update the record, so it stays current where a spec goes stale. It
is exportable: an agent can pull it before starting work, and a repository can keep it under source
control as `relayfacts/<feature>.md`.

An **onboarding backfill** runs over every past session of a project, builds the records from
scratch, and gives a new team or agent a wiki of the project: what exists, why, who asked for it and
where it stands.

## What exists today, and the gap

The first slice ([relayfacts.md](../relayfacts.md)) stores single decisions. It takes them from
`understanding.decisions` of a session analysis and files them under a topic derived from the
directory of the most-edited file. It stores no human words, and nothing ever changes a record's
status.

The analysis already produces most of what a feature record needs, and the first slice ignores it.
In `SessionSemanticAnalysis` (`src/session-analysis/engine/session-analysis-result.ts`):

| Analysis field | Use in a feature record |
|---|---|
| `outcomes[].intent` | What was asked, citing root `human-message` references |
| `outcomes[].changes` | What was done, as claimed by the analysis |
| `outcomes[].status` (`not-started` … `verified`, `superseded`) | One input to the derived state |
| `outcomes[].verification` | Whether the outcome was tested |
| `outcomes[].remaining` | What is still open |
| `outcomes[].contradictions` | Where the work diverged from the request |
| `outcomes[].supersededBy` | Replacement within the session |
| `intents`, `constraints`, `decisions`, `risks`, `blockers` | The record's rationale sections |
| `evidence.references[]` (`kind`, `at`, `summary`, `detail`) | Citations, and the human's words |
| `metrics.filesChanged` | One of the signals for linking sessions to a feature |

## Policy changes (approved by Khaliq, 2026-11-25)

- **Human words are stored.** A record may quote what a person said, as bounded, scrubbed excerpts
  taken only from `human-message` references of the cited session: at most 300 characters per quote
  and at most 20 quotes per feature (3 per asked item). A quote the scrubber would change is dropped, as decisions are
  today. The existing per-session and per-user erase must remove quotes with their evidence; no
  quote outlives its session's erase.
- **The backfill has an upfront model cost.** It runs one analysis per qualifying session under its
  own budget, separate from the claims scheduler's caps.
- **"Built" and "state" are anchored in evidence.** That means commits, merged or closed pull
  requests, reverts and flag defaults in the repository, not the analysis's summary alone. An
  analysis claim that no evidence backs stays a claim and is labelled as one.

## The record

```
Feature
  id            stable: hash of the founding evidence (first task brief, branch, or PR)
  title         short name
  repos         repositories the work touched
  state         planned | in_progress | shipped | shipped_off | partial | abandoned | superseded | regressed
  stateReason   one line, with the evidence that decided it
  asked[]       { quote, who (role, never a name the session did not say), at, session, ref }
  built[]       { what, kind: commit|pr|file|flag|migration|claim, ref, at, session }
  gaps[]        { asked, built, kind: unmet|diverged|unverified|open, evidence[] }
  decisions[]   the decision records of the first slice, linked to this feature
  constraints[]
  timeline[]    per contributing session: when, who, what it changed in this record
  sessions[]    { source, sessionId, role: founding|contributing|review }
  updatedAt, evidenceCount
```

A record is **event-sourced**. Each contributing session adds evidence events. Title, state and
gaps are recomputed from all events, and no event is ever edited. A later session can supersede an
earlier record explicitly, through a `supersedes` link backed by evidence; that is the follow-up the
first slice left open.

## Derivation

Five stages. Stages 1, 3 and 5 are deterministic. Stages 2 and 4 may use a model, but every output
must cite evidence ids that the scorer checks.

1. **Session facts.** From each analysis snapshot: asked items (outcome intents, with quotes from
   their cited human-message references), done items (changes), gaps (`contradictions`,
   `remaining`, `verification` of `not-observed` or `failed`), and the session's linking keys
   (repo, branch, PR and commit references, files, the relay task brief and its message id).
2. **Feature linking.** Group session facts into features. The strongest signals come first:
   - one relay task brief (a lead-to-worker delegation names one task);
   - the same PR or branch;
   - commits that reference each other;
   - overlapping file sets in the same repository within a time window;
   - intent similarity, and only as a tiebreaker: a session no key linked attaches to the group active
     within three days in the same repository whose asks, title and claimed changes are most alike
     (idf-weighted cosine of content words, at least 0.15; both must have the same known repository),
     with words that only say what a delegated
     session does ("review", "report", "findings", "commit") and commit ids left out. On the first
     corpus's dev split it attached 21 labelled singletons, 20 to the right feature;
   - a small delegation family: a parent session and the sessions it spawned (the directory's
     `parent_session_ids`), while the family has at most 12 members.

   Each link records the signal that made it. Planned fleets are one feature with several sessions,
   not several features. Measured on the first corpus's dev split, these rules decide linking quality:
   - A lead's large family mixes features (223 of 1,281 sibling pairs in the two families over 30
     shared a gold feature), while every sibling pair in a family of at most 12 did.
   - A session naming more than four feature branches or four commits is a coordinator (a lander, a
     release) and links on none of them. Its keys bridged unrelated features into one 38-session record.
   - A commit already on the default branch when the session started is its base, not its work, and
     never links. A commit a session only mentions, such as a reviewer's "review <sha>", resolves to the
     forge branch that holds it, when at most two branches do.
   - Review rounds of one branch (`-r2`, `-v3`) share a key. A branch name cut off mid-word never links.
   - A Codex approval assessor (a session whose first human message, by time, opens "The following is the
     Codex agent history", the harness handing it another session's pending action) reviews work and
     never does it. It joins a feature through its keys but never founds one, a group of assessors alone
     is no feature, and its own asks, claims, gaps and outcomes are left out. On the second `cloud`
     corpus, 864 of 1,824 sessions were assessors, and 72 features were made of nothing else.
3. **State.** Rules over the linked evidence, latest evidence first:
   - a revert after a merge gives `regressed` (a linked replacement giving `superseded` is not
     implemented; only analyses whose every outcome is superseded give it today);
   - merged, with the flag default off, gives `shipped_off`; merged and on gives `shipped`. A flag the
     docs do not list defaults off (this repository's rule for new behaviour);
   - an `unmet` ask in the latest session after a merge gives `partial`. Open `remaining` items do
     not: almost every analysis lists follow-ups;
   - commits but no merge give `in_progress`;
   - no activity for 21 days with no merge gives `abandoned`;
   - asked but nothing built gives `planned`.

   The rule that fired is the `stateReason`. A branch, PR or commit counts as built only when the
   session made it (a push, a commit line, its own `gh pr create`); one it only listed or viewed is
   a link key. A PR is created only by the successful result paired with a shell tool call whose
   structured command is nothing but `gh pr create` (after optional `cd` and `git push` steps); a
   heredoc, pipe, `||`, `;`, substitution, quoted text, comment, non-shell tool or truncated argument
   abstains. The analysis's references keep only string arguments, so a Codex argv call abstains there;
   the transcript reader takes the line of a `bash -lc` argv. A commit is on the default branch from the time it became reachable
   from it: a merged commit from its merge, a re-cut commit from its patch-id landing. A first-parent
   commit (a squash, a direct push, a fast-forward) has only its committer time, since the mirror holds
   no push times; that approximation is recorded as `defaultAtBasis: committer`. A re-cut commit keeps
   its own commit time; only `defaultAt` moves to the landing. A branch landed when its tip first became
   reachable from the default branch (the merge that brought it in), or when its patch's first
   default-branch copy did, never at its tip's own time. A revert takes effect when the reverting commit
   reached the default branch, so a revert written on a side branch and merged later is dated by the merge.
   Sealing the forge again after a later landing leaves every earlier cut's state unchanged (tested with
   real git histories). A branch whose tip predates the feature's first session is not its landing.

   A feature's pull requests are the ones a session created, any whose head branch is one of the
   feature's branches and that opened no earlier than seven days before its first session (GitHub
   deletes a head branch on merge, so the live branches rarely hold it: 99 of 2,267 merged `cloud` PRs
   kept theirs), and any that hold a commit a session made or pushed. A PR only mentioned never counts.
   A merged PR landed when its merge commit reached the default branch (`landedAt`), not at GitHub's
   `mergedAt`: a repository that merges PRs into a trunk and promotes the trunk with a merge train
   (`cloud`) lands a PR at the train's merge. A PR merged into its base and not yet on the default branch
   is `in_progress`, however quiet. A forge without `landedAt` reads `mergedAt`. A PR's commits are
   read from the final mirror, so a commit pushed to an open PR after the cut can link that PR at the
   cut; the PR's landing and closing are still cut strictly.

   **As-of.** Session facts are cut strictly at `asOf`: a timed item, and every evidence pointer and
   quote inside it, counts only before `asOf`; anything without a time of its own (a mention-only key,
   branch, file and task keys, outcome statuses) counts only once its session ended before `asOf`.
   Only the directory's own facts (repository, parent sessions) are known from the session's start. A
   replay at T is invariant to everything observed after T (tested). Forge facts are used only with a
   time before T: a branch counts only when its tip predates T, and a commit is on the default branch only
   from the time it reached it. The limits: a branch ref's creation time is unknown (a ref created after T
   at an older tip is indistinguishable); a first-parent commit's landing time is its committer time (a
   commit made before T and pushed after T reads as landed before T); and flag defaults are the docs as
   sealed. Times are canonical UTC at millisecond precision: a finer evidence time (PostgreSQL writes
   microseconds) is floored, and a cut must itself be a whole millisecond (a finer `--as-of` or gold
   `asOf` is refused), which makes every strict-before comparison exact. An impossible calendar date
   anywhere (evidence, forge, a run's records or its delete-by date) is invalid, never rolled over;
   `score` and `packets` refuse such a run before writing anything.
4. **Asked against built.** Each asked item is matched to the built items that answer it, or marked
   `unmet`. Contradictions and unverified outcomes carry over as gaps.
5. **Render and export.** Markdown per feature plus an index page, escaped exactly like the
   first-slice markdown, labelled as evidence rather than instructions. Served through the read API
   and MCP, and pushed to repositories with the existing GitHub push module
   (`src/session-analysis/relayfacts-github-push.ts`).

## The onboarding backfill

- **Frame:** every session of the workspace, from the session directory.
- **Qualifying sessions:** a coding harness, at least one root human message, and a repository or
  project.
- **Analysis (target):** request one through `POST /v1/sessions/:sessionId/analysis`, or reuse a
  current one through `GET`, resumable and budgeted per day.
- **Analysis (the benchmark prototype, `src/relayfacts/backfill/`):** the read API does not yet serve
  these analyses, so the prototype reads what the read API reads, with its credentials, and runs the
  pipeline's own `processSessionAnalysis` locally with the pinned versions. Khaliq approved this on
  2026-11-25, read-only and for the benchmark only: the read API role (`bulls96_read_api`, tenant-bound
  `READ ONLY` transactions through its `SECURITY DEFINER` readers, TLS verified against the pinned
  trust), the transcript store's read key, and a model key for local analyses. His words: "OK to use
  bulls96_read_api and the PIPELINE_RAW_S3 transcript-reader key read-only for the relayfacts
  benchmark, and the OpenAI key … for local analyses." Credentials come from the environment and are
  deleted from it on read; nothing is written back. It is resumable and bounded by `--max`, not by a
  daily budget.
  - **Data transfer.** "Local" means orchestrated on the operator's machine: `analyse` sends each
    session's bounded evidence to the model provider (the OpenAI Responses API), exactly as the hosted
    analysis does. No other command calls a model. Khaliq confirmed on 2026-11-26 that his approval
    covers that transfer for this benchmark ("confirmed", in reply to the question whether it does).
    Running it for another workspace needs its own approval covering that transfer.
  - **Scope before collection.** The first collecting command needs an empty output directory and
    creates its scope exclusively (tenant, repositories, window, a delete-by date at most 30 days out;
    `scope.json`); every later command on it, and `seal`, must name exactly the same scope, and nothing
    runs past the delete-by date. Every row a command writes carries the scope's id (the sha256 of
    `scope.json`), and `seal` admits no other row and no row under a name not derived from its own key.
    Session lists come only from the same collection's own `locate.jsonl` (the repository attribution
    this tool derived from canonical evidence), cut to the scope's repositories and to what the
    directory itself lists for the tenant inside the window, with the directory's own time; the rest
    are counted by reason. Without `--from-locate`, `analyse` selects only sessions whose directory git
    remote is exactly a scoped owner/name; a session with no remote is selected only through the
    collection's own attribution. Artifacts rows name their session and must match the row they join.
    A log's torn final fragment is truncated away by the next append; a malformed complete line is
    refused as corruption. Times are compared as instants (canonical UTC; git's offsets are converted),
    and only a zoned ISO 8601 instant with real calendar fields is a time: a bad cut, window or forge
    time is refused before anything is built or written. Repository names come from one exact parser
    (`features/repo.ts`): the host must be exactly github.com, so a mirror whose path merely contains
    `github.com` abstains.
    Built records carry the corpus's delete-by date, and `score` and `packets` refuse them after it.
  - **Private output.** Every output path is refused inside the repository before anything is created.
    Directories the tools create are 0700; an existing directory is never re-permissioned and must
    already be the user's own and closed to group and others. Files are written atomically (a fresh
    0600 temporary renamed into place), under opaque names (no harness or session id in a file name);
    every read, of outputs and of inputs (`--features`, `--gold`, `--forge`, the corpus), opens without
    following links and checks the opened file: regular, the user's own, closed to others, one link.
    Logs and error output hold codes from a closed set (canonical refusal reasons, known error classes,
    errno names, PostgreSQL SQLSTATE classes); only the tools' own refusals print a message, and those
    never quote a value from a file. On a volume mounted with ownership
    ignored, modes are stored but do not keep other local users out: keep such output on a single-user
    machine.
- **Build:** run stages 1 to 5 over the results, oldest first, so the timeline reads in order.
- **Afterwards:** each newly analysed session goes through the same stages incrementally.

## The benchmark

The benchmark decides whether records are good enough to turn on, and it is run per repository. It
follows the rules of the replay harness ([session-intel-labelling.md](../session-intel-labelling.md)):
labels come before outputs, labellers never see generator output, inputs are frozen by sha256, and
the held-out split is scored once.

### Corpus

A corpus is one repository over a window:
- the frozen analysis snapshots of its qualifying sessions;
- the repository's git history (commits, merges, reverts) and its pull requests;
- its documentation at the window's end.

Like replay snapshots, a corpus is kept outside this repository with a delete-by date. Only
synthetic fixtures are committed.

Planned repositories:

| Repository | Why |
|---|---|
| `relayhistory-pipeline` | Dense design docs per feature: the docs are a strong ground truth for features and their state |
| `cloud` | High volume, several people, a merge train: tests linking and state at scale |
| `relay` | Public, two founders and outside contributors: tests attribution and overlap |
| One repository with thin docs | Tests whether records add value where no docs exist |

### Gold labels

For each corpus, a labeller agent that never sees the generator's output produces the labels from
the repository and the transcripts:

- **Features:** from design docs, merged-PR clusters and the changelog. Each has a title, its
  founding evidence and its sessions.
- **State at the window's end**, with the evidence: merged, flag default, reverted, open.
- **Asked items:** what the human asked for, with the session and message.
- **Gaps:** asked against built, labelled `unmet`, `diverged`, `unverified` or `open`.

Two adjudicator agents review a sample blind, and their agreement is recorded. Labels are frozen
with a sha256 before the first generator run, and corrections go in a log.

### Metrics and proposed bars

The bars are proposals, to be calibrated on the dev split and then frozen.

| Metric | Definition | Bar |
|---|---|---|
| Grounding | Every asked, built and gap item cites evidence that exists in the corpus. Every quote is a verbatim substring of a human message in the cited session. Checked deterministically | 100% |
| Feature recall | Gold features matched by a generated record | ≥ 0.80 |
| Feature precision | Generated records that match a gold feature, or that adjudicators judge real | ≥ 0.90 |
| Linking | B-cubed F1 of the session-to-feature assignment against gold | ≥ 0.80 |
| State accuracy | Generated state equals gold state at the window's end | ≥ 0.85 |
| Gap recall | Gold gaps found | ≥ 0.70 |
| Gap precision | Generated gaps that are real (gold or adjudicated) | ≥ 0.85 |
| Freshness | Replay as of time T. Records built only from evidence before T match the gold state at T, and no record uses later evidence | 100% no leakage; state accuracy as above |
| Duplication | Gold features that are split across two or more records | ≤ 0.05 |
| Signal | Adjudicators answer "would an agent starting work on this feature be better off with this record?" on a 1 to 5 scale | median ≥ 4 |
| Cost | Model tokens per qualifying session and per feature, and wall time | reported, no bar |

A failed grounding check fails the whole run: an ungrounded record is worse than no record.

### Splits

Within a repository, whole features are assigned 60/40 to dev and held-out by a seeded shuffle,
recorded before any scoring. Across repositories, at least one repository is held out completely.
The generator is tuned on the dev features of the other repositories, frozen by hash, and run once
on the held-out repository.

Verdicts are per split and never pooled: the held-out verdict is the quality decision, the dev verdict
is tuning evidence only, and each needs at least ten labelled features of its own (else
`insufficient`).

Adjudicated label corrections (a blind adjudicator who reads transcripts and git, not generator code)
may change dev labels only, each recorded with its evidence and a new sha256; held-out labels are never
edited after they are frozen.

## Code layout

- `src/relayfacts/features/`: the pure core, with no I/O, clock or model. Types, stage 1
  extraction, stage 3 state rules, stage 5 rendering, and the linking and gap interfaces with
  deterministic implementations.
- `src/relayfacts/bench/`: the corpus format and loader, the gold-label format, the scorer (every
  metric above), and a CLI to run and score.
- `src/relayfacts/backfill/`: the read-only, resumable backfill: `survey`, `locate` (repository
  attribution), `artifacts` (build evidence from the transcript), `analyse`, `parents`, `seal`, and
  `forge.ts` (forge facts from a git mirror and `gh`).
- `src/relayfacts/out-path.ts`: the guard every tool's output goes through.

Tests use synthetic corpora only.

## Activation (each step needs Khaliq's own OK)

1. Backfill this workspace's analyses for the benchmark repositories, under a stated daily budget.
2. Publish the benchmark result per repository against the bars.
3. Add a migration for feature records, quotes and their erase, and wire the incremental update
   into the analysis tick behind a default-off flag.
4. Serve and export records, and push them to repositories that opt in.

## Open questions

- Where feature records live once a repository has them in source control: is the repository copy
  read-only output, or can a person edit it and have the edit flow back as evidence?
- Whether `decisions` stay a separate document type or only appear inside features.
- How a feature that spans repositories is filed in each repository's export.
