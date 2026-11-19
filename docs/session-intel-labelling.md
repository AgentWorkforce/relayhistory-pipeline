# Session intelligence: labelling protocol (M0-3)

Owner: sessions-intel-lead. This is how ground truth is built for the replay harness
([session-intel-replay.md](session-intel-replay.md)). It is the method in
[coordination-overlap-detection.md](coordination-overlap-detection.md), extended to the classes in
[the spec](proposals/session-intelligence.md).

Reading real session data needs Khaliq's OK in his own words, for a stated scope, each time. A
customer's data also needs the customer's written authorization. Nothing in this document grants
either.

## Rules

1. **Labels come before detectors.** The labeller never sees detector output, for this cohort or any
   earlier one. The labels are frozen, with a sha256, before the first replay. After that, every
   change goes in a correction log with its reason.
2. **Roles are separate.** One agent labels. Two different agents adjudicate later, blind to each
   other and to the detector names.
3. **Labels record identifiers only.** They hold the snapshot's hashed session ids, timestamps, and
   hashed repos and paths, never transcript text. The import mapping is used only to open a
   transcript while labelling.
4. **Pairs are unordered.**

## Sources of truth

Every source is an outcome. None comes from a detector.

| Kind | Evidence | `harmAt` |
|---|---|---|
| `duplicate_work` | Two different session roots wrote the same migration number or filename in one repo, confirmed in git: both committed or both proposed | The second write, or the second PR opening |
| `superseded_work` | Root B reverted or replaced commit X by root A: a `git revert`, or a PR closing A's work as superseded | The revert or supersede time |
| `intent_conflict` | A human correction contradicting a decision made in a *different* session on the same topic | The correction time |
| `constraint_violation` | A human stop, or a "not allowed", that cites a rule stated in another session or in repository policy | The stop time |

Hard negatives:

| Reason | How they are chosen |
|---|---|
| `planned_fleet` | Every pair sharing a parent session |
| `benign_overlap` | A sample of pairs on the same files or topic in the same window, with no harm outcome and no correction |
| `unrelated` | A random sample of pairs in the same project, at least twice the incident count |

## Split and scoring

1. **Split by workstream** (a repo plus a topic or branch family). Assign whole workstreams 60/40 to
   dev and held-out with a seeded shuffle. Record the split before any scoring.
2. **Score dev.** Change only documented parameters, then freeze the detector hash.
3. **Score held-out once.** The ledger enforces this.
4. **Adjudicate blind.** Take the top K alerts at a budget of 5 per 100 sessions, shuffled and
   stripped of detector names. Two adjudicators judge each one, and the verdicts are imported with
   their agreement.
5. **Report against the bars.** Each class × urgency row is pass, fail or insufficient against the
   spec's bars (per-pair precision, at least 20 adjudicated alerts, lead time and budget).

## Limits

State these up front in every report:

- the cohort, for example which machine, which people's agents, the window and the harnesses;
- coverage, for example Codex code-mode edits only after the importer fix;
- that conflict rows cannot be scored when the cohort has no claims, so they are reported as
  insufficient by construction.
