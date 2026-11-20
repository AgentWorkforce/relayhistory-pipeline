# Offline workspace pattern analysis

This is the exploratory step before alert evaluation. It reads existing, verified
`session-intel-snapshot/1` inputs and produces descriptive profiles, evidence-linked
candidates and a review queue. It has no network, model, runtime, route or migration
integration. Running it does not activate any feature.

## Analysis structure

1. **Corpus** (`patterns/corpus.ts`): check expiry, an identical observation window,
   one tenant/workspace, budgets and duplicate identity. Equal duplicates count once;
   conflicting duplicates fail. Each input manifest digest is retained.
2. **Profile** (`patterns/analyse.ts`): count evidence coverage by harness/repository,
   edit activity by UTC day, heavily shared files and overlapping session spans.
   Missing edits, outcomes and claims are visible as missing evidence.
3. **Discover** (`patterns/analyse.ts`): index exact paths inside a repository (or
   project when repository is absent), then compare nearby edit facts. Keep bounded
   event references, observed parent links and branch relations. Rank exploration
   candidates by shared-file count, closest gap and stable id. This is review order,
   not alert priority. Connected activity groups can suggest where to investigate;
   a transitive group does not establish one semantic workstream.
4. **Review** (`patterns/report.ts`): emit unreviewed cases and questions about
   intent, planned coordination, consequences, alternative explanations and useful
   warning time. Claims/summary text is never copied. Review source evidence privately.
5. **Evaluate** (existing `replay/`): build an independently sampled, labelled frame,
   freeze its dev/held-out split, replay as-of facts and score it. The exploratory
   queue is selected using file activity and cannot establish alert precision/recall.

New descriptive analysers should consume the verified corpus and return versioned,
evidence-linked observations in the typed report. Keep observations, human labels,
detector output and delivery outcomes distinct. Changes to deployed detectors,
models or live feature gates are separate work.

## Run

```sh
bun run src/session-intel/patterns/cli.ts \
  --snapshot /private-study/snapshot \
  --out /private-study/patterns-new
```

Multiple `--snapshot` inputs require `--same-identity-space`. This is an operator's
assertion, **not cryptographic verification** that salts, deduplication or collection
coverage match. Use it only for exports collected with a known shared namespace.
Independently salted exports cannot be joined by assuming their ids are comparable.
The report preserves that assertion and never claims workspace completeness.

Defaults: 30-minute nearby-edit window, 200,000 event comparisons, 100 review cases.
Options: `--nearby-ms` (0–86,400,000), `--max-comparisons` (1–5,000,000),
`--max-candidates` (1–1,000). After loading with the existing reader's byte bounds,
the analysis accepts at most 16 snapshots, 100,000 session rows and 500,000 total
facts (before deduplication). Discovery retains at most 10,000 candidate pairs;
each carries at most three event pairs. Groups use retained edges, show at most
50 groups and 20 member references each. The report distinguishes censored discovery
from omitted display rows. Budget censoring writes a usable report and exits 3;
it must not be described as an exhaustive scan.

The observation window is half-open (`since <= fact time < until`). Paths match
exactly, without guessing case sensitivity or rewriting imported evidence. A write
is an edit intent, not a verified successful filesystem mutation. Session spans
measure first-to-last records, not continuous activity. Equal paths in different
repositories are not compared. Unscoped sessions remain in coverage but cannot
produce file-based pairs. Parent links are observed metadata, not proof that work
was planned; missing links are not proof of independence.
Commit facts may have unknown keys; the profile separately counts keys shaped
like Git SHA abbreviations. Even those keys do not establish success, merge status
or harm without an independent source.

## Private outputs and reproducibility

The new output directory is mode 0700 and its files are 0600. Existing outputs,
including symlinks, are never reused; outputs inside any checkout are refused.
`report.json`, `report.md` and `review-queue.json` are pinned by a final manifest
with hashes, input digests, definition/configuration hash, implementation-source
digest and retention deadline. Candidate ids remain stable when display limits
change; the manifest records those limits separately.
The earliest input `deleteBy` is inherited; `--delete-by ISO` may only shorten it.
Delete the entire output alongside its source study by that deadline.
A crash can leave a partial directory without the final manifest. Treat that as
an incomplete run; use a fresh output directory rather than reusing its files.

Reports carry identifiers, paths and event references. They stay in private host
state and are shared with the workspace owner, never committed to this public repo.
Private reverse mappings can resolve hashed references for manual investigation;
the analyser does not read them, raw traces or credentials. Frozen studies, labels
and held-out ledgers are not modified. Fixtures and documentation examples are synthetic.
