# Session Intelligence overlap benchmark

A benchmark for **overlap and rework**: two lines of work, usually by different people's agents, converging on one change without knowing it. It measures, on real workspace data, how early and how precisely a detector could have warned. Its inputs are a cloud harvest ([session-intel-cloud-harvest.md](session-intel-cloud-harvest.md)) and forge outcomes ([session-intel-forge-outcomes.md](session-intel-forge-outcomes.md)). Its baseline is the shipped `outcome-facts` detector ([coordination-overlap-detection.md](coordination-overlap-detection.md)).

Code: `src/session-intel/overlap-bench/`. Tests: `src/session-intel/__tests__/overlap-bench.test.ts` and `overlap-eval.test.ts`.

All harvested data, labels, verdicts and bundles stay outside this repository. The code and these docs carry only aggregate numbers.

## Model

`model.ts` builds sessions from the harvest and groups them into **work roots**, following same-source parents, so a lead and its subagents are one line of work. Each session carries:

- time-ordered edits, each reduced to a repository and a repo-relative path;
- PR references, each a `create` or a `mention`;
- the branches and shas it named;
- the relay messages it received.

Events stamped before 2025 or with unparseable times are dropped.

**PR ownership.** A root owns a PR only when it created it or created or pushed its head branch, and only from the time that became known (`prOwnership`). A `gh pr create` call usually has no number. Only the call's own result may claim one: the very next raw event, by the harvest's `seq` (which counts events that were not kept), a tool result within 10 minutes, not failed, printing a full PR URL. Facts carry no tool-use id. So with parallel calls (the next raw event is another call), or in facts harvested before `seq` existed, the claim abstains. The Cloud harvest used here predates `seq`, so in it PR ownership comes from branches only. The PR is then owned from that result's time. Any other event ends the wait, so a later, unrelated URL is never claimed. An unresolved call stays an act of opening a PR that owns nothing. A root that only names a PR (a view, a review, a merge, a merge train listing it) does not own it. Otherwise one reviewer touching two PRs would make both look like one line of work. Detectors and as-of bundles ask who owned a PR as of the alert (`ownersAt`). Only scoring and hindsight adjudication use every owner a PR ever had.

**Observed paths.** `git status` and `--stat` output list what is dirty or changed, not what this session wrote. `ObservedPolicy` sets how they count:

- `all` counts them as edits (the default);
- `fresh` keeps only status paths the session's first status did not already list;
- `none` drops them.

On the dev split (provisional, see "Findings so far"), observed paths are 84% of recovered edit events. Dropping them does not change the ceiling: the combined ranker reaches 6 dev incidents with no budget under every policy. At the headline budget, `fresh` moves the ranker from 5 hits and 2 negatives to 4 hits and 0 negatives. With samples this small that is not a clear win, so the default stays `all`.

**Relay suppression.** A pair that communicated over Agent Relay before an alert is coordinated, not overlapping. `communicated` keys this by work root, so a lead messaging a worker covers both roots.

## Paths

`paths.ts` reduces every written path to `(repo, repo-relative path)` across checkouts, worktrees (`.claude/worktrees`, `.codex/worktrees`, `worktrees/` and others) and already-relative paths. It drops paths outside a repository, such as scratch directories, dotfiles and other repositories. `HOT_PATH` marks lockfiles, migration journals, snapshots and changelogs, which every line touches.

## Detectors

A **side** is a work root (`root:<session key>`) or a pull request (`pr:<owner/repo>#<n>`). Every detector is a pure function of the model, the forge state and its params. It never fires earlier than its evidence.

| Detector | Fires when |
|---|---|
| D1 `open-pr` | A root edits `minShared` files that another line's open PR had worked on by then |
| D2 `shared-trigger` | Two roots react to the same PR they do not own, opening a PR or editing within `windowMin` of naming it |
| D3 `concurrent` | Two roots edit the same uncommon paths in one repository within `windowH` hours. A path that more than `maxRootsPerPath` roots have edited is treated as hot. |
| `outcome-facts` | The shipped detector, run unchanged over the same evidence mapped onto its `LaneFacts` (`baseline.ts`) |

**Dated evidence only.** Every detector reads only what is dated before its alert. A directory row's PR field is known only as fetched; scoring may link through it, but detectors never read it. For D1 this means a PR's final file list is never used, because it is known only in hindsight. What a PR is working on at time t is the set of paths its owners (as of t) had edited before t, and `maxPrFiles` applies to that set. A PR with no owning root gives D1 no evidence.

D3's score sums each shared path's rarity, which is 1 over the number of roots that had edited the path by then. Lines meeting on files few others touch outrank lines meeting on a common file.

## Combined ranker

`ranker.ts` (D4) merges D1, D2 and D3 evidence per pair of lines and sums the weights in time order. A pair alerts when its running total first reaches `threshold`, with that total as its score, so only evidence seen by then counts and lead time stays honest. A D1 alert pairs a root with a PR; when the PR has an owning root, the alert is also counted against that root. The component detectors run loose and the ranker does the selecting. Its dev grid has 243 points.

## Scoring

Incidents are pairs of forge lines (a PR or a commit). A line links to its PR node and to every work root that created, named or pushed it. An alert **hits** an incident when its two sides link to the two lines and it fires before the harm. An alert on an adjudicated negative (a landing race, self-iteration, process) counts as a **negative hit**.

Alerts are first cut to a **weekly budget** of `perHundred` alerts per 100 coding sessions active that week, highest score first. A global budget would let one noisy week spend everyone else's alerts.

## Eval

`eval.ts` reports, for each detector point:

- **Recall curve.** Hits, negative hits, alerts and median lead time at budgets of 1, 5 (the headline budget), 20 and unlimited per 100 sessions.
- **Precision at the headline budget.** This comes from blind verdicts on the alerts themselves, with a 95% Wilson interval. The incident labels cover only overlaps the forge shows, so they cannot measure precision. It is conditional: the rate among the judged alerts, with unclear verdicts excluded. The sample is drawn from each detector's top-ranked alerts, so it estimates precision at the top of the ranking, not across every alert the budget admits. `unjudged` reports how many were not judged.
- **Ceiling.** Each incident is classed as `no_session` (no session evidence for either line), `one_side` or `both_sides`. A miss in the first two classes is the data's fault, not the detector's.

```
bun run src/session-intel/overlap-bench/cli.ts climb   --harvest <dir>[,<dir>] --forge <dir> --labels <labels.json> --out <dir>
bun run src/session-intel/overlap-bench/cli.ts eval    ... --frozen <frozen.json> [--verdicts <verdicts.json>]
bun run src/session-intel/overlap-bench/cli.ts sample  ... --frozen <frozen.json> [--verdicts <verdicts.json>] [--per-detector 15] [--seed v1] [--owner-labels <file>]
bun run src/session-intel/overlap-bench/cli.ts heldout ... --frozen <frozen.json>
```

## Precision

`sample` writes an adjudication bundle. It takes the top alerts of each frozen point at the headline budget, merges them by pair, skips pairs already judged, and shuffles them by a seeded hash. Detector names are withheld so a verdict cannot lean on them.

`bundle.ts` describes each pair from derived facts and forge metadata only: the PRs each side created, their titles, files and outcomes, branches, and edited files with times. Owners are shown as labels from a private file; anyone else is `member`. This hindsight form is for adjudicating what really happened.

With `asOf`, the bundle carries dated facts only, as a live system would have known them when the alert fired:

- the PRs that existed and that the side owned by then, with their author and creation time;
- comments made before the alert (each comment is dated);
- the side's own branches, edits and activity up to then, counting only the sessions with dated activity by then.

A PR's title and description appear only from dated history (forge `history`), as the version in effect at the alert. As fetched they would leak a later edit, so without history they are left out. A version whose edit record was deleted or empty is an unknown interval, and nothing is shown for it. Comment text is left out, because comments are fetched in their latest form and dated only by creation. File lists, merges and closes are always left out.

The judge's input is therefore more than PR text. It includes session-derived facts (branches named, files edited, activity span) and owner labels.

**One time form.** The model compares times as strings, so every time it holds takes one fixed width at full precision (`canonicalTs`): event facts (older harvests stored milliseconds and are widened when loaded), directory rows, forge PR times and their title and description histories, label harm times, and the alert time a bundle is described at. Mixed widths would misorder instants within a second (`…12:00:00Z` sorts after `…12:00:00.5Z`), and normalizing through `Date` would drop sub-millisecond order.

**Prefix invariance.** A test runs every detector (the shipped baseline included), the ranker and as-of bundles over a synthetic world and over the same world cut at several times T. Everything said about T must be identical in both. It fails if bundles count later sessions, or if D1 uses ownership established after its alert.

**Harvest scopes.** `loadContext` reads each harvest's manifest. Merged harvests must be of one workspace, and none may be past its delete-by. Every input (harvest, forge, labels, verdicts, owner labels, frozen file) and every output must pass the same private-destination checks as the harvest. Scope times are validated as canonical UTC instants with real calendar dates (`cloud-harvest/instant.ts`, the one parser for every admitted time; February 30 or 24:00 are refused, not rolled over) and compared as instants, failing closed. JSON-lines inputs are read strictly: a malformed line or a partial last record is refused, never skipped.

## Protocol

1. Climb on the dev split only. Labels are split by workstream, 44 dev incidents and 20 held-out.
2. Freeze the best point per detector (`overlap-bench-frozen/3`). The freeze records each point's hash (detector, params and sources) together with:
   - the sources hash;
   - the labels digest;
   - a digest of every input file (each harvest's manifest, directory and facts, and the forge PR and history files);
   - the alert budget;
   - the split scored (`heldout`);
   - the observed-path policy.
   The sources hash covers every local module the bench CLI reaches through its imports, transitively (`sourceFiles`, derived from the import graph): the bench, the shared harvest, forge and parser code, and the shipped detector the baseline runs (`packages/coordination-detect`). The freeze carries a hash of all of its own terms and points (`freezeHash`), so an edited freeze, such as a changed budget, is refused.
3. Score held-out once. `heldout` recomputes every term and every point hash and refuses any mismatch. It uses the frozen budget (a different `--per-hundred` is refused). The once-only ledgers (`heldout-stage1-*`, `heldout-stage2-*`) sit beside the labels, keyed by the labels digest, and each is claimed with an exclusive create before scoring. Neither a new output directory nor a new freeze can score the same held-out labels again.
4. Precision samples use fresh seeds, and a pair is never judged twice.
5. Stage 2 is frozen with stage 1:
   - `stage2-batch` binds a batch to the verified frozen file, the frozen ranker point (re-hashed), the prompt (by sha256), the cutoff, the seed, the exclusions and the judge (`--judge`, recorded);
   - it draws positives from the split's incidents and fresh negatives that no earlier round judged (`--exclude`, `--verdicts`), within the frozen budget;
   - judge-readable material (parts and rendered prompts) goes to `<out>/judge`, and the key and manifest go to `<out>/key`, which no prompt names;
   - the manifest records every part, prompt and the key by hash, and itself by hash.

   `stage2-score` verifies all of that, and requires exactly one valid verdict per pair: a known verdict, and a probability in [0, 1]. A partial, duplicated, foreign or out-of-range answer gets no score.
6. Held-out is evaluated once per labels identity, the sha256 of the labels' bytes (not their name or path), in a per-user registry (`once.ts`) that no command-line path chooses. Copying, renaming or moving labels, freezes or inputs, or choosing a new output directory, opens no second evaluation.
   - Every transition (claim, pin a batch, finish) holds an exclusive lock on the record, across processes.
   - A claim names its owner process. An active claim is never shared. A claim whose owner died is interrupted, and may be resumed under identical terms.
   - A stage-2 claim pins the batch made under it, and only that batch can be scored.
   - Receipts are written before the record is finished, so an exit in between only repeats the same deterministic run.
   - A stage-2 score pins the digest of its validated verdicts and its result in the record before any receipt is published. A resumed scoring, from any directory, must present the same verdicts, and republishes the pinned result.
   - The labels v2 held-out was spent before this identity existed, and is recorded as done under both its earlier and its byte identity.
7. A stage-2 batch carries the earliest delete-by of the data it was drawn from, compared as instants. `stage2-score` first checks that the batch directory and its `key/` and `judge/` subdirectories are private, owned, real directories outside the repository and that the batch is within its delete-by, and only then opens any part, prompt or verdict.

## Findings so far (dev split)

These come from a Jul 28 → Nov 5 harvest of one workspace: about 5.2M events, with no read errors. All of it is dev split only; nothing here is a held-out result.

**Status: provisional, pending a rerun.** The figures below were produced by revision 144b4ab2, before the review round that added the window, path and dated-evidence rules above. That revision had these known problems:
- D1 used final PR file lists;
- ownership was taken from full history;
- as-of bundles carried PR titles, descriptions and file lists as fetched;
- harvested events outside the window, and some non-path strings, were kept.

The D1, ranker and stage-2 numbers are withdrawn until they are rerun. The other figures are kept as orientation only. Use of the harvested outputs is suspended while their disposition is decided, so the rerun waits on that decision.

**Ceiling.** Of the 44 dev incidents, 19 have no session evidence on either side and 17 have it on one side only. Only 8 have both sides. Most misses are coverage, not detection.

**Recall (orientation only).** Hits out of 23 linkable incidents, with negative hits in brackets. D1 and the ranker are withdrawn, as above.

| Detector | per 100 = 1 | 5 | 20 | unlimited |
|---|---|---|---|---|
| `outcome-facts` (shipped) | 0 | 0 | 0 | 0 |
| D2 `shared-trigger` | 0 (1) | 3 (2) | 4 (6) | 4 (6) |
| D3 `concurrent` | 0 (1) | 1 (1) | 2 (1) | 5 (4) |

**Precision (orientation only).** In 73 blind-judged top alerts, judged in hindsight, 1 was an overlap, 3 were unclear and 69 were planned related work: stacked and follow-up PRs, handoffs, restarts, companion halves of one feature. Structure finds the right neighbourhood but cannot tell planned work from duplicated work.

**Naming and reference features do not separate them either.** PRs referencing each other, concurrency, a shared relay lead, and title and branch token similarity were each tried. None separated the two classes, and the experiment was not kept.

**An as-of intent judge, rerun strictly: no separation.** A first blind test suggested that a judge reading each pair as of its alert separates duplicated from planned work. Its bundles included PR titles and descriptions as fetched, so that result is withdrawn. The rerun used the same prompt (versioned), strict as-of bundles (dated facts only), the ranker frozen at the current revision, 6 positives (5 dev incident pairs and the hindsight overlap) and 40 fresh top-ranked alerts no judge had seen. The judge called all 46 not overlap. Positives scored p 0.05–0.25, and fresh alerts up to 0.3, so no threshold separates them.

The earlier separation came from undated intent text. Branches, edited files and dated comments do not carry enough intent on their own.

**With dated intent, the judge separates them (dev only).** The test was then rerun with PR titles and descriptions rebuilt from forge history as they stood at each alert (forge `history`). It used prompt v3, which differs from v2 only in describing that input, and 40 new fresh alerts that neither earlier round had judged. Same 6 positives, fresh blind judges, no lookups:

| Cutoff | Positives kept | Fresh top alerts kept |
|---|---|---|
| p ≥ 0.3 | 4 / 6 | 0 / 40 |
| p ≥ 0.5 | 3 / 6 | 0 / 40 |
| p ≥ 0.7 | 2 / 6 | 0 / 40 |

Fresh alerts scored at most 0.25. As on held-out, 0 of 40 is an observed keep rate among top alerts, not a false-alert rate. These bundles still carried comment text as fetched. The positives are dev items that earlier rounds also used, and the prompt was written on dev, so this is a dev result. The claim needs the held-out score.

## Held-out (scored once)

The freeze covered the sources at `cbc76e21`, the purged Cloud harvest, the forge PRs and histories, labels v2, a budget of 5 per 100 sessions, and the held-out split. It was then scored once. Later rounds changed how PR create claims are matched and removed comment text from as-of bundles, so these figures belong to that revision. The held-out set is spent and will not be rerun.

- **Stage 1.** Of the held-out incidents, 5 were linkable: both lines linked to session evidence. The ranker caught 1 of those 5 at the headline budget, with 2 negative hits and a median lead time of 1.5 h. Every other detector, the shipped `outcome-facts` included, caught 0. Recall is conditional on linkability; most held-out incidents were not linkable at all.
- **Stage 2.** Prompt v3 (sha256 `a0127f22…`) ran at cutoff 0.3 on a one-time batch, with comment text (as fetched) still in the bundles. The loose ranker reached only 1 held-out incident pair before its harm, so the batch held that 1 positive and 40 fresh top-ranked alerts no round had judged. The judge kept the positive (p 0.4) and none of the 40 (highest 0.25).

What this supports and what it does not:
- One positive cannot estimate stage 2's recall.
- 0 of 40 is an observed keep rate among top-ranked alerts. It is not a false-alert rate: fresh alerts have no independent truth (in the hindsight round about 1 in 70 top alerts was a real overlap), and its Wilson upper bound of about 8.8% assumes independent draws, which top-ranked alerts from one workspace are not.
- It does show that the judge, untuned on these alerts, passed none of 40 top alerts on.

## Stage 2 (proposed)## Stage 2 (proposed)

Structure gives recall but not precision, and an intent judge without dated intent text gives neither. Stage 2 therefore needs **dated intent**: what each line meant to do, as it stood at the alert. Two sources keep that inside the rules here:

1. **Forge history.** The forge records a PR's title and description edits with timestamps (title-rename timeline events and content-edit history). The title and description as of any time can be rebuilt from them. Fetching that history is forge metadata, read with the operator's own `gh`, and needs no transcript text.
2. **The live pipeline.** A judge running in the pipeline reads each session as of now, so its intent text (the brief, the create call's title) is dated by construction. Offline, this benchmark keeps no transcript text, so carrying such text across the privacy boundary would need its own decision.

Option 1 is built (forge `history`) and shows separation on dev (above). The next step is to freeze stage 1, the prompt and the cutoff together, and score held-out once.

Stage 2 should be the existing conflict model-judge seam ([session-intel-conflicts.md](session-intel-conflicts.md)), not a second judge. That seam provides an injected transport, verdicts that must cite their inputs, and no key read by the module. Enabling it in production needs Khaliq's explicit OK.

## For production

These findings apply to the live overlap path. Each change goes through review separately, and enabling anything needs Khaliq's explicit OK.

**Which edit evidence matters (dev, frozen ranker, provisional).** Production lanes take writes from `files_touched`, which #485 extends with `apply_patch` paths (Codex code-mode included) and Claude MultiEdit. Rerunning the frozen ranker with only some evidence sources:

| Lane evidence | Sessions with edits | Hits at budget 5 | Hits, no budget |
|---|---|---|---|
| `files_touched` | 1,314 | 0 / 21 | 6 / 21 |
| + patch paths (about what production has now) | 1,314 | 0 / 21 | 6 / 21 |
| + shell writes and `git add` | 3,901 | 0 / 21 | 6 / 21 |
| + `git status` and `--stat` listings | 5,591 | 5 / 21 | 6 / 21 |

Recovering shell writes and `git add` widens coverage but adds no recall, so it is not worth porting on this evidence. What lifts the ranker within budget is what sessions see in `git status` and `--stat`. Status alone gives 2 hits; counting only paths that became dirty during the session gives 0. The signal is therefore not what a session wrote. Two lines listing the same dirty paths **suggests** they shared a checkout, or that one picked up the other's uncommitted work. It does not establish either without independent evidence of the checkout or of the lineage. If confirmed, that is a real collision class with no production evidence today. The 5 hits are exploratory: a lead to test, not a result.

Porting it means a projection that derives observed-dirty paths from `git status` and `--stat` tool results. It must be a field of its own, never mixed into `files_touched` (they are observations, not writes), so it needs a schema decision.

**Other ports.**
- **PR ownership** from the create call's own result, dated, never from mentions.
- **Relay suppression:** pairs that communicated over Agent Relay should not alert. The facts store has no relay evidence yet.
- **Budget:** a weekly alert budget per 100 active coding sessions.
- **Two stages:** the ranker for candidates, then an intent judge on as-of bundles with dated PR titles and descriptions. The judge goes through the conflict model-judge seam.
