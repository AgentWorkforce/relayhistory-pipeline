# Session Intelligence replay harness

The offline harness that the last section of [coordination-live-advisories.md](coordination-live-advisories.md) requires before any advisor flag is proposed for enablement. It steps a simulated clock over a frozen session snapshot and runs the **real** advisor scan (`scanOnce` in `src/coordination-advisor/scan.ts`) at each tick. It records every advisory the worker would have sent, and when. It then scores the alerts per urgency row against labeled incidents and hard negatives, and builds blind adjudication packets.

It runs offline. It uses no network, database or credentials, and it enables nothing. The harness reads no environment itself, but the log module it loads with the scan (`src/shared/log.ts`) reads `OTEL_SERVICE_NAME`, `DEPLOYMENT` and `DEPLOYMENT_ID`. Scan warnings, such as a refused row or a failed workspace, print org and workspace ids to stderr, so keep that output as private as the snapshot. Code: `src/session-intel/replay/`. Tests and the synthetic fixture: `src/session-intel/__tests__/`.

## Snapshot format

A snapshot is a directory kept **outside this repository**, with a delete-by date (as in [coordination-overlap-detection.md](coordination-overlap-detection.md), "Harvest"). Only synthetic fixtures are committed.

This harness only reads a snapshot. Producing one from real sessions is M0-2: it reads session data, so it needs Khaliq's own OK each time.

- `manifest.json`: `{"format": "session-intel-snapshot/1", "createdAt", "deleteBy", "window": {"since", "until"}, "files": {"<name>": "<sha256>"}}`. `files` must list `sessions.jsonl`. File names are plain names: no separators, no `..`, no dotfiles. Symlinks are refused.
- `sessions.jsonl` holds one session per line:
  - **Directory fields:** `orgId`, `workspaceId`, `harness`, `sessionId`, `projectId`, `repo`, `branch`, `title`, `userId`, `taskRef`, `parentSessionIds`, `createdAt`, `lastActivityAt`.
  - **`shortSummary`:** a string, or `{"text"}`, or `{"timeline": [{"at", "text"}]}`.
  - **Facts shaped like `LaneFacts`:** `writes` `[{path, ts, eventId, kind, branch}]`, `commits` `[{sha, ts, eventId}]`, `outcomes` `[{key, ts, eventId, reverted}]` and `usage` `[{ts, eventId, tokens, costUsdMicros}]`.
  - **`claims`:** `[{id?, at, kind: intent|constraint|decision, scope?: session|project|repo, text, confidence, evidenceIds, supersededAt?, topic?}]`, read by the M2 conflict detectors ([session-intel-conflicts.md](session-intel-conflicts.md)). The advisor never reads them. `id` is unique within the session; it matches `[A-Za-z0-9_.-]{1,128}`, and when absent it is `c<index>`, the claim's position in the list. `scope` defaults to `session`. `supersededAt`, when set, is not before `at`.

`topic`, when present, is a relayfacts topic slug. The format stays `session-intel-snapshot/1`: `id`, `scope`, `supersededAt` and `topic` are optional, so a snapshot written without them still loads, with the same meaning. A loader from before them refuses a snapshot that uses them (unknown field), so the fields are never silently dropped. Because `snapshot.ts` and `views.ts` are hashed into the advisor's detector params, this change gave every advisor detector a new hash. No held-out score exists yet, so no score is affected.

The loader:

- refuses a snapshot once the clock is past `deleteBy`, before it reads any session;
- verifies every listed file's sha256;
- refuses unknown fields, so transcript text cannot slip in under a new key;
- bounds every size: 256 MiB per file, 4 MiB per line, 100,000 sessions, 20,000 facts per list, 4,000-character texts, and a window of at most 120 days;
- requires every fact to lie between `createdAt` and `lastActivityAt`;
- requires `(harness, sessionId)` to be unique, because labels and alerts name sessions that way.

## As-of views

At a simulated `now`:

- A session exists once `createdAt <= now`.
- A fact is visible only when `ts < now`. That is the production readers' `ts < until` bound.
- A claim is visible only when `at < now`. Its `supersededAt` appears only once `supersededAt < now`; before that, the view leaves the field out, so a detector cannot learn that a supersession is coming.
- Directory `last_activity_at` is the latest visible fact. The final `lastActivityAt` is used only once `now` reaches it.
- The short summary is the latest timeline entry at or before `now`. A summary with no timeline is visible only from `lastActivityAt`, so it never leaks early.
- `parentSessionIds`, `projectId`, `repo` and `title` are treated as known from creation.

Four of these assumptions can flatter lead time (make alerts look earlier than production would send them):

- At `now == lastActivityAt` the directory already shows the final time, while a fact stamped at that same time is still hidden (`ts < now`).
- A plain summary appears at `lastActivityAt`, with no summarisation lag.
- A timeline entry's `at` is not bounded by `lastActivityAt`.
- `projectId`, `title` and `repo` are known from creation, although production may learn them later.

The parts the scan calls:

- **`ReplayAdvisorStore`** mirrors `CANDIDATES_SQL`: active sessions with a project, newest first, at most 200 per workspace. It also mirrors the 062000 `upsert_v1`, `mark_delivered_v1` and `sweep_v1` semantics: an existing key is skipped, LOW is upgraded to HIGH, a row is never downgraded, and acked rows and other tenants are skipped. The artifact and evidence bounds raise the same error codes. `recent()` (062070) is derived from the recorded rows by `created_at`.
- **The facts backend** builds the production evidence, output and usage row shapes over the seven-day lookback, applies the production caps, and calls the real `normalizeCoordinationFacts`. Every read is bounded by `min(until, now)`.
- **A recording relay sender** records each DM with its simulated send time and sends nothing.

## Detectors and replay

`ReplayDetector` is `{name, version, params, run(view, now), diagnostics?}`. Each alert carries `{detector, alertId, kind, urgency, pair, artifact, sentAt, alertAt, channel, evidence}`. A detector's **hash** is the sha256 of its name, version, params and the replay's `intervalMs`.

`advisorDetector()` wraps `scanOnce` with the real `AdvisorLimits` at the config defaults: a 6-hour pair cooldown and 3 advisories per recipient per hour. Its store must answer the M3-1 `controls()`: the `controls` option sets them, and every feature defaults ON within the eval only (production's reader fails closed). Its `conflicts` option (default off) runs the scan with the M2-4 conflict rows, using the snapshot's claims as the 062150 reader. Both options are hashed. Its params include a sha256 of the source files that decide which alerts exist: the scan, rubric, template, relay sender, facts backend and detector package, and this harness's views, detector adapter, replay driver, snapshot loader and helpers. A change to any of them is a new detector hash. Like `advisorTick`, it runs the store's retention sweep once an hour of simulated time, so a row older than 7 days expires and the same finding can be written and DMed again. Only the first `sentAt` appears among the alerts. So in a window longer than 7 days, the alerts-per-100-sessions rate undercounts DMs. The diagnostics count every write (`written`) and every DM (`dmSent`), and the score report shows both.

- **Alert id.** Its `alertId` is `<advisoryId>/<urgency>`, so a LOW-to-HIGH upgrade counts as its own HIGH alert.
- **sentAt.** For HIGH it is the tick of the recorded DM; for LOW it is the tick the row became available to the pull path.
- **Incomplete replays.** Its diagnostics are the summed `ScanStats`. A replay where any workspace scan `failed` or any row was `rejected` is incomplete. `alerts.json` (format `session-intel-replay/2`) records the reasons in `incomplete`, and the CLI exits 1. Dev scoring reports the reasons. Held-out scoring refuses an incomplete replay, before anything is recorded.

The driver steps from `window.since` to `window.until` every `--interval-ms`, default 120000 (the `COORDINATION_ADVISOR_INTERVAL_SECONDS` default). Fix the interval for a study: a different `--interval-ms` is a new detector hash, and would buy another held-out score. It keeps each alert's first `sentAt` and orders alerts deterministically. M2's conflict detectors (`conflict-rule`, `conflict-model`) plug in behind the same interface. `--detectors` picks them by name: `coordination-advisor` (the default) and `conflict-rule`. The CLI refuses `conflict-model`, because it has no model transport.

## Classes

Scoring is class-aware, using the alert taxonomy (docs/proposals/session-intelligence.md, "Alert taxonomy"): `constraint_violation`, `intent_conflict`, `collision`, `overlap`, `planned_fleet` and `none`. `classes.ts` maps three things onto it.

- **A detector kind, with its urgency:**
  - `duplicate_work` and `superseded_work` are `collision`;
  - `concurrent_edit` is `collision` at HIGH (the 60-minute row). At LOW it is `overlap`. That is this harness's choice, not the rubric's: the rubric never emits `concurrent_edit` at LOW;
  - `nearby_work` and `similar_summary` are `overlap`;
  - a kind that names a class is that class;
  - any other kind is an error, never a silent `none`.

  A detector can set `alertClass` on an alert itself; M2's claim detectors will, and a detector with a new kind must. The advisor leaves it unset. The replay driver, the `alerts.json` reader and the `key.json` reader all refuse an alert whose class cannot be resolved.
- **A label incident kind:** `duplicate_work` and `superseded_work` are `collision`, `overlap` is `overlap`, and the conflict kinds are themselves.
- **A hard-negative reason:** `benign_overlap` is `overlap`, `planned_fleet` is `planned_fleet`, and `unrelated` is `none`. By the taxonomy, a benign overlap is still an overlap: the same project and shared files, with no conflicting claims. So an `overlap` alert on it is class-correct, and so is a `planned_fleet` alert on a planned fleet. In class-match, such an alert is a true positive with no incident to credit: it raises precision but not recall or lead time. An alert of any other class on that pair stays a false positive, and a `none` alert is never correct. In pair-match every hard negative stays a false positive. In the confusion table, each reason lands in its class's column.
- **An adjudication verdict:**
  - `collision` and `overlap` confirm their own class;
  - `real_overlap` (the M0 verdict) confirms either, because it does not say which;
  - `intent_conflict` and `constraint_violation` confirm themselves;
  - `planned` confirms `planned_fleet`;
  - `unrelated` and `unclear` confirm none.

Two verdicts agree when the classes they confirm intersect (`real_overlap` and `collision` agree), or when neither confirms a class and they are the same verdict. Consensus and agreement use this rule, so mixing the M0 vocabulary with the precise one is not counted as disagreement.

`classes.ts` is hashed (`classMapSha256`). The hash is in every score report and in each held-out ledger entry. It is never part of the held-out repeat check: a detector hash scored on held-out is refused again whatever the class map, because re-scoring after a class-map edit is tuning on held-out. `--force` lets a repeat through; the ledger entry records both hashes, `forced: true`, and the `--reason` text when one is given.

## Labels, scoring and adjudication

`labels.json` has two lists:

- `incidents`: `[{id, pair, kind, harmAt, split: dev|heldout, workstream}]`;
- `hardNegatives`: `[{pair, reason: planned_fleet|unrelated|benign_overlap, split?}]`.

A pair is two `<harness>:<sessionId>` refs, matched in either order.

For a split, the scorer classifies each alert by its pair:

- an incident of that split is a true positive;
- a hard negative is a false positive;
- anything else is an **unlabeled** false positive. Precision is therefore a lower bound until adjudication.

Dev scoring never reads held-out labels: an alert on a held-out pair is just unlabeled there, so no dev report shows which pairs are held out. Held-out scoring puts alerts on dev-labeled pairs out of scope.

It scores in two modes and reports both:

- **pair-match**: a pair match is a true positive (the M0 behaviour);
- **class-match**: a true positive needs the pair and the class (see Classes). A pair match whose class differs is counted as `misclassified`. It is not a true positive, but it stays in the precision denominator.

For each mode, the scorer reports per detector: per (urgency, kind) row, per alert class, and per (alert class, urgency). The (alert class, urgency) rows are the ones the spec's bars apply to. For each row it reports:

- alerts, true and false positives, misclassified, and recall. A class row's class-match recall is over the incidents of that class;
- precision two ways, both named. **Per alert** counts every alert. **Per pair** counts each (detector, pair) once, so a LOW upgraded to HIGH, or two artifacts on one pair, cannot count as two true positives. The spec's precision bars use **per-pair** precision, so duplicate alerts on one pair cannot inflate the score; per-alert precision is still reported;
- lead time (`harmAt` minus the first `sentAt` per detected incident): the counts before and after harm, and the median;
- alerts per 100 sessions (the rate the budget is measured against);
- the top K at 5 per 100 sessions (`--budget`), ranked HIGH first, then the earliest.

It also reports a confusion table of in-scope alerts: alert class (rows) by labeled class (columns). The columns are the incident's class; for a hard negative, its class (`overlap` for `benign_overlap`, `planned_fleet`, or `none` for `unrelated`); or `unlabeled`. An alert counts once per distinct class labeled on its pair. The table is in both reports.

Scoring uses `dev` unless you pass `--split heldout`. `--force` and `--reason` apply to held-out only, and dev refuses them.

Reports are split-named, so a dev run never overwrites a held-out report:

- dev: `report.dev.{json,md}`, rewritten by each dev run;
- held-out: `report.heldout.<hash12>.{json,md}`, where `<hash12>` is the first 12 hex digits of the detector hash. For a replay of several detectors, it is taken from the sha256 of their sorted hashes. A second detector that was never scored can therefore be scored into the same `--out`, and so the same ledger, without a name clash;
- a forced held-out re-score: `report.heldout.<hash12>.forced-<n>.{json,md}`, with the first free `n`.

A held-out report is never overwritten. Reports written before this naming (`report.heldout.json`, `report.heldout.forced-<n>.json`) are left alone: the scorer never reads a report, and the new names cannot collide with them.

Held-out is scored once per detector hash. `scored-hashes.json` in the output directory records each one: the detector hash, the class-map hash (`classMapSha256`), the scorer hash (`scorerSha256`, over `score.ts`, `classes.ts`, `labels.ts` and `util.ts`), `forced`, and the `--reason` text when given. `util.ts` is hashed whole: its pair identity, median and timestamp helpers decide scores. It also holds the file writers, so editing them changes the scorer hash too; that over-reports a scorer change and never hides one. A repeat is refused, whatever the class map, unless `--force`. Every score report also carries `classMapSha256` and `scorerSha256`.

The ledger is read, checked and written under an exclusive lock file, `scored-hashes.json.lock`, together with the reports. The lock is taken before the ledger is read, so the check always sees every entry recorded before it. A second run, or a stale lock left by a crash, is refused with a message naming the file. **Removing a stale lock is manual:** check that no scoring is running, then delete the file. The reports are written before the entry, so a report that cannot be written does not spend the hash. If the ledger write then fails, the report pair is removed, so no report exists for a score the ledger does not record, and the hash stays unspent. A damaged ledger entry is an error naming the file; it is never skipped.

The alerts reader refuses duplicate detector names and duplicate (detector, alertId) pairs. It also derives `incomplete` from the diagnostics as well as from the stored flag. A different `--out` or an edited `alerts.json` gets round the ledger: it is a protocol aid, not a control. Keep one evaluation directory per study.

The stricter readers kept their format versions: `session-intel-replay/2`, `session-intel-score/2` and `session-intel-scored-hashes/1`. The new ledger fields (`classMapSha256`, `scorerSha256`, `reason`) are optional, so a ledger written before them still reads as it did. Nothing old is misread: an old file either reads with the same meaning, or it is refused with an error (a repeated detector name or alert id). It is never silently reinterpreted.

`packet` pools each detector's top K and merges the same (pair, artifact) across detectors, keeping one member per detector. The same replay given twice is refused: the same file, or the same detector hash on the same snapshot. It strips detector names, kinds, urgencies, alert ids and evidence kinds, then shuffles with a seeded PRNG (`--seed`). It writes `packet.jsonl` for the adjudicators and a private `key.json`. `verdicts` imports `{alertKey, verdict, adjudicator}` records, where a verdict is one of `collision`, `overlap`, `real_overlap`, `intent_conflict`, `constraint_violation`, `planned`, `unrelated` or `unclear`. It reports pairwise agreement and Cohen's kappa (with 0/1 weights from the agreement rule above), and takes the majority verdict for each alert by class (a tie is `unclear`). The report format is `session-intel-verdicts/2`. It reports precision two ways, both excluding `unclear`:

- **per detector:** any real verdict is positive. It is reported per pooled entry and per (detector, pair);
- **per (detector, alert class):** the verdict must confirm the alert's class. A real verdict of another class is misclassified, and counts against precision.

The packet never shows the class; the private key records it. The artifact itself can hint at a tier (a migration number, the "similar short summaries" text); that is the evidence the adjudicators need.

## How to run

```sh
bun run src/session-intel/replay/cli.ts replay   --snapshot <dir> --out <eval>/replay [--interval-ms 120000] [--detectors coordination-advisor,conflict-rule]
bun run src/session-intel/replay/cli.ts score    --alerts <eval>/replay/alerts.json --labels <labels.json> --out <eval>/score [--split heldout]
bun run src/session-intel/replay/cli.ts packet   --alerts <eval>/replay/alerts.json [--alerts ...] --seed <seed> --out <eval>/packet
bun run src/session-intel/replay/cli.ts verdicts --key <eval>/packet/key.json --verdicts <verdicts.jsonl> --out <eval>/verdicts
```

Each command writes its reports to its `--out`: `report.md` and `report.json`, or the split-named pair for `score`. Outputs carry session ids, so `--out` inside this repository is refused. The check resolves symlinks through the nearest existing ancestor, and it counts a path as outside only when it climbs out of the repository (so `..cache` inside the repository is inside). `--allow-repo-out` is for synthetic runs only.

## How it implements the evaluation method

| [coordination-overlap-detection.md](coordination-overlap-detection.md) | Here |
|---|---|
| Frozen snapshot with hashes and a delete-by date | `manifest.json`, hash verification, the `deleteBy` refusal |
| Ground truth from outcomes, not detectors | `labels.json` is written by hand from outcomes; the scorer never derives labels |
| Dev/held-out split by workstream | `split` and `workstream` on each incident; dev by default |
| Detectors frozen by hash, held-out scored once | The detector hash (including the source hash) and the `scored-hashes.json` ledger |
| Blind adjudication of pooled top K at 5 per 100 sessions | `packet` and `verdicts` |

The synthetic fixture covers these cases:

- a T1 duplicate migration, HIGH, 58 minutes before harm;
- a planned fleet writing one file, which stays silent;
- a 45-minute shared file, HIGH, after harm;
- a LOW nearby-work pair labeled as a hard negative;
- a LOW nearby-work pair labeled as an `intent_conflict` incident, which class-match counts as misclassified;
- a summary and a write that appear later in the window, which must not be visible early;
- an unrelated pair, which stays silent.
