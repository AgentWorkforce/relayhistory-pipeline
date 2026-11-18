# Session Intelligence replay harness

The offline harness that the last section of [coordination-live-advisories.md](coordination-live-advisories.md) requires before any advisor flag is proposed for enablement. It steps a simulated clock over a frozen session snapshot and runs the **real** advisor scan (`scanOnce` in `src/coordination-advisor/scan.ts`) at each tick. It records every advisory the worker would have sent, and when. It then scores the alerts per urgency row against labeled incidents and hard negatives, and builds blind adjudication packets.

It runs offline. It uses no network, database or credentials, and reads no environment. It enables nothing. Code: `src/session-intel/replay/`. Tests and the synthetic fixture: `src/session-intel/__tests__/`.

## Snapshot format

A snapshot is a directory kept **outside this repository**, with a delete-by date (as in [coordination-overlap-detection.md](coordination-overlap-detection.md), "Harvest"). Only synthetic fixtures are committed.

- `manifest.json`: `{"format": "session-intel-snapshot/1", "createdAt", "deleteBy", "window": {"since", "until"}, "files": {"<name>": "<sha256>"}}`. `files` must list `sessions.jsonl`. File names are plain names: no separators, no `..`, no dotfiles. Symlinks are refused.
- `sessions.jsonl` holds one session per line:
  - **Directory fields:** `orgId`, `workspaceId`, `harness`, `sessionId`, `projectId`, `repo`, `branch`, `title`, `userId`, `taskRef`, `parentSessionIds`, `createdAt`, `lastActivityAt`.
  - **`shortSummary`:** a string, or `{"text"}`, or `{"timeline": [{"at", "text"}]}`.
  - **Facts shaped like `LaneFacts`:** `writes` `[{path, ts, eventId, kind, branch}]`, `commits` `[{sha, ts, eventId}]`, `outcomes` `[{key, ts, eventId, reverted}]` and `usage` `[{ts, eventId, tokens, costUsdMicros}]`.
  - **`claims`:** `[{at, kind: intent|constraint|decision, text, confidence, evidenceIds}]`, reserved for the M1/M2 detectors. The advisor never reads them.

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
- Directory `last_activity_at` is the latest visible fact. The final `lastActivityAt` is used only once `now` reaches it.
- The short summary is the latest timeline entry at or before `now`. A summary with no timeline is visible only from `lastActivityAt`, so it never leaks early.
- `parentSessionIds`, `repo` and `title` are treated as known from creation.

The parts the scan calls:

- **`ReplayAdvisorStore`** mirrors `CANDIDATES_SQL`: active sessions with a project, newest first, at most 200 per workspace. It also mirrors the 062000 `upsert_v1`, `mark_delivered_v1` and `sweep_v1` semantics: an existing key is skipped, LOW is upgraded to HIGH, a row is never downgraded, and acked rows and other tenants are skipped. The artifact and evidence bounds raise the same error codes. `recent()` (062070) is derived from the recorded rows by `created_at`.
- **The facts backend** builds the production evidence, output and usage row shapes over the seven-day lookback, applies the production caps, and calls the real `normalizeCoordinationFacts`. Every read is bounded by `min(until, now)`.
- **A recording relay sender** records each DM with its simulated send time and sends nothing.

## Detectors and replay

`ReplayDetector` is `{name, version, params, run(view, now), diagnostics?}`. Each alert carries `{detector, alertId, kind, urgency, pair, artifact, sentAt, alertAt, channel, evidence}`. A detector's **hash** is the sha256 of its name, version and params.

`advisorDetector()` wraps `scanOnce` with the real `AdvisorLimits` at the config defaults: a 6-hour pair cooldown and 3 advisories per recipient per hour. Its params include a sha256 of the advisor, rubric, template, backend, detector and view sources, so a code change is a new detector hash.

- **Alert id.** Its `alertId` is `<advisoryId>/<urgency>`, so a LOW-to-HIGH upgrade counts as its own HIGH alert.
- **sentAt.** For HIGH it is the tick of the recorded DM; for LOW it is the tick the row became available to the pull path.
- **Incomplete replays.** Its diagnostics are the summed `ScanStats`. The CLI marks a replay incomplete, and exits 1, when any workspace scan `failed` or any row was `rejected`.

The driver steps from `window.since` to `window.until` every `--interval-ms`, default 120000 (the `COORDINATION_ADVISOR_INTERVAL_SECONDS` default). It keeps each alert's first `sentAt` and orders alerts deterministically. M2's intent and constraint detectors plug in behind the same interface.

## Classes

Scoring is class-aware, using the alert taxonomy (docs/proposals/session-intelligence.md, "Alert taxonomy"): `constraint_violation`, `intent_conflict`, `collision`, `overlap`, `planned_fleet` and `none`. `classes.ts` maps three things onto it.

- **A detector kind, with its urgency:**
  - `duplicate_work` and `superseded_work` are `collision`;
  - `concurrent_edit` is `collision` at HIGH (the 60-minute row) and `overlap` otherwise;
  - `nearby_work` and `similar_summary` are `overlap`;
  - a kind that names a class is that class;
  - anything else is `none`.

  A detector can set `alertClass` on an alert itself; M2's claim detectors will. The advisor leaves it unset.
- **A label incident kind:** `duplicate_work` and `superseded_work` are `collision`, `overlap` is `overlap`, and the conflict kinds are themselves.
- **An adjudication verdict:**
  - `collision` and `overlap` confirm their own class;
  - `real_overlap` (the M0 verdict) confirms either, because it does not say which;
  - `intent_conflict` and `constraint_violation` confirm themselves;
  - `planned`, `unrelated` and `unclear` confirm none.

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

For each mode, the scorer reports per detector, per (urgency, kind) row and per alert class:

- alerts, true and false positives, misclassified, precision and recall. A class row's class-match recall is over the incidents of that class;
- lead time (`harmAt` minus the first `sentAt` per detected incident): the counts before and after harm, and the median;
- alerts per 100 sessions;
- the top K at 5 per 100 sessions (`--budget`), ranked HIGH first, then the earliest.

It also reports a confusion table of in-scope alerts: alert class (rows) by labeled class (columns). The columns are the incident's class; for a hard negative, `planned_fleet` or `none`; or `unlabeled`. An alert counts once per distinct class labeled on its pair. The table is in both `report.md` and `report.json`.

Scoring uses `dev` unless you pass `--split heldout`. Held-out is scored once per detector hash: `scored-hashes.json` in the output directory records each one. A repeat is refused unless `--force`, which is recorded as forced. The ledger is a protocol aid, not a security boundary: keep one evaluation directory per study.

`packet` pools each detector's top K and merges the same (pair, artifact) across detectors. It strips detector names, kinds, urgencies, alert ids and evidence kinds, then shuffles with a seeded PRNG (`--seed`). It writes `packet.jsonl` for the adjudicators and a private `key.json`. `verdicts` imports `{alertKey, verdict, adjudicator}` records, where a verdict is one of `collision`, `overlap`, `real_overlap`, `intent_conflict`, `constraint_violation`, `planned`, `unrelated` or `unclear`. It reports pairwise agreement and Cohen's kappa, and takes the majority verdict for each alert (a tie is `unclear`). It reports precision two ways, both excluding `unclear`:

- **per detector:** any real verdict is positive;
- **per (detector, alert class):** the verdict must confirm the alert's class. A real verdict of another class is misclassified, and counts against precision.

The packet never shows the class; the private key records it. The artifact itself can hint at a tier (a migration number, the "similar short summaries" text); that is the evidence the adjudicators need.

## How to run

```sh
bun run src/session-intel/replay/cli.ts replay   --snapshot <dir> --out <eval>/replay [--interval-ms 120000]
bun run src/session-intel/replay/cli.ts score    --alerts <eval>/replay/alerts.json --labels <labels.json> --out <eval>/score [--split heldout]
bun run src/session-intel/replay/cli.ts packet   --alerts <eval>/replay/alerts.json [--alerts ...] --seed <seed> --out <eval>/packet
bun run src/session-intel/replay/cli.ts verdicts --key <eval>/packet/key.json --verdicts <verdicts.jsonl> --out <eval>/verdicts
```

Each command writes `report.md` and `report.json` to its `--out`. Outputs carry session ids, so `--out` inside this repository is refused; `--allow-repo-out` is for synthetic runs only.

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
