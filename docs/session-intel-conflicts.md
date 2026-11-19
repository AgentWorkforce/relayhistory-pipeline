# Session Intelligence conflict detectors (M2-1, M2-2)

Two replay detectors for the conflict classes of the alert taxonomy, `constraint_violation` and `intent_conflict`, plus the informational `overlap`. The taxonomy is in [proposals/session-intelligence.md](proposals/session-intelligence.md), "Alert taxonomy" and "M2". They run **offline only**, under the M0 replay harness ([session-intel-replay.md](session-intel-replay.md)), over a frozen snapshot and a simulated clock.

**Nothing here is wired or flagged.** No `SESSION_INTEL_CONFLICTS_ENABLED` flag exists. The coordination advisor does not call these detectors, and no DM, pull row or dashboard entry is produced. That is M2-4, which comes after the held-out replay (M2-3) and needs its own review. No model is called either: the model judge takes an injected transport, and this branch ships none. It reads no key and no environment.

Code: `src/session-intel/conflict/`. Tests and the synthetic fixture: `src/session-intel/__tests__/conflict*.test.ts` and `src/session-intel/__tests__/fixtures/conflict/`.

## What is different from the failed P5 judge

The P5 model judge was asked whether two 50k-token transcripts collided. It found 0 of 4 held-out incidents, and its classifier called every planned pair an overlap. These detectors change the question in three ways:

1. **Structured claims, not transcripts.** The judge sees only claims (`intent`, `constraint`, `decision`, each with a scope, a time and evidence ids) and action ids. A claim's text is the snapshot's scrub-clean claim text, cut to 200 characters; it may contain paths. Session ids, org ids and the written paths never reach the model prompt: it uses short aliases (`A.c1`, `B.e2`) and, for actions, topic slugs.
2. **Citations or nothing.** A verdict must cite one claim id from each side, or a constraint claim id and an action event id of the other side. The ids are checked against the judge's own input, and a reply that fails any check is `none`. So the judge cannot assert a conflict it cannot point at.
3. **Small, local candidate sets.** Instead of ranking about 300,000 pairs, it asks about the pairs active in the same project within the advisor's 2-hour window that share a topic, at most 50 per workspace per tick. Inputs that cannot be cited are never sent to a judge.

## Snapshot claims

Claims are read from the snapshot (`claims` in `sessions.jsonl`). They carry an optional `id`, an optional `scope` (`session`, `project` or `repo`, default `session`) and an optional `supersededAt`. The format stays `session-intel-snapshot/1` with optional fields. Advisor detector hashes change as a result, because `snapshot.ts` and `views.ts` are in the advisor's source hash. No held-out score exists yet, so none is affected.

As of `now`, a claim is **live** when `at < now` and it is not superseded by `now` (`supersededAt < now`). The as-of view hides a future `supersededAt`, so no detector sees a supersession early.

## Candidates (M2-1)

`candidates.ts` pairs two sessions when all of these hold:

- they are in the same org, workspace and project;
- both are active within the 2-hour window (`CANDIDATE_WINDOW_MS`, the advisor's);
- at least one side has a live claim;
- they share a **topic**. A topic is a decisionTopic-style slug: the first two directory segments of a path (`supabase/migrations/x.sql` gives `supabase-migrations`). Two sessions share a topic when:
  - both wrote under it;
  - or a live claim of one side names a topic the other side wrote or claimed. A claim can name it by path (`src/api/routes.ts`, `supabase/migrations`), or by a word equal to a non-generic directory segment, plurals allowed (`"no new migrations"` names `db-migrations`).

Further rules:

- **Planned fleets** (shared parent ids, or one session the other's parent, as the advisor's `isPlannedFleet` defines them) are kept and flagged `fleet: true`, with their orchestrator ids. They are not dropped.
- **Self pairs** never form.
- **Cap:** pairs come from the newest 200 sessions per workspace, at most 50 pairs per workspace per tick. The order is deterministic: the most recently active pair first (by the older side's activity), then by pair. `truncated` counts the pairs that were cut. The cap applies before the citable check, so a pair that cannot be cited (for example, one whose only claim is a session-scope constraint) still takes one of the 50 places. M2-3 should check, on the real snapshot, whether this ever starves citable pairs.

## Classifier (M2-2)

`classify.ts`. The output is closed-choice: `constraint_violation`, `intent_conflict`, `overlap` or `none`.

### Vetoes

They are deterministic, and they apply whichever judge answers.

Before the judge, in the input builder:

- **A superseded claim is never used.** It is left out of the input, and citing it gives `none` (`superseded`).
- **A `session`-scope constraint never propagates.** It is left out (`session_scope`). Only `project` and `repo` constraints reach the judge.
- Claims that name none of the pair's shared topics are left out (`off_topic`).
- Actions are the visible writes in a shared topic. A commit or an outcome carries no path, so it counts only in the event of such a write (and is then that same action); any other commit or outcome of the side is never an action.

After the judge, in `validateCitations`:

- every cited id must be in the input. An id that appears on both sides is ambiguous and is refused: a session id or event id containing the `@` separator could make two refs print the same;
- a citation set from one side only is a **self-conflict**, never a pair alert. That includes a constraint and its holder's own write;
- `constraint_violation` needs exactly one constraint, plus either one non-constraint claim of the other side or an action of the other side. The action must not be earlier than the constraint: acting before a rule existed does not break it;
- `intent_conflict` needs one decision or intent claim from each side, and no constraint;
- `overlap` needs one claim from each side, and no action.

### Urgency

- **HIGH needs action evidence in the topic by the acting side.** For `constraint_violation`, the acting side is the violator, and it needs an in-topic action at or after the constraint. For `intent_conflict`, it is the intent's side (with two intents or two decisions, the later claim's side), and it needs an in-topic action at or after its own claim. The earliest qualifying action is cited, and a judge citing an earlier one is vetoed (`action_before_constraint`, `action_before_claim`). An intent alone is LOW.
- `overlap` is LOW and never a DM.
- **Fleets:** a conflict inside a planned fleet is LOW, addressed to the orchestrator only (`recipient: "orchestrator"`, with no DM or pull recipients among the workers). An overlap inside a fleet is SILENT: no alert.

## Judges

A `PairJudge` is `{ name, version, params, metered, estimateTokens?, judge(input): Promise<JudgeOutput> }`. It lives in `judges.ts`.

### `ruleJudge`

A deterministic, conservative lexical baseline. In order:

1. **A constraint against a write.** The constraint must be negated: `never`, `don't`, `do not`, `no`, `not`, `must not`, `stop` or `avoid`. Its clause runs to `until`, `unless`, `,`, `.` or the like. The target is the clause's path tokens and globs (`supabase/migrations`, `*/migrations/*`, `src/**/*.sql`), or else the head word of its object (`"no new migrations"` gives `migrations`). The rule matches when the other side has a write at or after the constraint whose path matches the glob, or whose directories contain the word. The glob matcher uses no regex. It matches segment by segment with a two-pointer star match inside each segment, so its cost is bounded by the pattern and path lengths and never grows exponentially. A glob with more than 8 wildcards or over 512 characters, or a path over 4,096 characters, never matches.

   Only write-ish or verbless constraints match writes: `touch`, `edit`, `change`, `modify`, `add`, `write`, `create`, `"no new X"`. `"Never apply migrations to a real DB"` does not match a migration file: this baseline cannot see commands.
2. **The same constraint against a stated intent or decision of the other side.** The claim must not itself be negated, and it must name the target and the verb.
3. **A decision or intent against one of the other side, on explicit opposites only.** The opposites are:
   - `enable`/`disable` (and their forms), `allow`/`deny`, `include`/`exclude`, `sync`/`async` and `true`/`false`;
   - `on`/`off` used as a state (`"is off"`, `"turn it on"`), so `"work on"` does not count;
   - two different HTTP statuses where both claims talk of returning or responding (`501` against `404`);
   - `use X` against `don't use X` or `avoid X`.

   The two claims must also be about the same subject: a shared topic, or a shared content word.
4. **Overlap.** Otherwise, when both sides have an in-topic claim, the answer is `overlap`, citing the earliest claim of each side.

### `modelJudge`

`modelJudge({ complete, model })` builds a bounded prompt and parses a strict reply.

- **Prompt bounds:** at most 8 claims per side (constraints first, then decisions, then intents, the latest first) and at most 8 actions per side. Each claim is cut to 200 characters, with control characters and newlines flattened, and quoted as JSON.
- **The reply:** exactly one JSON object with `verdict`, `claimIds` (at most 2) and optionally `actionId`. Each id must be an alias the prompt listed.
- **Refused replies:** garbage, a code fence, an extra field, a verdict outside the closed set, an unknown or real (non-alias) id, an action alias given as a claim, or a reply over 2,000 characters. Each gives `none`, with the reason counted as `invalidReplies`.
- **The transport** is injected: `complete(prompt): Promise<string>`. This branch has no transport that touches a network. The tests use fakes, and the CLI refuses `conflict-model`.

## The detectors

`conflict-rule` and `conflict-model` wrap the judges as `ReplayDetector`s (`detector.ts`). Each alert sets:

- `kind` and `alertClass` to the verdict;
- `urgency`;
- `artifact` to the shared topic;
- `channel`: `dm` when the delivery has DM recipients, `pull` otherwise;
- `evidence`: each cited claim's evidence ids, at the claim's time, and the action event;
- `conflict`: `{claimIds, actionId, fleet, delivery}`.

The alert id is `<pair>|<class>|<urgency>`, so a LOW that later turns HIGH is its own HIGH alert, as with the advisor.

The params hash covers:

- the judge's name, version and params (the model label and its bounds; the rule judge's word lists);
- the window and the caps;
- the budget;
- a sha256 of the conflict sources and the replay code they depend on.

**Model budget.** A per-workspace daily call cap and token cap, by simulated UTC day. The defaults are 200 calls and 400,000 estimated tokens. They are enforced before each call, with the estimate `ceil((prompt chars + 2,000) / 4)`. Over the cap, the pair is skipped (`budgetSkipped`). A result is cached while a pair's input is unchanged, so an unchanged pair costs one call, not one per tick. A transport that throws counts as `failed`, which makes the replay incomplete, so held-out scoring refuses it.

**Diagnostics:** `candidates`, `truncated`, `uncitable`, `judged`, `cacheHits`, `modelCalls`, `modelTokens`, `budgetSkipped`, `invalidReplies`, `vetoed`, `none`, `silenced`, `failed`, `alertsHigh` and `alertsLow`.

## Delivery recipients

These are recorded on the alert only. Nothing is sent.

- `constraint_violation`: the **violator** (the acting session) and the constraint's **owner**, the session holding it. HIGH DMs both; LOW puts it on both pull paths.
- A `repo`-scope constraint with no holding session (the M1-4 policy pack, which this snapshot format cannot yet express) has the owner `repo-policy` and **no DM recipient**. No recipient is invented for it. TODO(M2-4): the lander requires a definition of "repo owner" before any DM code goes in.
- `intent_conflict`: both sessions, as DMs at HIGH and on the pull path.
- `overlap`: both pull paths, never a DM.
- Fleet conflicts: the orchestrator only.

## The synthetic fixture

`fixtures/conflict/snapshot` and `labels.json`, at 120-second ticks:

| Case | Sessions | Expected | Replayed |
|---|---|---|---|
| (a) constraint, then a write against it | cv-owner, cv-actor | HIGH constraint_violation | HIGH at 09:42 |
| (b) the same constraint, session scope | ss-owner, ss-actor | none | none (`session_scope`) |
| (c) decision against an opposing intent, with writes | ic-a, ic-b | HIGH intent_conflict | HIGH at 10:02 |
| (d) the same, no writes | id-a, id-b | LOW intent_conflict | LOW at 10:02 |
| (e) a superseded decision | sup-a, sup-b | none | none |
| (f) a conflict inside a fleet | fl-1, fl-2 (lead fl-lead) | LOW, orchestrator only | LOW at 09:52, `recipient: orchestrator` |
| (g) benign overlap | cat-a, cat-b | overlap LOW | LOW at 09:32 |
| (h) a constraint stated at 11:31 | lk-owner, lk-actor | not visible early | LOW at 11:32 (intent), HIGH at 11:52 (write) |

Scored on dev in class-match mode with the M0 scorer: 7 alerts, 7 class-correct, 6 of 6 pairs, recall 5 of 5, every incident before harm. The (g) overlap counts as a true positive on its `benign_overlap` hard negative, as M0's class rules define. This is a synthetic smoke test of the wiring, not evidence for the spec's bars. Those need M2-3's held-out replay and adjudication.

```sh
bun run src/session-intel/replay/cli.ts replay --snapshot src/session-intel/__tests__/fixtures/conflict/snapshot --out <eval>/r --detectors conflict-rule
bun run src/session-intel/replay/cli.ts score  --alerts <eval>/r/alerts.json --labels src/session-intel/__tests__/fixtures/conflict/labels.json --out <eval>/s
```
