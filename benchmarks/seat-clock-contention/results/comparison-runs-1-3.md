# Seat clock contention at 471d291a: runs 1-4 (file name kept from runs 1-3)

Runs 1-3 are one run per cell over the matrix W1/W2 x batch 250/500 x C 4/16/64, with C1, C2 and no control respectively.
Run 4 adds isolated distinct-origin cells, matched none-vs-C2 rotations at W1/500/C4, and a configuration variant.

- Source: `baseline-471d291a/` (run 1, C1), `baseline-471d291a-c2/` (run 2, C2), `baseline-471d291a-none/` (run 3, no
  control) and `unit4-471d291a/` (run 4).
- Server settings, image, host and load generator are the same in every run. The harness was not identical across runs:
  - run 1 predates the `controls` plan field and the `controlKind`, `sampledCoverage`, `callTimingMs`, `tuples`,
    `advisoryHeldByStage` and `commitsQueuedOnClock` result fields;
  - run 4 adds sampling of granted advisory locks, which lengthens each sample;
  - the sampler period differs by run and cell (p50 0.53-2.84 ms).
- The host is shared. Spread between runs is reported, not removed, and includes these harness changes as well as host
  load.

## Definitions

- **Tuple**: one (org, workspace, user, origin) load stream. The origin-claim key is
  `hashtextextended(['96bulls:ingest-origin-claim-v1', org, workspace, user, origin])`. The native-origin key is
  `['96bulls:lazy-native-origin-v1', org, workspace, origin]`, which has no user.
- **W1, distinct origins**: C tuples, one per worker. Each worker has its own user `scb-u<k>` and origin `scb-o<k>`; orgs
  rotate over org-a..org-d in one workspace. No origin id is shared between users, so W1 is the isolated distinct-origin
  workload.
- **W2, hot origin**: one hot tuple `org-a/ws-a/scb-u0/scb-hot` gets 50% of batches over C/2 workers. C/2 tail tuples (one
  worker each, own user and origin) share the other 50%. No origin id is shared between users.
- **Control users**: their own tuples, never part of the load.
  - C1 uses `org-a/ws-a/scb-control`, reset.
  - C2 uses `org-a/ws-a/scb-control2/scb-control2-o`, selection disable of a fresh session per attempt.
- **C1 (diagnostic)**: `sessions.ingest_lazy_clear(reset)` as the owner, the clock FOR UPDATE step alone.
- **C2 (acceptance)**: `ledger.applyOrderedControl -> ingest_set_session_uploads -> ingest_apply_ordered_control` as
  `bulls96_session_ingest`, lock_timeout 2 s, statement_timeout 5 s, relayhistory-cloud control witness.
- **Control schedule**: one control kind per cell, at a fixed rate of one slot per 1000 ms, one at a time.
  - **Slots scheduled** = started + missed. A slot is missed when the previous control was still running at its start.
  - **Completed during load** = finished before the last load worker stopped.
  - **Drain-only** = finished only after uploads had stopped. Controls are not started after load ends.
- **Measured per call** (client clock, every call):
  - **whole-call age** = BEGIN sent -> COMMIT returned;
  - **statement** = the stage function's SELECT, including every lock wait inside it;
  - **COMMIT response** = COMMIT sent -> returned.
  - Also: stage latency, drain time, commits/s, durable records/s, failures by SQLSTATE.
- **Sampled** (one superuser connection polling back to back; period p50 0.5-2.8 ms per cell, reported as
  `sampler.periodMs`):
  - **Clock hold**: from the first sample showing the transaction as the clock-row locker (pgrowlocks) in an exclusive
    mode, to min(client-observed COMMIT completion, last such sample + period p50).
    - Hold n counts every commit transaction with an observed hold, including ones that failed after taking the clock.
    - The start uses the server clock (sampler) and the end uses the client clock (same host).
  - **Clock wait**: first sample waiting on the clock row -> first sample holding it.
    - A transaction that failed while waiting has no holder sample; its wait runs to its client end, which is about 5 s
      (the statement timeout) or 2 s (lock_timeout).
    - A commit never seen waiting counts as 0.
    - Both are included in the percentiles. So a p95 near 5.0 s at C64 is the timeout, censored, and says nothing about the
      tail beyond it.
  - **Lock wait by class**: number of samples x period p50.
  - **Granted advisory locks (run 4) and the wait graph**.
  - **Coverage**: the reported ratio "observed holds / committed" counts failed commits that held the clock in its
    numerator, so it can exceed 1. It is not a coverage measure. Use `unobservedHolds` instead: transactions of the
    commit or control stage that succeeded with no hold sample.
    - It is 0 in every cell of runs 2-4.
    - In run 1 it is 1, 1, 4, 2, 15, 1, 1, 2 in eight cells, all of them C1 controls, whose holds (about 1-3 ms) are near
      or below the sample period. Every committed commit in runs 1-4 had an observed hold.
    - Holds shorter than one period can be missed.
- **Retries**: a failed delivery is re-uploaded after 100-300 ms. Retries/batch = extra attempts per batch.
- **Smoke run (scope)**: an earlier 16-worker smoke figure was produced by a prior single-threaded version of the
  harness: one cell (W1, n=250, C=16), 8,000 records, with the sampler on the client thread. It is not part of these
  results and is not comparable to them.

## Which stages take which lock (evidence, run 4)

From granted advisory locks sampled per transaction (`advisoryHeldByStage` in `unit4-471d291a/summary.json`), with the
per-origin claim fence on:

- Every claim, prepare and commit transaction holds the origin-claim key in ExclusiveLock (`pg_advisory_xact_lock`). This
  is `ingest_origin_claim_enter`, taken first by each wrapper.
  - W1 isolated 500/C16: 128/128 claims, 128/128 prepares, 128/128 commits.
- Every claim, prepare and commit also holds the erase gate (961996,50000) in ShareLock.
- Claim and commit also hold the native-origin key in ExclusiveLock. In commit it is taken after the clock.
  - In W1 busy-off 500/C16, all 141 commit transactions held origin-claim, but only the 128 that committed reached
    native-origin. The 13 that failed were stopped at the clock, which is consistent with the lock order origin-claim ->
    erase gate -> fence -> clock -> native-origin -> slots.
- **Why W1 commits queue on the clock together.** The W1 tuples are distinct, so their origin-claim keys are distinct, and
  two W1 commits never exclude each other on the wrapper lock. The dominant shared lock is the clock row (staged ACK NO
  KEY UPDATE vs NO KEY UPDATE). In the W1 cells of run 4 the clock accounts for more than 99% of wait samples. The rest:
  - claim waiting on another claim's transaction id (row lock or unique-index insert conflict): up to 865 samples at
    500/C16;
  - commit waiting on a transaction id: 8-39 samples per cell;
  - relation-extension waits on `ingest_lazy_native_slots` between claims and commits: up to 180 samples in rotA1-c2;
  - single samples of commit-on-commit transaction waits.
  - In every sample with two or more commits queued on the clock, the queued commits held distinct origin-claim keys
    (W1 500/C16: up to 13 queued, p50 12 distinct keys). No sample in any run-4 cell had two queued commits holding the
    same key.
  - In W2 the hot tuple's commits serialize on the origin-claim key first, so at most one hot commit is queued on the
    clock at a time (W2 500/C16: up to 7 queued, all distinct keys).

## Records per second, 250 vs 500 (corrected)

"About equal records/s at 250 and 500" holds only where retries are rare (C4, C16; at most 0.14 retries per batch).
There 250/500 ranges from 0.85 to 1.16 in every run, with two exceptions:

- W1 C4 in run 2 (1.64), the cell with the C2 effect below;
- W2 C16 in run 2 (1.35).

The isolated W1 cells of run 4 agree: 3,458 / 3,405 records/s at C4, 3,491 / 3,457 at C16.

In the retry-heavy C64 cells, 250 delivers more than 500 (records/s 250 vs 500, retries/batch 250 vs 500):

| cell | run 1 (C1) | run 2 (C2) | run 3 (none) |
|---|---|---|---|
| W1 C64 | 2,578 vs 2,263 (0.31 vs 0.87) | 2,711 vs 2,478 (0.28 vs 0.70) | 2,855 vs 2,160 (0.33 vs 1.02) |
| W2 C64 (mixed) | 1,352 vs 1,274 (0.95 vs 2.88) | **2,273 vs 1,147** (0.41 vs 3.21) | 1,822 vs 1,051 (0.73 vs 3.81) |

Retries per batch at 500 are 2.5-7.8x those at 250 (W1 C64: 2.8, 2.5, 3.1; W2 C64: 3.0, 7.8, 5.2 in runs 1, 2, 3). A
possible mechanism, untested: at 500 each failed attempt discards twice the work and waits about twice as long in the
clock queue. The mixed C64 cell is also the noisiest between runs (250: 1,352 / 2,273 / 1,822).

## No control vs C1 vs C2, one run per cell (runs 1-3)

Format in each cell: none / C1 / C2. Clock wait is sampled; latencies are in ms. Upload failures exclude the control's
own failures.

| workload | batch | C | drain s none / C1 / C2 | commits/s none / C1 / C2 | clock wait p50 none / C1 / C2 | clock wait p95 none / C1 / C2 | commit stage p95 none / C1 / C2 | upload failures none / C1 / C2 |
|---|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | 18.9 / 22.2 / 19.4 | 13.6 / 11.5 / 13.2 | 37 / 52 / 43 | 90 / 283 / 258 | 175 / 391 / 324 | 0 / 0 / 0 |
| W1 | 250 | 16 | 18.2 / 18.8 / 20.4 | 14.1 / 13.6 / 12.5 | 690 / 864 / 914 | 1793 / 1897 / 2067 | 1864 / 1983 / 2202 | 0 / 0 / 0 |
| W1 | 250 | 64 | 22.4 / 24.8 / 23.6 | 11.4 / 10.3 / 10.8 | 2949 / 3986 / 3481 | 4996 / 4997 / 4996 | 4869 / 4931 / 4947 | 85 / 79 / 71 |
| W1 | 500 | 4 | 17.9 / 23.6 / 31.7 | 7.2 / 5.4 / 4.0 | 68 / 106 / 63 | 146 / 706 / 551 | 290 / 986 / 705 | 0 / 0 / 0 |
| W1 | 500 | 16 | 20.4 / 21.0 / 21.3 | 6.3 / 6.1 / 6.0 | 2054 / 908 / 1298 | 3247 / 4158 / 3493 | 3411 / 4294 / 3641 | 0 / 0 / 0 |
| W1 | 500 | 64 | 29.6 / 28.3 / 25.8 | 4.3 / 4.5 / 5.0 | 3734 / 4650 / 4207 | 4996 / 5008 / 5000 | 4930 / 4876 / 4857 | 131 / 111 / 90 |
| W2 | 250 | 4 | 23.2 / 23.8 / 26.3 | 11.0 / 10.8 / 9.8 | 0 / 14 / 6 | 62 / 81 / 79 | 216 / 231 / 225 | 0 / 0 / 1 |
| W2 | 250 | 16 | 28.6 / 32.9 / 25.8 | 8.9 / 7.8 / 9.9 | 41 / 19 / 9 | 834 / 748 / 763 | 1154 / 1172 / 1272 | 11 / 16 / 8 |
| W2 | 250 | 64 | 35.1 / 47.3 / 28.2 | 7.3 / 5.4 / 9.1 | 0 / 0 / 0 | 3765 / 3763 / 3601 | 3402 / 3865 / 3704 | 188 / 244 / 104 |
| W2 | 500 | 4 | 26.9 / 24.1 / 28.3 | 4.8 / 5.3 / 4.5 | 0 / 5 / 12 | 130 / 216 / 146 | 400 / 361 / 425 | 0 / 0 / 0 |
| W2 | 500 | 16 | 31.2 / 27.9 / 34.7 | 4.1 / 4.6 / 3.7 | 22 / 0 / 0 | 1694 / 1410 / 1861 | 1905 / 1718 / 2259 | 12 / 10 / 18 |
| W2 | 500 | 64 | 60.9 / 50.2 / 55.8 | 2.1 / 2.5 / 2.3 | 0 / 0 / 0 | 4405 / 4785 / 4384 | 4532 / 4677 / 4094 | 488 / 369 / 411 |

C1 and C2 slot accounting for runs 1-2 is in each run's `report.md`. Those runs predate the `scheduledSlots` field;
scheduled = started + missed.

## W1/500/C4: none vs C2, six matched rotations in both orders (run 4)

| rotation | order | arm | drain s | commits/s | durable rec/s | clock wait p95 | commit statement p95 | commit COMMIT-response p95 | upload failures | C2 slots / missed / started / done in load / drain-only / failed | C2 latency p50 / p95 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A1 | none then C2 | none | 17.5 | 7.31 | 3653 | 158 | 293 | 4.3 | 0 | - | - |
| A1 | none then C2 | c2 | 23.4 | 5.47 | 2735 | 674 | 805 | 14.9 | 0 | 25 / 3 / 22 / 22 / 0 / {} | 652 / 1931 |
| B1 | C2 then none | c2 | 23.7 | 5.39 | 2697 | 741 | 869 | 16.8 | 1 | 23 / 1 / 22 / 22 / 0 / {} | 716 / 1878 |
| B1 | C2 then none | none | 19.5 | 6.56 | 3278 | 161 | 323 | 5.8 | 0 | - | - |
| A2 | none then C2 | none | 20.5 | 6.24 | 3121 | 167 | 327 | 5.5 | 0 | - | - |
| A2 | none then C2 | c2 | 21.4 | 5.97 | 2984 | 703 | 833 | 7.0 | 0 | 22 / 1 / 21 / 21 / 0 / {} | 633 / 1648 |
| B2 | C2 then none | c2 | 26.2 | 4.88 | 2442 | 1441 | 1314 | 21.4 | 2 | 30 / 5 / 25 / 25 / 0 / {} | 665 / 2282 |
| B2 | C2 then none | none | 18.8 | 6.81 | 3405 | 105 | 238 | 6.9 | 0 | - | - |
| A3 | none then C2 | none | 20.2 | 6.33 | 3165 | 169 | 305 | 26.6 | 0 | - | - |
| A3 | none then C2 | c2 | 23.0 | 5.58 | 2788 | 740 | 882 | 14.8 | 0 | 22 / 5 / 17 / 17 / 0 / {} | 709 / 2648 |
| B3 | C2 then none | c2 | 22.4 | 5.71 | 2856 | 485 | 613 | 23.7 | 1 | 28 / 2 / 26 / 25 / 0 / {"55P03": 1} | 326 / 801 |
| B3 | C2 then none | none | 18.0 | 7.12 | 3560 | 158 | 295 | 10.3 | 0 | - | - |

- none: n=6; commits/s mean 6.73 (min 6.24, max 7.31, sd 0.43); drain s mean 19.1 (17.5-20.5); clock wait p95 105-169 ms
- c2: n=6; commits/s mean 5.50 (min 4.88, max 5.97, sd 0.37); drain s mean 23.4 (21.4-26.2); clock wait p95 485-1441 ms
- order A none: commits/s 7.31, 6.24, 6.33
- order A c2: commits/s 5.47, 5.97, 5.58
- order B none: commits/s 6.56, 6.81, 7.12
- order B c2: commits/s 5.39, 4.88, 5.71

Reading:

- At W1/500/C4, running C2 is associated with lower commit throughput: commits/s 6.73 -> 5.50 on average (-18%).
  - The ranges do not overlap (none 6.24-7.31, C2 4.88-5.97).
  - The association holds in both orders (order A, none first: 6.24-7.31 vs 5.47-5.97; order B, C2 first: 6.56-7.12 vs
    4.88-5.71).
- Clock wait p95 is 105-169 ms without C2 and 485-1,441 ms with it.
- Commit statement p95 is 238-327 ms without C2 and 613-1,314 ms with it.
- Commit COMMIT-response p95 is higher with C2 in 5 of 6 pairs (none / C2: A1 4.3 / 14.9, A2 5.5 / 7.0, A3 26.6 / 14.8,
  B1 5.8 / 16.8, B2 6.9 / 21.4, B3 10.3 / 23.7 ms). So COMMIT time is not excluded as a contributor, although it is small
  next to the statement increase.
- **C2 dose differs per cell**, because the control runs at a fixed rate for as long as the cell runs. A cell with C2
  drains more slowly, so it gets more control attempts.
  - Started / missed per C2 cell: A1 22 / 3, A2 21 / 1, A3 17 / 5, B1 22 / 1, B2 25 / 5, B3 26 / 2.
  - Drain time in those cells is 21.4-26.2 s.
  - The arms differ in the number of controls as well as in their presence.
- **Mechanism, inferred from the wait graph and not directly observed**: C2's lazy_clear step queues FOR UPDATE on the
  clock. The graph shows it waiting on claim transactions (FOR UPDATE conflicts with their KEY SHARE) and shows staged
  ACKs queued behind it. That new claims keep re-taking KEY SHARE while C2 waits is not observed directly.
- Earlier single-run differences at C16/C64 (runs 1-3) are within the spread between runs and are not attributed to
  controls.

## Configuration variant: buffer-lock busy path on (W1 and W2, 500, C16; one cell each, run 4)

This is a variant only, not a statement about any install. The client still re-uploads after 100-300 ms and does not wait
for the busy answer's Retry-After.

- **W1**: busy-off drained in 22.8 s at 2,806 records/s, with 13 commit failures (55P03 x9, 57014 x4). Busy-on drained in
  28.0 s at 2,289 records/s, with 15 commit failures, all 57014. The busy path converts only lock-wait (55P03) stage
  failures, so 57014 still surfaces as `delivery_unavailable`.
- **W2**: busy-off drained in 29.8 s at 2,144 records/s. Busy-on drained in 40.1 s at 1,596 records/s, with 17
  `delivery_busy` and 5 `delivery_unavailable`.
- One cell each on a shared host. The slower busy-on drains are not separated from run-to-run spread. The same
  configuration as W1 busy-off (W1, 500, C16, no control) drained in 18.5-22.8 s across runs 3 and 4: 20.4 s in run 3;
  18.5 s (isolated) and 22.8 s (busy-off) in run 4. Including runs 1-2, which had controls, the W1 500/C16 range is
  18.5-22.8 s.

## Evidence gaps

1. **Hold tails at C64** (p99 0.6-1.4 s, max 2.7 s): the cause is not isolated (CPU vs a wait inside the hold).
   - The harness does not time phases inside `ingest_lazy_ack_batch`.
   - Container CPU was not recorded in any committed run.
2. **Configuration assumptions**: originClaimFence is on; bufferLockRetry is off; bufferLockBusy is off except in the
   variant cells. The client retry pause is fixed. No production setting is modeled.
3. **Not run**: the set-based ACK body A/B (gate off vs on); P0W01 vs 55P03 on a held second slot; the FOR KEY SHARE ->
   FOR UPDATE -> FOR NO KEY UPDATE ordering experiment.
4. **C16/C64 control effects**: one run per cell in runs 1-3. Attributing them would need matched rotations as at W1/500/C4.
5. **C2 dose**: the number of control attempts per cell grows with drain time and is not held constant between arms.
6. **Environment**: loopback round trip (about 0.03 ms) instead of a network and connection proxy; local storage and fsync;
   synthetic records. Production COMMIT round trips sit inside the hold and are not represented.
7. **Sampler limits**:
   - The observer has a cost: one extra backend querying pg_locks, pg_blocking_pids and pgrowlocks back to back.
   - Its period varies (C64 p99 up to about 16 ms).
   - Each sample's subqueries are not one atomic snapshot of the lock table.
   - Lock-wait-by-class figures are sample count x period p50, so they are approximate.
   - Waits and holds shorter than one period can be missed.
8. **Clock mixing**: hold start (server clock) and hold end or transaction window (client clock) come from different
   clocks on the same host. Transactions are matched to samples by pid and client-side window with a 1 ms tolerance.
9. **Timeout censoring**: waits of failed transactions stop at the 2 s or 5 s limit. Percentiles near those limits are
   censored.
10. **Run-1 coverage holes**: 28 C1 control holds with no sample (`unobservedHolds`). Commits are fully covered.
11. **Harness changes between runs**: see the top of this file. Cross-run spread includes them.
12. **No warm-up**: no warm-up delivery runs before timing. Each cell starts on a fresh copy of the template database,
    and first-seen user and org adoption happens inside the timed phase, in each stream's first delivery.
13. **No checkpoint or vacuum control** between cells or runs. Autovacuum and checkpoints run on their defaults during
    cells.
14. **Non-clock waits in W1**: small but present (claim-on-claim transaction waits, relation extension). Not analyzed
    further.
15. **Busy-path variant**: a single cell per workload; no rotations.
