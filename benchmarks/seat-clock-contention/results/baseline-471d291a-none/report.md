## baseline-471d291a-none

PostgreSQL: PostgreSQL 16.16 (Debian 16.16-1.pgdg13+1); history switch live: true; origin claim fence: true; records per run: 64000; control every 1000 ms; loopback RTT p50 0.03 ms.

### Throughput and failures

| workload | batch | C | drained | drain s | durable rec/s | commits/s | retries/batch | stage failures (stage:SQLSTATE) |
|---|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | yes | 18.9 | 3392 | 13.57 | 0.00 | none |
| W1 | 250 | 16 | yes | 18.2 | 3512 | 14.05 | 0.00 | none |
| W1 | 250 | 64 | yes | 22.4 | 2855 | 11.42 | 0.33 | commit:57014 x85 |
| W1 | 500 | 4 | yes | 17.9 | 3577 | 7.15 | 0.00 | none |
| W1 | 500 | 16 | yes | 20.4 | 3130 | 6.26 | 0.00 | none |
| W1 | 500 | 64 | yes | 29.6 | 2160 | 4.32 | 1.02 | commit:57014 x93, commit:55P03 x38 |
| W2 | 250 | 4 | yes | 23.2 | 2760 | 11.04 | 0.00 | none |
| W2 | 250 | 16 | yes | 28.6 | 2234 | 8.94 | 0.04 | commit:55P03 x8, prepare:55P03 x2, claim:55P03 x1 |
| W2 | 250 | 64 | yes | 35.1 | 1822 | 7.29 | 0.73 | claim:55P03 x96, commit:55P03 x70, prepare:55P03 x22 |
| W2 | 500 | 4 | yes | 26.9 | 2379 | 4.76 | 0.00 | none |
| W2 | 500 | 16 | yes | 31.2 | 2053 | 4.11 | 0.09 | commit:55P03 x9, prepare:55P03 x2, claim:55P03 x1 |
| W2 | 500 | 64 | yes | 60.9 | 1051 | 2.10 | 3.81 | claim:55P03 x297, commit:55P03 x155, prepare:55P03 x27, commit:57014 x9 |

### Clock hold vs wait, commit transactions (ms, p50 / p95 / p99 / max)

Sampled: hold start and wait come from the lock sampler (period p50 per cell in the last table, about 0.6-2.8 ms); hold end is the COMMIT completion. Coverage = observed holds (commit transactions of any outcome, incl. ones that later failed 57014) / committed commits.

| workload | batch | C | hold | wait for clock | commit stage latency (ok) | observed holds / committed | sample period p50 |
|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | 69 / 88 / 145 / 410 | 37 / 90 / 183 / 381 | 108 / 175 / 281 / 475 | 256/256 | 0.72 ms |
| W1 | 250 | 16 | 69 / 85 / 100 / 171 | 690 / 1793 / 2020 / 2186 | 758 / 1864 / 2094 / 2268 | 256/256 | 1.29 ms |
| W1 | 250 | 64 | 71 / 111 / 330 / 748 | 2949 / 4996 / 5019 / 5041 | 1374 / 4869 / 4951 / 4972 | 268/256 | 2.66 ms |
| W1 | 500 | 4 | 133 / 157 / 168 / 312 | 68 / 146 / 313 / 326 | 205 / 290 / 465 / 473 | 128/128 | 0.71 ms |
| W1 | 500 | 16 | 136 / 192 / 373 / 1620 | 2054 / 3247 / 4026 / 4058 | 2172 / 3411 / 4185 / 4197 | 128/128 | 1.21 ms |
| W1 | 500 | 64 | 130 / 333 / 1513 / 2726 | 3734 / 4996 / 5009 / 5019 | 2529 / 4930 / 5118 / 5167 | 160/128 | 1.79 ms |
| W2 | 250 | 4 | 63 / 78 / 88 / 164 | 0 / 62 / 73 / 94 | 100 / 216 / 234 / 261 | 256/256 | 0.6 ms |
| W2 | 250 | 16 | 64 / 84 / 383 / 1008 | 41 / 834 / 1083 / 1124 | 407 / 1154 / 2299 / 2623 | 256/256 | 0.61 ms |
| W2 | 250 | 64 | 70 / 110 / 592 / 2047 | 0 / 3765 / 4261 / 4276 | 1449 / 3402 / 3914 / 4337 | 256/256 | 0.71 ms |
| W2 | 500 | 4 | 125 / 155 / 292 / 1256 | 0 / 130 / 202 / 236 | 162 / 400 / 506 / 1258 | 128/128 | 0.57 ms |
| W2 | 500 | 16 | 141 / 206 / 307 / 1082 | 22 / 1694 / 2025 / 2122 | 903 / 1905 / 2286 / 2338 | 128/128 | 0.65 ms |
| W2 | 500 | 64 | 134 / 201 / 310 / 1092 | 0 / 4405 / 4991 / 5009 | 1829 / 4532 / 4833 / 4908 | 129/128 | 0.67 ms |

### Controls during load (fixed rate, one at a time; latency and hold in ms)

C1 (diagnostic) = sessions.ingest_lazy_clear(reset) as owner, the clock FOR UPDATE step alone. C2 (acceptance) = ledger.applyOrderedControl -> ingest_set_session_uploads -> ingest_apply_ordered_control, runtime role, 2 s / 5 s. One control kind per run.
Completed during load = finished before the last load worker stopped; after load end = finished only once uploads had stopped (drain-only).

| workload | batch | C | control | started | completed during load | completed after load end | failed (SQLSTATE) | missed slots | latency p50 / p95 / p99 / max | clock hold p50 / max |
|---|---|---|---|---|---|---|---|---|---|---|

### Lock waits by stage and lock class (ms per transaction that waited, p50 / p95 / p99 / max; n = transactions)

**W1 n=250 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 236 | 36 / 79 / 166 / 457 |
| commit: xid(row or unique-insert) | 16 | 1 / 1 / 1 / 1 |

**W1 n=250 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 5 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 12 | 61 / 66 / 66 / 66 |
| commit: clock | 254 | 805 / 1418 / 1601 / 1632 |
| commit: xid(row or unique-insert) | 56 | 1 / 1 / 1 / 1 |

**W1 n=250 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_claim_attempts_records_gin_idx | 4 | 3 / 3 / 3 / 3 |
| claim: extend:ingest_lazy_native_slots | 20 | 3 / 5 / 5 / 5 |
| claim: extend:ingest_lazy_native_slots_pkey | 5 | 3 / 3 / 3 / 3 |
| claim: xid(row or unique-insert) | 60 | 88 / 120 / 122 / 122 |
| commit: clock | 335 | 3056 / 4248 / 4956 / 5134 |
| commit: xid(row or unique-insert) | 106 | 3 / 3 / 3 / 5 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 119 | 67 / 151 / 293 / 297 |
| commit: xid(row or unique-insert) | 12 | 1 / 1 / 1 / 1 |

**W1 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 2 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 12 | 122 / 142 / 142 / 142 |
| commit: clock | 126 | 1915 / 2827 / 3140 / 3140 |
| commit: xid(row or unique-insert) | 30 | 1 / 1 / 1 / 1 |

**W1 n=500 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:70850 | 4 | 2 / 2 / 2 / 2 |
| claim: extend:ingest_claim_attempts_records_gin_idx | 8 | 4 / 4 / 4 / 4 |
| claim: extend:ingest_lazy_native_slots | 45 | 2 / 5 / 7 / 7 |
| claim: extend:ingest_lazy_native_slots_pkey | 8 | 2 / 4 / 4 / 4 |
| claim: xid(row or unique-insert) | 60 | 77 / 98 / 102 / 102 |
| commit: clock | 257 | 1982 / 4556 / 4709 / 4874 |
| commit: extend:ingest_lazy_native_slots | 1 | 2 / 2 / 2 / 2 |
| commit: xid(row or unique-insert) | 55 | 2 / 2 / 4 / 4 |

**W2 n=250 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 51 | 71 / 122 / 141 / 141 |
| commit: advisory:origin-claim | 82 | 71 / 104 / 114 / 114 |
| commit: clock | 115 | 31 / 63 / 73 / 81 |
| commit: xid(row or unique-insert) | 9 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 128 | 41 / 85 / 94 / 221 |

**W2 n=250 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 138 | 287 / 1034 / 1155 / 1165 |
| claim: xid(row or unique-insert) | 5 | 31 / 32 / 32 / 32 |
| commit: advisory:origin-claim | 135 | 368 / 1132 / 1957 / 1959 |
| commit: clock | 142 | 160 / 534 / 653 / 676 |
| commit: extend:ingest_lazy_ack_decisions | 1 | 4 / 4 / 4 / 4 |
| commit: xid(row or unique-insert) | 26 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 137 | 312 / 734 / 1131 / 1952 |

**W2 n=250 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 287 | 681 / 1658 / 1807 / 1845 |
| claim: extend:ingest_lazy_native_slots | 21 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_lazy_native_slots_pkey | 1 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 29 | 18 / 25 / 25 / 25 |
| commit: advisory:origin-claim | 170 | 1279 / 1872 / 1953 / 1962 |
| commit: clock | 156 | 563 / 1065 / 1071 / 1072 |
| commit: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: xid(row or unique-insert) | 45 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 192 | 998 / 1804 / 1901 / 1953 |

**W2 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 53 | 169 / 242 / 279 / 279 |
| commit: advisory:origin-claim | 14 | 186 / 293 / 293 / 293 |
| commit: clock | 59 | 68 / 125 / 202 / 202 |
| commit: xid(row or unique-insert) | 7 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 64 | 115 / 158 / 1405 / 1405 |

**W2 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 75 | 719 / 1312 / 1589 / 1589 |
| claim: xid(row or unique-insert) | 5 | 67 / 73 / 73 / 73 |
| commit: advisory:origin-claim | 73 | 771 / 1496 / 1762 / 1762 |
| commit: clock | 70 | 274 / 813 / 845 / 845 |
| commit: xid(row or unique-insert) | 18 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 75 | 662 / 1411 / 1773 / 1773 |

**W2 n=500 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 542 | 1376 / 1871 / 1960 / 2178 |
| claim: extend:ingest_lazy_native_slots | 20 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_lazy_native_slots_pkey | 2 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 29 | 44 / 48 / 51 / 51 |
| commit: advisory:origin-claim | 219 | 1715 / 1930 / 1959 / 1960 |
| commit: clock | 73 | 809 / 1271 / 1309 / 1309 |
| commit: xid(row or unique-insert) | 20 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 246 | 1069 / 1812 / 1946 / 2223 |

### Top wait-graph edges (samples: waiter stage -> lock [mode] <- blocker stages)

**W1 n=250 C=4** (sampler 24735 samples, period p50 0.72 ms)

- 13110: commit -> clock [ShareLock] <- commit
- 509: commit -> clock [ExclusiveLock] <- commit
- 36: commit -> clock [ShareLock] <- untagged
- 16: commit -> xid(row or unique-insert) [ShareLock] <- none
- 7: commit -> clock [ShareLock] <- none
- 1: commit -> clock [ExclusiveLock] <- none

**W1 n=250 C=16** (sampler 12805 samples, period p50 1.29 ms)

- 116332: commit -> clock [ExclusiveLock] <- commit
- 18169: commit -> clock [ShareLock] <- commit
- 549: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 104: commit -> clock [ShareLock] <- untagged
- 54: commit -> clock [ExclusiveLock] <- none
- 53: commit -> xid(row or unique-insert) [ShareLock] <- none
- 10: commit -> clock [ShareLock] <- none
- 5: claim -> extend:ingest_lazy_native_slots [ExclusiveLock] <- none

**W1 n=250 C=64** (sampler 7792 samples, period p50 2.66 ms)

- 255770: commit -> clock [ExclusiveLock] <- commit
- 8845: commit -> clock [ShareLock] <- commit
- 2022: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 1070: commit -> clock [ExclusiveLock] <- commit+untagged
- 111: commit -> clock [ShareLock] <- untagged
- 92: commit -> clock [ExclusiveLock] <- none
- 90: commit -> xid(row or unique-insert) [ShareLock] <- none
- 30: claim -> xid(row or unique-insert) [ShareLock] <- none

**W1 n=500 C=4** (sampler 23552 samples, period p50 0.71 ms)

- 11874: commit -> clock [ShareLock] <- commit
- 555: commit -> clock [ExclusiveLock] <- commit
- 16: commit -> clock [ShareLock] <- untagged
- 12: commit -> xid(row or unique-insert) [ShareLock] <- none
- 5: commit -> clock [ShareLock] <- none
- 1: commit -> clock [ExclusiveLock] <- none

**W1 n=500 C=16** (sampler 14969 samples, period p50 1.21 ms)

- 131013: commit -> clock [ExclusiveLock] <- commit
- 19222: commit -> clock [ShareLock] <- commit
- 1211: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 32: commit -> clock [ShareLock] <- untagged
- 29: commit -> clock [ExclusiveLock] <- none
- 27: commit -> xid(row or unique-insert) [ShareLock] <- none
- 9: claim -> xid(row or unique-insert) [ShareLock] <- none
- 4: commit -> clock [ShareLock] <- none

**W1 n=500 C=64** (sampler 10686 samples, period p50 1.79 ms)

- 249733: commit -> clock [ExclusiveLock] <- commit
- 12992: commit -> clock [ShareLock] <- commit
- 2443: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 654: commit -> clock [ExclusiveLock] <- commit+untagged
- 63: commit -> clock [ShareLock] <- untagged
- 45: claim -> xid(row or unique-insert) [ShareLock] <- none
- 45: commit -> clock [ExclusiveLock] <- none
- 43: claim -> extend:ingest_lazy_native_slots [ExclusiveLock] <- none

**W2 n=250 C=4** (sampler 35378 samples, period p50 0.6 ms)

- 10067: commit -> advisory:origin-claim [ExclusiveLock] <- claim
- 7988: prepare -> advisory:origin-claim [ExclusiveLock] <- commit
- 6148: commit -> clock [ShareLock] <- commit
- 3116: claim -> advisory:origin-claim [ExclusiveLock] <- commit
- 2976: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 1898: prepare -> advisory:origin-claim [ExclusiveLock] <- claim
- 55: commit -> clock [ExclusiveLock] <- commit
- 34: claim -> advisory:origin-claim [ExclusiveLock] <- prepare

**W2 n=250 C=16** (sampler 39631 samples, period p50 0.61 ms)

- 40282: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 33574: commit -> clock [ExclusiveLock] <- commit
- 22831: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 20811: prepare -> advisory:origin-claim [ExclusiveLock] <- commit+prepare
- 18563: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 17957: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 17769: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 15981: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare

**W2 n=250 C=64** (sampler 33394 samples, period p50 0.71 ms)

- 181000: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 172934: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 162722: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 106325: commit -> clock [ExclusiveLock] <- commit
- 68441: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 52486: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 51581: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 45603: prepare -> advisory:origin-claim [ExclusiveLock] <- commit+prepare

**W2 n=500 C=4** (sampler 42229 samples, period p50 0.57 ms)

- 8512: prepare -> advisory:origin-claim [ExclusiveLock] <- commit
- 7920: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 6662: claim -> advisory:origin-claim [ExclusiveLock] <- commit
- 6649: commit -> clock [ShareLock] <- commit
- 5532: prepare -> advisory:origin-claim [ExclusiveLock] <- claim
- 4708: commit -> advisory:origin-claim [ExclusiveLock] <- claim
- 230: commit -> clock [ExclusiveLock] <- commit
- 105: claim -> advisory:origin-claim [ExclusiveLock] <- prepare

**W2 n=500 C=16** (sampler 39146 samples, period p50 0.65 ms)

- 32175: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 27966: commit -> clock [ExclusiveLock] <- commit
- 27131: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 21199: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 20425: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 17667: commit -> advisory:origin-claim [ExclusiveLock] <- commit+prepare
- 17054: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+prepare
- 14322: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare

**W2 n=500 C=64** (sampler 71378 samples, period p50 0.67 ms)

- 341540: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 278499: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 268063: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 254952: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 194729: claim -> advisory:origin-claim [ExclusiveLock] <- claim+prepare
- 149166: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 147371: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 70656: commit -> clock [ExclusiveLock] <- commit

### Client checks

| workload | batch | C | load threads | client CPU s | event-loop lag ms p50 / p95 / p99 / max | sampler period ms p50 / p95 / p99 / max |
|---|---|---|---|---|---|---|
| W1 | 250 | 4 | 4 | 18.23 | 0 / 3 / 36 / 47 | 0.72 / 1.23 / 1.54 / 3.52 |
| W1 | 250 | 16 | 8 | 20.74 | 0 / 1 / 36 / 88 | 1.29 / 2.37 / 2.88 / 40.11 |
| W1 | 250 | 64 | 8 | 25.77 | 0 / 1 / 35 / 218 | 2.66 / 5.28 / 7.54 / 93.83 |
| W1 | 500 | 4 | 4 | 17.44 | 0 / 1 / 65 / 91 | 0.71 / 1.21 / 1.49 / 4.85 |
| W1 | 500 | 16 | 8 | 20.29 | 0 / 0 / 9 / 134 | 1.21 / 2.28 / 2.98 / 51.1 |
| W1 | 500 | 64 | 8 | 38.47 | 0 / 1 / 64 / 515 | 1.79 / 5.37 / 13.69 / 169.74 |
| W2 | 250 | 4 | 4 | 18.29 | 0 / 2 / 35 / 47 | 0.6 / 1.11 / 1.4 / 3.17 |
| W2 | 250 | 16 | 8 | 21.61 | 0 / 0 / 25 / 60 | 0.61 / 1.42 / 1.85 / 4.68 |
| W2 | 250 | 64 | 8 | 30.97 | 0 / 0 / 28 / 103 | 0.71 / 2.82 / 3.84 / 63.45 |
| W2 | 500 | 4 | 4 | 18.72 | 0 / 1 / 62 / 94 | 0.57 / 1.09 / 1.37 / 3.84 |
| W2 | 500 | 16 | 8 | 24.82 | 0 / 0 / 4 / 121 | 0.65 / 1.73 / 2.3 / 17.45 |
| W2 | 500 | 64 | 8 | 53.97 | 0 / 0 / 7 / 187 | 0.67 / 1.58 / 3.31 / 89.62 |
