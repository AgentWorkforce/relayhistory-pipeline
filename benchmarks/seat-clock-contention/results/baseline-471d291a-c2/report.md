## baseline-471d291a-c2

PostgreSQL: PostgreSQL 16.16 (Debian 16.16-1.pgdg13+1); history switch live: true; origin claim fence: true; records per run: 64000; control every 1000 ms; loopback RTT p50 0.03 ms.

Tables only. Definitions, the qualified records/s reading (250 vs 500 is about equal only where retries are rare; the retry-heavy C64 cells differ), lock evidence and evidence gaps: ../comparison-runs-1-3.md.

### Throughput and failures

| workload | batch | C | drained | drain s | durable rec/s | commits/s | retries/batch | stage failures (stage:SQLSTATE) |
|---|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | yes | 19.4 | 3300 | 13.20 | 0.00 | none |
| W1 | 250 | 16 | yes | 20.4 | 3130 | 12.52 | 0.00 | none |
| W1 | 250 | 64 | yes | 23.6 | 2711 | 10.84 | 0.28 | commit:57014 x71, control2:57014 x1 |
| W1 | 500 | 4 | yes | 31.7 | 2018 | 4.04 | 0.00 | control2:55P03 x2 |
| W1 | 500 | 16 | yes | 21.3 | 3002 | 6.00 | 0.00 | none |
| W1 | 500 | 64 | yes | 25.8 | 2478 | 4.96 | 0.70 | commit:57014 x89, control2:57014 x2, commit:55P03 x1, control2:55P03 x1 |
| W2 | 250 | 4 | yes | 26.3 | 2436 | 9.75 | 0.00 | prepare:55P03 x1 |
| W2 | 250 | 16 | yes | 25.8 | 2483 | 9.93 | 0.03 | commit:55P03 x4, claim:55P03 x2, prepare:55P03 x2 |
| W2 | 250 | 64 | yes | 28.2 | 2273 | 9.09 | 0.41 | claim:55P03 x70, commit:55P03 x27, prepare:55P03 x7 |
| W2 | 500 | 4 | yes | 28.3 | 2265 | 4.53 | 0.00 | control2:55P03 x1 |
| W2 | 500 | 16 | yes | 34.7 | 1844 | 3.69 | 0.14 | commit:55P03 x10, claim:55P03 x5, prepare:55P03 x3 |
| W2 | 500 | 64 | yes | 55.8 | 1147 | 2.29 | 3.21 | claim:55P03 x252, commit:55P03 x118, prepare:55P03 x31, commit:57014 x10 |

### Clock hold vs wait, commit transactions (ms, p50 / p95 / p99 / max)

Sampled: hold start and wait come from the lock sampler (period p50 per cell in the last table, about 0.6-2.8 ms); hold end is the COMMIT completion. Coverage = observed holds (commit transactions of any outcome, incl. ones that later failed 57014) / committed commits.

| workload | batch | C | hold | wait for clock | commit stage latency (ok) | observed holds / committed | sample period p50 |
|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | 68 / 82 / 86 / 103 | 43 / 258 / 483 / 539 | 115 / 324 / 555 / 598 | 256/256 | 0.72 ms |
| W1 | 250 | 16 | 71 / 86 / 403 / 518 | 914 / 2067 / 2471 / 2744 | 983 / 2202 / 2533 / 2823 | 256/256 | 1.18 ms |
| W1 | 250 | 64 | 71 / 109 / 547 / 1344 | 3481 / 4996 / 5014 / 5035 | 2262 / 4947 / 4994 / 5014 | 271/256 | 2.12 ms |
| W1 | 500 | 4 | 130 / 159 / 196 / 211 | 63 / 551 / 1056 / 1335 | 207 / 705 / 1205 / 1451 | 128/128 | 0.62 ms |
| W1 | 500 | 16 | 136 / 188 / 766 / 810 | 1298 / 3493 / 3645 / 3680 | 1444 / 3641 / 3790 / 3804 | 128/128 | 1.12 ms |
| W1 | 500 | 64 | 134 / 413 / 1341 / 1460 | 4207 / 5000 / 5043 / 5074 | 2324 / 4857 / 4956 / 5002 | 131/128 | 1.42 ms |
| W2 | 250 | 4 | 65 / 82 / 129 / 2092 | 6 / 79 / 122 / 206 | 101 / 225 / 322 / 2117 | 256/256 | 0.57 ms |
| W2 | 250 | 16 | 62 / 80 / 88 / 91 | 9 / 763 / 925 / 995 | 447 / 1272 / 2157 / 2432 | 256/256 | 0.64 ms |
| W2 | 250 | 64 | 64 / 95 / 266 / 467 | 0 / 3601 / 3929 / 3991 | 1445 / 3704 / 4031 / 4724 | 256/256 | 0.73 ms |
| W2 | 500 | 4 | 127 / 157 / 165 / 175 | 12 / 146 / 231 / 240 | 168 / 425 / 485 / 1372 | 128/128 | 0.54 ms |
| W2 | 500 | 16 | 126 / 158 / 169 / 171 | 0 / 1861 / 2331 / 2440 | 821 / 2259 / 2576 / 3122 | 128/128 | 0.53 ms |
| W2 | 500 | 64 | 129 / 217 / 510 / 1110 | 0 / 4384 / 4992 / 4994 | 1725 / 4094 / 4705 / 4937 | 130/128 | 0.67 ms |

### Controls during load (fixed rate, one at a time; latency and hold in ms)

C1 (diagnostic) = sessions.ingest_lazy_clear(reset) as owner, the clock FOR UPDATE step alone. C2 (acceptance) = ledger.applyOrderedControl -> ingest_set_session_uploads -> ingest_apply_ordered_control, runtime role, 2 s / 5 s. One control kind per run.
Completed during load = finished before the last load worker stopped; after load end = finished only once uploads had stopped (drain-only).

| workload | batch | C | control | slots scheduled | missed slots | started | completed during load | completed after load end | failed (SQLSTATE) | latency p50 / p95 / p99 / max | clock hold p50 / max |
|---|---|---|---|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | C2 | 19 | 0 | 19 | 19 | 0 | {} | 386 / 1174 / 1174 / 1174 | 1.7 / 4.5 |
| W1 | 250 | 16 | C2 | 21 | 9 | 12 | 12 | 0 | {} | 2019 / 2258 / 2258 / 2258 | 1.9 / 16.1 |
| W1 | 250 | 64 | C2 | 23 | 16 | 7 | 6 | 0 | {"57014":1} | 3254 / 4934 / 4934 / 4934 | 2.3 / 12.2 |
| W1 | 500 | 4 | C2 | 32 | 6 | 26 | 24 | 0 | {"55P03":2} | 547 / 1552 / 5016 / 5016 | 1.7 / 5011.1 |
| W1 | 500 | 16 | C2 | 21 | 11 | 10 | 10 | 0 | {} | 2172 / 4133 / 4133 / 4133 | 2.2 / 7.0 |
| W1 | 500 | 64 | C2 | 25 | 18 | 7 | 4 | 0 | {"57014":2,"55P03":1} | 4626 / 4762 / 4762 / 4762 | 2.3 / 2.3 |
| W2 | 250 | 4 | C2 | 28 | 0 | 28 | 28 | 0 | {} | 52 / 323 / 1381 / 1381 | 1.5 / 7.8 |
| W2 | 250 | 16 | C2 | 30 | 0 | 30 | 30 | 0 | {} | 70 / 961 / 995 / 995 | 1.5 / 2.6 |
| W2 | 250 | 64 | C2 | 28 | 6 | 22 | 21 | 1 | {} | 42 / 3470 / 3838 / 3838 | 1.2 / 51.5 |
| W2 | 500 | 4 | C2 | 29 | 1 | 28 | 27 | 0 | {"55P03":1} | 106 / 527 / 1216 / 1216 | 1.7 / 36.9 |
| W2 | 500 | 16 | C2 | 36 | 3 | 33 | 33 | 0 | {} | 81 / 2060 / 2529 / 2529 | 1.4 / 4.6 |
| W2 | 500 | 64 | C2 | 55 | 6 | 49 | 49 | 0 | {} | 10 / 2108 / 4008 / 4008 | 1.4 / 9.1 |

### Lock waits by stage and lock class (ms per transaction that waited, p50 / p95 / p99 / max; n = transactions)

**W1 n=250 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 195 | 51 / 269 / 490 / 528 |
| commit: xid(row or unique-insert) | 16 | 1 / 1 / 1 / 1 |
| control2: clock | 19 | 326 / 1077 / 1077 / 1077 |
| control2: xid(row or unique-insert) | 2 | 1 / 1 / 1 / 1 |

**W1 n=250 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: clock | 1 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 12 | 52 / 58 / 58 / 58 |
| commit: clock | 246 | 919 / 1854 / 2228 / 2485 |
| commit: xid(row or unique-insert) | 55 | 1 / 1 / 1 / 1 |
| control2: clock | 12 | 1584 / 1958 / 1958 / 1958 |
| control2: xid(row or unique-insert) | 2 | 1 / 1 / 1 / 1 |

**W1 n=250 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:60579 | 2 | 2 / 2 / 2 / 2 |
| claim: extend:ingest_claim_attempts_records_gin_idx | 21 | 4 / 6 / 8 / 8 |
| claim: extend:ingest_lazy_native_slots | 35 | 2 / 6 / 6 / 6 |
| claim: extend:ingest_lazy_native_slots_pkey | 4 | 2 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 60 | 53 / 66 / 66 / 66 |
| commit: clock | 321 | 2400 / 3119 / 6072 / 6108 |
| commit: extend:ingest_lazy_native_slots | 2 | 4 / 4 / 4 / 4 |
| commit: xid(row or unique-insert) | 119 | 2 / 2 / 2 / 4 |
| control2: clock | 7 | 2860 / 4380 / 4380 / 4380 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 93 | 81 / 578 / 1125 / 1125 |
| commit: xid(row or unique-insert) | 5 | 1 / 1 / 1 / 1 |
| control2: clock | 23 | 505 / 2352 / 2393 / 2393 |
| control2: xid(row or unique-insert) | 4 | 1 / 1 / 1 / 1 |

**W1 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: xid(row or unique-insert) | 12 | 1079 / 1090 / 1090 / 1090 |
| commit: clock | 124 | 1275 / 2820 / 3238 / 3305 |
| commit: xid(row or unique-insert) | 28 | 1 / 1 / 1 / 1 |
| control2: clock | 9 | 1766 / 3487 / 3487 / 3487 |
| control2: xid(row or unique-insert) | 2 | 264 / 264 / 264 / 264 |

**W1 n=500 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 55 | 3 / 7 / 9 / 9 |
| claim: extend:ingest_lazy_native_slots_pkey | 12 | 1 / 4 / 4 / 4 |
| claim: xid(row or unique-insert) | 60 | 64 / 95 / 98 / 98 |
| commit: clock | 217 | 1593 / 3831 / 4670 / 4943 |
| commit: extend:ingest_lazy_native_slots | 1 | 3 / 3 / 3 / 3 |
| commit: xid(row or unique-insert) | 51 | 1 / 1 / 1 / 1 |
| control2: clock | 7 | 1722 / 4567 / 4567 / 4567 |
| control2: xid(row or unique-insert) | 2 | 72 / 72 / 72 / 72 |

**W2 n=250 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 66 | 63 / 117 / 157 / 157 |
| claim: extend:ingest_lazy_native_slots | 2 | 23 / 23 / 23 / 23 |
| claim: extend:ingest_lazy_native_slots_pkey | 1 | 203 / 203 / 203 / 203 |
| commit: advisory:origin-claim | 70 | 68 / 99 / 126 / 126 |
| commit: clock | 135 | 34 / 83 / 112 / 188 |
| commit: xid(row or unique-insert) | 10 | 1 / 1 / 1 / 1 |
| control2: clock | 21 | 71 / 231 / 1319 / 1319 |
| control2: xid(row or unique-insert) | 4 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 127 | 43 / 105 / 207 / 1924 |

**W2 n=250 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 135 | 302 / 867 / 1069 / 1069 |
| claim: clock | 1 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 5 | 27 / 35 / 35 / 35 |
| commit: advisory:origin-claim | 131 | 370 / 915 / 1083 / 1142 |
| commit: clock | 136 | 248 / 417 / 484 / 484 |
| commit: xid(row or unique-insert) | 19 | 1 / 1 / 1 / 1 |
| control2: clock | 25 | 172 / 497 / 696 / 696 |
| control2: xid(row or unique-insert) | 5 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 134 | 325 / 528 / 1082 / 1199 |

**W2 n=250 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 231 | 703 / 1497 / 1662 / 1751 |
| claim: clock | 1 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_lazy_native_slots | 10 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_lazy_native_slots_pkey | 1 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 29 | 23 / 28 / 28 / 28 |
| commit: advisory:origin-claim | 155 | 1113 / 1750 / 1850 / 1859 |
| commit: clock | 134 | 524 / 1179 / 1260 / 1286 |
| commit: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: xid(row or unique-insert) | 48 | 1 / 1 / 1 / 1 |
| control2: clock | 18 | 61 / 1205 / 1205 / 1205 |
| control2: xid(row or unique-insert) | 4 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 162 | 1102 / 1645 / 1774 / 1784 |

**W2 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 53 | 149 / 215 / 227 / 227 |
| claim: clock | 1 | 20 / 20 / 20 / 20 |
| commit: advisory:origin-claim | 14 | 175 / 1129 / 1129 / 1129 |
| commit: clock | 67 | 75 / 153 / 197 / 197 |
| commit: xid(row or unique-insert) | 7 | 1 / 1 / 1 / 1 |
| control2: clock | 22 | 147 / 1097 / 2411 / 2411 |
| control2: xid(row or unique-insert) | 3 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 62 | 93 / 165 / 184 / 184 |

**W2 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 81 | 709 / 1669 / 1916 / 1916 |
| claim: xid(row or unique-insert) | 5 | 66 / 90 / 90 / 90 |
| commit: advisory:origin-claim | 74 | 712 / 1660 / 1882 / 1882 |
| commit: clock | 64 | 251 / 949 / 1000 / 1000 |
| commit: xid(row or unique-insert) | 15 | 1 / 1 / 1 / 1 |
| control2: clock | 22 | 120 / 1035 / 1166 / 1166 |
| control2: xid(row or unique-insert) | 3 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 77 | 739 / 1409 / 1781 / 1781 |

**W2 n=500 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 464 | 1313 / 1872 / 1972 / 1991 |
| claim: clock | 1 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_lazy_native_slots | 12 | 1 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 29 | 40 / 52 / 52 / 52 |
| commit: advisory:origin-claim | 181 | 1715 / 1912 / 1973 / 2213 |
| commit: clock | 73 | 583 / 1305 / 1311 / 1311 |
| commit: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: xid(row or unique-insert) | 16 | 1 / 1 / 1 / 1 |
| control2: clock | 28 | 42 / 906 / 1062 / 1062 |
| control2: xid(row or unique-insert) | 12 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 212 | 1151 / 1772 / 1905 / 2111 |

### Top wait-graph edges (samples: waiter stage -> lock [mode] <- blocker stages)

**W1 n=250 C=4** (sampler 25721 samples, period p50 0.72 ms)

- 10397: commit -> clock [ShareLock] <- commit
- 7330: commit -> clock [ExclusiveLock] <- control2
- 6545: control2 -> clock [ShareLock] <- claim
- 2913: control2 -> clock [ShareLock] <- commit
- 919: commit -> clock [ExclusiveLock] <- commit+control2
- 755: commit -> clock [ExclusiveLock] <- commit
- 598: control2 -> clock [AccessExclusiveLock] <- commit
- 48: commit -> clock [ShareLock] <- control2

**W1 n=250 C=16** (sampler 16148 samples, period p50 1.18 ms)

- 78559: commit -> clock [ExclusiveLock] <- commit+control2
- 65300: commit -> clock [ExclusiveLock] <- commit
- 16572: commit -> clock [ShareLock] <- commit
- 10972: control2 -> clock [AccessExclusiveLock] <- commit
- 2818: commit -> clock [ExclusiveLock] <- control2
- 2212: control2 -> clock [ShareLock] <- commit
- 670: control2 -> clock [ShareLock] <- claim
- 522: claim -> xid(row or unique-insert) [ShareLock] <- claim

**W1 n=250 C=64** (sampler 9615 samples, period p50 2.12 ms)

- 137065: commit -> clock [ExclusiveLock] <- commit
- 118766: commit -> clock [ExclusiveLock] <- commit+control2
- 9322: commit -> clock [ShareLock] <- commit
- 7949: control2 -> clock [AccessExclusiveLock] <- commit
- 1418: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 501: commit -> clock [ExclusiveLock] <- control2
- 483: control2 -> clock [ShareLock] <- commit
- 272: commit -> clock [ExclusiveLock] <- commit+control2+untagged

**W1 n=500 C=4** (sampler 47009 samples, period p50 0.62 ms)

- 17044: control2 -> clock [ShareLock] <- claim
- 11562: commit -> clock [ExclusiveLock] <- control2
- 7204: commit -> clock [ShareLock] <- commit
- 4852: control2 -> clock [ShareLock] <- commit
- 3480: commit -> clock [ExclusiveLock] <- commit+control2
- 1835: control2 -> clock [AccessExclusiveLock] <- commit
- 977: commit -> clock [ExclusiveLock] <- commit
- 44: commit -> clock [ShareLock] <- control2

**W1 n=500 C=16** (sampler 18076 samples, period p50 1.12 ms)

- 70634: commit -> clock [ExclusiveLock] <- commit+control2
- 49262: commit -> clock [ExclusiveLock] <- commit
- 15481: commit -> clock [ShareLock] <- commit
- 11873: control2 -> clock [AccessExclusiveLock] <- commit
- 11554: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 2772: commit -> clock [ExclusiveLock] <- control2
- 1938: control2 -> clock [ShareLock] <- commit
- 1021: control2 -> clock [ShareLock] <- claim

**W1 n=500 C=64** (sampler 10682 samples, period p50 1.42 ms)

- 117234: commit -> clock [ExclusiveLock] <- commit
- 93528: commit -> clock [ExclusiveLock] <- commit+control2
- 9241: commit -> clock [ShareLock] <- commit
- 7688: control2 -> clock [AccessExclusiveLock] <- commit
- 2643: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 1697: control2 -> clock [ShareLock] <- commit
- 1603: commit -> clock [ExclusiveLock] <- control2
- 569: commit -> clock [ExclusiveLock] <- commit+control2+untagged

**W2 n=250 C=4** (sampler 42852 samples, period p50 0.57 ms)

- 8467: prepare -> advisory:origin-claim [ExclusiveLock] <- commit
- 8401: commit -> advisory:origin-claim [ExclusiveLock] <- claim
- 7410: commit -> clock [ShareLock] <- commit
- 5488: prepare -> advisory:origin-claim [ExclusiveLock] <- claim
- 3893: claim -> advisory:origin-claim [ExclusiveLock] <- commit
- 3811: control2 -> clock [ShareLock] <- commit
- 3581: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 1406: control2 -> clock [ShareLock] <- claim

**W2 n=250 C=16** (sampler 34357 samples, period p50 0.64 ms)

- 24751: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 22077: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 19489: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 19469: commit -> clock [ExclusiveLock] <- commit+control2
- 16431: prepare -> advisory:origin-claim [ExclusiveLock] <- commit+prepare
- 16284: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 16139: commit -> clock [ExclusiveLock] <- commit
- 15116: claim -> advisory:origin-claim [ExclusiveLock] <- claim

**W2 n=250 C=64** (sampler 26674 samples, period p50 0.73 ms)

- 152676: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 147103: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 141929: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 54776: commit -> clock [ExclusiveLock] <- commit
- 46265: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 39539: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 36385: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 34714: commit -> clock [ExclusiveLock] <- commit+control2

**W2 n=500 C=4** (sampler 47296 samples, period p50 0.54 ms)

- 9023: control2 -> clock [ShareLock] <- claim
- 7373: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 6899: commit -> clock [ShareLock] <- commit
- 6716: claim -> advisory:origin-claim [ExclusiveLock] <- commit
- 6149: prepare -> advisory:origin-claim [ExclusiveLock] <- commit
- 5673: commit -> advisory:origin-claim [ExclusiveLock] <- claim
- 5061: prepare -> advisory:origin-claim [ExclusiveLock] <- claim
- 2693: control2 -> clock [ShareLock] <- commit

**W2 n=500 C=16** (sampler 51838 samples, period p50 0.53 ms)

- 50235: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 37673: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 27331: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 27252: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 25304: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 20825: commit -> advisory:origin-claim [ExclusiveLock] <- claim
- 20679: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 18477: commit -> clock [ExclusiveLock] <- commit+control2

**W2 n=500 C=64** (sampler 65123 samples, period p50 0.67 ms)

- 357134: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 266464: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 223014: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 205083: claim -> advisory:origin-claim [ExclusiveLock] <- claim+prepare
- 172837: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 78701: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 69610: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 39547: commit -> clock [ExclusiveLock] <- commit

### Client checks

| workload | batch | C | load threads | client CPU s | event-loop lag ms p50 / p95 / p99 / max | sampler period ms p50 / p95 / p99 / max |
|---|---|---|---|---|---|---|
| W1 | 250 | 4 | 4 | 18.41 | 0 / 2 / 35 / 47 | 0.72 / 1.31 / 1.62 / 3.72 |
| W1 | 250 | 16 | 8 | 20.49 | 0 / 0 / 31 / 77 | 1.18 / 2.16 / 2.71 / 40.93 |
| W1 | 250 | 64 | 8 | 25.17 | 0 / 1 / 32 / 140 | 2.12 / 5.16 / 6.4 / 97.96 |
| W1 | 500 | 4 | 4 | 19.34 | 0 / 1 / 63 / 99 | 0.62 / 1.19 / 1.51 / 4.69 |
| W1 | 500 | 16 | 8 | 20.39 | 0 / 0 / 8 / 155 | 1.12 / 2.22 / 2.78 / 50.22 |
| W1 | 500 | 64 | 8 | 32.74 | 0 / 1 / 63 / 375 | 1.42 / 5.25 / 11.98 / 120.1 |
| W2 | 250 | 4 | 4 | 18.86 | 0 / 2 / 34 / 46 | 0.57 / 1.06 / 1.36 / 4.04 |
| W2 | 250 | 16 | 8 | 21.06 | 0 / 0 / 26 / 60 | 0.64 / 1.53 / 1.96 / 3.85 |
| W2 | 250 | 64 | 8 | 24.88 | 0 / 0 / 27 / 105 | 0.73 / 2.58 / 3.64 / 69.79 |
| W2 | 500 | 4 | 4 | 18.44 | 0 / 1 / 58 / 89 | 0.54 / 1.05 / 1.37 / 3.28 |
| W2 | 500 | 16 | 8 | 23.65 | 0 / 0 / 2 / 105 | 0.53 / 1.41 / 1.84 / 4.4 |
| W2 | 500 | 64 | 8 | 48.94 | 0 / 0 / 7 / 242 | 0.67 / 1.65 / 3.12 / 86.26 |
