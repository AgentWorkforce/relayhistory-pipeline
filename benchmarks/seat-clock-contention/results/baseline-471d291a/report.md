## baseline-471d291a

PostgreSQL: PostgreSQL 16.16 (Debian 16.16-1.pgdg13+1); history switch live: true; origin claim fence: true; records per run: 64000; control every 1000 ms; loopback RTT p50 0.03 ms.

### Throughput and failures

| workload | batch | C | drained | drain s | durable rec/s | commits/s | retries/batch | stage failures (stage:SQLSTATE) |
|---|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | yes | 22.2 | 2886 | 11.54 | 0.00 | none |
| W1 | 250 | 16 | yes | 18.8 | 3399 | 13.60 | 0.00 | none |
| W1 | 250 | 64 | yes | 24.8 | 2578 | 10.31 | 0.31 | commit:57014 x79, control:57014 x1 |
| W1 | 500 | 4 | yes | 23.6 | 2707 | 5.41 | 0.00 | none |
| W1 | 500 | 16 | yes | 21.0 | 3052 | 6.10 | 0.00 | none |
| W1 | 500 | 64 | yes | 28.3 | 2263 | 4.53 | 0.87 | commit:57014 x108, commit:55P03 x3, control:57014 x2 |
| W2 | 250 | 4 | yes | 23.8 | 2694 | 10.78 | 0.00 | none |
| W2 | 250 | 16 | yes | 32.9 | 1944 | 7.78 | 0.06 | commit:55P03 x9, prepare:55P03 x7 |
| W2 | 250 | 64 | yes | 47.3 | 1352 | 5.41 | 0.95 | claim:55P03 x121, commit:55P03 x108, prepare:55P03 x15 |
| W2 | 500 | 4 | yes | 24.1 | 2660 | 5.32 | 0.00 | none |
| W2 | 500 | 16 | yes | 27.9 | 2291 | 4.58 | 0.08 | commit:55P03 x7, prepare:55P03 x2, claim:55P03 x1 |
| W2 | 500 | 64 | yes | 50.3 | 1274 | 2.55 | 2.88 | claim:55P03 x231, commit:55P03 x113, prepare:55P03 x17, commit:57014 x8 |

### Clock hold vs wait, commit transactions (ms, p50 / p95 / p99 / max)

| workload | batch | C | hold | wait for clock | commit stage latency (ok) | holds unobserved |
|---|---|---|---|---|---|---|
| W1 | 250 | 4 | 69 / 181 / 288 / 566 | 52 / 283 / 1224 / 1283 | 124 / 391 / 1289 / 1510 | 0 |
| W1 | 250 | 16 | 70 / 86 / 98 / 140 | 864 / 1897 / 2043 / 2076 | 940 / 1983 / 2146 / 2148 | 1 |
| W1 | 250 | 64 | 73 / 124 / 623 / 855 | 3986 / 4997 / 5023 / 5039 | 2207 / 4931 / 4994 / 5005 | 1 |
| W1 | 500 | 4 | 140 / 166 / 666 / 1047 | 106 / 706 / 945 / 1123 | 257 / 986 / 1216 / 1277 | 0 |
| W1 | 500 | 16 | 147 / 200 / 226 / 361 | 908 / 4158 / 4617 / 4653 | 1057 / 4294 / 4748 / 4809 | 0 |
| W1 | 500 | 64 | 147 / 273 / 1430 / 2723 | 4650 / 5008 / 5069 / 5173 | 2530 / 4876 / 4959 / 5040 | 0 |
| W2 | 250 | 4 | 66 / 83 / 105 / 665 | 14 / 81 / 105 / 112 | 101 / 231 / 570 / 740 | 4 |
| W2 | 250 | 16 | 70 / 89 / 113 / 181 | 19 / 748 / 867 / 924 | 476 / 1172 / 2116 / 2421 | 2 |
| W2 | 250 | 64 | 72 / 96 / 137 / 504 | 0 / 3763 / 3867 / 4063 | 1669 / 3865 / 4120 / 4448 | 15 |
| W2 | 500 | 4 | 127 / 152 / 156 / 194 | 5 / 216 / 233 / 250 | 158 / 361 / 411 / 468 | 1 |
| W2 | 500 | 16 | 126 / 167 / 278 / 333 | 0 / 1410 / 1756 / 1805 | 827 / 1718 / 2708 / 2824 | 1 |
| W2 | 500 | 64 | 131 / 170 / 235 / 1296 | 0 / 4785 / 4990 / 4991 | 1695 / 4677 / 4973 / 4988 | 2 |

### Controls during load (fixed rate, one at a time each; latency and hold in ms)

C1 = sessions.ingest_lazy_clear(reset) as owner (the clock FOR UPDATE step). C2 = sessions.ingest_reset_uploaded_sessions as the runtime role (full ingest_apply_ordered_control).

| workload | batch | C | control | attempted | completed | failed (SQLSTATE) | missed slots | latency p50 / p95 / p99 / max | clock hold p50 / max |
|---|---|---|---|---|---|---|---|---|---|
| W1 | 250 | 4 | C1 | 23 | 23 | {} | 1 | 301 / 1146 / 1351 / 1351 | 1.4 / 5.0 |
| W1 | 250 | 16 | C1 | 13 | 13 | {} | 7 | 1493 / 2121 / 2121 / 2121 | 1.2 / 3.7 |
| W1 | 250 | 64 | C1 | 8 | 7 | {"57014":1} | 17 | 2643 / 5001 / 5001 / 5001 | 1.2 / 2.5 |
| W1 | 500 | 4 | C1 | 29 | 29 | {} | 3 | 618 / 1647 / 1654 / 1654 | 1.3 / 8.6 |
| W1 | 500 | 16 | C1 | 7 | 7 | {} | 14 | 2805 / 4333 / 4333 / 4333 | 3.5 / 6.5 |
| W1 | 500 | 64 | C1 | 7 | 5 | {"57014":2} | 22 | 4583 / 4638 / 4638 / 4638 | 3.4 / 25.4 |
| W2 | 250 | 4 | C1 | 35 | 35 | {} | 0 | 70 / 220 / 223 / 223 | 1.2 / 19.6 |
| W2 | 250 | 16 | C1 | 40 | 40 | {} | 0 | 45 / 1361 / 1500 / 1500 | 0.7 / 6.0 |
| W2 | 250 | 64 | C1 | 70 | 70 | {} | 7 | 5 / 816 / 4928 / 4928 | 0.9 / 359.4 |
| W2 | 500 | 4 | C1 | 29 | 29 | {} | 0 | 123 / 963 / 1218 / 1218 | 1.1 / 12.3 |
| W2 | 500 | 16 | C1 | 32 | 32 | {} | 3 | 42 / 1827 / 2599 / 2599 | 1.2 / 12.6 |
| W2 | 500 | 64 | C1 | 47 | 47 | {} | 7 | 3 / 1893 / 4946 / 4946 | 0.7 / 4.1 |

### Lock waits by stage and lock class (ms per transaction that waited, p50 / p95 / p99 / max; n = transactions)

**W1 n=250 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 6 | 6 / 12 / 12 / 12 |
| commit: clock | 228 | 50 / 284 / 1196 / 1318 |
| commit: extend:ingest_lazy_native_slots | 5 | 4 / 6 / 6 / 6 |
| commit: xid(row or unique-insert) | 20 | 1 / 1 / 1 / 1 |
| control: clock | 22 | 279 / 1103 / 1449 / 1449 |
| control: xid(row or unique-insert) | 1 | 1 / 1 / 1 / 1 |

**W1 n=250 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: xid(row or unique-insert) | 12 | 52 / 55 / 55 / 55 |
| commit: clock | 248 | 946 / 1630 / 1756 / 1795 |
| commit: xid(row or unique-insert) | 60 | 1 / 1 / 1 / 1 |
| control: clock | 13 | 1248 / 1802 / 1802 / 1802 |

**W1 n=250 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_claim_attempts_records_gin_idx | 2 | 3 / 3 / 3 / 3 |
| claim: extend:ingest_lazy_native_slots | 59 | 3 / 17 / 17 / 17 |
| claim: xid(row or unique-insert) | 60 | 80 / 102 / 105 / 105 |
| commit: clock | 332 | 3439 / 4243 / 4470 / 4510 |
| commit: extend:ingest_lazy_native_slots | 4 | 6 / 11 / 11 / 11 |
| commit: xid(row or unique-insert) | 94 | 3 / 3 / 3 / 3 |
| control: clock | 8 | 3445 / 4203 / 4203 / 4203 |
| control: xid(row or unique-insert) | 2 | 3 / 3 / 3 / 3 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: clock | 88 | 181 / 707 / 1056 / 1056 |
| commit: xid(row or unique-insert) | 8 | 1 / 1 / 1 / 1 |
| control: clock | 28 | 580 / 1441 / 1444 / 1444 |
| control: xid(row or unique-insert) | 5 | 1 / 1 / 1 / 1 |

**W1 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: xid(row or unique-insert) | 12 | 114 / 129 / 129 / 129 |
| commit: clock | 119 | 1724 / 3460 / 3683 / 3722 |
| commit: xid(row or unique-insert) | 28 | 1 / 1 / 1 / 1 |
| control: clock | 7 | 2422 / 3582 / 3582 / 3582 |

**W1 n=500 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_claim_attempts_records_gin_idx | 12 | 2 / 5 / 5 / 5 |
| claim: extend:ingest_lazy_native_slots | 74 | 5 / 10 / 14 / 14 |
| claim: extend:ingest_lazy_native_slots_pkey | 2 | 2 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 60 | 134 / 234 / 258 / 258 |
| commit: clock | 237 | 2634 / 4555 / 5182 / 5808 |
| commit: extend:ingest_lazy_native_slots | 2 | 5 / 5 / 5 / 5 |
| commit: xid(row or unique-insert) | 94 | 2 / 12 / 14 / 14 |
| control: clock | 7 | 2459 / 6171 / 6171 / 6171 |
| control: xid(row or unique-insert) | 3 | 2 / 14 / 14 / 14 |

**W2 n=250 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 57 | 64 / 101 / 121 / 121 |
| claim: clock | 1 | 3 / 3 / 3 / 3 |
| commit: advisory:origin-claim | 73 | 64 / 96 / 564 / 564 |
| commit: clock | 147 | 32 / 77 / 87 / 113 |
| commit: xid(row or unique-insert) | 10 | 1 / 1 / 1 / 1 |
| control: clock | 26 | 70 / 159 / 163 / 163 |
| control: xid(row or unique-insert) | 2 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 127 | 44 / 93 / 125 / 131 |

**W2 n=250 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 143 | 334 / 766 / 937 / 1012 |
| claim: clock | 1 | 1 / 1 / 1 / 1 |
| claim: xid(row or unique-insert) | 5 | 37 / 38 / 38 / 38 |
| commit: advisory:origin-claim | 136 | 393 / 1214 / 2122 / 2161 |
| commit: clock | 141 | 258 / 417 / 471 / 514 |
| commit: xid(row or unique-insert) | 17 | 1 / 1 / 1 / 1 |
| control: clock | 28 | 312 / 1500 / 1769 / 1769 |
| control: xid(row or unique-insert) | 8 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 144 | 384 / 1989 / 2186 / 2224 |

**W2 n=250 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 371 | 943 / 1911 / 1945 / 1984 |
| claim: extend:ingest_lazy_native_slots | 9 | 1 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 29 | 23 / 30 / 31 / 31 |
| commit: advisory:origin-claim | 235 | 1583 / 1941 / 1986 / 1993 |
| commit: clock | 134 | 470 / 1096 / 1165 / 1189 |
| commit: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: xid(row or unique-insert) | 39 | 1 / 1 / 2 / 2 |
| control: clock | 33 | 51 / 1062 / 1519 / 1519 |
| control: xid(row or unique-insert) | 2 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 251 | 1156 / 1817 / 1962 / 1985 |

**W2 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 62 | 152 / 229 / 296 / 296 |
| claim: clock | 1 | 7 / 7 / 7 / 7 |
| commit: advisory:origin-claim | 3 | 150 / 222 / 222 / 222 |
| commit: clock | 66 | 95 / 200 / 212 / 212 |
| commit: xid(row or unique-insert) | 6 | 1 / 1 / 1 / 1 |
| control: clock | 25 | 120 / 756 / 870 / 870 |
| control: xid(row or unique-insert) | 2 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 63 | 88 / 166 / 192 / 192 |

**W2 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 73 | 709 / 1099 / 1261 / 1261 |
| claim: xid(row or unique-insert) | 5 | 71 / 72 / 72 / 72 |
| commit: advisory:origin-claim | 71 | 704 / 1087 / 1396 / 1396 |
| commit: clock | 67 | 319 / 750 / 847 / 847 |
| commit: xid(row or unique-insert) | 11 | 1 / 1 / 1 / 1 |
| control: clock | 20 | 107 / 1136 / 1136 / 1136 |
| control: xid(row or unique-insert) | 3 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 73 | 630 / 1119 / 1256 / 1256 |

**W2 n=500 C=64**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 424 | 1089 / 1819 / 1895 / 1928 |
| claim: extend:28638 | 2 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_claim_attempts_records_gin_idx | 3 | 1 / 1 / 1 / 1 |
| claim: extend:ingest_lazy_native_slots | 24 | 1 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 29 | 41 / 50 / 52 / 52 |
| commit: advisory:origin-claim | 176 | 1678 / 1973 / 1979 / 2032 |
| commit: clock | 73 | 828 / 1358 / 1390 / 1390 |
| commit: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: xid(row or unique-insert) | 18 | 1 / 1 / 1 / 1 |
| control: clock | 23 | 53 / 1036 / 1478 / 1478 |
| control: xid(row or unique-insert) | 7 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 194 | 1073 / 1775 / 2000 / 2045 |

### Top wait-graph edges (samples: waiter stage -> lock [mode] <- blocker stages)

**W1 n=250 C=4** (sampler 29615 samples, period p50 0.73 ms)

- 15662: commit -> clock [ShareLock] <- commit
- 6358: commit -> clock [ExclusiveLock] <- control
- 4337: control -> clock [ShareLock] <- commit
- 4011: control -> clock [ShareLock] <- claim
- 3961: commit -> clock [ExclusiveLock] <- commit+control
- 2827: control -> clock [AccessExclusiveLock] <- commit
- 2065: commit -> clock [ExclusiveLock] <- commit
- 59: commit -> clock [ShareLock] <- control

**W1 n=250 C=16** (sampler 14729 samples, period p50 1.23 ms)

- 78811: commit -> clock [ExclusiveLock] <- commit+control
- 57517: commit -> clock [ExclusiveLock] <- commit
- 15135: commit -> clock [ShareLock] <- commit
- 10337: control -> clock [AccessExclusiveLock] <- commit
- 2188: commit -> clock [ExclusiveLock] <- control
- 1619: control -> clock [ShareLock] <- commit
- 668: control -> clock [ShareLock] <- claim
- 469: claim -> xid(row or unique-insert) [ShareLock] <- claim

**W1 n=250 C=64** (sampler 8207 samples, period p50 2.84 ms)

- 145233: commit -> clock [ExclusiveLock] <- commit
- 134702: commit -> clock [ExclusiveLock] <- commit+control
- 8803: commit -> clock [ShareLock] <- commit
- 7259: control -> clock [AccessExclusiveLock] <- commit
- 1611: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 652: commit -> clock [ExclusiveLock] <- commit+untagged
- 521: control -> clock [ShareLock] <- commit
- 274: commit -> clock [ExclusiveLock] <- control

**W1 n=500 C=4** (sampler 30314 samples, period p50 0.73 ms)

- 18105: commit -> clock [ExclusiveLock] <- control
- 10880: control -> clock [ShareLock] <- commit
- 10418: control -> clock [ShareLock] <- claim
- 6180: commit -> clock [ShareLock] <- commit
- 5738: commit -> clock [ExclusiveLock] <- commit+control
- 3253: control -> clock [AccessExclusiveLock] <- commit
- 544: commit -> clock [ExclusiveLock] <- commit
- 39: commit -> clock [ShareLock] <- control

**W1 n=500 C=16** (sampler 13941 samples, period p50 1.36 ms)

- 71295: commit -> clock [ExclusiveLock] <- commit+control
- 41512: commit -> clock [ExclusiveLock] <- commit
- 12949: commit -> clock [ShareLock] <- commit
- 9517: control -> clock [AccessExclusiveLock] <- commit
- 3404: commit -> clock [ExclusiveLock] <- control
- 1850: control -> clock [ShareLock] <- commit
- 1554: control -> clock [ShareLock] <- claim
- 965: claim -> xid(row or unique-insert) [ShareLock] <- claim

**W1 n=500 C=64** (sampler 8952 samples, period p50 2.39 ms)

- 145759: commit -> clock [ExclusiveLock] <- commit
- 78876: commit -> clock [ExclusiveLock] <- commit+control
- 9429: commit -> clock [ShareLock] <- commit
- 7557: control -> clock [AccessExclusiveLock] <- commit
- 3714: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 767: commit -> clock [ExclusiveLock] <- control
- 632: control -> clock [ShareLock] <- commit
- 505: commit -> clock [ExclusiveLock] <- commit+untagged

**W2 n=250 C=4** (sampler 36236 samples, period p50 0.59 ms)

- 9467: commit -> advisory:origin-claim [ExclusiveLock] <- claim
- 8672: prepare -> advisory:origin-claim [ExclusiveLock] <- commit
- 6885: commit -> clock [ShareLock] <- commit
- 3249: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 3221: claim -> advisory:origin-claim [ExclusiveLock] <- commit
- 2090: prepare -> advisory:origin-claim [ExclusiveLock] <- claim
- 2011: control -> clock [ShareLock] <- commit
- 1134: control -> clock [ShareLock] <- claim

**W2 n=250 C=16** (sampler 40557 samples, period p50 0.71 ms)

- 35719: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 29750: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 20953: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 20235: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 20228: prepare -> advisory:origin-claim [ExclusiveLock] <- commit+prepare
- 16851: commit -> clock [ExclusiveLock] <- commit
- 16822: commit -> clock [ExclusiveLock] <- commit+control
- 16631: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare

**W2 n=250 C=64** (sampler 47954 samples, period p50 0.78 ms)

- 258668: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 223727: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 195624: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 147399: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 102018: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 92502: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 61486: claim -> advisory:origin-claim [ExclusiveLock] <- claim+prepare
- 46778: commit -> clock [ExclusiveLock] <- commit

**W2 n=500 C=4** (sampler 36994 samples, period p50 0.58 ms)

- 8344: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 8005: claim -> advisory:origin-claim [ExclusiveLock] <- commit
- 7543: commit -> clock [ShareLock] <- commit
- 6097: prepare -> advisory:origin-claim [ExclusiveLock] <- claim
- 4448: prepare -> advisory:origin-claim [ExclusiveLock] <- commit
- 4064: control -> clock [ShareLock] <- commit
- 3559: control -> clock [ShareLock] <- claim
- 2230: commit -> clock [ExclusiveLock] <- control

**W2 n=500 C=16** (sampler 37849 samples, period p50 0.6 ms)

- 32971: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 24021: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 22429: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 22062: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 18105: commit -> clock [ExclusiveLock] <- commit+control
- 15975: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 15024: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 14298: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+prepare

**W2 n=500 C=64** (sampler 59822 samples, period p50 0.66 ms)

- 262199: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 225261: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 224304: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 213898: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 121735: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 106688: claim -> advisory:origin-claim [ExclusiveLock] <- claim+prepare
- 75534: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 38600: commit -> clock [ExclusiveLock] <- commit

### Client checks

| workload | batch | C | load threads | client CPU s | event-loop lag ms p50 / p95 / p99 / max | sampler period ms p50 / p95 / p99 / max |
|---|---|---|---|---|---|---|
| W1 | 250 | 4 | 4 | 19.4 | 0 / 2 / 38 / 56 | 0.73 / 1.31 / 1.64 / 3.54 |
| W1 | 250 | 16 | 8 | 20.72 | 0 / 1 / 34 / 81 | 1.23 / 2.14 / 2.7 / 36.49 |
| W1 | 250 | 64 | 8 | 26.52 | 0 / 1 / 34 / 183 | 2.84 / 5.17 / 7.12 / 82.37 |
| W1 | 500 | 4 | 4 | 18.91 | 0 / 1 / 61 / 98 | 0.73 / 1.33 / 1.65 / 4.4 |
| W1 | 500 | 16 | 8 | 21.39 | 0 / 0 / 10 / 136 | 1.36 / 2.45 / 3.13 / 51.98 |
| W1 | 500 | 64 | 8 | 38.38 | 0 / 1 / 65 / 473 | 2.39 / 5.9 / 15.88 / 108.14 |
| W2 | 250 | 4 | 4 | 18.73 | 0 / 2 / 35 / 51 | 0.59 / 1.13 / 1.42 / 4.29 |
| W2 | 250 | 16 | 8 | 24.29 | 0 / 0 / 24 / 61 | 0.71 / 1.6 / 2.02 / 4.32 |
| W2 | 250 | 64 | 8 | 35.45 | 0 / 0 / 23 / 109 | 0.78 / 2.32 / 3.61 / 69.5 |
| W2 | 500 | 4 | 4 | 18.26 | 0 / 1 / 64 / 96 | 0.58 / 1.11 / 1.43 / 5.1 |
| W2 | 500 | 16 | 8 | 22.86 | 0 / 0 / 5 / 124 | 0.6 / 1.51 / 2.02 / 4.34 |
| W2 | 500 | 64 | 8 | 45.26 | 0 / 0 / 8 / 207 | 0.66 / 1.71 / 3.15 / 93.07 |
