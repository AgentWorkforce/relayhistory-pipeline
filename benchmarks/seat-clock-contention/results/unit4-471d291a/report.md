## unit4-471d291a

PostgreSQL: PostgreSQL 16.16 (Debian 16.16-1.pgdg13+1); history switch live: true; origin claim fence: true; records per run: 64000; control every 1000 ms; loopback RTT p50 0.04 ms.

Tables only. Definitions, the qualified records/s reading (250 vs 500 is about equal only where retries are rare; the retry-heavy C64 cells differ), lock evidence and evidence gaps: ../comparison-runs-1-3.md.

### Throughput and failures

| workload | batch | C | drained | drain s | durable rec/s | commits/s | retries/batch | stage failures (stage:SQLSTATE) |
|---|---|---|---|---|---|---|---|---|
| W1 (isolated) | 250 | 4 | yes | 18.5 | 3458 | 13.83 | 0.00 | none |
| W1 (isolated) | 250 | 16 | yes | 18.3 | 3491 | 13.96 | 0.00 | none |
| W1 (isolated) | 500 | 4 | yes | 18.8 | 3405 | 6.81 | 0.00 | none |
| W1 (isolated) | 500 | 16 | yes | 18.5 | 3457 | 6.91 | 0.00 | none |
| W1 (rotA1-none) | 500 | 4 | yes | 17.5 | 3653 | 7.31 | 0.00 | none |
| W1 (rotA1-c2) | 500 | 4 | yes | 23.4 | 2735 | 5.47 | 0.00 | none |
| W1 (rotB1-c2) | 500 | 4 | yes | 23.7 | 2697 | 5.39 | 0.01 | commit:55P03 x1 |
| W1 (rotB1-none) | 500 | 4 | yes | 19.5 | 3278 | 6.56 | 0.00 | none |
| W1 (rotA2-none) | 500 | 4 | yes | 20.5 | 3121 | 6.24 | 0.00 | none |
| W1 (rotA2-c2) | 500 | 4 | yes | 21.4 | 2984 | 5.97 | 0.00 | none |
| W1 (rotB2-c2) | 500 | 4 | yes | 26.2 | 2442 | 4.88 | 0.02 | commit:55P03 x2 |
| W1 (rotB2-none) | 500 | 4 | yes | 18.8 | 3405 | 6.81 | 0.00 | none |
| W1 (rotA3-none) | 500 | 4 | yes | 20.2 | 3165 | 6.33 | 0.00 | none |
| W1 (rotA3-c2) | 500 | 4 | yes | 23.0 | 2788 | 5.58 | 0.00 | none |
| W1 (rotB3-c2) | 500 | 4 | yes | 22.4 | 2856 | 5.71 | 0.01 | commit:55P03 x1, control2:55P03 x1 |
| W1 (rotB3-none) | 500 | 4 | yes | 18.0 | 3560 | 7.12 | 0.00 | none |
| W1 (busy-off) | 500 | 16 | yes | 22.8 | 2806 | 5.61 | 0.10 | commit:55P03 x9, commit:57014 x4 |
| W1 (busy-on, busy path on) | 500 | 16 | yes | 28.0 | 2289 | 4.58 | 0.12 | commit:57014 x15 |
| W2 (busy-off) | 500 | 16 | yes | 29.8 | 2144 | 4.29 | 0.10 | commit:55P03 x9, prepare:55P03 x2, claim:55P03 x2 |
| W2 (busy-on, busy path on) | 500 | 16 | yes | 40.1 | 1596 | 3.19 | 0.17 | commit:55P03 x12, prepare:55P03 x5, claim:55P03 x5 |

### Clock hold vs wait, commit transactions (ms, p50 / p95 / p99 / max)

Sampled: hold start and wait come from the lock sampler (period p50 per cell in the last table). Hold end = min(client COMMIT completion, last holder sample + period). Hold n includes commits that failed after taking the clock. Clock wait includes commits that failed while waiting (about 2 s or 5 s, the timeouts) and never-waited commits (0), so percentiles near 5 s are censored. Observed holds / committed is not a coverage measure (its numerator includes failed commits); unobserved = successful commit or control transactions with no hold sample.

| workload | batch | C | hold | wait for clock | commit stage latency (ok) | observed holds / committed | unobserved | sample period p50 |
|---|---|---|---|---|---|---|---|---|
| W1 (isolated) | 250 | 4 | 71 / 85 / 91 / 262 | 41 / 83 / 147 / 265 | 112 / 166 / 246 / 347 | 256/256 | 0 | 0.99 ms |
| W1 (isolated) | 250 | 16 | 68 / 85 / 144 / 188 | 672 / 1864 / 1954 / 2054 | 754 / 1941 / 2016 / 2143 | 256/256 | 0 | 1.7 ms |
| W1 (isolated) | 500 | 4 | 144 / 170 / 192 / 270 | 82 / 153 / 246 / 348 | 228 / 306 / 383 / 503 | 128/128 | 0 | 0.95 ms |
| W1 (isolated) | 500 | 16 | 140 / 164 / 224 / 376 | 1259 / 3225 / 3393 / 3476 | 1389 / 3358 / 3539 / 3607 | 128/128 | 0 | 1.6 ms |
| W1 (rotA1-none) | 500 | 4 | 133 / 159 / 169 / 258 | 98 / 158 / 284 / 286 | 231 / 297 / 429 / 546 | 128/128 | 0 | 0.9 ms |
| W1 (rotA1-c2) | 500 | 4 | 131 / 163 / 1063 / 1142 | 98 / 674 / 1210 / 2207 | 234 / 970 / 2310 / 2355 | 128/128 | 0 | 0.82 ms |
| W1 (rotB1-c2) | 500 | 4 | 131 / 163 / 449 / 2054 | 95 / 741 / 1574 / 2000 | 233 / 873 / 1716 / 2251 | 128/128 | 0 | 0.83 ms |
| W1 (rotB1-none) | 500 | 4 | 134 / 159 / 681 / 1522 | 97 / 161 / 1474 / 1490 | 235 / 326 / 1609 / 2160 | 128/128 | 0 | 0.83 ms |
| W1 (rotA2-none) | 500 | 4 | 133 / 163 / 918 / 2230 | 80 / 167 / 309 / 1255 | 216 / 335 / 1394 / 2317 | 128/128 | 0 | 0.84 ms |
| W1 (rotA2-c2) | 500 | 4 | 129 / 159 / 172 / 176 | 55 / 703 / 1174 / 1393 | 190 / 837 / 1301 / 1525 | 128/128 | 0 | 0.8 ms |
| W1 (rotB2-c2) | 500 | 4 | 131 / 168 / 1131 / 1474 | 51 / 1441 / 2351 / 2405 | 192 / 1317 / 2540 / 2752 | 128/128 | 0 | 0.78 ms |
| W1 (rotB2-none) | 500 | 4 | 134 / 160 / 198 / 986 | 51 / 105 / 206 / 306 | 194 / 245 / 448 / 1065 | 128/128 | 0 | 0.84 ms |
| W1 (rotA3-none) | 500 | 4 | 137 / 178 / 899 / 1581 | 89 / 169 / 316 / 749 | 227 / 375 / 930 / 1661 | 128/128 | 0 | 0.83 ms |
| W1 (rotA3-c2) | 500 | 4 | 129 / 162 / 198 / 1697 | 40 / 740 / 1240 / 1573 | 177 / 905 / 1715 / 1986 | 128/128 | 0 | 0.84 ms |
| W1 (rotB3-c2) | 500 | 4 | 135 / 169 / 1133 / 2122 | 111 / 485 / 1192 / 2001 | 253 / 686 / 1331 / 2374 | 128/128 | 0 | 0.84 ms |
| W1 (rotB3-none) | 500 | 4 | 135 / 159 / 175 / 388 | 94 / 158 / 337 / 415 | 230 / 326 / 519 / 569 | 128/128 | 0 | 0.89 ms |
| W1 (busy-off) | 500 | 16 | 144 / 196 / 295 / 2985 | 1189 / 3781 / 4996 / 4997 | 1225 / 3594 / 4002 / 4022 | 128/128 | 0 | 1.69 ms |
| W1 (busy-on, busy path on) | 500 | 16 | 155 / 457 / 1219 / 1674 | 1460 / 4997 / 5005 / 5040 | 1322 / 4446 / 5079 / 5638 | 130/128 | 0 | 1.74 ms |
| W2 (busy-off) | 500 | 16 | 132 / 211 / 574 / 652 | 26 / 1750 / 1957 / 1965 | 859 / 1971 / 2294 / 2474 | 128/128 | 0 | 0.79 ms |
| W2 (busy-on, busy path on) | 500 | 16 | 138 / 183 / 498 / 1455 | 0 / 1504 / 1604 / 1685 | 1039 / 1802 / 2960 / 2999 | 128/128 | 0 | 0.77 ms |

### Controls during load (fixed rate, one at a time; latency and hold in ms)

C1 (diagnostic) = sessions.ingest_lazy_clear(reset) as owner, the clock FOR UPDATE step alone. C2 (acceptance) = ledger.applyOrderedControl -> ingest_set_session_uploads -> ingest_apply_ordered_control, runtime role, 2 s / 5 s. One control kind per run.
Completed during load = finished before the last load worker stopped; after load end = finished only once uploads had stopped (drain-only).

| workload | batch | C | control | slots scheduled | missed slots | started | completed during load | completed after load end | failed (SQLSTATE) | latency p50 / p95 / p99 / max | clock hold p50 / max |
|---|---|---|---|---|---|---|---|---|---|---|---|
| W1 (rotA1-c2) | 500 | 4 | C2 | 25 | 3 | 22 | 22 | 0 | {} | 652 / 1931 / 2523 / 2523 | 1.7 / 13.0 |
| W1 (rotB1-c2) | 500 | 4 | C2 | 23 | 1 | 22 | 22 | 0 | {} | 716 / 1878 / 2201 / 2201 | 1.7 / 14.4 |
| W1 (rotA2-c2) | 500 | 4 | C2 | 22 | 1 | 21 | 21 | 0 | {} | 633 / 1648 / 2047 / 2047 | 1.7 / 2.6 |
| W1 (rotB2-c2) | 500 | 4 | C2 | 30 | 5 | 25 | 25 | 0 | {} | 665 / 2282 / 2393 / 2393 | 1.7 / 7.4 |
| W1 (rotA3-c2) | 500 | 4 | C2 | 22 | 5 | 17 | 17 | 0 | {} | 709 / 2648 / 2648 / 2648 | 3.2 / 8.3 |
| W1 (rotB3-c2) | 500 | 4 | C2 | 28 | 2 | 26 | 25 | 0 | {"55P03":1} | 326 / 801 / 1281 / 1281 | 2.0 / 23.3 |

### Call timing, measured per call (client clock; ms p50 / p95 / max)

Whole-call age = BEGIN sent -> COMMIT returned. Statement = the stage function's SELECT, including every lock wait inside it. COMMIT response = COMMIT sent -> returned. Lock-wait time (the sections above and below) is sampled; these are measured.

| workload | batch | C | stage | n | whole call | statement | COMMIT response |
|---|---|---|---|---|---|---|---|
| W1 (isolated) | 250 | 4 | claim | 256 | 113.0 / 148.3 / 311.6 | 110.4 / 145.3 / 276.8 | 1.9 / 3.6 / 219.8 |
| W1 (isolated) | 250 | 4 | prepare | 256 | 6.8 / 10.0 / 19.7 | 3.8 / 6.3 / 11.1 | 1.8 / 3.3 / 13.3 |
| W1 (isolated) | 250 | 4 | commit | 256 | 112.0 / 165.7 / 347.4 | 110.2 / 162.2 / 344.6 | 1.9 / 3.4 / 189.4 |
| W1 (isolated) | 250 | 16 | claim | 256 | 87.2 / 247.7 / 387.6 | 84.3 / 215.8 / 385.2 | 1.9 / 5.3 / 138.4 |
| W1 (isolated) | 250 | 16 | prepare | 256 | 7.0 / 17.8 / 129.5 | 4.1 / 9.6 / 68.4 | 2.2 / 5.1 / 124.9 |
| W1 (isolated) | 250 | 16 | commit | 256 | 753.5 / 1941.4 / 2142.8 | 751.5 / 1939.9 / 2139.7 | 2.2 / 4.6 / 124.7 |
| W1 (isolated) | 500 | 4 | claim | 128 | 245.2 / 321.2 / 394.5 | 239.5 / 317.3 / 340.2 | 3.0 / 5.7 / 164.5 |
| W1 (isolated) | 500 | 4 | prepare | 128 | 9.2 / 12.9 / 228.9 | 5.6 / 8.4 / 11.4 | 2.5 / 4.8 / 223.3 |
| W1 (isolated) | 500 | 4 | commit | 128 | 228.1 / 305.5 / 503.3 | 224.3 / 296.7 / 501.3 | 2.9 / 5.1 / 106.5 |
| W1 (isolated) | 500 | 16 | claim | 128 | 224.7 / 756.0 / 896.3 | 220.2 / 750.9 / 893.5 | 3.0 / 5.3 / 152.9 |
| W1 (isolated) | 500 | 16 | prepare | 128 | 9.9 / 23.4 / 106.6 | 6.5 / 16.9 / 104.5 | 2.3 / 5.2 / 10.4 |
| W1 (isolated) | 500 | 16 | commit | 128 | 1389.2 / 3357.6 / 3606.7 | 1387.5 / 3353.6 / 3602.2 | 2.5 / 4.9 / 76.7 |
| W1 (rotA1-none) | 500 | 4 | claim | 128 | 208.3 / 251.3 / 404.4 | 204.6 / 244.2 / 399.0 | 2.7 / 4.9 / 167.4 |
| W1 (rotA1-none) | 500 | 4 | prepare | 128 | 8.4 / 12.3 / 19.6 | 5.2 / 7.8 / 15.7 | 2.3 / 4.1 / 10.4 |
| W1 (rotA1-none) | 500 | 4 | commit | 128 | 230.8 / 296.6 / 545.5 | 228.2 / 293.2 / 427.5 | 2.7 / 4.3 / 135.4 |
| W1 (rotA1-c2) | 500 | 4 | claim | 128 | 220.3 / 306.9 / 1458.8 | 213.0 / 292.7 / 882.3 | 2.9 / 28.3 / 817.7 |
| W1 (rotA1-c2) | 500 | 4 | prepare | 128 | 8.4 / 16.9 / 796.7 | 5.2 / 8.4 / 20.2 | 2.5 / 10.6 / 792.7 |
| W1 (rotA1-c2) | 500 | 4 | commit | 128 | 234.2 / 969.9 / 2354.5 | 223.9 / 804.8 / 2338.9 | 2.6 / 14.9 / 955.4 |
| W1 (rotA1-c2) | 500 | 4 | control2 | 22 | 652.5 / 1930.6 / 2522.7 | - | 1.2 / 3.4 / 11.7 |
| W1 (rotB1-c2) | 500 | 4 | claim | 129 | 253.2 / 329.2 / 949.5 | 249.0 / 301.7 / 689.1 | 2.9 / 33.5 / 260.0 |
| W1 (rotB1-c2) | 500 | 4 | prepare | 129 | 9.1 / 15.7 / 246.4 | 5.0 / 8.1 / 12.4 | 2.5 / 10.4 / 233.5 |
| W1 (rotB1-c2) | 500 | 4 | commit | 128 | 233.1 / 873.1 / 2251.4 | 226.2 / 868.6 / 1745.6 | 3.7 / 16.8 / 505.6 |
| W1 (rotB1-c2) | 500 | 4 | control2 | 22 | 715.6 / 1878.1 / 2201.2 | - | 1.2 / 11.3 / 15.1 |
| W1 (rotB1-none) | 500 | 4 | claim | 128 | 209.8 / 254.1 / 1432.4 | 205.3 / 250.3 / 1175.7 | 3.0 / 5.4 / 256.4 |
| W1 (rotB1-none) | 500 | 4 | prepare | 128 | 8.5 / 13.2 / 582.3 | 5.3 / 8.0 / 18.3 | 2.4 / 5.0 / 576.6 |
| W1 (rotB1-none) | 500 | 4 | commit | 128 | 235.3 / 325.6 / 2159.8 | 232.3 / 322.6 / 2157.0 | 2.7 / 5.8 / 76.8 |
| W1 (rotA2-none) | 500 | 4 | claim | 128 | 229.1 / 288.3 / 2361.2 | 225.5 / 281.1 / 383.3 | 3.4 / 8.1 / 2070.4 |
| W1 (rotA2-none) | 500 | 4 | prepare | 128 | 9.1 / 15.2 / 900.5 | 5.3 / 8.5 / 11.6 | 3.1 / 9.1 / 888.7 |
| W1 (rotA2-none) | 500 | 4 | commit | 128 | 216.3 / 334.9 / 2317.4 | 212.7 / 327.1 / 1390.4 | 2.9 / 5.5 / 988.8 |
| W1 (rotA2-c2) | 500 | 4 | claim | 128 | 227.9 / 290.6 / 319.6 | 225.4 / 286.3 / 315.6 | 2.5 / 4.8 / 24.4 |
| W1 (rotA2-c2) | 500 | 4 | prepare | 128 | 8.6 / 11.2 / 72.8 | 5.1 / 7.5 / 10.8 | 2.5 / 4.3 / 67.9 |
| W1 (rotA2-c2) | 500 | 4 | commit | 128 | 189.6 / 837.0 / 1524.5 | 188.3 / 833.1 / 1521.2 | 2.8 / 7.0 / 50.8 |
| W1 (rotA2-c2) | 500 | 4 | control2 | 21 | 633.3 / 1648.3 / 2046.9 | - | 1.2 / 1.4 / 2.1 |
| W1 (rotB2-c2) | 500 | 4 | claim | 130 | 256.4 / 339.8 / 1980.4 | 248.7 / 321.6 / 1586.3 | 2.6 / 33.5 / 909.1 |
| W1 (rotB2-c2) | 500 | 4 | prepare | 130 | 8.7 / 15.4 / 134.7 | 5.1 / 7.6 / 17.6 | 2.7 / 9.0 / 130.8 |
| W1 (rotB2-c2) | 500 | 4 | commit | 128 | 192.3 / 1317.0 / 2752.1 | 188.1 / 1313.9 / 2535.5 | 3.2 / 21.4 / 998.6 |
| W1 (rotB2-c2) | 500 | 4 | control2 | 25 | 664.7 / 2282.3 / 2392.6 | - | 1.3 / 6.9 / 7.2 |
| W1 (rotB2-none) | 500 | 4 | claim | 128 | 261.3 / 332.3 / 1111.9 | 256.8 / 320.2 / 503.8 | 3.0 / 10.1 / 847.0 |
| W1 (rotB2-none) | 500 | 4 | prepare | 128 | 9.3 / 12.9 / 882.8 | 5.2 / 8.0 / 10.5 | 3.2 / 6.3 / 876.5 |
| W1 (rotB2-none) | 500 | 4 | commit | 128 | 194.2 / 245.4 / 1065.5 | 191.7 / 238.4 / 445.4 | 3.0 / 6.9 / 838.8 |
| W1 (rotA3-none) | 500 | 4 | claim | 128 | 224.9 / 298.0 / 1706.4 | 219.9 / 279.7 / 305.9 | 3.0 / 26.6 / 1433.8 |
| W1 (rotA3-none) | 500 | 4 | prepare | 128 | 9.3 / 16.1 / 1491.5 | 5.3 / 8.4 / 10.9 | 2.9 / 11.0 / 1484.8 |
| W1 (rotA3-none) | 500 | 4 | commit | 128 | 227.4 / 374.8 / 1660.8 | 219.1 / 304.6 / 868.1 | 3.2 / 26.6 / 1439.9 |
| W1 (rotA3-c2) | 500 | 4 | claim | 128 | 248.3 / 329.2 / 1845.7 | 245.2 / 314.4 / 470.0 | 3.0 / 9.3 / 1550.5 |
| W1 (rotA3-c2) | 500 | 4 | prepare | 128 | 8.6 / 12.2 / 24.2 | 5.2 / 7.2 / 11.5 | 2.5 / 4.9 / 19.8 |
| W1 (rotA3-c2) | 500 | 4 | commit | 128 | 176.7 / 904.9 / 1986.0 | 171.2 / 882.2 / 1702.9 | 2.9 / 14.8 / 1550.7 |
| W1 (rotA3-c2) | 500 | 4 | control2 | 17 | 708.7 / 2648.3 / 2648.3 | - | 2.7 / 7.9 / 7.9 |
| W1 (rotB3-c2) | 500 | 4 | claim | 129 | 209.4 / 263.5 / 2111.8 | 207.3 / 256.1 / 1313.4 | 2.8 / 9.7 / 1367.0 |
| W1 (rotB3-c2) | 500 | 4 | prepare | 129 | 8.6 / 14.0 / 2015.3 | 5.2 / 7.5 / 10.4 | 2.6 / 6.3 / 2007.1 |
| W1 (rotB3-c2) | 500 | 4 | commit | 128 | 252.8 / 686.4 / 2373.6 | 248.7 / 612.8 / 1323.1 | 2.8 / 23.7 / 1088.5 |
| W1 (rotB3-c2) | 500 | 4 | control2 | 25 | 325.6 / 800.6 / 1281.4 | - | 1.3 / 13.9 / 22.9 |
| W1 (rotB3-none) | 500 | 4 | claim | 128 | 211.9 / 248.4 / 448.1 | 206.1 / 233.8 / 418.0 | 3.7 / 26.0 / 222.6 |
| W1 (rotB3-none) | 500 | 4 | prepare | 128 | 9.2 / 21.5 / 72.9 | 5.3 / 9.1 / 10.8 | 3.3 / 13.3 / 66.8 |
| W1 (rotB3-none) | 500 | 4 | commit | 128 | 229.8 / 325.8 / 568.8 | 225.8 / 294.8 / 558.8 | 3.3 / 10.3 / 262.6 |
| W1 (busy-off) | 500 | 16 | claim | 141 | 228.4 / 854.8 / 3046.6 | 220.2 / 697.3 / 820.7 | 3.2 / 620.7 / 2820.9 |
| W1 (busy-off) | 500 | 16 | prepare | 141 | 11.1 / 92.7 / 123.8 | 7.2 / 17.3 / 107.9 | 2.6 / 10.2 / 102.6 |
| W1 (busy-off) | 500 | 16 | commit | 128 | 1225.0 / 3594.5 / 4021.6 | 1216.2 / 3592.3 / 4019.0 | 3.0 / 11.6 / 2939.5 |
| W1 (busy-on, busy path on) | 500 | 16 | claim | 143 | 257.2 / 1049.6 / 1753.0 | 245.6 / 907.5 / 1097.2 | 3.5 / 193.0 / 1454.9 |
| W1 (busy-on, busy path on) | 500 | 16 | prepare | 143 | 11.8 / 147.9 / 919.2 | 7.1 / 54.1 / 116.6 | 3.1 / 142.4 / 913.2 |
| W1 (busy-on, busy path on) | 500 | 16 | commit | 128 | 1322.4 / 4446.1 / 5638.1 | 1319.3 / 4290.7 / 4936.9 | 3.3 / 177.8 / 1482.3 |
| W2 (busy-off) | 500 | 16 | claim | 139 | 579.3 / 1706.2 / 2246.7 | 540.4 / 1701.3 / 2240.5 | 3.5 / 60.2 / 446.6 |
| W2 (busy-off) | 500 | 16 | prepare | 137 | 295.4 / 1362.8 / 1813.7 | 286.0 / 1339.8 / 1811.8 | 2.0 / 40.5 / 459.8 |
| W2 (busy-off) | 500 | 16 | commit | 128 | 859.0 / 1971.2 / 2474.2 | 845.0 / 1908.0 / 2471.5 | 3.8 / 83.9 / 485.4 |
| W2 (busy-on, busy path on) | 500 | 16 | claim | 145 | 680.2 / 1909.7 / 2595.5 | 677.1 / 1899.9 / 2228.7 | 3.1 / 16.6 / 1150.8 |
| W2 (busy-on, busy path on) | 500 | 16 | prepare | 140 | 367.4 / 1684.8 / 1983.1 | 323.1 / 1641.8 / 1980.9 | 1.4 / 12.4 / 1315.9 |
| W2 (busy-on, busy path on) | 500 | 16 | commit | 128 | 1039.3 / 1801.5 / 2999.0 | 1032.9 / 1759.4 / 2992.3 | 3.5 / 12.9 / 1309.5 |

### Tuples and wrapper-lock evidence

Tuples = distinct (org/workspace/user/origin) of the load streams. Held = transactions seen holding each granted advisory lock in at least one sample, out of the stage's transactions (sampled, so short holds can be missed). Queued commits = samples with two or more commits queued on the clock: how many origin-claim keys they held, and in how many samples two of them held the same key.

**W1 (isolated) 250 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (256 txns): advisory:origin-claim [ExclusiveLock] 256; advisory:erase-gate(961996,50000) [ShareLock] 256; advisory:native-origin [ExclusiveLock] 256 | prepare (256 txns): advisory:origin-claim [ExclusiveLock] 256; advisory:erase-gate(961996,50000) [ShareLock] 256 | commit (256 txns): advisory:origin-claim [ExclusiveLock] 256; advisory:erase-gate(961996,50000) [ShareLock] 256; advisory:native-origin [ExclusiveLock] 256
- queued commits: 277 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (isolated) 250 16**: 16 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-a/ws-a/scb-u8/scb-o8 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-b/ws-a/scb-u9/scb-o9 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-c/ws-a/scb-u10/scb-o10 x1w, org-d/ws-a/scb-u3/scb-o3 x1w, org-d/ws-a/scb-u11/scb-o11 x1w, org-a/ws-a/scb-u4/scb-o4 x1w, org-a/ws-a/scb-u12/scb-o12 x1w, org-b/ws-a/scb-u5/scb-o5 x1w, org-b/ws-a/scb-u13/scb-o13 x1w, org-c/ws-a/scb-u6/scb-o6 x1w, org-c/ws-a/scb-u14/scb-o14 x1w, org-d/ws-a/scb-u7/scb-o7 x1w, org-d/ws-a/scb-u15/scb-o15 x1w); origin ids shared across users: none; control user: none.
- held: claim (256 txns): advisory:origin-claim [ExclusiveLock] 256; advisory:erase-gate(961996,50000) [ShareLock] 256; advisory:native-origin [ExclusiveLock] 256 | prepare (256 txns): advisory:erase-gate(961996,50000) [ShareLock] 256; advisory:origin-claim [ExclusiveLock] 256 | commit (256 txns): advisory:erase-gate(961996,50000) [ShareLock] 256; advisory:origin-claim [ExclusiveLock] 256; advisory:native-origin [ExclusiveLock] 256
- queued commits: 9340 samples with 2+ commits queued on the clock (max 14); distinct origin-claim keys among them p50 12, max 14; samples where two queued commits held the same key: 0

**W1 (isolated) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 323 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (isolated) 500 16**: 16 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-a/ws-a/scb-u8/scb-o8 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-b/ws-a/scb-u9/scb-o9 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-c/ws-a/scb-u10/scb-o10 x1w, org-d/ws-a/scb-u3/scb-o3 x1w, org-d/ws-a/scb-u11/scb-o11 x1w, org-a/ws-a/scb-u4/scb-o4 x1w, org-a/ws-a/scb-u12/scb-o12 x1w, org-b/ws-a/scb-u5/scb-o5 x1w, org-b/ws-a/scb-u13/scb-o13 x1w, org-c/ws-a/scb-u6/scb-o6 x1w, org-c/ws-a/scb-u14/scb-o14 x1w, org-d/ws-a/scb-u7/scb-o7 x1w, org-d/ws-a/scb-u15/scb-o15 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 9555 samples with 2+ commits queued on the clock (max 13); distinct origin-claim keys among them p50 12, max 13; samples where two queued commits held the same key: 0

**W1 (rotA1-none) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 310 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotA1-c2) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: org-a/ws-a/scb-control2/scb-control2-o.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | control2 (22 txns): advisory:erase-gate(961996,50000) [ShareLock] 22; advisory:other(int8) [ExclusiveLock] 22
- queued commits: 3954 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotB1-c2) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: org-a/ws-a/scb-control2/scb-control2-o.
- held: claim (129 txns): advisory:origin-claim [ExclusiveLock] 129; advisory:erase-gate(961996,50000) [ShareLock] 129; advisory:native-origin [ExclusiveLock] 129 | prepare (129 txns): advisory:origin-claim [ExclusiveLock] 129; advisory:erase-gate(961996,50000) [ShareLock] 129 | commit (129 txns): advisory:origin-claim [ExclusiveLock] 129; advisory:erase-gate(961996,50000) [ShareLock] 129; advisory:native-origin [ExclusiveLock] 128 | control2 (22 txns): advisory:erase-gate(961996,50000) [ShareLock] 22; advisory:other(int8) [ExclusiveLock] 22
- queued commits: 4935 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotB1-none) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 2267 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotA2-none) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 511 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotA2-c2) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: org-a/ws-a/scb-control2/scb-control2-o.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | control2 (21 txns): advisory:erase-gate(961996,50000) [ShareLock] 21; advisory:other(int8) [ExclusiveLock] 21
- queued commits: 4514 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotB2-c2) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: org-a/ws-a/scb-control2/scb-control2-o.
- held: claim (130 txns): advisory:origin-claim [ExclusiveLock] 130; advisory:erase-gate(961996,50000) [ShareLock] 130; advisory:native-origin [ExclusiveLock] 130 | prepare (130 txns): advisory:origin-claim [ExclusiveLock] 130; advisory:erase-gate(961996,50000) [ShareLock] 130 | commit (130 txns): advisory:origin-claim [ExclusiveLock] 130; advisory:erase-gate(961996,50000) [ShareLock] 130; advisory:native-origin [ExclusiveLock] 128 | control2 (25 txns): advisory:erase-gate(961996,50000) [ShareLock] 25; advisory:other(int8) [ExclusiveLock] 25
- queued commits: 11585 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotB2-none) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 273 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotA3-none) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 402 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotA3-c2) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: org-a/ws-a/scb-control2/scb-control2-o.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:origin-claim [ExclusiveLock] 128 | commit (128 txns): advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:origin-claim [ExclusiveLock] 128; advisory:native-origin [ExclusiveLock] 128 | control2 (17 txns): advisory:erase-gate(961996,50000) [ShareLock] 17; advisory:other(int8) [ExclusiveLock] 17
- queued commits: 4288 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotB3-c2) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: org-a/ws-a/scb-control2/scb-control2-o.
- held: claim (129 txns): advisory:origin-claim [ExclusiveLock] 129; advisory:erase-gate(961996,50000) [ShareLock] 129; advisory:native-origin [ExclusiveLock] 129 | prepare (129 txns): advisory:origin-claim [ExclusiveLock] 129; advisory:erase-gate(961996,50000) [ShareLock] 129 | commit (129 txns): advisory:origin-claim [ExclusiveLock] 129; advisory:erase-gate(961996,50000) [ShareLock] 129; advisory:native-origin [ExclusiveLock] 128 | control2 (26 txns): advisory:erase-gate(961996,50000) [ShareLock] 26; advisory:other(int8) [ExclusiveLock] 26
- queued commits: 2477 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (rotB3-none) 500 4**: 4 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w); origin ids shared across users: none; control user: none.
- held: claim (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128 | prepare (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128 | commit (128 txns): advisory:origin-claim [ExclusiveLock] 128; advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 417 samples with 2+ commits queued on the clock (max 3); distinct origin-claim keys among them p50 2, max 3; samples where two queued commits held the same key: 0

**W1 (busy-off) 500 16**: 16 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-a/ws-a/scb-u8/scb-o8 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-b/ws-a/scb-u9/scb-o9 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-c/ws-a/scb-u10/scb-o10 x1w, org-d/ws-a/scb-u3/scb-o3 x1w, org-d/ws-a/scb-u11/scb-o11 x1w, org-a/ws-a/scb-u4/scb-o4 x1w, org-a/ws-a/scb-u12/scb-o12 x1w, org-b/ws-a/scb-u5/scb-o5 x1w, org-b/ws-a/scb-u13/scb-o13 x1w, org-c/ws-a/scb-u6/scb-o6 x1w, org-c/ws-a/scb-u14/scb-o14 x1w, org-d/ws-a/scb-u7/scb-o7 x1w, org-d/ws-a/scb-u15/scb-o15 x1w); origin ids shared across users: none; control user: none.
- held: claim (141 txns): advisory:origin-claim [ExclusiveLock] 141; advisory:erase-gate(961996,50000) [ShareLock] 141; advisory:native-origin [ExclusiveLock] 141 | prepare (141 txns): advisory:origin-claim [ExclusiveLock] 141; advisory:erase-gate(961996,50000) [ShareLock] 141 | commit (141 txns): advisory:origin-claim [ExclusiveLock] 141; advisory:erase-gate(961996,50000) [ShareLock] 141; advisory:native-origin [ExclusiveLock] 128
- queued commits: 10445 samples with 2+ commits queued on the clock (max 14); distinct origin-claim keys among them p50 12, max 14; samples where two queued commits held the same key: 0

**W1 (busy-on, busy path on) 500 16**: 16 tuples (org-a/ws-a/scb-u0/scb-o0 x1w, org-a/ws-a/scb-u8/scb-o8 x1w, org-b/ws-a/scb-u1/scb-o1 x1w, org-b/ws-a/scb-u9/scb-o9 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-c/ws-a/scb-u10/scb-o10 x1w, org-d/ws-a/scb-u3/scb-o3 x1w, org-d/ws-a/scb-u11/scb-o11 x1w, org-a/ws-a/scb-u4/scb-o4 x1w, org-a/ws-a/scb-u12/scb-o12 x1w, org-b/ws-a/scb-u5/scb-o5 x1w, org-b/ws-a/scb-u13/scb-o13 x1w, org-c/ws-a/scb-u6/scb-o6 x1w, org-c/ws-a/scb-u14/scb-o14 x1w, org-d/ws-a/scb-u7/scb-o7 x1w, org-d/ws-a/scb-u15/scb-o15 x1w); origin ids shared across users: none; control user: none.
- held: claim (143 txns): advisory:origin-claim [ExclusiveLock] 143; advisory:erase-gate(961996,50000) [ShareLock] 143; advisory:native-origin [ExclusiveLock] 143 | prepare (143 txns): advisory:erase-gate(961996,50000) [ShareLock] 143; advisory:origin-claim [ExclusiveLock] 143 | commit (143 txns): advisory:erase-gate(961996,50000) [ShareLock] 143; advisory:origin-claim [ExclusiveLock] 143; advisory:native-origin [ExclusiveLock] 130
- queued commits: 12572 samples with 2+ commits queued on the clock (max 14); distinct origin-claim keys among them p50 11, max 14; samples where two queued commits held the same key: 0

**W2 (busy-off) 500 16**: 9 tuples (org-a/ws-a/scb-u0/scb-hot x8w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w, org-a/ws-a/scb-u4/scb-o4 x1w, org-b/ws-a/scb-u5/scb-o5 x1w, org-c/ws-a/scb-u6/scb-o6 x1w, org-d/ws-a/scb-u7/scb-o7 x1w, org-a/ws-a/scb-u8/scb-o8 x1w); origin ids shared across users: none; control user: none.
- held: claim (141 txns): advisory:origin-claim [ExclusiveLock] 139; advisory:erase-gate(961996,50000) [ShareLock] 139; advisory:native-origin [ExclusiveLock] 139 | prepare (139 txns): advisory:erase-gate(961996,50000) [ShareLock] 137; advisory:origin-claim [ExclusiveLock] 137 | commit (137 txns): advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:origin-claim [ExclusiveLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 6886 samples with 2+ commits queued on the clock (max 7); distinct origin-claim keys among them p50 5, max 7; samples where two queued commits held the same key: 0

**W2 (busy-on, busy path on) 500 16**: 9 tuples (org-a/ws-a/scb-u0/scb-hot x8w, org-b/ws-a/scb-u1/scb-o1 x1w, org-c/ws-a/scb-u2/scb-o2 x1w, org-d/ws-a/scb-u3/scb-o3 x1w, org-a/ws-a/scb-u4/scb-o4 x1w, org-b/ws-a/scb-u5/scb-o5 x1w, org-c/ws-a/scb-u6/scb-o6 x1w, org-d/ws-a/scb-u7/scb-o7 x1w, org-a/ws-a/scb-u8/scb-o8 x1w); origin ids shared across users: none; control user: none.
- held: claim (150 txns): advisory:origin-claim [ExclusiveLock] 145; advisory:erase-gate(961996,50000) [ShareLock] 145; advisory:native-origin [ExclusiveLock] 145 | prepare (145 txns): advisory:origin-claim [ExclusiveLock] 140; advisory:erase-gate(961996,50000) [ShareLock] 140 | commit (140 txns): advisory:erase-gate(961996,50000) [ShareLock] 128; advisory:origin-claim [ExclusiveLock] 128; advisory:native-origin [ExclusiveLock] 128
- queued commits: 6346 samples with 2+ commits queued on the clock (max 7); distinct origin-claim keys among them p50 5, max 7; samples where two queued commits held the same key: 0


### Lock waits by stage and lock class (ms per transaction that waited, p50 / p95 / p99 / max; n = transactions)

**W1 n=250 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 238 | 42 / 87 / 136 / 187 |
| commit: xid(row or unique-insert) | 12 | 1 / 1 / 1 / 1 |

**W1 n=250 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: xid(row or unique-insert) | 12 | 66 / 70 / 70 / 70 |
| commit: clock | 249 | 763 / 1533 / 1656 / 1710 |
| commit: xid(row or unique-insert) | 40 | 2 / 2 / 3 / 3 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 119 | 82 / 152 / 209 / 302 |
| commit: xid(row or unique-insert) | 10 | 1 / 1 / 1 / 1 |

**W1 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 2 | 2 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 12 | 114 / 125 / 125 / 125 |
| commit: clock | 125 | 1429 / 2614 / 2739 / 2763 |
| commit: extend:ingest_lazy_native_slots | 1 | 2 / 2 / 2 / 2 |
| commit: xid(row or unique-insert) | 22 | 2 / 2 / 2 / 2 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 126 | 89 / 151 / 248 / 280 |
| commit: xid(row or unique-insert) | 8 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 1 | 116 / 116 / 116 / 116 |
| commit: clock | 88 | 122 / 795 / 2121 / 2121 |
| commit: extend:ingest_lazy_native_slots | 1 | 34 / 34 / 34 / 34 |
| commit: xid(row or unique-insert) | 11 | 1 / 1 / 1 / 1 |
| control2: clock | 22 | 505 / 1600 / 2329 / 2329 |
| control2: xid(row or unique-insert) | 7 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 1 | 34 / 34 / 34 / 34 |
| commit: clock | 95 | 110 / 651 / 1842 / 1842 |
| commit: extend:ingest_lazy_native_slots | 1 | 28 / 28 / 28 / 28 |
| commit: xid(row or unique-insert) | 6 | 1 / 1 / 1 / 1 |
| control2: clock | 22 | 605 / 1583 / 2066 / 2066 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 2 | 272 / 272 / 272 / 272 |
| commit: clock | 126 | 90 / 175 / 1531 / 1539 |
| commit: extend:ingest_lazy_native_slots | 2 | 54 / 54 / 54 / 54 |
| commit: xid(row or unique-insert) | 12 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 123 | 75 / 152 / 291 / 1323 |
| commit: xid(row or unique-insert) | 10 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 81 | 158 / 641 / 1270 / 1270 |
| commit: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: xid(row or unique-insert) | 3 | 1 / 1 / 1 / 1 |
| control2: clock | 21 | 563 / 1480 / 1504 / 1504 |
| control2: xid(row or unique-insert) | 6 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: clock | 2 | 7 / 7 / 7 / 7 |
| claim: extend:ingest_lazy_native_slots | 1 | 95 / 95 / 95 / 95 |
| commit: clock | 85 | 140 / 1950 / 2179 / 2179 |
| commit: extend:ingest_lazy_native_slots | 1 | 60 / 60 / 60 / 60 |
| commit: xid(row or unique-insert) | 8 | 1 / 1 / 1 / 1 |
| control2: clock | 24 | 541 / 2083 / 2267 / 2267 |
| control2: xid(row or unique-insert) | 4 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 1 | 1 / 1 / 1 / 1 |
| commit: clock | 114 | 50 / 106 / 171 / 260 |
| commit: xid(row or unique-insert) | 3 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 122 | 81 / 149 / 247 / 856 |
| commit: xid(row or unique-insert) | 11 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: clock | 2 | 5 / 5 / 5 / 5 |
| commit: clock | 77 | 109 / 943 / 1559 / 1559 |
| commit: xid(row or unique-insert) | 7 | 1 / 1 / 1 / 1 |
| control2: clock | 17 | 580 / 2252 / 2252 / 2252 |
| control2: xid(row or unique-insert) | 3 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: clock | 2 | 19 / 19 / 19 / 19 |
| claim: extend:ingest_lazy_native_slots | 1 | 58 / 58 / 58 / 58 |
| claim: xid(row or unique-insert) | 1 | 1 / 1 / 1 / 1 |
| commit: clock | 117 | 101 / 446 / 1116 / 2163 |
| commit: extend:ingest_lazy_native_slots | 1 | 20 / 20 / 20 / 20 |
| commit: xid(row or unique-insert) | 9 | 1 / 1 / 1 / 1 |
| control2: clock | 26 | 292 / 1179 / 2350 / 2350 |
| control2: xid(row or unique-insert) | 3 | 1 / 1 / 1 / 1 |

**W1 n=500 C=4**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| commit: clock | 125 | 90 / 145 / 337 / 408 |
| commit: xid(row or unique-insert) | 6 | 1 / 1 / 1 / 1 |

**W1 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 5 | 2 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 12 | 123 / 135 / 135 / 135 |
| commit: clock | 137 | 1288 / 3089 / 4063 / 4196 |
| commit: extend:ingest_lazy_native_slots | 2 | 2 / 2 / 2 / 2 |
| commit: xid(row or unique-insert) | 17 | 2 / 3 / 3 / 3 |

**W1 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: extend:ingest_lazy_native_slots | 4 | 2 / 2 / 2 / 2 |
| claim: xid(row or unique-insert) | 12 | 134 / 144 / 144 / 144 |
| commit: clock | 140 | 1225 / 4253 / 4571 / 4766 |
| commit: extend:ingest_lazy_native_slots | 1 | 3 / 3 / 3 / 3 |
| commit: xid(row or unique-insert) | 46 | 2 / 753 / 792 / 792 |

**W2 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 76 | 702 / 1315 / 1447 / 1447 |
| claim: xid(row or unique-insert) | 5 | 55 / 84 / 84 / 84 |
| commit: advisory:origin-claim | 72 | 778 / 1176 / 1332 / 1332 |
| commit: clock | 70 | 314 / 916 / 1016 / 1016 |
| commit: xid(row or unique-insert) | 12 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 75 | 661 / 1198 / 1416 / 1416 |

**W2 n=500 C=16**

| stage: lock class | n | p50 / p95 / p99 / max |
|---|---|---|
| claim: advisory:origin-claim | 85 | 694 / 1807 / 2006 / 2006 |
| claim: xid(row or unique-insert) | 5 | 75 / 99 / 99 / 99 |
| commit: advisory:origin-claim | 73 | 926 / 1897 / 2007 / 2007 |
| commit: clock | 67 | 473 / 720 / 809 / 809 |
| commit: xid(row or unique-insert) | 9 | 1 / 1 / 1 / 1 |
| prepare: advisory:origin-claim | 81 | 747 / 1913 / 2118 / 2118 |

### Top wait-graph edges (samples: waiter stage -> lock [mode] <- blocker stages)

**W1 n=250 C=4** (sampler 18061 samples, period p50 0.99 ms)

- 9897: commit -> clock [ShareLock] <- commit
- 303: commit -> clock [ExclusiveLock] <- commit
- 55: commit -> clock [ShareLock] <- untagged
- 13: commit -> clock [ShareLock] <- none
- 12: commit -> xid(row or unique-insert) [ShareLock] <- none
- 1: commit -> clock [ExclusiveLock] <- none

**W1 n=250 C=16** (sampler 10101 samples, period p50 1.7 ms)

- 91890: commit -> clock [ExclusiveLock] <- commit
- 13827: commit -> clock [ShareLock] <- commit
- 447: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 165: commit -> clock [ShareLock] <- untagged
- 39: commit -> xid(row or unique-insert) [ShareLock] <- none
- 35: commit -> clock [ExclusiveLock] <- none
- 9: claim -> xid(row or unique-insert) [ShareLock] <- untagged
- 6: commit -> clock [ShareLock] <- none

**W1 n=500 C=4** (sampler 18598 samples, period p50 0.95 ms)

- 9978: commit -> clock [ShareLock] <- commit
- 390: commit -> clock [ExclusiveLock] <- commit
- 23: commit -> clock [ShareLock] <- untagged
- 10: commit -> xid(row or unique-insert) [ShareLock] <- none
- 2: commit -> clock [ShareLock] <- none

**W1 n=500 C=16** (sampler 10513 samples, period p50 1.6 ms)

- 86068: commit -> clock [ExclusiveLock] <- commit
- 13479: commit -> clock [ShareLock] <- commit
- 807: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 72: commit -> clock [ShareLock] <- untagged
- 22: commit -> xid(row or unique-insert) [ShareLock] <- none
- 13: commit -> clock [ExclusiveLock] <- none
- 4: claim -> xid(row or unique-insert) [ShareLock] <- untagged
- 2: claim -> xid(row or unique-insert) [ShareLock] <- none

**W1 n=500 C=4** (sampler 18513 samples, period p50 0.9 ms)

- 12514: commit -> clock [ShareLock] <- commit
- 326: commit -> clock [ExclusiveLock] <- commit
- 23: commit -> clock [ShareLock] <- untagged
- 8: commit -> xid(row or unique-insert) [ShareLock] <- none
- 4: commit -> clock [ShareLock] <- none
- 1: commit -> clock [ExclusiveLock] <- none

**W1 n=500 C=4** (sampler 26602 samples, period p50 0.82 ms)

- 10801: commit -> clock [ExclusiveLock] <- control2
- 9233: commit -> clock [ShareLock] <- commit
- 6948: control2 -> clock [ShareLock] <- claim
- 5875: control2 -> clock [ShareLock] <- commit
- 3616: control2 -> clock [AccessExclusiveLock] <- commit
- 2010: commit -> clock [ExclusiveLock] <- commit
- 1944: commit -> clock [ExclusiveLock] <- commit+control2
- 141: claim -> extend:ingest_lazy_native_slots [ExclusiveLock] <- commit

**W1 n=500 C=4** (sampler 26321 samples, period p50 0.83 ms)

- 12522: commit -> clock [ExclusiveLock] <- control2
- 10726: control2 -> clock [ShareLock] <- claim
- 8181: commit -> clock [ShareLock] <- commit
- 5870: control2 -> clock [ShareLock] <- commit
- 2542: commit -> clock [ExclusiveLock] <- commit+control2
- 1735: commit -> clock [ExclusiveLock] <- commit
- 1052: control2 -> clock [AccessExclusiveLock] <- commit
- 65: commit -> clock [ShareLock] <- control2

**W1 n=500 C=4** (sampler 21498 samples, period p50 0.83 ms)

- 15449: commit -> clock [ShareLock] <- commit
- 2336: commit -> clock [ExclusiveLock] <- commit
- 436: claim -> extend:ingest_lazy_native_slots [ExclusiveLock] <- commit
- 119: commit -> extend:ingest_lazy_native_slots [ExclusiveLock] <- claim
- 32: commit -> clock [ShareLock] <- untagged
- 12: commit -> xid(row or unique-insert) [ShareLock] <- none
- 8: commit -> clock [ShareLock] <- none
- 4: claim -> extend:ingest_lazy_native_slots [ExclusiveLock] <- none

**W1 n=500 C=4** (sampler 22379 samples, period p50 0.84 ms)

- 12169: commit -> clock [ShareLock] <- commit
- 553: commit -> clock [ExclusiveLock] <- commit
- 22: commit -> clock [ShareLock] <- untagged
- 10: commit -> xid(row or unique-insert) [ShareLock] <- none
- 5: commit -> clock [ShareLock] <- none

**W1 n=500 C=4** (sampler 24843 samples, period p50 0.8 ms)

- 12313: commit -> clock [ExclusiveLock] <- control2
- 8520: control2 -> clock [ShareLock] <- claim
- 5792: control2 -> clock [ShareLock] <- commit
- 4986: commit -> clock [ShareLock] <- commit
- 3779: commit -> clock [ExclusiveLock] <- commit+control2
- 1642: control2 -> clock [AccessExclusiveLock] <- commit
- 511: commit -> clock [ExclusiveLock] <- commit
- 32: control2 -> clock [ShareLock] <- untagged

**W1 n=500 C=4** (sampler 30740 samples, period p50 0.78 ms)

- 16682: commit -> clock [ExclusiveLock] <- control2
- 13133: control2 -> clock [ShareLock] <- claim
- 10344: commit -> clock [ExclusiveLock] <- commit+control2
- 7425: control2 -> clock [ShareLock] <- commit
- 6388: commit -> clock [ShareLock] <- commit
- 3348: control2 -> clock [AccessExclusiveLock] <- commit
- 927: commit -> clock [ExclusiveLock] <- commit
- 120: claim -> extend:ingest_lazy_native_slots [ExclusiveLock] <- commit

**W1 n=500 C=4** (sampler 20666 samples, period p50 0.84 ms)

- 7015: commit -> clock [ShareLock] <- commit
- 346: commit -> clock [ExclusiveLock] <- commit
- 25: commit -> clock [ShareLock] <- untagged
- 3: commit -> clock [ShareLock] <- none
- 3: commit -> xid(row or unique-insert) [ShareLock] <- none
- 1: claim -> extend:ingest_lazy_native_slots [ExclusiveLock] <- none

**W1 n=500 C=4** (sampler 22334 samples, period p50 0.83 ms)

- 12523: commit -> clock [ShareLock] <- commit
- 441: commit -> clock [ExclusiveLock] <- commit
- 23: commit -> clock [ShareLock] <- untagged
- 11: commit -> xid(row or unique-insert) [ShareLock] <- none
- 5: commit -> clock [ShareLock] <- none

**W1 n=500 C=4** (sampler 24501 samples, period p50 0.84 ms)

- 12938: control2 -> clock [ShareLock] <- claim
- 12031: commit -> clock [ExclusiveLock] <- control2
- 6256: commit -> clock [ShareLock] <- commit
- 3856: control2 -> clock [ShareLock] <- commit
- 3696: commit -> clock [ExclusiveLock] <- commit+control2
- 1120: control2 -> clock [AccessExclusiveLock] <- commit
- 636: commit -> clock [ExclusiveLock] <- commit
- 66: commit -> clock [ShareLock] <- control2

**W1 n=500 C=4** (sampler 24507 samples, period p50 0.84 ms)

- 9659: commit -> clock [ExclusiveLock] <- control2
- 9186: commit -> clock [ShareLock] <- commit
- 6378: control2 -> clock [ShareLock] <- commit
- 4862: control2 -> clock [ShareLock] <- claim
- 1451: control2 -> clock [AccessExclusiveLock] <- commit
- 1214: commit -> clock [ExclusiveLock] <- commit+control2
- 990: commit -> clock [ExclusiveLock] <- commit
- 105: commit -> clock [ShareLock] <- control2

**W1 n=500 C=4** (sampler 19258 samples, period p50 0.89 ms)

- 12571: commit -> clock [ShareLock] <- commit
- 485: commit -> clock [ExclusiveLock] <- commit
- 30: commit -> clock [ShareLock] <- untagged
- 6: commit -> xid(row or unique-insert) [ShareLock] <- none
- 4: commit -> clock [ShareLock] <- none

**W1 n=500 C=16** (sampler 12430 samples, period p50 1.69 ms)

- 94504: commit -> clock [ExclusiveLock] <- commit
- 15233: commit -> clock [ShareLock] <- commit
- 865: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 57: commit -> clock [ShareLock] <- untagged
- 20: commit -> clock [ExclusiveLock] <- commit+untagged
- 18: commit -> clock [ExclusiveLock] <- none
- 17: commit -> xid(row or unique-insert) [ShareLock] <- none
- 6: untagged -> clock [ExclusiveLock] <- commit+untagged

**W1 n=500 C=16** (sampler 14750 samples, period p50 1.74 ms)

- 106312: commit -> clock [ExclusiveLock] <- commit
- 17458: commit -> clock [ShareLock] <- commit
- 2990: commit -> xid(row or unique-insert) [ShareLock] <- commit
- 893: claim -> xid(row or unique-insert) [ShareLock] <- claim
- 75: commit -> clock [ShareLock] <- untagged
- 26: commit -> clock [ExclusiveLock] <- commit+untagged
- 25: commit -> xid(row or unique-insert) [ShareLock] <- none
- 12: commit -> clock [ExclusiveLock] <- none

**W2 n=500 C=16** (sampler 31147 samples, period p50 0.79 ms)

- 31874: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 24966: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 22868: commit -> clock [ExclusiveLock] <- commit
- 19171: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 15248: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+prepare
- 14450: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 11081: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 10913: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit

**W2 n=500 C=16** (sampler 44577 samples, period p50 0.77 ms)

- 30713: commit -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 29885: claim -> advisory:origin-claim [ExclusiveLock] <- claim
- 28087: prepare -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 24193: commit -> clock [ExclusiveLock] <- commit
- 20890: commit -> advisory:origin-claim [ExclusiveLock] <- commit
- 17387: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit+prepare
- 16113: claim -> advisory:origin-claim [ExclusiveLock] <- claim+commit
- 15961: commit -> advisory:origin-claim [ExclusiveLock] <- claim

### Client checks

| workload | batch | C | load threads | client CPU s | event-loop lag ms p50 / p95 / p99 / max | sampler period ms p50 / p95 / p99 / max |
|---|---|---|---|---|---|---|
| W1 (isolated) | 250 | 4 | 4 | 18.8 | 0 / 3 / 38 / 51 | 0.99 / 1.58 / 1.98 / 3.51 |
| W1 (isolated) | 250 | 16 | 8 | 19.71 | 0 / 1 / 32 / 86 | 1.7 / 2.96 / 3.57 / 34.95 |
| W1 (isolated) | 500 | 4 | 4 | 18.32 | 0 / 1 / 67 / 96 | 0.95 / 1.59 / 2.02 / 4.25 |
| W1 (isolated) | 500 | 16 | 8 | 19.88 | 0 / 0 / 27 / 138 | 1.6 / 2.86 / 3.58 / 56.04 |
| W1 (rotA1-none) | 500 | 4 | 4 | 17.44 | 0 / 1 / 66 / 99 | 0.9 / 1.48 / 1.9 / 6.25 |
| W1 (rotA1-c2) | 500 | 4 | 4 | 17.53 | 0 / 1 / 61 / 86 | 0.82 / 1.47 / 1.81 / 4.53 |
| W1 (rotB1-c2) | 500 | 4 | 4 | 17.86 | 0 / 1 / 62 / 94 | 0.83 / 1.45 / 1.78 / 4.52 |
| W1 (rotB1-none) | 500 | 4 | 4 | 17.62 | 0 / 1 / 63 / 99 | 0.83 / 1.45 / 1.82 / 3.45 |
| W1 (rotA2-none) | 500 | 4 | 4 | 17.47 | 0 / 1 / 64 / 98 | 0.84 / 1.44 / 1.77 / 3.15 |
| W1 (rotA2-c2) | 500 | 4 | 4 | 17.62 | 0 / 1 / 63 / 93 | 0.8 / 1.47 / 1.81 / 3.93 |
| W1 (rotB2-c2) | 500 | 4 | 4 | 18.3 | 0 / 1 / 57 / 93 | 0.78 / 1.45 / 1.79 / 5.31 |
| W1 (rotB2-none) | 500 | 4 | 4 | 17.47 | 0 / 1 / 65 / 99 | 0.84 / 1.42 / 1.78 / 53.4 |
| W1 (rotA3-none) | 500 | 4 | 4 | 17.82 | 0 / 1 / 64 / 94 | 0.83 / 1.45 / 1.8 / 3.02 |
| W1 (rotA3-c2) | 500 | 4 | 4 | 17.9 | 0 / 1 / 62 / 86 | 0.84 / 1.51 / 1.83 / 3.76 |
| W1 (rotB3-c2) | 500 | 4 | 4 | 18.18 | 0 / 1 / 60 / 98 | 0.84 / 1.53 / 1.85 / 3.77 |
| W1 (rotB3-none) | 500 | 4 | 4 | 17.56 | 0 / 1 / 66 / 89 | 0.89 / 1.45 / 1.75 / 3.19 |
| W1 (busy-off) | 500 | 16 | 8 | 22.57 | 0 / 0 / 9 / 184 | 1.69 / 3.07 / 3.71 / 54.18 |
| W1 (busy-on, busy path on) | 500 | 16 | 8 | 25.27 | 0 / 0 / 8 / 151 | 1.74 / 3.33 / 4.38 / 57.76 |
| W2 (busy-off) | 500 | 16 | 8 | 23.59 | 0 / 0 / 4 / 136 | 0.79 / 2.02 / 2.57 / 4.09 |
| W2 (busy-on, busy path on) | 500 | 16 | 8 | 26.23 | 0 / 0 / 2 / 109 | 0.77 / 1.77 / 2.33 / 17.76 |
