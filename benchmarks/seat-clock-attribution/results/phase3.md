# Phase 3 results: mode p (20261105062250) against the current body and T2 (PostgreSQL 16.16, synthetic cells only)

Two runs on the branch head, same host, same container (`phase3-pg16-a`: bodies in the order main, t2, p per cell;
`phase3-pg16-b`: p, t2, main). 32,000 records per cell, W1/W2 as the baseline (benchmarks/seat-clock-contention at
1befee7a), the baseline's C2 ordered control every 1000 ms in every cell, 2 s / 5 s limits as `bulls96_session_ingest`.
`main` = gate off (the base body's statements), `t2` = fenced/t2, `p` = fenced/p. JIT on (server default). Shared host:
two runs of the same body differ by up to about 40% in single cells (main: W1 n500 C16 130 vs 179 ms), so single-cell differences of 10% are inside that
spread. The paired same-run comparison below is the more stable reading.

Measured span: HOLD = server stamp after the statement that took the clock exclusively (main and p: the clock FOR NO KEY
UPDATE; t2: the upgrade) to client COMMIT completion. CLOCK WAIT = that statement's duration. Commit stmt = the commit
statement as the caller sees it, every attempt (ok or failed). `>=2s` / `>=5s`: attempts reaching the 2 s lock_timeout /
5 s statement_timeout. Ordered controls: completed during load / scheduled slots (failed by SQLSTATE). Pre-plan: mode p's
statement before the clock (it runs while the fence row and (961996,50000) are held), and how many commits used its
candidate list / recomputed it under the lock. Barrier free share: share of 100 ms try-locks of (961996,50000) exclusive
that succeeded (what a setter, down or erase would find).

## Paired summary (same run, same cell)

| run | cells where p's hold p50 < main's | median p/main hold p50 | median t2/main hold p50 | ordered controls completed during load / scheduled (failed): main, p, t2 | durable rec/s summed over cells: main, p, t2 | p commits recomputed | barrier free share: main, p, t2 |
|---|---|---|---|---|---|---|---|
| a | 11 / 12 | 0.918 | 0.766 | 195/243 (5), 181/219 (5), 147/213 (13) | 28771, 31677, 34581 | 0 | 0.039, 0.048, 0.042 |
| b | 11 / 12 | 0.888 | 0.775 | 224/269 (3), 220/260 (4), 135/198 (13) | 26795, 27338, 30396 | 0 | 0.045, 0.054, 0.053 |

Reading, for these synthetic cells only:
- p's hold is about 8-11% below the current body's at the median, in the direction expected from phase 1 (about -15% / -12%
  at n 250 / 500 was the estimate); it is smaller than the spread between two runs of one body.
- Ordered controls complete at the current body's rate under p; under t2 fewer complete and more fail (55P03, 57014).
- p does not bring the deep cells under the budgets: at C=64 commit statements still reach 5 s (57014) in every body.
- Every p commit used its pre-plan (no recompute in either run). The pre-plan statement ran 6.5-11 ms p50 at n 250 and
  12-23 ms p50 at n 500 in the cells up to C=16; at C=64 it ran 10-11 ms p50 (p95 88-93 ms) at n 250 and 32-57 ms p50
  (p95 122-219 ms) at n 500. That time is spent before the clock wait, holding the fence row and (961996,50000) shared,
  and counts against the 5 s statement timeout.

## Every cell

| W | n | C | body | run | hold p50/p95 | clock wait p50/p95 | commit stmt p50/p95 | >=2s | >=5s | durable rec/s | stage failures (SQLSTATE) | ordered controls: completed in load / scheduled (failed) | pre-plan ms p50/p95, used/recomputed | barrier free share |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| W1 | 250 | 2 | main | a | 61/102 | 0.0/50 | 62/110 | 0 | 0 | 1574 | none | 37 / 37 | - | 0.07 |
| W1 | 250 | 2 | main | b | 74/305 | 0.0/66 | 75/342 | 0 | 0 | 1420 | none | 39 / 39 | - | 0.072 |
| W1 | 250 | 2 | t2 | a | 50/85 | 0.1/32 | 69/105 | 0 | 0 | 1061 | none | 33 / 44 (55P03 x5) | - | 0.02 |
| W1 | 250 | 2 | t2 | b | 41/53 | 0.1/0.1 | 58/72 | 0 | 0 | 1977 | none | 22 / 25 (55P03 x1) | - | 0.119 |
| W1 | 250 | 2 | p | a | 56/69 | 0.1/25 | 64/85 | 0 | 0 | 2435 | none | 23 / 23 | 6.5/9.2, 128/0 | 0.054 |
| W1 | 250 | 2 | p | b | 65/80 | 0.1/36 | 74/112 | 0 | 0 | 2003 | none | 27 / 27 | 7.2/9.5, 128/0 | 0.082 |
| W1 | 250 | 4 | main | a | 70/93 | 75/234 | 148/310 | 0 | 0 | 3064 | none | 18 / 18 | - | 0 |
| W1 | 250 | 4 | main | b | 71/84 | 70/282 | 145/343 | 0 | 0 | 3120 | none | 19 / 19 | - | 0 |
| W1 | 250 | 4 | t2 | a | 52/65 | 1.6/59 | 80/133 | 0 | 0 | 3765 | none | 1 / 9 (57014 x1) | - | 0 |
| W1 | 250 | 4 | t2 | b | 60/72 | 12/59 | 88/132 | 0 | 0 | 3728 | none | 2 / 8 (57014 x1) | - | 0.012 |
| W1 | 250 | 4 | p | a | 60/108 | 40/202 | 107/257 | 0 | 0 | 3172 | none | 19 / 19 | 7.8/19, 128/0 | 0 |
| W1 | 250 | 4 | p | b | 91/145 | 77/270 | 191/411 | 0 | 0 | 2112 | none | 23 / 23 | 11/19, 128/0 | 0.013 |
| W1 | 250 | 16 | main | a | 71/364 | 591/2076 | 1213/5002 | 29 | 11 | 1560 | commit:57014 x11, commit:55P03 x9 | 8 / 22 (55P03 x3) | - | 0 |
| W1 | 250 | 16 | main | b | 65/86 | 709/1757 | 776/1853 | 2 | 0 | 3526 | none | 7 / 11 | - | 0 |
| W1 | 250 | 16 | t2 | a | 53/73 | 271/1656 | 342/1902 | 6 | 1 | 3889 | commit:57014 x1 | 4 / 10 (55P03 x1) | - | 0.062 |
| W1 | 250 | 16 | t2 | b | 52/71 | 250/1685 | 340/2340 | 9 | 0 | 3202 | commit:55P03 x4 | 2 / 10 (57014 x1, 55P03 x1) | - | 0.01 |
| W1 | 250 | 16 | p | a | 59/75 | 631/1393 | 705/1469 | 0 | 0 | 3831 | none | 7 / 8 | 8.2/11, 128/0 | 0 |
| W1 | 250 | 16 | p | b | 62/113 | 674/1714 | 789/2623 | 17 | 0 | 2727 | commit:55P03 x13 | 8 / 13 (55P03 x1) | 7.7/13, 128/0 | 0.009 |
| W1 | 250 | 64 | main | a | 74/130 | 1968/4732 | 2451/5001 | 84 | 18 | 2715 | commit:57014 x18 | 3 / 12 | - | 0.017 |
| W1 | 250 | 64 | main | b | 68/104 | 2574/4847 | 2747/5001 | 87 | 11 | 2841 | commit:57014 x11 | 3 / 12 | - | 0.018 |
| W1 | 250 | 64 | t2 | a | 61/180 | 1309/3853 | 1775/5002 | 65 | 23 | 2923 | commit:57014 x23 | 0 / 10 (57014 x1, 55P03 x1) | - | 0.019 |
| W1 | 250 | 64 | t2 | b | 60/138 | 1033/4237 | 1439/5002 | 59 | 24 | 2874 | commit:57014 x24 | 2 / 12 (57014 x1, 55P03 x1) | - | 0.009 |
| W1 | 250 | 64 | p | a | 68/142 | 2595/4430 | 2910/5001 | 94 | 14 | 2710 | commit:57014 x13 | 3 / 12 (57014 x1) | 9.7/93, 128/0 | 0.017 |
| W1 | 250 | 64 | p | b | 57/84 | 2678/4720 | 2742/4781 | 84 | 0 | 3344 | none | 2 / 8 | 11/88, 128/0 | 0.022 |
| W2 | 250 | 4 | main | a | 54/69 | 10/78 | 79/170 | 0 | 0 | 2107 | none | 26 / 27 | - | 0.02 |
| W2 | 250 | 4 | main | b | 66/103 | 15/144 | 90/238 | 0 | 0 | 1982 | none | 27 / 27 | - | 0.038 |
| W2 | 250 | 4 | t2 | a | 42/52 | 0.1/42 | 60/111 | 0 | 0 | 3216 | none | 14 / 14 | - | 0.061 |
| W2 | 250 | 4 | t2 | b | 46/58 | 0.1/46 | 70/151 | 0 | 0 | 2677 | none | 16 / 16 | - | 0.059 |
| W2 | 250 | 4 | p | a | 53/68 | 0.1/70 | 73/146 | 0 | 0 | 3086 | none | 19 / 19 | 6.6/8.8, 128/0 | 0.049 |
| W2 | 250 | 4 | p | b | 55/71 | 0.1/88 | 78/185 | 0 | 0 | 2799 | none | 20 / 20 | 7.1/9.7, 128/0 | 0.035 |
| W2 | 250 | 16 | main | a | 57/90 | 14/692 | 310/2039 | 11 | 0 | 2316 | commit:55P03 x10, claim:55P03 x5, prepare:55P03 x2 | 19 / 22 (55P03 x1) | - | 0.074 |
| W2 | 250 | 16 | main | b | 63/81 | 12/789 | 443/944 | 3 | 0 | 2320 | claim:55P03 x1, commit:55P03 x2 | 21 / 21 | - | 0.051 |
| W2 | 250 | 16 | t2 | a | 47/60 | 6.4/542 | 319/767 | 3 | 0 | 2703 | commit:55P03 x2 | 17 / 19 | - | 0.111 |
| W2 | 250 | 16 | t2 | b | 47/63 | 0.2/669 | 272/968 | 4 | 0 | 2698 | commit:55P03 x3 | 17 / 18 | - | 0.111 |
| W2 | 250 | 16 | p | a | 59/83 | 43/708 | 444/1543 | 1 | 0 | 2240 | none | 22 / 23 | 9.1/17, 128/0 | 0.085 |
| W2 | 250 | 16 | p | b | 61/83 | 35/567 | 459/2010 | 11 | 0 | 1880 | commit:55P03 x9, claim:55P03 x4, prepare:55P03 x1 | 24 / 28 (55P03 x1) | 8.5/11, 128/0 | 0.083 |
| W1 | 500 | 2 | main | a | 102/119 | 0.0/8.7 | 101/119 | 0 | 0 | 2569 | none | 23 / 23 | - | 0.041 |
| W1 | 500 | 2 | main | b | 136/187 | 0.0/84 | 155/211 | 0 | 0 | 1804 | none | 28 / 29 | - | 0.074 |
| W1 | 500 | 2 | t2 | a | 74/86 | 0.1/22 | 97/134 | 1 | 0 | 2327 | none | 21 / 22 | - | 0 |
| W1 | 500 | 2 | t2 | b | 101/133 | 0.1/85 | 140/214 | 0 | 0 | 1928 | none | 30 / 30 | - | 0.061 |
| W1 | 500 | 2 | p | a | 89/107 | 0.1/13 | 101/131 | 0 | 0 | 2622 | none | 21 / 21 | 12/16, 64/0 | 0.142 |
| W1 | 500 | 2 | p | b | 126/172 | 0.1/42 | 144/195 | 0 | 0 | 1748 | none | 32 / 32 | 17/23, 64/0 | 0.094 |
| W1 | 500 | 4 | main | a | 126/175 | 26/727 | 144/861 | 0 | 0 | 2913 | none | 14 / 15 | - | 0.009 |
| W1 | 500 | 4 | main | b | 171/272 | 145/421 | 336/599 | 0 | 0 | 2310 | none | 19 / 20 | - | 0.022 |
| W1 | 500 | 4 | t2 | a | 91/106 | 0.1/68 | 131/181 | 0 | 0 | 4196 | none | 8 / 10 | - | 0.013 |
| W1 | 500 | 4 | t2 | b | 133/158 | 23/131 | 203/324 | 0 | 0 | 2929 | none | 6 / 10 | - | 0.019 |
| W1 | 500 | 4 | p | a | 111/154 | 17/348 | 152/470 | 0 | 0 | 2403 | none | 16 / 19 (55P03 x1) | 14/19, 64/0 | 0.023 |
| W1 | 500 | 4 | p | b | 145/197 | 99/353 | 242/514 | 0 | 0 | 2627 | none | 22 / 22 | 20/28, 64/0 | 0.017 |
| W1 | 500 | 16 | main | a | 130/168 | 1744/3253 | 1857/3393 | 31 | 0 | 3329 | none | 4 / 10 | - | 0.011 |
| W1 | 500 | 16 | main | b | 179/328 | 2308/4021 | 2480/4160 | 39 | 0 | 2310 | none | 4 / 13 | - | 0.015 |
| W1 | 500 | 16 | t2 | a | 97/121 | 436/3384 | 587/3506 | 11 | 0 | 4228 | none | 0 / 6 (55P03 x2) | - | 0.014 |
| W1 | 500 | 16 | t2 | b | 143/365 | 582/2471 | 982/5003 | 19 | 8 | 1838 | commit:57014 x8, commit:55P03 x4 | 1 / 17 (57014 x2, 55P03 x2) | - | 0.018 |
| W1 | 500 | 16 | p | a | 121/168 | 1562/2862 | 1687/3001 | 24 | 0 | 3364 | none | 4 / 9 | 18/32, 64/0 | 0.021 |
| W1 | 500 | 16 | p | b | 155/223 | 1993/2715 | 2218/2905 | 33 | 0 | 2774 | none | 5 / 11 | 23/43, 64/0 | 0.018 |
| W1 | 500 | 64 | main | a | 132/508 | 2124/4686 | 2927/5002 | 72 | 30 | 2144 | commit:57014 x30, commit:55P03 x3 | 2 / 12 (57014 x1) | - | 0.028 |
| W1 | 500 | 64 | main | b | 165/457 | 2685/4572 | 4278/5005 | 79 | 32 | 1849 | claim:57014 x13, commit:57014 x32, commit:55P03 x3 | 2 / 17 (57014 x1, 55P03 x1) | - | 0.036 |
| W1 | 500 | 64 | t2 | a | 101/357 | 1840/4241 | 2806/5002 | 48 | 13 | 2813 | commit:57014 x13 | 0 / 9 (57014 x1, 55P03 x1) | - | 0.046 |
| W1 | 500 | 64 | t2 | b | 115/435 | 1755/4164 | 2744/5004 | 53 | 21 | 2413 | commit:57014 x20, claim:57014 x12 | 0 / 10 (57014 x1, 55P03 x1) | - | 0.024 |
| W1 | 500 | 64 | p | a | 121/806 | 1652/4195 | 2502/5002 | 56 | 26 | 2192 | commit:55P03 x2, commit:57014 x26 | 2 / 14 (57014 x2) | 57/219, 64/0 | 0.036 |
| W1 | 500 | 64 | p | b | 146/476 | 3443/4565 | 4477/5038 | 94 | 50 | 1683 | commit:57014 x50, commit:55P03 x2, claim:57014 x14 | 4 / 21 (57014 x1, 55P03 x1) | 32/122, 64/0 | 0.033 |
| W2 | 500 | 4 | main | a | 124/184 | 21/205 | 164/368 | 0 | 0 | 2447 | none | 22 / 23 | - | 0.054 |
| W2 | 500 | 4 | main | b | 156/198 | 7.3/253 | 202/419 | 0 | 0 | 1723 | prepare:55P03 x1 | 29 / 32 (55P03 x1) | - | 0.06 |
| W2 | 500 | 4 | t2 | a | 205/262 | 0.2/153 | 296/474 | 0 | 0 | 1191 | none | 32 / 39 | - | 0.049 |
| W2 | 500 | 4 | t2 | b | 107/147 | 0.1/103 | 155/334 | 0 | 0 | 2237 | none | 20 / 22 | - | 0.071 |
| W2 | 500 | 4 | p | a | 119/166 | 0.2/134 | 159/414 | 1 | 0 | 1910 | commit:55P03 x1, claim:55P03 x1 | 24 / 26 | 17/23, 64/0 | 0.036 |
| W2 | 500 | 4 | p | b | 141/268 | 8.8/178 | 181/392 | 0 | 0 | 1836 | none | 28 / 28 | 18/23, 64/0 | 0.07 |
| W2 | 500 | 16 | main | a | 146/243 | 94/2032 | 683/2255 | 7 | 0 | 2033 | claim:55P03 x7, commit:55P03 x2, prepare:55P03 x1 | 19 / 22 | - | 0.11 |
| W2 | 500 | 16 | main | b | 155/268 | 103/1728 | 1055/2003 | 6 | 0 | 1590 | claim:55P03 x5, prepare:55P03 x1, commit:55P03 x3 | 26 / 29 | - | 0.081 |
| W2 | 500 | 16 | t2 | a | 97/165 | 40/1084 | 690/2002 | 4 | 0 | 2269 | commit:55P03 x3, claim:55P03 x1 | 17 / 21 | - | 0.101 |
| W2 | 500 | 16 | t2 | b | 134/190 | 5.5/1339 | 685/2264 | 5 | 0 | 1895 | claim:55P03 x2, prepare:55P03 x2, commit:55P03 x1 | 17 / 20 (55P03 x1) | - | 0.072 |
| W2 | 500 | 16 | p | a | 121/202 | 0.2/1252 | 695/2723 | 8 | 0 | 1712 | commit:55P03 x7, claim:55P03 x11, prepare:55P03 x1 | 21 / 26 (55P03 x1) | 18/23, 64/0 | 0.071 |
| W2 | 500 | 16 | p | b | 137/185 | 53/1033 | 872/2003 | 5 | 0 | 1805 | claim:55P03 x1, commit:55P03 x4, prepare:55P03 x1 | 25 / 27 | 21/32, 64/0 | 0.109 |
