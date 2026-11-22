# Matched runs at 471d291a: no control (run 3) vs C1 diagnostic (run 1) vs C2 acceptance (run 2)

Same harness, host, settings and matrix; one run per cell, so run-to-run spread is not removed. C1 = sessions.ingest_lazy_clear(reset) as owner. C2 = ledger.applyOrderedControl -> ingest_set_session_uploads -> ingest_apply_ordered_control as the runtime role. Both at 1/s. Clock wait is sampled (period p50 0.5-2.8 ms); latencies in ms. Upload failures exclude the control's own failures.

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

Reading:

- **Added delay is clear at C=4 in W1.** With few concurrent ACKs, the claims overlap the gaps between ACKs, and a queued control waits on them (FOR UPDATE vs KEY SHARE) while ACKs queue behind it.
  - n=250: clock wait p95 goes from 90 ms to 283 / 258 ms; commit stage p95 from 175 to 391 / 324 ms.
  - n=500: clock wait p95 goes from 146 to 706 / 551 ms; commit stage p95 from 290 to 986 / 705 ms; commits/s from 7.2 to 5.4 / 4.0 (drain 17.9 to 23.6 / 31.7 s).
- **At C=16 and C=64, with one run per cell, the difference is within the spread between runs**, for example W2 250 C64 drains in 35.1 / 47.3 / 28.2 s. The ACK queue on the clock dominates there, and a control is one more short holder in that queue. Separating a control effect at these levels needs several rotations per cell.
- No 40P01 in any cell of the three runs.
