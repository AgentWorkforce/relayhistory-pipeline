# Raw local reports

SHAPE FIDELITY: PARTIAL. Sanitized aggregate anchors only; not production parity.

Local runs of 2026-10-31 on a Mac mini, data on an external drive, vanilla ClickHouse
26.8.34.13, synthetic data. Produced from the working tree that became `6785229`, on the
superseded branch `feat/search-e2e-proof` (head `60507a87df59a9a57c3ec6ca0303aa9e61743f78`). CI runs there:
41176407917 and 41178011021. Timings are relative.

CI reports on the way to this head are kept as run artifacts: 41182057571 sweep at
`7c48aba`, 41183351809 variants at `2fba88d`, 41184915870 rejected candidate-first run at
`b734698`, 41180498306 at `727cd0e`, which held a false-green multi-org result.
