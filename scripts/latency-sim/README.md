# T134 capacity sweep: offline, synthetic-only reproduction

Scenario tooling for T134 (sessions latency). It replays an arrival trace against N worker slots with per-session
coalescing, then sweeps an ASSUMED proof-read cost `c`, an ASSUMED coverage-failure rate `f`, and the fallback mode:
`immediate` (T126-style covering search) or `hold` (T124-style 300 s bounded hold, then one inline pass). Latency is
scored to the COVERING pass: start > 7 s and finish > 19 s.

**Nothing here is a production estimate.** `c`, `f`, the cost profiles and the hold length are assumptions. Production
sizing stays open until observed proof-read durations and outcomes, broken out as completed with coverage, completed
with coverage absent, and failed/timed out, can be joined with authorized reads.

## Inputs: synthetic only

`synthetic-arrivals.json` is generated deterministically by `synth_arrivals.py` (default seed 20261128) and contains no
production data. Only aggregate SHAPE parameters came from the observed peak hour: arrivals per tenant rank, sessions per
tenant rank, a 3,600 s window and within-session gap quantiles. Tenants are integers and sessions are synthetic strings.
It yields 4,751 rows, 36 tenants and 518 sessions; 4,747 remain after the sweep's (session, ms) dedup.

The OBSERVED-trace run that the T134 findings cite used an anonymised production trace held outside this repository
(custodian: sessions-latency-owner; SHA-256 prefixes are in the T134 receipt). It is not reproduced here. Results on the
synthetic trace differ in detail but show the same qualitative pattern: proof-read cost dominates, and a hold costs
about one 300 s miss per failed batch.

## Reproduce

```sh
cd scripts/latency-sim
python3 synth_arrivals.py synthetic-arrivals.json      # deterministic; byte-identical for the same seed
python3 sweep.py synthetic-arrivals.json > sweep-synthetic-out.txt
```
Python 3.9+, standard library only. Seeds 1-5 come from `sweep.py`; the coverage-failure RNG is
`random.Random(seed*7919+13)`.
