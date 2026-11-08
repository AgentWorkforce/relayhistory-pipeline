# G2 Worker source/build review — f9409ccb

Reviewed at 2026-11-07T20:25:30.464857+00:00. Verdict: **PASS for the supplied source/build supplement; execution remains on HOLD.** No new blocking source/build finding in this tuple. This is the root adversarial and contextual review requested for the whole-plan packet, not deployment authorization.

## Exact evidence

- Worker JavaScript: `5fcad962a5a61975edaed69715193e36e6f751e460b77a4b930e1c3ae5e3c555` (461913 bytes).
- Source map: `e79085b83f6391a2922a0eedab5b0e34d62a6c133545b26d695a59c4316ee13d`.
- SDK aggregate, independently recomputed using `bundleOutputFromFiles -> sha256Object -> stableValue`: `52c1352d3f758d533f7e4d68c41e91f5fa34ee3549effa10439448cc2d98ac2b`.
- Builder: `b331236487f55cbc8565b29437324cbe22dfdfc4c196373b5c60596c746686b6`; whole-plan packet: `03e931f7e12928e4705f04fa70d887aa10f49cd1ccf6aac37f8c67376e546876`.
- Native plan `54e0ed241e7ed2dad0d31c8cc7098bcd888ac9f23060031bbc062e4b130dfd5a`; detailed plan `84d407190f743ed66135d4378e18049d9dff132af5156fa652124f9bfb9060fd`; provenance `aa794147f330cda735015cae6f2cadf86fa1dd18c6f939713dffe35add91b1c4`.
- Recomputed semantic input digest `79df2a5dc4633438044ad7b5b846984304dff26efe2cb2c4d75c5b76d8007796` and capture generation `52abe803258804e8078486fc931e05a09ec69704bba1a525cace43774b0b760d` both match. Capture generation is distinct from schema G2 `e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce`.

## Adversarial pass

All 83 repository sources embedded in the source map match Git objects at `f9409ccb9e46c3f100cf3017847d8ed62bc716c7` byte-for-byte. All 78 dependency sources match the installed files in the Book's exact frozen checkout. The remaining 11 sources are the reviewed generated builtin ESM adapters. There are zero source mismatches. The actual JS/map file hashes and sizes match the manifest. The source map is provenance evidence under the reviewed builder/SDK trust boundary; it is not an independent proof that arbitrary JavaScript implements those sources.

The builder invokes the same `WorkerBundle.build` options as the installed provider's `prepareBundle` (lines 2191–2211): external entry, main, stack/stage, no extra build options, and **the same `getCompatibility` resolver**. Effective compatibility is `2026-10-01`, `nodejs_compat` plus `new_module_registry`. The installed provider returns changed on metadata-hash drift before source compilation; this explains why the native PLAN succeeded without creating a bundle. The pure build is a valid separate supplement to that plan.

The pure build selects no StateStore/provider lifecycle or credential loader. Its fetch guard throws. That guard is not an OS network sandbox, and its manifest's zero-request/no-credential fields are declarations rather than independent telemetry. The inspected installed SDK and dependencies remain the trusted build boundary. This review made only local/source reads and verified-host reads of existing files; no provider, credential, SQL, build, upload or deployment operation ran.

## Contextual pass

The native plan is one Worker update with one application-binding change: `INGEST_EXPECT_GENERATION` G1 to G2. The other 14 application bindings are no-ops; fanout queue remains paused, consumer and DLQ are no-ops; routes empty, workers.dev false, domain null. There is no new C3, custody, retention or Neon-exit flag activation in the reviewed inputs. The prior broad source compatibility review still applies, including the lead's explicit acceptance of the additive `/v1/turns/batches` endpoint and the UNKNOWN caller `TURNS_WRITE_PATH` value; this review does not relabel that endpoint default-off.

## Execution limits retained

The original native capture helper's artifact postcondition **failed** after the metadata short circuit. Nothing here rewrites it as a complete capture. Root did not independently rebuild or execute the compiled Worker in Cloudflare. The supplied packet still lacks the fresh post-capture Cloudflare bracket; apply-time StateStore authority recheck, exact-candidate CI/reviews, qualified G2 schema/runtime window and direct operation approval remain required by the existing execution plan.

The operator capture binds source, tools, inputs, plans and state authority; it does **not** itself include the separate supplement's JS/map/SDK aggregate hashes. The eventual reviewed execution must verify/bind the exact artifact bytes it will upload (or demonstrate its rebuilt output equals this tuple). This source review does not assert that the current generic operator already enforces that supplement binding, and it gives no upload/deployment clearance.

`review-receipt.json` records the machine-readable checks and exact input hashes. Production data, source and runtime state were unchanged.
