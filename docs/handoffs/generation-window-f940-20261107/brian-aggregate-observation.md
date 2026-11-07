# Bounded ingest aggregate observation

Read-only, one v2 attempt, after the separately reported v1 permission refusal. No production writes, role grants, credential/principal fallback or automatic retry. Exact v2 SQL SHA256 8930e4cfc0497aa4eb56b20add752f8eaa60fadd6c00ee53068a38ee1b4ae3cf; raw receipt SHA256 e54a299c17781ad0b63150d8cd57fb98ec4be7313314b5ec2ab5f09b1236cb32.

The v1 preflight used its literal cli_login_postgres pin and returned42501 without rows; no aggregate block ran. V2 explicitly matches the existing owner context current_user=postgres/session_user=cli_login_postgres, using the same qualified sf-frame temporary CLI transport. Each of the TWO distinct READ ONLY transactions sets LOCAL ROLE postgres; the first ROLLBACK does not carry the role into the second. The source helper reasserts the role and verifies the reviewed preflight plus owner/login/database/schema-owner/read-only guards in each phase. Each transaction ends in ROLLBACK. Statement_timeout3s, lock_timeout250ms; no nested BEGIN wrapper.

Preflight observations at 2026-11-07T19:48:47.491Z and19:48:47.928Z both returned exactly:
~~~json
[{"expected_owner_role":true,"stats_visible":true,"slots_select":true,"slots_rls_active":false}]
~~~

Aggregate queries ran 2026-11-07T19:48:47.928Z through19:48:48.025Z. Rows verbatim:
~~~json
{"activityRows":[{"state_bucket":"idle","wait_bucket":"Client","backend_age_bucket":"ge300s","xact_age_bucket":"none_or_masked","backend_count":19}],"slotRows":[{"lease_state":"active","lease_age_bucket":"60to90s","remaining_bucket":"10to60s","holder_rows":1,"overflow_counts_are_partial":false},{"lease_state":"expired","lease_age_bucket":"ge90s","remaining_bucket":"expired","holder_rows":2,"overflow_counts_are_partial":false}]}
~~~

Only fixed labels, integer counts and the SQL's overflow boolean were emitted from these statistics queries. No tenant/user/workspace/holder identity, SQL text, PID or client address was collected. Both actual visibility guards passed; individual none_or_masked labels retain their defined meaning and are not independently reclassified. Empty result sets would not imply no production traffic outside this snapshot. The one active lease and two expired leases are observations, not a command to remove them or proof they caused503s. This one sample does not establish pool capacity, a repeated interval, current request state or an outage bound.

The release owner was sent about-to-run message244910962293792768 for one paired30s tail. Its actual tail interval/receipt had not been received when this packet was written; do not claim correlation until that receipt is joined. Exact rows and receipt were sent to release owner244911117670555648 and lead244911124468101120. No automatic resampling was requested.

The original schema plan/capture expired at19:47:10Z, independently of this diagnostic. They must be refreshed before any production apply. This snapshot is not migration approval, proof the missing roles were created, or a replacement for the release owner's qualified runtime/artifact/health checks.


## Tail receipt received: NO TIME OVERLAP

Release owner supplied the already-collected counts-only receipt ingest-unpaired-tail-receipt.json, verified SHA2564e77499fb02e73e42c24dd7ad1e9884c33b85dfabec7e42dd3b4b0d500fc1c54. Its source helper SHA2569faa3633e147b8ed7b8a3cef37c9aa328ab8cd1abe6c7a2c80c80737f33ec3ac; Root copied/hashed the receipt, did not rerun the tail. Actual start2026-11-07T19:49:16.645893Z, duration30.002s (end about19:49:46.648Z). It parsed49 delivery requests:42 HTTP200,5 HTTP503,2 HTTP429,0 unknown statuses. Raw events/errors were not persisted.

This begins AFTER the SQL interval ended19:48:48.025Z: **NO OVERLAP and NO CORRELATION CLAIM**. The original receipt kind contains correlated30 because that was the intended collection, not because overlap succeeded. It is separate unpaired traffic evidence, not an attribution of the503s/429s to any sampled backend or lease. It also predates the19:52 publisher observation. No repeated tail or SQL sample was requested or run. Lead/release owner were already notified. This supersedes the earlier pending/unreceived-tail status only; all other qualifications remain unchanged.
