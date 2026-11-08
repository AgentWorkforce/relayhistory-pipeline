# Bounded publisher activity observation

One qualified read-only run on sf-frame; no new credentials/principal, SQL write, role grant, configuration change or retry. Exact SQL SHA256631ac8c57800c765b93db8e44cc3f930652521266c9602957c1480c812467ffa. Actual f940 publication-runtime.ts:68 sets application_name=96bulls-canonical-publication through gcPoolConfig and verifies the runtime is bulls96_session_ingest at :71. The statistics filter matches that source contract; it cannot discover a differently named, disconnected or offline process.

Two separate READ ONLY/ROLLBACK transactions, each SET LOCAL ROLE postgres, repeat the reviewed preflight and owner/session/database/schema-owner/read-only guard. No role context crosses the first ROLLBACK. Statement timeout3s / lock timeout250ms. Preflights at19:52:02.089Z and19:52:02.517Z returned:
~~~json
[{"expected_owner_route":true,"stats_visible":true,"tables_visible":true,"any_rls_active":false}]
~~~

Aggregate interval 2026-11-07T19:52:02.517Z..19:52:02.618Z. Exact rows:
~~~json
[{"publisher_or_gc_db_backends":"0","publication_activity":{"rows":0,"created15m":0,"created24h":0,"published15m":0,"published24h":0,"futureTimestamps":0,"activeUnfinishedLeases":0},"publication_object_activity":{"rows":0,"created15m":0,"created24h":0,"futureCreated":0,"activeGcLeases":0}}]
~~~

pg returns the outer COUNT(bigint) as string "0"; the JSONB counts are numeric0. The strict output projector preserves those representations, admits only the fixed count keys, and refuses noninteger/negative/unsafe shapes. No PID/address/query text, tenant identifiers, object key or payload was selected.

This is a bounded no-activity observation with full statistics/table visibility: zero matching DB backends, zero attempts, zero objects, hence zero recent counts or active leases in those tables. It does NOT prove no idle/disconnected/offline daemon, identify an actual host/launcher, settle runtime identity or authorize a stop/deploy. Keep the lead's documented-INACTIVE/no-redeploy planning disposition and the conditional requirement to handle any running canonical publisher found before rebind. This sample happened after, not simultaneously with, the19:48 ingest aggregate. No repeated polling or causal/health conclusion is inferred.

Raw receipt publisher-activity-result.json SHA2568aae951f892362d70623e44f0aab0a6d4a302c1e8f546730f0d537ad4b358b9f. Rows/receipt sent lead244911903111860224 and release owner244911909262188544; full JSON copied to the release owner's relay-briefs. Original schema plan/capture are expired and no production bootstrap/apply/rebind has been executed here.
