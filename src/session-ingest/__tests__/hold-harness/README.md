# Hold test harness (062320)

Test-only fixtures for the per-org admission hold (`docs/design/org-admission-hold.md`, slot
`20261105062320_ingest_org_admission_fences`). They don't depend on the design: they provide the real PostgreSQL,
MinIO and interleaving tools that the design's proof obligations need. Nothing here is imported by production code.

| Piece | File | What it gives a test |
|---|---|---|
| PostgreSQL fixture | `pg.ts` | `fixturePgUrl()` (SESSION_INGEST_TEST_PG_URL, loopback superuser fixture; `SESSION_INGEST_TEST_PG_REQUIRED=1` turns a skip into a failure), `serverMajor()` (checked against `SESSION_INGEST_TEST_PG_MAJOR`), `scratchDatabase(url, { bootstrap })` (a fresh database owned by the non-superuser `OWNER_ROLE`; `bootstrap: true` = `ownerDatabase`, ready for `migrateAsOwner`) |
| Two connections | `two-conn.ts` | `openPair()` (a, b, and a superuser observer), `expectBlocks`, `expectBlocksBothWays`, `expectNotBlocked`, and the lower-level `start`/`waitUntilBlocked`/`observeWait`. A block is proven from `pg_stat_activity` (wait_event_type `Lock`), an ungranted `pg_locks` row and `pg_blocking_pids`, never from timing. The waiter's result or PostgreSQL error (for example RH042) comes back as an `Outcome` |
| MinIO fixture | `s3.ts` | `fixtureS3()` (SESSION_INGEST_TEST_S3_ENDPOINT / _ACCESS_KEY / _SECRET_KEY, loopback only; `_REQUIRED=1`), `scratchBucket()` with `store(endpoint?)` (the repo's own `createS3Store`) and `raw()` signed requests |
| PUT gate | `put-gate.ts` | A loopback proxy in front of MinIO: `hold(match)` pauses exactly one matching request; `arrived`, `abandon()` (the client sees a dropped socket), `respond(status)`, `deliver()` (forward it now, byte for byte, with the original SigV4 headers). Use it to deliver an old PUT after a later read or a later writer |
| Runner | `run.ts` | Starts disposable `postgres:15/16/17` and MinIO containers (the `versions.lock.json` pin), runs `bun test` on the given files with the env above set and REQUIRED, fails on any skip, and removes every container |

```sh
bun src/session-ingest/__tests__/hold-harness/run.ts                       # self-tests, PG 15/16/17 + MinIO
bun src/session-ingest/__tests__/hold-harness/run.ts --pg 17 -- src/session-ingest/__tests__/your.postgres.test.ts
```

```ts
const db = await scratchDatabase(fixturePgUrl()!, { bootstrap: true });
await migrateAsOwner(db.ownerUrl);
const pair = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
const { aHolds, bHolds } = await expectBlocksBothWays(pair, install, admit);
expect(aHolds.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
expect(bHolds.waited).toMatchObject({ ok: false, error: { code: "RH042" } });
```

CI: pg-proof's `ingest-postgres` job (PG 15/16/17) and the CI flow's local pg-proof lane run `run.ts --pg-url … --pg <major>`.
Add your suite's files there, or call `run.ts` with them.

Things that will bite you:

- On PostgreSQL 15 the NOINHERIT owner cannot CREATE in `public`; 16 and 17 allow it. Create your objects in a schema of your own.
- fetch silently resends an idempotent request when a reused keep-alive socket is reset. The gate closes every
  connection so that `abandon()` fails the client instead of turning into a second PUT, and `gate.log` lets a test
  check that no retry reached the store.
- The S3 client's create-only put treats identical bytes that are already stored as an idempotent success. To see a
  refusal, use different bytes (or check the 412 with `raw`).
