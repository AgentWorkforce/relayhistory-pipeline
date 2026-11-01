# Runtime use of temporary objects (evidence for the no-TEMPORARY runtime role)

The migrator refuses a runtime role with effective `TEMPORARY` on the database
(`src/hoopdb/migrate-guards.ts`). This file records the search showing that no HoopDb
runtime code path creates temporary objects, so that refusal removes nothing a service
needs. Regenerate it with the commands below. Searched at commit `613627a73d8471a063eade824c04c27770bb968f` (the parent of
the commit that adds this file).

## What runs at runtime

The service entrypoints (`docker/entrypoint.sh`, the image's only process selector):

```
hoopdb-migrate)   exec bun run /app/src/hoopdb/migrate.ts ;;
locker-init)      exec bun run /app/src/locker/init.ts ;;
alleyoop-ingest)  exec bun run /app/src/alleyoop/ingest.ts ;;
alleyoop-courier) exec bun run /app/src/alleyoop/courier.ts ;;
hoopdb-indexer)   exec bun run /app/src/hoopdb/indexer.ts ;;
referee)          exec bun run /app/src/referee/main.ts ;;
bob-costas)       exec bun run /app/src/bob-costas/main.ts ;;
```

These import only from `src/`, `packages/pipeline/src` (`@rhc/*`, tsconfig paths) and
`packages/object-store/src` (`@rhc-object-store`). The SQL they call runs the function
bodies in `supabase/migrations` and `src/hoopdb/migrations`; invoker functions run as the
caller, so those bodies were searched too. `vendor/relayhistory-cloud` and
`vendor/relayhistory` were searched as well, although they are not runtime code.

## Search

Pattern (extended regex, case-insensitive):

```
TEMP(ORARY)?[[:space:]]+(TABLE|SEQUENCE|VIEW)|CREATE[[:space:]]+(LOCAL[[:space:]]+|GLOBAL[[:space:]]+)?TEMP|ON[[:space:]]+COMMIT[[:space:]]+(DROP|DELETE|PRESERVE)|pg_temp|temp_tablespaces
```

Command, for each root:

```sh
grep -rniE "$PATTERN" <root> --include='*.ts' --include='*.js' --include='*.sql' --include='*.sh' \
  --exclude-dir=node_modules --exclude-dir=__tests__ --exclude-dir=test --exclude-dir=tests | grep -viE '\.test\.'
```

The second column counts matches other than `pg_temp` in a `SET search_path = ...` clause.
That clause puts the temporary schema last, which is standard hardening and creates
nothing. It also excludes the migrator guard's own pattern in `src/hoopdb/migrate-guards.ts`.

| Root                        | All matches | Matches that create or use a temporary object |
| --------------------------- | ----------- | --------------------------------------------- |
| `src`                       | 2           | 0                                             |
| `packages/pipeline/src`     | 0           | 0                                             |
| `packages/object-store/src` | 0           | 0                                             |
| `supabase/migrations`       | 33          | 0                                             |
| `src/hoopdb/migrations`     | 2           | 0                                             |
| `vendor/relayhistory-cloud` | 136         | 0                                             |
| `vendor/relayhistory`       | 0           | 0                                             |
| `docker`                    | 0           | 0                                             |

**Result:** zero matches that create or use a temporary object in any root. Every match is
`pg_temp` in a `SET search_path` clause, or the guard's own pattern. No service needs
`TEMPORARY`.

## Consequence

- **selfhosted:** `supabase/bootstrap/hoopdb_runtime_role.sql` revokes `TEMPORARY` on the
  HoopDb database from PUBLIC. That is database-wide: every role there that relied on
  PUBLIC for it loses it (the owner and superusers keep it). A real PostgreSQL test shows
  an unrelated role losing it (`scripts/pg-proof/role-proof.ts`, "selfhosted bootstrap
  revokes TEMPORARY from PUBLIC").
- **supabase:** the migrator only detects and refuses. The operator owns the project
  database's ACL: see the runbook.
