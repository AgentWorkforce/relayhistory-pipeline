# rds-activation/v1 fragments

These are the ordered SQL fragments for `src/hoopdb/rds-activation.ts`. The interface
is in `docs/rds-activation-executor.md`.

The fragments have no `BEGIN`, `COMMIT` or `ROLLBACK` of their own. The driver first
binds the `hoopdb_activation.*` settings with `set_config(..., true)`: phase,
provisioner, the generated role spec and the SCRAM verifiers. It then runs `00`
through `70` inside one transaction, in file-name order. `70_validate.sql` also runs on
its own, read-only, as `hoop` (the `verify` phase).

Do not run these files by hand with `psql -f`. Outside the driver's transaction, a
refusal partway through would not undo the earlier fragments.
