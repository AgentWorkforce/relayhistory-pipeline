# Supabase access

**Status (2026-11-02).** The HoopDb schema is live on the production project
`pyamkrhoyszgzirqqffq`. Operators applied it with `src/hoopdb/migrate.ts` as `postgres`,
credentials from 1Password, TLS `verify-full` (see
`docs/activation/supabase-incremental-upgrade-1d99ccc8-to-eff8b3be.md`). The provisioner and IaC
path described below is still a proposal: no provisioner token or Alchemy Supabase resource has
been created.

Settled on 2026-10-28: the 96Bulls team owns the Supabase org. Not yet
approved: creating the provisioner token, the 1Password service accounts and
vault group assignments. Until then no IaC in this repo contacts Supabase. Operator migrator runs
(above) are the only contact.

**Vaults (created 2026-10-28):** `96Bulls-prod`, `96Bulls-staging`, `96Bulls-dev`,
`96Bulls-provisioner`. Tokens and DB credentials live only there.

**Pattern.** Every CLI or IaC call runs through `scripts/sb`, which refuses a
saved Supabase login or a `.env` token and execs `op run` so
`SUPABASE_ACCESS_TOKEN` exists only in the child process. Humans unlock with
the 1Password desktop app; agents on a developer machine use that same
session (a human is prompted); sandboxes and CI never hold a Supabase token
and call the Pippin Tools provisioner instead, which reads the
`96Bulls-provisioner` item with a platform-injected 1Password Service Account.

**IaC.** `stacks/supabase/project.ts` is a custom Alchemy resource
(`Supabase.Project`) over the Management API: reconcile by name (adopts an
existing project), delete by id. State holds project refs only; the token and
the database password come from the environment at call time.

**Needs from Khaliq:** plan tier; whether a management token exists; OK to
create the provisioner token (stored at `op://96Bulls-provisioner/...`), the
service account for the provisioner, and vault group membership.
