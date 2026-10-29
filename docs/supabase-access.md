# Supabase access (proposal, code only)

Settled on 2026-10-28: the 96Bulls team owns the Supabase org. Not yet
approved: creating the provisioner token, the 1Password service accounts and
vault group assignments. Until then nothing in this repo contacts Supabase.

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
