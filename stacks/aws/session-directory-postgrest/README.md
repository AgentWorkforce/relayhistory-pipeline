# Session directory search on self-hosted PostgREST

The AWS proof profile: PostgREST in the cluster, against Postgres on CloudNativePG.
Design: `docs/session-directory-tenant-issuer.md`. These files hold no keys or secrets,
and nothing here has been deployed. The suite in `integration/session-directory-postgrest`
runs these exact files against real Postgres 17 and PostgREST 16.5.

## Run the suite against a throwaway cluster

Set `POSTGREST_BIN`, `SD_TEST_PG_DISPOSABLE=yes` and `SD_TEST_PG_ADMIN_URL` to a superuser URL
that PostgREST can reach too, then run
`bun test ./integration/session-directory-postgrest --timeout 40000`. Optionally set
`RELAYAUTH_DIR` to include tokens minted by RelayAuth. The suite refuses a database with a
`sessions` schema and leaves its roles behind, so use a fresh cluster each run. Never point it
at a real environment.

## Hard precondition: TLS before the token exchange

The exchange request carries RelayHistory's minting API key and the caller's token.
`stacks/aws/relayauth.ts` serves RelayAuth over plain HTTP in the cluster. **The exchange
must not run over that Service until RelayAuth has TLS.** Reading the JWKS needs no secret,
but its integrity still matters, so copy it to PostgREST over a trusted channel.

## Steps

Each step touches a real environment and needs Khaliq's direct OK.

1. Apply `supabase/tests/supabase_shim.sql`. It creates the roles the migrations grant to.
   This is for plain Postgres only, never a Supabase project.
2. Apply `supabase/migrations` in name order.
3. Apply `authenticator.sql`, and set that role's password out of band.
4. Add the policy row, with the search issuer from the RelayAuth chart:
   ```sql
   INSERT INTO sessions.session_directory_search_token_policy (issuer, audience)
   VALUES ('<issuer>', 'session-directory-search');
   ```
5. Mount the JWKS at the `jwt-secret` path and start PostgREST with `postgrest.conf`.
   The JWKS comes from RelayAuth's `/.well-known/session-directory/jwks.json`. PostgREST does
   not fetch URLs, so a file must be updated for each key change.

## Key rotation

1. Add the `next` key to the mounted JWKS, and run `NOTIFY pgrst, 'reload config'`.
2. Promote `next` to `current` in RelayAuth, and move the old key to `previous`.
3. Wait at least 2 minutes: 60 seconds of token lifetime plus PostgREST's 30-second clock skew,
   with a margin.
4. Remove the old key from the JWKS, and reload the same way.

This profile does not depend on Supabase's 30-minute pickup.

## Kill switches

Either stops search at once. Both are production changes.

- `REVOKE session_directory_search_worker FROM session_directory_authenticator;`
- `DELETE FROM sessions.session_directory_search_token_policy;`

## Differences from hosted Supabase

| | Self-hosted | Hosted Supabase |
| --- | --- | --- |
| Trust | JWKS file holding only this issuer | Custom OIDC issuer, needing discovery RelayAuth does not serve |
| `authenticator` | Dedicated role, Worker only | Supabase's role, also holds `anon`, `authenticated`, `service_role` |
| `jwt-aud` | Set | Unconfirmed |
| No token | Refused | Runs as `anon` |
