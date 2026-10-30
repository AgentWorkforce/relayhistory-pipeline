# Session directory tenant JWT issuer

**Status:** proposed design. Khaliq has not approved the separate-issuer direction. Nothing here is built, deployed or configured.
**Resolves:** the activation HOLD on #48 and open question 4 in its RelayHistory #226 contract amendment.
**Reviewed at:** #48 `134afa8`, #38 `71b8ffdd`, relayauth `main` `58a40a2`, relayauth #110/#111, relayhistory-cloud #226 `1610785`.
**External facts:** Supabase and PostgREST behavior below comes from their documentation, read 2026-10-31. Nothing was tested against a live project.

## Decision

RelayAuth mints the token, under a **separate issuer identity with its own signing key**. Supabase trusts only that issuer, never the general RelayAuth issuer.

The RelayHistory Worker exchanges the caller's own verified RelayAuth token for a 60-second search token. RelayAuth takes the tenant from the signed subject token, not from a Worker-supplied org. The RPC then pins `iss` and `aud`, so another issuer trusted by the project cannot reach it.

## What the current code requires

- `sessions.search_session_directory` trusts `request.jwt.claims`. It checks `role`, `app_metadata.org_id` and `app_metadata.workspace_ids`. It does not check `iss`, `aud`, or lifetime, and signature verification is left to PostgREST.
- `GRANT session_directory_search_worker TO authenticator` is not in #48. It is a separate deployment step.
- #226 sends one static `SESSION_READS_TOKEN` as both `apikey` and bearer. It cannot carry per-request tenant claims.
- The Worker already holds a verified identity. `readWorkspaceRecallScope` refuses a `workspace` that differs from the token's, but `identityFromAuth` falls back to `orgId` when a token has no workspace. Search must not use that fallback.
- RelayAuth signs RS256 only, with `current`, `next` and `previous` key slots and `kid` as the RFC 7638 thumbprint. Its JWKS is served with `max-age=3600`.
- RelayAuth's token routes on `main` set no top-level `role` or `app_metadata`.
- RelayAuth has `/.well-known/agent-configuration` but no `/.well-known/openid-configuration`, which Supabase custom OIDC requires.

## Options considered

| Option | Verdict | Reason |
| --- | --- | --- |
| Trust the general RelayAuth issuer | Rejected | Supabase trusts an issuer wholesale, and that key signs every product's tokens. A bug in any RelayAuth mint path would then reach every tenant's search. |
| Worker signs with a key imported into Supabase | Rejected | The private key lives in both Supabase and the Worker. A Worker bug can sign for any tenant, so it is the static token with a shorter expiry. If the key must be `current` to be trusted, Supabase Auth also signs with it. |
| Keep a static token and filter in the Worker | Rejected | #48 refuses it by design, and isolation would rest on Worker code alone. |
| Dedicated issuer in RelayAuth, subject-token exchange | **Chosen** | Separate key and trust scope. A Worker bug cannot name another tenant without that tenant's token. Cost: a network hop and new RelayAuth code. |

## Issuer and exchange

- **Issuer:** a new, distinct HTTPS URL on hosted RelayAuth, for example `https://relayauth.dev/session-directory`, with discovery at `<issuer>/.well-known/openid-configuration` and a `jwks_uri` listing only this issuer's keys. The final URL is an implementation choice, but it must not equal the general issuer `https://relayauth.dev`.
- **Endpoint:** `POST /v1/tokens/session-directory-search`, modeled on the #110 migration assertion, with no refresh token.
- **Caller:** the Worker, using an `x-api-key` whose only scope is `relayauth:assertion:create:session-directory-search`. As in #110, wildcard keys and bearer callers are refused.
- **Request:** `{ "subjectToken": "<caller's token>", "workspaceId": "<ws>" }`. There is no org field, and the request cannot name claims.
- **Off by default:** issuance needs an explicit enable flag, as #111 does.

### Binding rules

RelayAuth refuses unless all of these hold for the subject token:

1. It verifies under the **general** issuer's keys, is unexpired and unrevoked, and has `token_type: access`.
2. Its audience is a RelayHistory audience, and its scopes contain `rth:read`.
3. It has a non-empty `wks`, and that value equals `workspaceId` after normalization. There is no fallback from workspace to org.
4. Its token class is not `migration_assertion` or any token minted by this endpoint.

`org_id` and `workspace_ids` are derived only from the subject's `org` and `wks`, with exactly one `relay_` prefix removed. That must match `stripRelayPrefix` and the values stored in `session_directory`; the tests pin this.

A token RelayAuth cannot verify is refused, including anything admitted only by RelayHistory's `ALLOW_INTERIM_RELAYAUTH_BEARER` path.

### Issued token

```json
{
  "iss": "<session directory issuer>",
  "aud": "session-directory-search",
  "sub": "<subject user id>",
  "role": "session_directory_search_worker",
  "app_metadata": { "org_id": "<org>", "workspace_ids": ["<ws>"] },
  "iat": 0, "nbf": 0, "exp": 60, "jti": "<id>"
}
```

- Header: `alg: RS256` and a `kid`.
- `exp - iat` is at most 60 seconds, and `workspace_ids` always has exactly one element.
- The Worker may reuse a token in memory for up to 30 seconds, keyed by subject token hash and workspace. It never persists one.
- Each mint is audited and rate limited per subject and workspace.
- If RelayAuth is down, search returns 503. It never falls back to a static credential.

### Proposed pins for #48

The RPC does not check `iss`, `aud` or lifetime today. Supabase's docs say a project trusts every issuer added, and PostgREST skips audience checks unless `jwt-aud` is set. Whether hosted Supabase sets it is unconfirmed. So any other trusted issuer could otherwise satisfy the RPC.

This is the proposed change, not applied to #48 and not executed. The issuer differs per environment, so it lives in an owner-only table instead of a custom setting. The migration seeds no row, so search stays refused until a row is added for that environment, and adding one is a production change.

```sql
CREATE TABLE sessions.session_directory_search_token_policy (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  issuer text NOT NULL CHECK (issuer LIKE 'https://%'),
  audience text NOT NULL CHECK (audience <> ''),
  max_lifetime_seconds integer NOT NULL DEFAULT 60
    CHECK (max_lifetime_seconds BETWEEN 1 AND 60)
);
ALTER TABLE sessions.session_directory_search_token_policy ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_directory_search_token_policy
  FROM PUBLIC, anon, authenticated, service_role;
```

In `sessions.search_session_directory`, declare `v_policy sessions.session_directory_search_token_policy%ROWTYPE;` and add this straight after the existing claims-shape check:

```sql
SELECT * INTO v_policy
  FROM sessions.session_directory_search_token_policy WHERE singleton;
IF NOT FOUND
   OR (v_claims ->> 'iss') IS DISTINCT FROM v_policy.issuer
   OR jsonb_typeof(v_claims -> 'aud') IS DISTINCT FROM 'string'
   OR (v_claims ->> 'aud') IS DISTINCT FROM v_policy.audience
   OR jsonb_typeof(v_claims -> 'exp') IS DISTINCT FROM 'number'
   OR jsonb_typeof(v_claims -> 'iat') IS DISTINCT FROM 'number'
   OR (v_claims ->> 'exp')::numeric - (v_claims ->> 'iat')::numeric
        NOT BETWEEN 1 AND v_policy.max_lifetime_seconds
   -- A far-future iat must not stretch the window; 30s allows for clock skew.
   OR (v_claims ->> 'exp')::numeric - extract(epoch FROM clock_timestamp())
        > v_policy.max_lifetime_seconds + 30
   OR jsonb_array_length(v_claims -> 'app_metadata' -> 'workspace_ids') <> 1 THEN
  RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: tenant claims missing or invalid';
END IF;
```

- `aud` must be a plain string, which is what this issuer emits. An array is refused.
- The error message is the existing one, so a refusal does not reveal which pin failed.
- The rollback drops the table. #48's RPC tests need a seeded policy row, because every hand-set claim set now also needs `iss`, `aud`, `iat` and `exp`.
- Deleting the policy row stops all search at once, so it is a second kill switch.

## Keys and rotation

- **Key:** a dedicated RS256 key, never shared with the general issuer. Add `RELAYAUTH_SESSION_DIRECTORY_SIGNING_KEY_PEM`, `_NEXT` and `_PREVIOUS`, reusing the `signing-keys.ts` slot rules. Using ES256 would add a second signing path to RelayAuth for no isolation benefit.
- **Storage:** 1Password as system of record, per the repo rules. Key creation needs Khaliq's OK.
- **JWKS cache:** serve this JWKS with `max-age=300`, not 3600.
- **Planned rotation, proposed quarterly:**
  1. Publish `next`.
  2. Wait at least 35 minutes. Supabase says to allow up to 30 minutes for key changes.
  3. Promote `next` to `current`, and move the old key to `previous`.
  4. Wait at least 35 minutes again, because token lifetime is short but Supabase pickup is not.
  5. Remove `previous`.
- **Compromise:** JWKS removal can take 30 minutes to apply, so it is not the kill switch. In order:
  1. Run `REVOKE session_directory_search_worker FROM authenticator`. It applies to new requests at once and stops all search.
  2. Remove the key and rotate.
  3. Restore the grant.

  Both the revoke and the restore are production changes and need Khaliq's OK at the time, unless he pre-approves the revoke for incident use.
- **API key:** the Worker's minting key rotates through the normal API-key flow, with old and new overlapping. Leaking it alone mints nothing, because a valid subject token is still required.

## Accepted risk

- **RelayAuth signing compromise:** the general key can forge subject tokens for any tenant, so this design does not protect against it. It only prevents such a compromise from directly producing tokens Supabase accepts without going through the endpoint.
- **`relayhistory-assertion` key:** #110 records that this key can mint for any org. Its assertions are valid subject tokens, so that key remains an any-org path until it is narrowed.
- **Worker compromise:** an attacker in the Worker sees live callers' tokens and can search those tenants while the compromise lasts, but not tenants with no live caller.

## Supabase unknowns

These decide whether the design works as written. All need a non-production project.

| Unknown | Why it matters | If the answer is no |
| --- | --- | --- |
| Does hosted Supabase accept a custom `role` from a custom OIDC issuer? Docs describe only `anon` and `authenticated`. | The RPC requires the Worker role. | Use the custom-role fallback below. |
| Is a path-based issuer accepted? | The issuer is not a bare origin. | Use a dedicated hostname. |
| Does the gateway accept a publishable `apikey` with this bearer? | #226 sends the same value for both. | Revisit the #226 request shape. |
| Is `sessions` an exposed schema, and is `statement_timeout` applied? | The RPC and its 2s limit depend on both. | Change project config, or set the limit in the function. |
| Is custom OIDC available on our plan? | Docs do not say. | Fall back to a key imported into Supabase, which needs a new decision. |

Custom OIDC is not available through the Supabase CLI or `config.toml`, only the dashboard or Management API. It will need a runbook step rather than a migration.

### Checks awaiting their own OK

The custom-role question and the other unknowns above are **non-production checks, and none has been run**. They require adding an issuer to a Supabase project, which needs Khaliq's direct OK even on a non-production project. The custom-role check comes first, because it decides between #48 as drafted and the fallback below.

## Custom-role fallback

Use this only if the check above shows Supabase refuses the custom role. #48 would then run as `authenticated` and gate on claims. It works, but it is weaker, so the primary design stays preferred.

**What changes in #48**
- The issuer sets `role: authenticated`.
- `EXECUTE` goes to `authenticated`, and the `session_directory_search_worker` role and `authenticator` grant are dropped.
- The role guard and the claims check require `authenticated` instead of the Worker.
- The `iss`, `aud` and lifetime pins above become **required gates**, and activation cannot proceed without them.
- `aud` stays `session-directory-search`, so an ordinary `authenticated` token fails it.

**What gets weaker**

| Property | Worker role | Fallback |
| --- | --- | --- |
| Who can call the RPC | Only the Worker role, enforced by Postgres | Every `authenticated` token. Only the claim pins refuse the others. |
| Direct table access | None | The token also gets RLS-scoped reads: that workspace's directory and `session_index` pointers, and `sessions.machines` for the whole org. |
| 2s `statement_timeout` | Role setting | Lost. `authenticated`'s own limit applies. A function-level `SET` is not expected to re-arm a running statement's timer, so it must be measured before anyone relies on it. |
| Kill switch | `REVOKE` from `authenticator` | Delete the policy row. Revoking `authenticated` would break every other reader. |

**Review and tests**
- This is a new grants review, not a small edit. #48's authorization tests pin the opposite, including "no role except the Worker can execute".
- Tests 11 and 12 change: an `authenticated` token without the pinned `iss` and `aud` must be refused by the RPC, and direct table reads with the search token must return only that tenant's permitted rows.
- Add a test that a real Supabase Auth user token cannot run the RPC.

## Self-hosted stacks

Hosted Supabase custom OIDC does not cover the self-hosted stacks. `stacks/aws/relayauth.ts` uses a plain-HTTP in-cluster issuer with no public JWKS, and Supabase requires HTTPS. The token shape and exchange still apply, but a self-hosted PostgREST would load the JWKS directly through `jwt-secret`. Neither `docker/` nor `stacks/` runs a PostgREST service today. This is follow-up work and does not block hosted activation.

## Required cross-tenant tests

#48's tests run on PGlite and set `request.jwt.claims` by hand, so they never exercise signature checks, role switching or the `authenticator` binding. These tests use real Postgres, real PostgREST at the hosted version, real RS256 tokens and a local JWKS, with no `set_config`. Every positive path asserts row contents, and every refusal asserts the HTTP status and SQLSTATE.

**Tenant isolation**
1. Tokens for org A and org B searching the same term each return only their own rows.
2. Org A and org B hold the **same workspace id string**. Each returns only its own org's rows.
3. A token for workspace A1 asking for A2 in the same org gets 403.
4. An org mismatch in the claims returns no other tenant's rows.
5. Tombstoned rows and summaries are absent for every tenant.

**Forgery**

6. Refused with 401, or 42501 where the signature is valid:
   - `alg: none`
   - HS256 signed with the public key
   - a wrong key using a valid `kid`
   - an unknown `kid`
   - a tampered payload
   - expired or not-yet-valid tokens
7. A correctly shaped token signed by the **general** RelayAuth key is refused.
8. A validly signed token with wrong `iss`, wrong `aud`, a lifetime over 60s, or a missing `iat` is refused by the RPC.
9. `app_metadata` claims moved to the top level or given the wrong type are refused, as are an empty policy table, `aud` as an array, two entries in `workspace_ids`, and a far-future `iat`.

**Role boundary**

10. `role` of `service_role`, `postgres`, `authenticator`, or a nonexistent role never reaches the data.
11. `anon`, `authenticated`, publishable-key-only and no-token callers cannot run the RPC.
12. The Worker cannot read or ILIKE any `sessions` table directly, including through resource embedding.
13. `authenticator` stays NOINHERIT and the Worker stays NOLOGIN after the binding migration.

**PostgREST surface**

14. `select`, `order`, `limit`, filters, `Prefer` headers and `Accept` variants on the RPC cannot exceed 51 rows or expose extra columns.
15. Profile-header changes cannot reach another schema.
16. 22023, 42501 and 57014 reach the Worker and map to 400, 403 and 503, and errors leak no tenant data.

**Concurrency and cache**

17. Interleaved requests from many tenants over a small pool never return another tenant's rows, and claims never leak between requests on a reused connection.
18. An expired token is refused even when PostgREST's JWT cache holds it.
19. The 2s `statement_timeout` applies through PostgREST.

**Rotation**

20. `next` is accepted once published, `previous` is accepted until removed, and a removed key is refused.
21. Revoking the Worker from `authenticator` stops search at once, and restoring it recovers.

**Exchange, in relayauth**

22. Each binding rule has a refusal test: wrong workspace, no `wks`, revoked or expired subject, missing `rth:read`, wrong audience, and a migration or search token as subject.
23. A bearer, wildcard, wrong-scope or other-org API key is refused, and issuance is off by default.
24. `relay_`-prefixed and unprefixed values produce claims that match stored rows, and a double prefix does not.

**Hosted, non-production Supabase project**

25. Rerun 1, 2, 6, 7 and 11 through the real gateway. Depends on the checks awaiting their own OK.

## What blocks #48 activation

**Decisions**
1. Khaliq accepts or changes this design.
2. Khaliq approves the non-production checks, and they answer the Supabase unknowns. The custom-role result picks the primary design or the fallback.

**Code**

3. RelayAuth: separate issuer, discovery, JWKS, exchange endpoint and its tests.
4. #48: adopts the proposed pins. Independent review is still pending.
5. #38 lands first, since #48 is stacked on it.
6. #226: move to the RPC with the token flow and the rest of #48's contract amendment. It stays frozen until accepted.
7. A reviewed migration for `GRANT session_directory_search_worker TO authenticator`.
8. The real PostgREST suite above passes.

**Also open**

9. Projector precedence is provisional, and the rollout marker stays incomplete until that is recorded.
10. The search token cannot serve #226's list, get, pointer or rollout reads, because the Worker role has no table access. Those still use the static `SESSION_READS_TOKEN`, with isolation enforced by Worker filters, and the cutover gate requires that token. It does not block search, but it needs its own decision before cutover. The likely answer is a second token profile from this issuer.

**Needs Khaliq's direct OK when reached**
- Generating the issuer key and the Worker's API key.
- Adding the issuer to any Supabase project, including a non-production one.
- Applying any migration, including the `authenticator` grant.
- Deploying RelayAuth or RelayHistory.
- Merging #38, #48 and #226 on exact heads.
