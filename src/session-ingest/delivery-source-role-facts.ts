// GENERATED from AgentWorkforce/relayhistory-cloud packages/relayhistory/src/lib/delivery-source-role-facts.ts @ 689143c3cd6b75af9954727c6493965f7c24672b
// SHA256 fbf405fb40781cf61c11e15364de2cbfa222d4df3e2b31e465141ba13b6ab912; regenerate: bun scripts/sync-history-source-contract.ts
/** Pure source-role catalog protocol shared by Worker and Node readers. */
// Include roles reachable through membership even for NOINHERIT logins.
// Catalog capabilities only: no connection URL, credential, tenant or record data.
export const DELIVERY_SOURCE_ROLE_SQL = `
WITH identities AS (
 SELECT session_user::text login, current_user::text active
), capabilities AS (
 SELECT name, jsonb_build_object(
  'superuser',EXISTS(SELECT 1 FROM pg_roles member WHERE pg_has_role(r.oid,member.oid,'MEMBER') AND member.rolsuper),'createRole',EXISTS(SELECT 1 FROM pg_roles member WHERE pg_has_role(r.oid,member.oid,'MEMBER') AND member.rolcreaterole),
  'createDb',EXISTS(SELECT 1 FROM pg_roles member WHERE pg_has_role(r.oid,member.oid,'MEMBER') AND member.rolcreatedb),'replication',EXISTS(SELECT 1 FROM pg_roles member WHERE pg_has_role(r.oid,member.oid,'MEMBER') AND member.rolreplication),'bypassRls',EXISTS(SELECT 1 FROM pg_roles member WHERE pg_has_role(r.oid,member.oid,'MEMBER') AND member.rolbypassrls),
  'sourceOwner',EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND pg_has_role(r.oid,c.relowner,'MEMBER')),
  'neonSuperuser',COALESCE(pg_has_role(r.oid,to_regrole('neon_superuser'),'MEMBER'),false),
  'schemaCreate',EXISTS(SELECT 1 FROM pg_roles member WHERE pg_has_role(r.oid,member.oid,'MEMBER') AND has_schema_privilege(member.oid,'sessions','CREATE')),
  'privateExecute',EXISTS(SELECT 1 FROM pg_proc p CROSS JOIN pg_roles member WHERE pg_has_role(r.oid,member.oid,'MEMBER') AND p.oid=ANY(ARRAY[
   to_regprocedure('sessions.owner_delivery_cutover_v1(text,text,text)'),
   to_regprocedure('sessions.require_delivery_cutover_control_v1(text,text,text)'),
   to_regprocedure('sessions.begin_delivery_cutover_control_v1(text,text,text,text,text,jsonb)'),
   to_regprocedure('sessions.snapshot_delivery_cutover_privacy_v1(text,text,text)')
  ]) AND has_function_privilege(member.oid,p.oid,'EXECUTE'))
 ) facts
 FROM identities i CROSS JOIN LATERAL (VALUES(i.login),(i.active)) names(name)
 JOIN pg_roles r ON r.rolname=names.name
)
SELECT jsonb_build_object('sessionUser',i.login,'currentUser',i.active,
 'roleSetting',current_setting('role'),
 'sessionCapabilities',(SELECT facts FROM capabilities WHERE name=i.login LIMIT 1),
 'currentCapabilities',(SELECT facts FROM capabilities WHERE name=i.active LIMIT 1)
) role_facts FROM identities i
`;
const CAPABILITIES = [
  "superuser",
  "createRole",
  "createDb",
  "replication",
  "bypassRls",
  "sourceOwner",
  "neonSuperuser",
  "schemaCreate",
  "privateExecute",
] as const;
const role = (value: unknown): value is string =>
  typeof value === "string" && /^[a-z_][a-z0-9_]{0,62}$/.test(value);
export function sanitizedDeliveryRoleFacts(value: unknown) {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw Error("invalid role facts");
  const v = value as Record<string, unknown>;
  if (!role(v.sessionUser) || !role(v.currentUser) || !role(v.roleSetting))
    throw Error("unreportable role");
  const capabilities = (input: unknown) => {
    if (!input || typeof input !== "object" || Array.isArray(input))
      throw Error("invalid capabilities");
    const x = input as Record<string, unknown>;
    return Object.fromEntries(
      CAPABILITIES.map((key) => {
        if (typeof x[key] !== "boolean") throw Error("invalid capability");
        return [key, x[key]];
      }),
    ) as Record<(typeof CAPABILITIES)[number], boolean>;
  };
  const sessionCapabilities = capabilities(v.sessionCapabilities);
  const currentCapabilities = capabilities(v.currentCapabilities);
  return {
    sessionUser: v.sessionUser,
    currentUser: v.currentUser,
    roleSetting: v.roleSetting,
    sessionCapabilities,
    currentCapabilities,
    restricted:
      !Object.values(sessionCapabilities).some(Boolean) &&
      !Object.values(currentCapabilities).some(Boolean),
  };
}
