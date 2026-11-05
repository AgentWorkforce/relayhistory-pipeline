// GENERATED from AgentWorkforce/relayhistory-cloud packages/relayhistory/src/lib/history-custody-authority-sql.ts @ 689143c3cd6b75af9954727c6493965f7c24672b
// SHA256 80a41772759c5c72521039215550a2dac394fc3b918bdae97b8382abf841bf62; regenerate: bun scripts/sync-history-source-contract.ts
/** Pure private full-publication catalog/role authority. No data rows or credentials. */
import { CURRENT_CUSTODY_AUTHORITY_SQL } from "./delivery-source-authority-sql.ts";
export const HISTORY_CUSTODY_AUTHORITY_SQL = `
WITH base AS (${CURRENT_CUSTODY_AUTHORITY_SQL}), descriptor AS(
 SELECT value||jsonb_build_object(
  'protocol','history-publication-source-v1',
  'publicationIndexes',(SELECT jsonb_agg(jsonb_build_object(
    'name',c.relname,'table',i.indrelid::regclass::text,'oid',c.oid::text,
    'definition',pg_get_indexdef(c.oid),'method',am.amname,
    'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,
    'unique',i.indisunique,'keyCount',i.indnkeyatts,'columnCount',i.indnatts,
    'predicate',pg_get_expr(i.indpred,i.indrelid),
    'expressions',pg_get_expr(i.indexprs,i.indrelid),
    'keys',(SELECT jsonb_agg(jsonb_build_object(
      'name',a.attname,'columnCollation',a.attcollation::text,
      'indexCollation',i.indcollation[k.n-1]::text,'option',i.indoption[k.n-1])
      ORDER BY k.n)
      FROM unnest(i.indkey) WITH ORDINALITY k(num,n)
      JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num
      WHERE k.n<=i.indnkeyatts)
    ) ORDER BY c.relname)
    FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
    JOIN pg_namespace ns ON ns.oid=c.relnamespace JOIN pg_am am ON am.oid=c.relam
    WHERE ns.nspname='sessions' AND c.relname IN(
      'delivery_records_record_idx','convergence_events_delivery_record_idx')),
  'publicationFunction',(SELECT jsonb_build_object(
    'definition',pg_get_functiondef(p.oid),
    'sha256',encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),
    'owner',pg_get_userbyid(p.proowner),'volatility',p.provolatile::text,
    'definer',p.prosecdef,'strict',p.proisstrict,'parallel',p.proparallel::text,
    'leakproof',p.proleakproof,'set',p.proretset,
    'result',pg_get_function_result(p.oid),'args',oidvectortypes(p.proargtypes),
    'language',(SELECT lanname FROM pg_language WHERE oid=p.prolang),
    'config',p.proconfig,'callable',has_function_privilege(current_user,p.oid,'EXECUTE'),
    'acl',(SELECT jsonb_agg(jsonb_build_object(
      'grantee',a.grantee::text,'role',r.rolname,'privilege',a.privilege_type,
      'grantable',a.is_grantable) ORDER BY a.grantee,a.privilege_type)
      FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
      LEFT JOIN pg_roles r ON r.oid=a.grantee)
    ) FROM pg_proc p WHERE p.oid=to_regprocedure(
      'sessions.read_history_custody_page_v1(text,text,text,text,jsonb,jsonb)')),
  'writableSource',EXISTS(
    SELECT 1 FROM pg_roles member
    WHERE (pg_has_role(session_user,member.oid,'MEMBER') OR pg_has_role(current_user,member.oid,'MEMBER'))
    AND (
      EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace
        WHERE CASE WHEN ns.nspname='sessions' AND c.relkind IN('r','p','v','m','f')
          THEN has_table_privilege(member.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
          ELSE false END)
      OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace
        WHERE CASE WHEN ns.nspname='sessions' AND c.relkind='S'
          THEN has_sequence_privilege(member.oid,c.oid,'USAGE,UPDATE')
          ELSE false END)
      OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace
        WHERE ns.nspname='sessions' AND p.prosecdef AND p.provolatile='v'
          AND p.prorettype NOT IN('trigger'::regtype,'event_trigger'::regtype)
          AND has_function_privilege(member.oid,p.oid,'EXECUTE'))
    )
  )
 ) value FROM base
), provider AS(
 SELECT jsonb_build_object('protocol','history-source-provider-v1',
  'hostname',value->'hostname','database',value->'database','databaseOid',value->'databaseOid',
  'serverVersion',value->'serverVersion',
  'catalogSequenceOid',value->'sequences'->'delivery_catalog_acceptance_seq',
  'projectionSequenceOid',value->'sequences'->'delivery_projection_acceptance_seq') value FROM base
)
SELECT d.value AS descriptor,p.value AS provider_identity,
 encode(sha256(convert_to(d.value::text,'UTF8')),'hex') AS source_authority_sha256,
 encode(sha256(convert_to(p.value::text,'UTF8')),'hex') AS provider_identity_sha256
 FROM descriptor d CROSS JOIN provider p
`;
