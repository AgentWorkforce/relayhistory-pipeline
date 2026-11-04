// GENERATED from AgentWorkforce/relayhistory-cloud packages/relayhistory/src/lib/delivery-source-authority-sql.ts @ 689143c3cd6b75af9954727c6493965f7c24672b
// SHA256 83b28761d8b6236f6a92206360de94b7f075baa3cda793f72ed3e4c766045b7e; regenerate: bun scripts/sync-history-source-contract.ts
/** Pure existing ACK source-authority query; exact SQL bytes are preserved. */
import {
  DELIVERY_SOURCE_ROLE_SQL,
  sanitizedDeliveryRoleFacts,
} from "./delivery-source-role-facts.ts";
export const CURRENT_CUSTODY_AUTHORITY_SQL = `
 WITH role_probe AS MATERIALIZED(${DELIVERY_SOURCE_ROLE_SQL})
 SELECT jsonb_build_object('connectionRole',(SELECT role_facts FROM role_probe),'protocol','relayhistory-current-source-v1','hostname',$1::text,
 'database',current_database(),'databaseOid',(SELECT oid::text FROM pg_database WHERE datname=current_database()),
 'serverVersion',current_setting('server_version_num'),
 'databaseCollation',(SELECT jsonb_build_object('collate',datcollate,'ctype',datctype,'provider',datlocprovider::text,'locale',coalesce(to_jsonb(d)->>'datlocale','')) FROM pg_database d WHERE datname=current_database()),
 'sequences',(SELECT jsonb_object_agg(c.relname,c.oid::text) FROM pg_class c WHERE c.oid=ANY(ARRAY['sessions.delivery_catalog_acceptance_seq'::regclass,'sessions.delivery_projection_acceptance_seq'::regclass])),
 'functions',(SELECT jsonb_object_agg(p.proname,jsonb_build_object('sha256',encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),'definer',p.prosecdef,'volatility',p.provolatile::text,'owner',pg_get_userbyid(p.proowner),'language',(SELECT lanname FROM pg_language WHERE oid=p.prolang),'acl',p.proacl::text,'config',p.proconfig))
 FROM pg_proc p WHERE p.oid=ANY(ARRAY[
 'sessions.delivery_cutover_witness_context_v1(text,text,text)'::regprocedure,
 'sessions.snapshot_delivery_cutover_privacy_v1(text,text,text)'::regprocedure,
 'sessions.begin_delivery_cutover_control_v1(text,text,text,text,text,jsonb)'::regprocedure,
 'sessions.complete_delivery_cutover_control_v1(text,text,text,text,bigint,text,text,text)'::regprocedure,
 'sessions.guard_delivery_cutover_v1()'::regprocedure,
 'sessions.record_delivery_projection_acceptance(sessions.delivery_records)'::regprocedure,
 'sessions.project_delivery_catalog()'::regprocedure])),
 'relations',(SELECT jsonb_agg(jsonb_build_object('name',c.relname,'owner',pg_get_userbyid(c.relowner),'rls',c.relrowsecurity,'forceRls',c.relforcerowsecurity) ORDER BY c.relname)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND c.relname=ANY(ARRAY['delivery_origins','delivery_receipts','delivery_records','deleted_upload_sessions','session_resets','uploaded_support_identities','delivery_replay_divergence','delivery_catalog_acceptance','delivery_projection_acceptance','delivery_cutover_state','delivery_cutover_control_heads','convergence_events'])),
 'primaryKey',(SELECT jsonb_build_object('oid',i.indexrelid::text,'valid',i.indisvalid,'ready',i.indisready,'unique',i.indisunique,'keys',
 (SELECT jsonb_agg(jsonb_build_object('name',a.attname,'columnCollation',a.attcollation::text,'indexCollation',i.indcollation[k.n-1]::text,'option',i.indoption[k.n-1]) ORDER BY k.n)
 FROM unnest(i.indkey) WITH ORDINALITY k(num,n) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num WHERE k.n<=i.indnkeyatts))
 FROM pg_index i WHERE i.indrelid='sessions.delivery_records'::regclass AND i.indisprimary),
 'nativeKey',(SELECT jsonb_agg(jsonb_build_object('name',a.attname,'collationOid',a.attcollation::text,'collationName',c.collname,'provider',c.collprovider::text) ORDER BY a.attname)
 FROM pg_attribute a JOIN pg_collation c ON c.oid=a.attcollation
 WHERE a.attrelid='sessions.delivery_records'::regclass AND a.attname=ANY(ARRAY['org_id','workspace_id','origin_id','record_id','revision_id','kind','source','session_id','user_id'])),
 'guards',(SELECT jsonb_agg(jsonb_build_object('table',c.relname,'name',t.tgname,'type',t.tgtype,'enabled',t.tgenabled::text,'functionOid',t.tgfoid::text) ORDER BY c.relname,t.tgname)
 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='sessions' AND NOT t.tgisinternal AND t.tgname IN('delivery_cutover_statement','delivery_cutover_row','delivery_cutover_projection'))
 ) AS value`;
const object = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
export function compatibleCustodySourceAuthority(value: unknown): boolean {
  if (
    !object(value) ||
    !object(value.databaseCollation) ||
    !Array.isArray(value.nativeKey) ||
    !Array.isArray(value.relations) ||
    !object(value.primaryKey) ||
    !object(value.functions)
  )
    return false;
  try {
    if (!sanitizedDeliveryRoleFacts(value.connectionRole).restricted)
      return false;
  } catch {
    return false;
  }
  const d = value.databaseCollation,
    p = value.primaryKey,
    fs = value.functions;
  const context = fs.delivery_cutover_witness_context_v1,
    snapshot = fs.snapshot_delivery_cutover_privacy_v1,
    guard = fs.guard_delivery_cutover_v1;
  if (
    !object(context) ||
    !object(snapshot) ||
    !object(guard) ||
    context.volatility !== "s" ||
    snapshot.volatility !== "s" ||
    context.definer !== true ||
    snapshot.definer !== false ||
    context.language !== "sql" ||
    snapshot.language !== "plpgsql" ||
    context.owner !== snapshot.owner ||
    context.owner !== guard.owner
  )
    return false;
  for (const fn of [context, snapshot, guard]) {
    if (
      !Array.isArray(fn.config) ||
      fn.config.length !== 1 ||
      typeof fn.config[0] !== "string" ||
      fn.config[0].replaceAll(" ", "") !==
        "search_path=pg_catalog,sessions,pg_temp"
    )
      return false;
  }
  return (
    d.collate === "C.UTF-8" &&
    d.ctype === "C.UTF-8" &&
    d.provider === "b" &&
    d.locale === "C.UTF-8" &&
    value.nativeKey.length === 9 &&
    value.nativeKey.every(
      (a) => object(a) && a.collationName === "default" && a.provider === "d",
    ) &&
    value.relations.length === 12 &&
    value.relations.every(
      (r) => object(r) && r.rls === false && r.forceRls === false,
    ) &&
    p.valid === true &&
    p.ready === true &&
    p.unique === true &&
    Array.isArray(p.keys) &&
    p.keys.length === 4 &&
    p.keys.map((k) => (object(k) ? k.name : "")).join(",") ===
      "org_id,workspace_id,origin_id,record_id" &&
    p.keys.every(
      (k) =>
        object(k) && k.columnCollation === k.indexCollation && k.option === 0,
    )
  );
}
