/** A separate readonly public-CA source connection; never reuse Supabase/owner. */
import pg, { type Pool } from "pg";
import {SemanticPostgresExecutor} from "./semantic-copy-postgres.ts";
import {SUPABASE_PROD_CA_2021} from "../session-pipeline/supabase-ca.ts";
import type { FeatureReader } from "./source.ts";
export function semanticReadonlyPool(url: string): Pool {
    const u = new URL(url);
    if (!/^postgres(?:ql)?:$/u.test(u.protocol))
        throw new Error("feature source URL invalid");
    for (const key of ["sslmode", "sslrootcert", "sslcert", "sslkey", "ssl", "options"])
        u.searchParams.delete(key);
    const pool = new pg.Pool({ connectionString: u.toString(), max: 1, connectionTimeoutMillis:1000, ssl: { rejectUnauthorized: true }, application_name: "96bulls-feature-copy-readonly", options: "-c default_transaction_read_only=on -c statement_timeout=15000", idleTimeoutMillis: 60000 });
    pool.on("error", () => { });
    return pool;
}
export function semanticFeatureReader(pool:Pool){return new SemanticPostgresExecutor(pool,true)}
/** Dedicated copier metadata connections; unrelated Lambda work keeps its pool. */
export function semanticMetadataPool(url:string):Pool {
 const u=new URL(url);if(!/^postgres(?:ql)?:$/u.test(u.protocol))throw new Error("feature metadata URL invalid");
 for(const key of ["sslmode","sslrootcert","sslcert","sslkey","ssl","options"])u.searchParams.delete(key);
 const pool=new pg.Pool({connectionString:u.toString(),max:2,connectionTimeoutMillis:1000,ssl:{ca:SUPABASE_PROD_CA_2021,rejectUnauthorized:true},application_name:"96bulls-feature-copy-metadata",idleTimeoutMillis:60000});pool.on("error",()=>{});return pool;
}

/** Runtime credential proof; readonly URL options do not substitute for a readonly principal. */
export async function verifySemanticReadonlyRole(db: FeatureReader, relations: readonly string[], mutators: readonly string[]) {
    const r = (await db.query<any>("SELECT rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,rolinherit FROM pg_catalog.pg_roles WHERE rolname=current_user")).rows;
    if (r.length !== 1 || Object.values(r[0]).some(v => v !== false))
        throw new Error("feature source role unsafe");
    const privileges = (await db.query<any>("SELECT EXISTS(SELECT 1 FROM unnest($1::text[]) t(name) CROSS JOIN LATERAL unnest(ARRAY['INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']||CASE WHEN current_setting('server_version_num')::integer>=170000 THEN ARRAY['MAINTAIN'] ELSE ARRAY[]::text[] END) p(privilege) WHERE has_table_privilege(current_user,to_regclass(t.name),p.privilege)) OR EXISTS(SELECT 1 FROM unnest($1::text[]) t(name) CROSS JOIN unnest(ARRAY['INSERT','UPDATE','REFERENCES']) p(privilege) WHERE has_any_column_privilege(current_user,to_regclass(t.name),p.privilege)) OR EXISTS(SELECT 1 FROM unnest($2::text[]) f(name) WHERE has_function_privilege(current_user,to_regprocedure(f.name),'EXECUTE')) unsafe", [relations, mutators])).rows;
    if (privileges.length !== 1 || privileges[0].unsafe !== false)
        throw new Error("feature source role unsafe");
}
