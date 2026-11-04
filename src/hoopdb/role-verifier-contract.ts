/** One authoritative inventory for migrator, live gates and detached bundles. */
import{fileURLToPath}from"node:url";
const at=(name:string)=>fileURLToPath(new URL("../../supabase/bootstrap/"+name,import.meta.url));
export const ROLE_VERIFIERS=Object.freeze([
 {key:"searchWorker",name:"verify:session_directory_search_worker",path:at("session_directory_search_worker_verify.sql")},
 {key:"tokenVerifier",name:"verify:session_token_verifier",path:at("session_token_verifier_verify.sql")},
 {key:"ingestSeeder",name:"verify:session_ingest_seeder",path:at("session_ingest_seeder_verify.sql")},
 {key:"ingest",name:"verify:session_ingest",path:at("session_ingest_verify.sql")},
 {key:"modelWorker",name:"verify:session_history_model_worker",path:at("session_history_model_verify.sql")},
]as const);
export type SchemaVerifierSql={readonly[K in typeof ROLE_VERIFIERS[number]["key"]]:string};
