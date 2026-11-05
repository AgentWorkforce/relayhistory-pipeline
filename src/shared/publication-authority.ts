/** Shared final-write authority for managed immutable upload images.
 * Hydration/model/remote I/O happens BEFORE withPublicationAuthority. Its callback
 * performs only short metadata statements on the supplied transaction.
 */
import type {SqlExecutor} from "@rhc/lib/pipeline/queue.ts";
import type {Transact} from "./db.ts";
import type {ManifestV1} from "../../packages/object-store/src/index.ts";
export interface PublicationEvidence {
 orgId:string;workspaceId:string;harness:string;sessionId:string;machineId:string;manifestKey:string;revision:string;schemaGeneration:string;
}
export interface PublicationAuthority {managed:boolean;current:boolean;generation:string|null;revision:string}
export function publicationEvidence(orgId:string,manifestKey:string,manifest:ManifestV1,schemaGeneration:string):PublicationEvidence{
 const s=manifest.session;
 return {orgId,workspaceId:s.workspace_id,harness:s.harness,sessionId:s.session_id,machineId:s.machine_id,manifestKey,revision:String(manifest.revision),schemaGeneration};
}
async function authority(db:SqlExecutor,fn:"probe"|"lock",evidence:PublicationEvidence):Promise<PublicationAuthority>{
 const r=await db.query<{result:unknown}>("SELECT sessions.session_publication_"+fn+"($1::jsonb) AS result",[JSON.stringify(evidence)]);
 const v=r.rows[0]?.result as Partial<PublicationAuthority>|undefined;
 if(r.rows.length!==1||!v||typeof v.managed!=="boolean"||typeof v.current!=="boolean"||
  !(v.generation===null||typeof v.generation==="string"&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(v.generation))||
  typeof v.revision!=="string"||!/^[0-9]+$/.test(v.revision))throw Object.assign(Error("publication authority unavailable"),{code:"publication_authority_invalid"});
 if(v.managed?(v.generation===null):(v.generation!==null))throw Object.assign(Error("publication authority unavailable"),{code:"publication_authority_invalid"});
 return v as PublicationAuthority;
}
export const probePublication=(db:SqlExecutor,evidence:PublicationEvidence)=>authority(db,"probe",evidence);
export async function withPublicationAuthority<T>(
 transact:Transact,evidence:PublicationEvidence,work:(tx:SqlExecutor,authority:PublicationAuthority)=>Promise<T>,
):Promise<{current:false;authority:PublicationAuthority}|{current:true;authority:PublicationAuthority;value:T}>{
 return transact(async tx=>{
  const checked=await authority(tx,"lock",evidence);
  if(checked.managed&&!checked.current)return {current:false,authority:checked} as const;
  return {current:true,authority:checked,value:await work(tx,checked)} as const;
 });
}

export async function registerPublicationObject(db:SqlExecutor,pointer:PublicationEvidence,spec:{key:string;sha256:string;bytes:number}):Promise<void>{
 const r=await db.query<{result:unknown}>("SELECT sessions.session_publication_register_object($1::jsonb) AS result",[JSON.stringify({pointer,...spec})]);
 const v=r.rows[0]?.result as {registered?:unknown}|undefined;
 if(r.rows.length!==1||v?.registered!==true)throw Object.assign(Error("publication object registration refused"),{code:"publication_registration_invalid"});
}
/** Qualified replacement for the unqualified legacy Referee RPC. */

export async function setPublicationState(db:SqlExecutor,evidence:PublicationEvidence,state:string,at:Date):Promise<boolean>{
 const e=evidence;
 const r=await db.query<{applied:unknown}>("SELECT sessions.set_session_state_for_publication($1,$2,$3,$4,$5,$6,$7::bigint,$8,$9::timestamptz,$10) AS applied",
  [e.orgId,e.workspaceId,e.harness,e.sessionId,e.machineId,e.manifestKey,e.revision,state,at.toISOString(),e.schemaGeneration]);
 if(r.rows.length!==1||typeof r.rows[0]?.applied!=="boolean")throw Object.assign(Error("publication state result invalid"),{code:"publication_state_invalid"});
 return r.rows[0].applied;
}
