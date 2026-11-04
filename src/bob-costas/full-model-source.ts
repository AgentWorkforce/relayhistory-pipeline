/** Bounded ordinal adapter over Native's complete MASKED canonical artifact
 * graph. No raw decoder, independent fold, Neon fallback, prompt sample or
 * content truncation. The same codec authenticates arbitrary raw/tool fields. */
import{createHash}from"node:crypto";
import type{SqlExecutor}from"../../packages/pipeline/src/lib/pipeline/queue.ts";
import type{ObjectStore}from"../../packages/object-store/src/index.ts";
import{readCapped}from"../../packages/pipeline/src/lib/pipeline/adapter.ts";
import{boundedObjectStore}from"../shared/bounded-object-store.ts";
import{readHistoryReplayDescriptor,type HistorySourceReference}from"../shared/history-source-authority.ts";
import{ARTIFACT_TABLES,ARTIFACT_MAX_OBJECT_BYTES,type ArtifactIndex,type ArtifactPublication}from"../session-read-api/canonical-artifact.ts";
import{canonicalArtifactNodePage,artifactOrdinalCursor}from"../session-read-api/canonical-artifact-reader.ts";
import{nodeReadBudget,type NodeReadProfile}from"../session-read-api/node-read-profile.ts";
import type{FullModelProjectionRow}from"./full-model-input.ts";
function no(code:string):never{throw Object.assign(Error(code),{code})}
const sameIdentity=(a:any,b:any)=>["orgId","workspaceId","source","sessionId"].every(k=>a?.[k]===b?.[k]);
const sameSource=(a:any,b:any)=>a?.epoch===b?.epoch&&a?.fingerprint===b?.fingerprint;
function samePublication(a:ArtifactPublication|null,b:ArtifactPublication):boolean{
 return a!==null&&a.generation===b.generation&&a.prefix===b.prefix&&sameIdentity(a.identity,b.identity)&&sameSource(a.source,b.source)&&a.index.key===b.index.key&&a.index.sha256===b.index.sha256&&a.index.bytes===b.index.bytes;
}
export interface FullModelSourceReader{
 readonly publication:ArtifactPublication;readonly totalRows:string;
 readRow(ordinal:string,signal:AbortSignal):Promise<FullModelProjectionRow|null>;
}
export async function openFullModelSource(args:{
 db:SqlExecutor;dbForSignal?:(signal:AbortSignal)=>SqlExecutor;store:ObjectStore;captured:HistorySourceReference;publication?:ArtifactPublication;signal:AbortSignal;profile?:NodeReadProfile;
}):Promise<FullModelSourceReader>{
 const {captured,db}=args,budget=nodeReadBudget(args.profile);
 args.signal.throwIfAborted();
 const publication=await readHistoryReplayDescriptor(db,captured);
 if(!publication)no("model_source_graph_pending");
 if(args.publication&&!samePublication(publication,args.publication))no("model_source_graph_changed");
 const store=boundedObjectStore(args.store,args.signal);
 const object=await store.getObject(publication.index.key as never);
 if(object.meta.size>ARTIFACT_MAX_OBJECT_BYTES){await object.body.cancel().catch(()=>{});no("model_source_index_bound");}
 const data=await readCapped(object.body,Math.min(ARTIFACT_MAX_OBJECT_BYTES,publication.index.bytes));
 if(!data||data.byteLength!==publication.index.bytes||createHash("sha256").update(data).digest("hex")!==publication.index.sha256)no("model_source_index_digest");
 let index:ArtifactIndex;
 try{index=JSON.parse(new TextDecoder("utf-8",{fatal:true}).decode(data))}catch{no("model_source_index");}
 if(index.version!==1||index.kind!=="canonical-read-index"||index.codec!=="json-utf8-v1"||!sameIdentity(index.identity,captured.identity)||!sameSource(index.source,captured.source)||index.generation!==publication.generation||index.prefix!==publication.prefix||!index.tables||typeof index.tables!=="object")no("model_source_index");
 const ranges:{table:string;first:number;count:number}[]=[];let total=0;
 for(const table of new Set([...ARTIFACT_TABLES,...Object.keys(index.tables).sort()])){
  const selected=index.tables[table]?.raw;
  if(!selected||!Number.isSafeInteger(selected.count)||selected.count<0||(selected.count===0?selected.root!==null:!selected.root))no("model_source_table_incomplete");
  ranges.push({table,first:total,count:selected.count});total+=selected.count;
  if(!Number.isSafeInteger(total)||total>budget.projectedRows)no("model_source_row_bound");
 }
 // Hash-map/object insertion order is not scope identity. The shared source
 // helper already pins identity fields; canonical cursor constructor rechecks.
 if(!samePublication(await readHistoryReplayDescriptor(db,captured),publication))no("model_source_changed");
 let cached:{ordinal:string;row:FullModelProjectionRow}|undefined;
 return{
  publication,totalRows:String(total),
  async readRow(ordinal,signal){
   signal.throwIfAborted();args.signal.throwIfAborted();
   if(!/^(0|[1-9][0-9]{0,18})$/.test(ordinal)||BigInt(ordinal)>BigInt(total))no("model_source_ordinal");
   if(BigInt(ordinal)===BigInt(total)){
    if(!samePublication(await readHistoryReplayDescriptor(db,captured),publication))no("model_source_changed");
    return null;
   }
   if(cached?.ordinal===ordinal)return cached.row;
   const position=Number(ordinal),range=ranges.find(r=>position>=r.first&&position<r.first+r.count);
   if(!range)no("model_source_coverage");
   const deps={signal:AbortSignal.any([args.signal,signal]),
    publication:async(_identity:unknown,readSignal:AbortSignal)=>{const p=await readHistoryReplayDescriptor(args.dbForSignal?.(readSignal)??db,captured);if(!samePublication(p,publication))no("model_source_changed");return{...p,exists:true,current:true};},
    read:async(key:string,readSignal:AbortSignal)=>boundedObjectStore(args.store,readSignal).get(key as never)};
   const result=await canonicalArtifactNodePage(deps,captured.identity,{table:range.table,view:"raw",order:"asc",limit:1,generation:publication.generation,
    cursor:artifactOrdinalCursor(captured.identity,publication,{table:range.table,view:"raw",order:"asc",position:position-range.first})});
   if(result.exists!==true||result.rows.length!==1||result.rows[0]?.table!==range.table)no("model_source_coverage");
   cached={ordinal,row:result.rows[0]!};return cached.row;
  }
 };
}
