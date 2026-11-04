/** Finite-duration, default-off runtime core. Managed caller supplies isolated
 * bounded metadata/heartbeat clients and an explicitly configured provider.
 * The durable630 ledger, not a process callback or transient EC2, resumes work. */
import type{ObjectStore}from"../../packages/object-store/src/index.ts";
import{scopeSemanticSql,type SemanticBoundedSql}from"../search-features/semantic-copy-postgres.ts";
import type{WorkspaceKeyring}from"./summary-objects.ts";
import type{FullModelConfig,FullModelTransport}from"./full-model-provider.ts";
import{captureHistorySourceReference,readHistoryReplayDescriptor}from"../shared/history-source-authority.ts";
import{fullModelGraphBinding}from"./full-model-input.ts";
import{modelArtifactPorts,prepareFullModelResults}from"./full-model-artifacts.ts";
import{openFullModelSource}from"./full-model-source.ts";
import{stepFullModelWork}from"./full-model-work.ts";
import{discoverFullModelScopes,claimFullModelWork,failFullModelWork,heartbeatFullModelWork,checkpointFullModelWork,cancelFullModelWork,enqueueFullModelWork,modelWorkConfig}from"./full-model-ledger.ts";
export async function runFullModelWorkTick(args:{
 enabled:boolean;schemaGeneration:string;db:SemanticBoundedSql;heartbeatDb:SemanticBoundedSql;
 store:ObjectStore;keyring:WorkspaceKeyring;config:FullModelConfig|null;model:FullModelTransport|null;
 remainingMs:()=>number;signal?:AbortSignal;now?:()=>number;maxSteps?:number;
}){
 const counts={enabled:false,claimed:0,enqueued:0,modelCalls:0,checkpointed:0,completed:0,sourceChanged:0,graphPending:0,configChanged:0,failed:0};
 if(!args.enabled||!args.config||!args.model)return counts;
 if(!Number.isInteger(args.maxSteps??16)||(args.maxSteps??16)<1||(args.maxSteps??16)>64)throw Error("model_tick_step_bound");
 counts.enabled=true;
 const now=args.now??Date.now,stop=new AbortController(),deadline=now()+Math.max(1,Math.min(240000,args.remainingMs()-15000));
 const signal=AbortSignal.any([stop.signal,args.signal??AbortSignal.timeout(Math.max(1,deadline-now())),AbortSignal.timeout(Math.max(1,deadline-now()))]);
 const db=scopeSemanticSql(args.db,{signal,deadline,now});
 let cachedSource:{binding:string;reader:Awaited<ReturnType<typeof openFullModelSource>>}|undefined;
 try{
  if(deadline-now()<args.config.timeoutMs+25000)return counts;
  for(const wake of await discoverFullModelScopes(db,args.schemaGeneration)){
   if(deadline-now()<args.config.timeoutMs+25000)break;
   try{
    const captured=await captureHistorySourceReference(db,wake.identity,{schemaGeneration:args.schemaGeneration,profile:"large",signal});
    if(captured.authorityId!==wake.authorityId||captured.maskSha256!==wake.maskSha256||captured.source.epoch!==wake.source.epoch||captured.source.fingerprint!==wake.source.fingerprint)continue;
    const graph=await readHistoryReplayDescriptor(db,captured);if(!graph){counts.graphPending++;continue;}
    const key=await args.keyring.currentKey({orgId:captured.identity.orgId,workspaceId:captured.identity.workspaceId});
    const work=await enqueueFullModelWork(db,captured,modelWorkConfig(args.config.model,key.keyId,graph),wake.wakeToken);
    if(work.current)counts.enqueued++;
   }catch(e){if((e as any)?.code==="history_source_unavailable"){counts.sourceChanged++;continue;}throw e}
  }
  for(let step=0;step<(args.maxSteps??16);step++){
   if(deadline-now()<args.config.timeoutMs+25000)break;
   signal.throwIfAborted();
   const ticket=await claimFullModelWork(db,args.schemaGeneration,args.config.model);
   if(!ticket)break;counts.claimed++;
   const stepStop=new AbortController(),stepSignal=AbortSignal.any([signal,stepStop.signal]);
   const stepDb=scopeSemanticSql(args.db,{signal:stepSignal,deadline,now});
   let pulse:Promise<void>|undefined,interval:ReturnType<typeof setInterval>|undefined;
   try{
    const currentKey=await args.keyring.currentKey({orgId:ticket.captured.identity.orgId,workspaceId:ticket.captured.identity.workspaceId});
    if(currentKey.keyId!==ticket.config.keyId){
     // No stale-key continuation is rebased. A new config gets independent
     // durable work, while the exact original lease is canceled metadata-only.
     await enqueueFullModelWork(stepDb,ticket.captured,modelWorkConfig(args.config.model,currentKey.keyId,ticket.config.graph));
     await cancelFullModelWork(stepDb,ticket);counts.configChanged++;continue;
    }
    interval=setInterval(()=>{
     if(pulse||stepSignal.aborted)return;
     const heartbeatDeadline=Math.min(deadline,now()+10000);
     const heartbeat=scopeSemanticSql(args.heartbeatDb,{signal:stepSignal,deadline:heartbeatDeadline,now});
     pulse=heartbeatFullModelWork(heartbeat,ticket).then(ok=>{if(!ok)stepStop.abort(Error("model_work_lease_lost"))})
      .catch(()=>{stepStop.abort(Error("model_work_heartbeat_failed"))}).finally(()=>{pulse=undefined});
    },20000);
    const sourceBinding=fullModelGraphBinding(ticket.captured,ticket.config.graph);
    if(cachedSource?.binding!==sourceBinding)cachedSource={binding:sourceBinding,reader:await openFullModelSource({db,dbForSignal:s=>scopeSemanticSql(args.db,{signal:AbortSignal.any([signal,s]),deadline,now}),store:args.store,captured:ticket.captured,publication:ticket.config.graph,signal,profile:"large"})};
    const source=cachedSource.reader;
    const objects=modelArtifactPorts({db:stepDb,store:args.store,keyring:args.keyring,captured:ticket.captured,workId:ticket.workId,configSha256:ticket.config.protocolSha256,sourceBinding:ticket.checkpoint.sourceBinding,keyId:ticket.config.keyId});
    const result=await stepFullModelWork({ports:{...objects,readRow:source.readRow,model:args.model},checkpoint:ticket.checkpoint,
     sourceBinding:fullModelGraphBinding(ticket.captured,ticket.config.graph),configSha256:ticket.config.protocolSha256,signal:stepSignal});
    if(result.modelCalled)counts.modelCalls++;
    if(interval){clearInterval(interval);interval=undefined;}
    // Stop and drain this job's own heartbeat before checkpoint can release its
    // original lease; no detached late heartbeat may race the next lease.
    if(pulse)await pulse;
    stepSignal.throwIfAborted();
    const done=result.kind==="complete"?await prepareFullModelResults({db:stepDb,store:args.store,keyring:args.keyring,captured:ticket.captured,workId:ticket.workId,configSha256:ticket.config.protocolSha256,sourceBinding:ticket.checkpoint.sourceBinding,keyId:ticket.config.keyId},
     {conclusion:result.conclusion,checkpoint:result.next,model:result.next.actualModel??args.config.model,builtAt:new Date(now()).toISOString(),signal:stepSignal}):undefined;
    const advanced=await checkpointFullModelWork(stepDb,ticket,result.next,done);
    if(!advanced){counts.sourceChanged++;continue;}
    counts.checkpointed++;if(done)counts.completed++;
   }catch(error){
    if((error as any)?.code==="model_source_graph_pending"){counts.graphPending++;continue;}
    if(["model_source_changed","model_source_graph_changed"].includes((error as any)?.code)){counts.sourceChanged++;continue;}
    if(interval){clearInterval(interval);interval=undefined;}if(pulse)await pulse;
    if(!stepSignal.aborted){
     const raw=(error as any)?.code,code=typeof raw==="string"&&/^[a-z0-9_]{1,64}$/.test(raw)?raw:"model_step_error";
     const retryable=typeof(error as any)?.retryable==="boolean"?(error as any).retryable:!code.endsWith("_invalid");
     await failFullModelWork(stepDb,ticket,{code,retryable});counts.failed++;continue;
    }
    throw error;
   }finally{
    if(interval)clearInterval(interval);
    stepStop.abort(Error("model_step_finished"));
    if(pulse)await pulse;
   }
  }
  return counts;
 }finally{stop.abort(Error("model_tick_finished"))}
}
