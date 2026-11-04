/** Finite-duration, default-off runtime core. Managed caller supplies isolated
 * bounded metadata/heartbeat clients and an explicitly configured provider.
 * The durable630 ledger, not a process callback or transient EC2, resumes work. */
import type{ObjectStore}from"../../packages/object-store/src/index.ts";
import{scopeSemanticSql,type SemanticBoundedSql}from"../search-features/semantic-copy-postgres.ts";
import type{WorkspaceKeyring}from"./summary-objects.ts";
import type{FullModelConfig,FullModelTransport}from"./full-model-provider.ts";
import{fullModelSourceBinding}from"./full-model-input.ts";
import{modelArtifactPorts,prepareFullModelResults}from"./full-model-artifacts.ts";
import{openFullModelSource}from"./full-model-source.ts";
import{stepFullModelWork}from"./full-model-work.ts";
import{claimFullModelWork,heartbeatFullModelWork,checkpointFullModelWork,cancelFullModelWork,enqueueFullModelWork,modelWorkConfig}from"./full-model-ledger.ts";
export async function runFullModelWorkTick(args:{
 enabled:boolean;schemaGeneration:string;db:SemanticBoundedSql;heartbeatDb:SemanticBoundedSql;
 store:ObjectStore;keyring:WorkspaceKeyring;config:FullModelConfig|null;model:FullModelTransport|null;
 remainingMs:()=>number;signal?:AbortSignal;now?:()=>number;maxSteps?:number;
}){
 const counts={enabled:false,claimed:0,modelCalls:0,checkpointed:0,completed:0,sourceChanged:0,graphPending:0,configChanged:0};
 if(!args.enabled||!args.config||!args.model)return counts;
 if(!Number.isInteger(args.maxSteps??16)||(args.maxSteps??16)<1||(args.maxSteps??16)>64)throw Error("model_tick_step_bound");
 counts.enabled=true;
 const now=args.now??Date.now,stop=new AbortController(),deadline=now()+Math.max(1,Math.min(240000,args.remainingMs()-15000));
 const signal=AbortSignal.any([stop.signal,args.signal??AbortSignal.timeout(Math.max(1,deadline-now())),AbortSignal.timeout(Math.max(1,deadline-now()))]);
 const db=scopeSemanticSql(args.db,{signal,deadline,now});
 try{
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
     await enqueueFullModelWork(stepDb,ticket.captured,modelWorkConfig(args.config.model,currentKey.keyId));
     await cancelFullModelWork(stepDb,ticket);counts.configChanged++;continue;
    }
    interval=setInterval(()=>{
     if(pulse||stepSignal.aborted)return;
     const heartbeatDeadline=Math.min(deadline,now()+10000);
     const heartbeat=scopeSemanticSql(args.heartbeatDb,{signal:stepSignal,deadline:heartbeatDeadline,now});
     pulse=heartbeatFullModelWork(heartbeat,ticket).then(ok=>{if(!ok)stepStop.abort(Error("model_work_lease_lost"))})
      .catch(()=>{stepStop.abort(Error("model_work_heartbeat_failed"))}).finally(()=>{pulse=undefined});
    },20000);
    const source=await openFullModelSource({db:stepDb,store:args.store,captured:ticket.captured,signal:stepSignal});
    const objects=modelArtifactPorts({db:stepDb,store:args.store,keyring:args.keyring,captured:ticket.captured,workId:ticket.workId,configSha256:ticket.config.protocolSha256});
    const result=await stepFullModelWork({ports:{...objects,readRow:source.readRow,model:args.model},checkpoint:ticket.checkpoint,
     sourceBinding:fullModelSourceBinding(ticket.captured),configSha256:ticket.config.protocolSha256,signal:stepSignal});
    if(result.modelCalled)counts.modelCalls++;
    if(interval){clearInterval(interval);interval=undefined;}
    // Stop and drain this job's own heartbeat before checkpoint can release its
    // original lease; no detached late heartbeat may race the next lease.
    if(pulse)await pulse;
    stepSignal.throwIfAborted();
    const done=result.kind==="complete"?await prepareFullModelResults({db:stepDb,store:args.store,keyring:args.keyring,captured:ticket.captured,workId:ticket.workId,configSha256:ticket.config.protocolSha256},
     {conclusion:result.conclusion,checkpoint:result.next,model:result.next.actualModel??args.config.model,builtAt:new Date(now()).toISOString(),signal:stepSignal}):undefined;
    const advanced=await checkpointFullModelWork(stepDb,ticket,result.next,done);
    if(!advanced){counts.sourceChanged++;continue;}
    counts.checkpointed++;if(done)counts.completed++;
   }catch(error){
    if((error as any)?.code==="model_source_graph_pending"){counts.graphPending++;continue;}
    if((error as any)?.code==="model_source_changed"){counts.sourceChanged++;continue;}
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
