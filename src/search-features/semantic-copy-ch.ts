import {createClickHouseClient,type ClickHouseClient,type ClickHouseConfig,type RequestOptions} from "../session-pipeline/vendor/search-index/clickhouse.ts";
import type {SemanticQueryContext} from "./semantic-copy-postgres.ts";
export interface SemanticClickHouseClient extends ClickHouseClient {scoped(context:SemanticQueryContext):ClickHouseClient}
/** Use the vendored client's existing fetch hook; never fork its pinned source. */
export function createSemanticClickHouseClient(config:ClickHouseConfig,fetcher:typeof fetch=fetch):SemanticClickHouseClient{
 const base=createClickHouseClient(config,fetcher);
 return {...base,scoped(context){
  const scoped=createClickHouseClient(config,(input,init)=>{
   context.signal.throwIfAborted();const remaining=Math.ceil(context.deadline-(context.now??Date.now)());
   if(remaining<=0)throw new Error("semantic_ch_deadline");
   const signals=[context.signal,AbortSignal.timeout(remaining)];if(init?.signal)signals.push(init.signal);
   return fetcher(input,{...init,signal:AbortSignal.any(signals)});
  });
  return scopedSemanticCh(scoped,context);
 }};
}
/** Remaining-budget options apply to EVERY command/select/insert. */
export function scopedSemanticCh(ch:ClickHouseClient,context:SemanticQueryContext):ClickHouseClient{
 const options=(value:RequestOptions={})=>{
  context.signal.throwIfAborted();const remaining=Math.ceil(context.deadline-(context.now??Date.now)());
  if(remaining<=0)throw new Error("semantic_ch_deadline");
  return {...value,timeoutMs:Math.min(value.timeoutMs??30000,remaining),
   settings:{...value.settings,max_execution_time:Math.max(1,Math.min(Number(value.settings?.max_execution_time??30),Math.ceil(remaining/1000)))}};
 };
 return {database:ch.database,command:(sql,o)=>ch.command(sql,options(o)),select:<T>(sql:string,o?:RequestOptions&{params?:Record<string,any>})=>ch.select<T>(sql,{...options(o),params:o?.params}),insert:(table,rows,o)=>ch.insert(table,rows,options(o))};
}
