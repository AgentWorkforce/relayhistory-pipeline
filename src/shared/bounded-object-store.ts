/** Abortable object-store facade. Callers register immutable write intents before
 * PUT: aborting the waiter cannot guarantee an underlying late write stopped. */
import type {ObjectStore} from "../../packages/object-store/src/index.ts";
export function withAbortSignal<T>(work:()=>Promise<T>,signal:AbortSignal):Promise<T>{
 signal.throwIfAborted();
 return new Promise((resolve,reject)=>{
  const abort=()=>reject(signal.reason??Object.assign(Error("publication_aborted"),{name:"AbortError"}));
  signal.addEventListener("abort",abort,{once:true});
  Promise.resolve().then(()=>{signal.throwIfAborted();return work();}).then(
   value=>{signal.removeEventListener("abort",abort);if(signal.aborted)abort();else resolve(value);},
   error=>{signal.removeEventListener("abort",abort);reject(error);});
  if(signal.aborted)abort();
 });
}
function body(source:ReadableStream<Uint8Array>,signal:AbortSignal):ReadableStream<Uint8Array>{
 const reader=source.getReader();let stopped=false;
 let abort:()=>void=()=>{};
 return new ReadableStream<Uint8Array>({
  start(controller){
   abort=()=>{if(stopped)return;stopped=true;controller.error(signal.reason??Error("publication_aborted"));void reader.cancel().catch(()=>{});};
   signal.addEventListener("abort",abort,{once:true});
   void(async()=>{
    try{
     while(!stopped){
      const next=await reader.read();signal.throwIfAborted();
      if(next.done){stopped=true;controller.close();break;}controller.enqueue(next.value);
     }
    }catch(error){if(!stopped){stopped=true;controller.error(error);}}
    finally{signal.removeEventListener("abort",abort);try{reader.releaseLock();}catch{}}
   })();
   if(signal.aborted)abort();
  },
  cancel(){stopped=true;signal.removeEventListener("abort",abort);void reader.cancel().catch(()=>{});}
 });
}
/** Preserves the caller's capabilities; a readonly GET port gains no writes. */
export function boundedObjectStore<T extends Partial<ObjectStore>>(store:T,signal:AbortSignal):T{
 return new Proxy(store,{get(target,key){
  const value=Reflect.get(target,key);
  if(typeof value!=="function")return value;
  if(key==="getObject")return (...args:unknown[])=>withAbortSignal(async()=>{
   const object=await Reflect.apply(value,target,args);return {...object,body:body(object.body,signal)};
  },signal);
  if(key==="get")return (...args:unknown[])=>withAbortSignal(async()=>body(await Reflect.apply(value,target,args),signal),signal);
  return (...args:unknown[])=>withAbortSignal(async()=>Reflect.apply(value,target,args),signal);
 }});
}
