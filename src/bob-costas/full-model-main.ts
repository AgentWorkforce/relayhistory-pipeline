/** Permanent Lambda/portable job entry. No daemon or transient-runner dependency. */
import{runFullModelJob}from"./full-model-job.ts";
export{runFullModelJob}from"./full-model-job.ts";
export async function handler(event:unknown,context:{getRemainingTimeInMillis():number}){
 if(!event||typeof event!=="object"||(event as any).fullModel!==true)return{enabled:false,claimed:0,completed:0};
 try{return await runFullModelJob(process.env,()=>context.getRemainingTimeInMillis())}
 catch(error){const code=(error as any)?.code;console.error(JSON.stringify({stage:"full-model",ok:false,error_code:typeof code==="string"&&/^[A-Za-z0-9_]{1,64}$/.test(code)?code:"model_job_failed"}));throw Error("model_job_failed")}
}
if(process.argv.includes("--once")){
 try{console.log(JSON.stringify(await runFullModelJob(process.env,()=>300000)))}
 catch{console.error(JSON.stringify({stage:"full-model",ok:false,error_code:"model_job_failed"}));process.exitCode=1}
}
