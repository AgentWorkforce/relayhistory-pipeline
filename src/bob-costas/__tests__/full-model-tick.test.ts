import{test,expect}from"bun:test";
import{runFullModelWorkTick}from"../full-model-tick.ts";
test("default off, absent provider and insufficient runtime budget cannot claim, fetch data or spend",async()=>{
 let io=0;
 const db={query:async()=>{io++;throw Error("unexpected IO")},queryBounded:async()=>{io++;throw Error("unexpected IO")}};
 const base={enabled:false,schemaGeneration:"a".repeat(64),db,heartbeatDb:db,store:{}as never,keyring:{}as never,config:{model:"fixture",url:"https://api.openai.com/v1/responses",timeoutMs:45000,maxOutputTokens:2048},model:{complete:async()=>{io++;throw Error("unexpected model")}},remainingMs:()=>300000};
 expect((await runFullModelWorkTick(base)).claimed).toBe(0);
 expect((await runFullModelWorkTick({...base,enabled:true,config:null,model:null})).enabled).toBe(false);
 expect((await runFullModelWorkTick({...base,enabled:true,remainingMs:()=>30000})).claimed).toBe(0);
 expect(io).toBe(0);
});
