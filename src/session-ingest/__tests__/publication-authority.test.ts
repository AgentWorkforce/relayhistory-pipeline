
import {test,expect} from "bun:test";
import {probePublication,withPublicationAuthority,type PublicationEvidence} from "../../shared/publication-authority.ts";
import type {Transact} from "../../shared/db.ts";
const evidence:PublicationEvidence={orgId:"org",workspaceId:"ws",harness:"claude",sessionId:"s",machineId:"srv_m",manifestKey:"key",revision:"1",schemaGeneration:"a".repeat(64)};
const generation="12345678-1234-1234-1234-123456789abc";
test("stale managed result never invokes final metadata callback",async()=>{
 let writes=0,commits=0;
 const transact:Transact=async work=>{const v=await work({query:async()=>({rows:[{result:{managed:true,current:false,generation,revision:"2"}}]})} as never);commits++;return v;};
 const result=await withPublicationAuthority(transact,evidence,async()=>{writes++;});
 expect(result.current).toBe(false);expect(writes).toBe(0);expect(commits).toBe(1);
});
test("exact current authority and final writes share the same transaction executor",async()=>{
 let queries=0;const db={query:async(sql:string)=>{queries++;if(sql.includes("session_publication_lock"))return {rows:[{result:{managed:true,current:true,generation,revision:"1"}}]};return {rows:[]};}};
 const transact:Transact=async work=>work(db as never);
 const r=await withPublicationAuthority(transact,evidence,async tx=>{expect(tx).toBe(db);await tx.query("UPDATE synthetic_metadata");return "done";});
 expect(r.current).toBe(true);expect("value"in r&&r.value).toBe("done");expect(queries).toBe(2);
});
test("missing or contradictory authority refuses instead of treating absent registry as current",async()=>{
 for(const result of[null,{}, {managed:true,current:true,generation:null,revision:"1"},{managed:false,current:true,generation,revision:"1"}]){
  const db={query:async()=>({rows:[{result}]})};await expect(probePublication(db as never,evidence)).rejects.toMatchObject({code:"publication_authority_invalid"});
 }
});


test("prepared derived body and qualified state carry exact source schema without inferring it",async()=>{
 const {registerPublicationObject,setPublicationState}=await import("../../shared/publication-authority.ts");
 const calls:{sql:string,args:unknown[]|undefined}[]=[];
 const db={query:async(sql:string,args?:unknown[])=>{calls.push({sql,args});return {rows:[sql.includes("register_object")?{result:{registered:true}}:{applied:true}]};}};
 await registerPublicationObject(db as never,evidence,{key:"prepared-key",sha256:"b".repeat(64),bytes:42});
 const submitted=JSON.parse(String(calls[0]!.args![0]));
 expect(submitted.pointer.schemaGeneration).toBe(evidence.schemaGeneration);expect(submitted.key).toBe("prepared-key");expect(submitted.bytes).toBe(42);
 expect(await setPublicationState(db as never,evidence,"done",new Date("2026-11-04T00:00:00Z"))).toBe(true);
 expect(calls[1]!.args![9]).toBe(evidence.schemaGeneration);
});

