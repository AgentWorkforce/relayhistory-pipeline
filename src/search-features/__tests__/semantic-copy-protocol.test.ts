import {expect,test} from "bun:test";
import {semanticCheckpoint,semanticSha256,semanticDataHashInput,semanticAuthorityHashInput,semanticStatusVersion,parseSemanticProfile,foldSemanticQuery} from "../semantic-copy-protocol.ts";
test("authority generation fences writes while stable data hashes preserve unchanged cursor identity",async()=>{
 const v={sourceId:"legacy-main",kind:"event" as const,sourceAuthorityHash:"a".repeat(64),targetHash:"b".repeat(64),profileHash:"c".repeat(64),stateHash:"d".repeat(64)};
 const data=await semanticSha256(semanticDataHashInput(v));expect(data).toMatch(/^[a-f0-9]{64}$/);expect(await semanticSha256(semanticDataHashInput({...v}))).toBe(data);
 expect(await semanticSha256(semanticAuthorityHashInput(data,"5",true))).not.toBe(await semanticSha256(semanticAuthorityHashInput(data,"6",true)));
 expect(semanticStatusVersion("9007199254740993",true)).toBe("18014398509481986");expect(semanticStatusVersion("9007199254740993",false)).toBe("18014398509481987");
});
test("checkpoint validation refuses invented completeness or reset baseline",()=>{
 expect(semanticCheckpoint({phase:"baseline",baselineH:null,baselineCursor:null,checkpoint:"0",observedRevision:"0",polled:false}).baselineH).toBeNull();
 for(const v of [{phase:"journal",baselineH:null,baselineCursor:null,checkpoint:"5",observedRevision:"5",polled:true},{phase:"baseline",baselineH:"5",baselineCursor:null,checkpoint:"5",observedRevision:"5",polled:true},{phase:"journal",baselineH:"5",baselineCursor:null,checkpoint:"6",observedRevision:"5",polled:true}])expect(()=>semanticCheckpoint(v)).toThrow("feature_checkpoint_invalid");
});
test("fold profile preserves source mapping and refuses duplicate or unbounded metadata",()=>{
 const json=JSON.stringify({source:{datcollate:"en_US.utf8",datctype:"en_US.utf8",datlocprovider:"c"},entries:[["İ","i"]]});expect(foldSemanticQuery(parseSemanticProfile(json),"İ_100%")).toBe("i_100%");
 expect(()=>parseSemanticProfile(json.replace('[["İ","i"]]','[["İ","i"],["İ","x"]]'))).toThrow("feature_profile_invalid");
 expect(()=>parseSemanticProfile(" ".repeat(262145))).toThrow("feature_profile_bound");
});
