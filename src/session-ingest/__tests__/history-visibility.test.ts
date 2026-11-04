import{test,expect}from"bun:test";
import{chooseHistoryCurrentWinners,historyRecordVisible,historyRecordContentSha256,type HistoryCurrentSeat,type HistoryLogicalMask}from"../../shared/history-visibility.ts";
import{parseRelayhistoryRecord}from"../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
const rec=(origin:string,revision:string,seat:string,text:string,operation="upsert")=>parseRelayhistoryRecord(JSON.stringify({kind:"history",origin_id:origin,record_id:"native",revision,accepted_seq:seat,operation,received_at:"2026-11-01T00:00:00.000Z",payload:operation==="delete"?null:{prompt:text,timestamp_ms:1761955200000}}));
function seat(r:ReturnType<typeof rec>,sessionId:string|null):HistoryCurrentSeat{return{originId:r.origin_id,recordId:r.record_id,kind:r.kind,revision:r.revision,acceptedSeq:r.accepted_seq!,operation:r.operation,userId:"user",source:"claude",sessionId,contentSha256:historyRecordContentSha256(r),lineage:"1"}}
const mask=(sessionId:string,winners:HistoryLogicalMask["winners"]):HistoryLogicalMask=>({scope:{orgId:"org",workspaceId:"ws",source:"claude",sessionId},source:{epoch:"1",fingerprint:"a".repeat(64)},winners});
test("independent A bytes stay hidden while later B wins, then reappear only when B current custody is erased",()=>{
 const a=rec("a","1","9007199254740994","retained A"),b=rec("b","1","9007199254740995","current B");
 const both=chooseHistoryCurrentWinners([seat(a,"A"),seat(b,"B")]);expect(historyRecordVisible(a,"user",mask("A",both))).toBe(false);expect(historyRecordVisible(b,"user",mask("B",both))).toBe(true);
 const withoutB=chooseHistoryCurrentWinners([seat(a,"A")]);expect(historyRecordVisible(a,"user",mask("A",withoutB))).toBe(true);
 const withoutA=chooseHistoryCurrentWinners([seat(b,"B")]);expect(historyRecordVisible(b,"user",mask("B",withoutA))).toBe(true);
});
test("qualified new lifecycle lower revision dominates historical unqualified higher raw; source MAX is not offered as current",()=>{
 const old=rec("a","10","100","old erased10"),fresh=rec("a","8","9007199254740996","fresh8");const winners=chooseHistoryCurrentWinners([seat(fresh,"B")]);
 expect(historyRecordVisible(old,"user",mask("A",winners))).toBe(false);expect(historyRecordVisible(fresh,"user",mask("B",winners))).toBe(true);
 expect(()=>chooseHistoryCurrentWinners([seat(old,"A"),seat(fresh,"B")])).toThrow("history_visibility_duplicate_current");
});
test("current tombstone or unattached winner suppresses older visible copies without inventing a public NULL session",()=>{
 const a=rec("a","1","101","old"),t=rec("b","2","102","","delete");const winners=chooseHistoryCurrentWinners([seat(a,"A"),seat(t,null)]);
 expect(historyRecordVisible(a,"user",mask("A",winners))).toBe(false);expect(historyRecordVisible(t,"user",mask("A",winners))).toBe(false);
 const empty=new Map([["native",null]]);expect(historyRecordVisible(a,"user",mask("A",empty))).toBe(false);
});
test("missing authority refuses; exact current seat wrong body refuses; stale seat/foreign owner cannot adopt a body",()=>{
 const a=rec("a","1","9007199254740994","current");const w=chooseHistoryCurrentWinners([seat(a,"A")]);expect(()=>historyRecordVisible(a,"user",mask("A",new Map()))).toThrow("history_visibility_unknown");
 const bad=rec("a","1",a.accepted_seq!,"changed first body");expect(()=>historyRecordVisible(bad,"user",mask("A",w))).toThrow("history_visibility_body_mismatch");expect(historyRecordVisible(a,"foreign",mask("A",w))).toBe(false);
 const stale=rec("a","1","100","current");expect(historyRecordVisible(stale,"user",mask("A",w))).toBe(false);
});
