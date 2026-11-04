import{test,expect}from'bun:test';
import{parseReceiptSeatSnapshot,acceptedReceiptSeat,injectReceiptSeat}from'../publication-seats.ts';
import{normalizeRelayhistoryRecords,parseRelayhistoryRecord}from'../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts';
const HEX='a'.repeat(64),OTHER='b'.repeat(64),FLOOR='9007199254740993';
const record={ord:0,source:'claude',session_id:'session:1',record_id:'native-r',revision_id:'revision-1',revision:1,digest:HEX};
const stage={key:'unused',sha256:HEX,bytes:1,groupOrdinals:[[0]]};
function value(){return{controlEpoch:HEX,sourceIdentitySha256:OTHER,allocatedHighWater:FLOOR,lastAssignedSeat:'9007199254740999',records:[{ord:0,seat:'9007199254740994',bodyAccepted:true,candidateDigest:HEX,firstDigest:HEX,resetEpoch:'0',fenceEpoch:'2'}]};}
const line=(origin:string,text:string,received='2026-11-04T01:00:00.000000Z',revision='1')=>JSON.stringify({kind:'session_event',origin_id:origin,record_id:record.record_id,revision,operation:'upsert',received_at:received,payload:{text,message_id:'native-message',ts_ms:1762218000000}});
test('large decimal seats remain exact strings and inject without changing payload numeric lexemes or escapes',()=>{
 const parsed=parseReceiptSeatSnapshot(value(),[record],stage),slot=acceptedReceiptSeat(parsed,0,{resetEpoch:'0',fenceEpoch:'2'});
 const raw='{"kind":"session_event","origin_id":"origin-a","record_id":"native-r","revision":"1","operation":"upsert","received_at":"2026-11-04T01:00:00Z","payload":{"text":"literal \\u00e9","accepted_seq":"untrusted nested","counter":9007199254740993,"decimal":1.00000000000000000001}}';
 const out=injectReceiptSeat(raw,slot,'origin-a');expect(out.slice(0,raw.length-1)).toBe(raw.slice(0,-1));
 expect(out.endsWith(',"accepted_seq":"9007199254740994"}')).toBe(true);expect(parseRelayhistoryRecord(out).accepted_seq).toBe('9007199254740994');
});
test('coverage, digest, clock bounds and distinct-record seat aliases refuse',()=>{
 for(const mutate of[(v:any)=>v.records.pop(),(v:any)=>v.records[0].candidateDigest=OTHER,(v:any)=>v.records[0].seat=FLOOR,(v:any)=>v.records[0].seat='9007199254741000',(v:any)=>v.records[0].seat=9007199254740994,(v:any)=>v.unreviewed=true]){
  const v=value();mutate(v);expect(()=>parseReceiptSeatSnapshot(v,[record],stage)).toThrow('publication_seats_');
 }
 const other={...record,ord:1,record_id:'another-native'};const v=value();v.records.push({...v.records[0]!,ord:1});
 expect(()=>parseReceiptSeatSnapshot(v,[record,other],{...stage,groupOrdinals:[[0,1]]})).toThrow('publication_seats_seat_alias');
});
test('later losing body keeps original seat but cannot be selected or injected',()=>{
 const v=value();v.records[0]!.firstDigest=OTHER;v.records[0]!.bodyAccepted=false;const snapshot=parseReceiptSeatSnapshot(v,[record],stage);
 expect(()=>acceptedReceiptSeat(snapshot,0)).toThrow('publication_seats_losing_body');
 expect(()=>injectReceiptSeat(line('origin-a','later losing'),snapshot.slots.get(0)!,'origin-a')).toThrow('publication_seats_losing_body');
});
test('same revision duplicates share a seat and cannot be reseated by a later ordinal',()=>{
 const duplicate={...record,ord:1};const v=value();v.records.push({...v.records[0]!,ord:1});
 const snap=parseReceiptSeatSnapshot(v,[record,duplicate],{...stage,groupOrdinals:[[0,1]]});expect(snap.slots.get(0)!.seat.seat).toBe(snap.slots.get(1)!.seat.seat);
 v.records[1]!.seat='9007199254740995';expect(()=>parseReceiptSeatSnapshot(v,[record,duplicate],{...stage,groupOrdinals:[[0,1]]})).toThrow('publication_seats_seat_alias');
});
test('header-forged acceptance order, wrong original record/origin/revision and mismatched epochs refuse',()=>{
 const snap=parseReceiptSeatSnapshot(value(),[record],stage),slot=acceptedReceiptSeat(snap,0);
 for(const raw of[line('wrong-origin','x'),line('origin-a','x',undefined,'2'),line('origin-a','x').replace('"native-r"','"other"'),line('origin-a','x').replace(/}$/,',"accepted_seq":"999999999999999999"}')])
  expect(()=>injectReceiptSeat(raw,slot,'origin-a')).toThrow('publication_seats_line_identity');
 expect(()=>acceptedReceiptSeat(snap,0,{resetEpoch:'1',fenceEpoch:'2'})).toThrow('publication_seats_epoch');
 expect(()=>acceptedReceiptSeat(snap,0,{resetEpoch:'0',fenceEpoch:'3'})).toThrow('publication_seats_epoch');
});
test('authoritative cross-origin winner uses receipt order despite reversed drain/received_at order',()=>{
 const first=parseReceiptSeatSnapshot(value(),[record],stage);const later=value();later.records[0]!.seat='9007199254740995';
 const second=parseReceiptSeatSnapshot(later,[record],stage);
 const a=parseRelayhistoryRecord(injectReceiptSeat(line('origin-a','first ACK','2026-11-04T03:00:00Z'),acceptedReceiptSeat(first,0),'origin-a'));
 const b=parseRelayhistoryRecord(injectReceiptSeat(line('origin-b','second ACK','2026-11-04T01:00:00Z'),acceptedReceiptSeat(second,0),'origin-b'));
 for(const rows of[[a,b],[b,a]])expect(normalizeRelayhistoryRecords(rows).map(r=>r.payload!.text)).toEqual(['second ACK']);
 const gone={...b,operation:'delete' as const,payload:null};expect(normalizeRelayhistoryRecords([a,gone])).toEqual([]);
});
test('metadata input mutation after capture cannot alter a trusted seat or record identity',()=>{
 const v=value(),r={...record};const snap=parseReceiptSeatSnapshot(v,[r],stage);v.records[0]!.seat='9007199254740999';r.record_id='changed';
 const out=injectReceiptSeat(line('origin-a','x'),acceptedReceiptSeat(snap,0),'origin-a');expect(parseRelayhistoryRecord(out).accepted_seq).toBe('9007199254740994');
});

test('losing scope alias captures the original winner seat and never promotes after capture',()=>{
 const alias={...record,ord:1,session_id:'session:2',digest:OTHER};const v=value();
 v.records.push({...v.records[0]!,ord:1,candidateDigest:OTHER,bodyAccepted:false});
 const snap=parseReceiptSeatSnapshot(v,[record,alias],{...stage,groupOrdinals:[[0],[1]]});
 expect(acceptedReceiptSeat(snap,0).seat.seat).toBe('9007199254740994');
 expect(()=>acceptedReceiptSeat(snap,1)).toThrow('publication_seats_losing_body');
 v.records[1]!.bodyAccepted=true;expect(()=>acceptedReceiptSeat(snap,1)).toThrow('publication_seats_losing_body');
 const falseSameDigest=value();falseSameDigest.records[0]!.bodyAccepted=false;
 const falseSnap=parseReceiptSeatSnapshot(falseSameDigest,[record],stage);
 expect(()=>injectReceiptSeat(line('origin-a','x'),falseSnap.slots.get(0)!,'origin-a')).toThrow('publication_seats_losing_body');
 const invalid=value();invalid.records[0]!.firstDigest=OTHER;
 expect(()=>parseReceiptSeatSnapshot(invalid,[record],stage)).toThrow('publication_seats_decision');
});


test('lower losing revisions reference the current winner seat without inventing its revision',()=>{
 const records=[{...record,revision:8},{...record,ord:1,revision:1,digest:OTHER},{...record,ord:2,revision:2,digest:"c".repeat(64)}];
 const v=value();
 v.records.push({...v.records[0]!,ord:1,candidateDigest:OTHER,bodyAccepted:false},{...v.records[0]!,ord:2,candidateDigest:"c".repeat(64),bodyAccepted:false});
 const snap=parseReceiptSeatSnapshot(v,records,{...stage,groupOrdinals:[[0,1,2]]});
 expect(acceptedReceiptSeat(snap,0).record.revision).toBe(8);
 for(const ord of[1,2])expect(()=>acceptedReceiptSeat(snap,ord)).toThrow('publication_seats_losing_body');
});
