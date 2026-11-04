/** Immutable metadata mask tree. Native PostgreSQL assigns every page digest;
 * hashes computed here address record IDs only, never emulate JSONB hashing. */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import type { HistoryCurrentSeat } from "./history-visibility.ts";
export interface HistoryMaskItem { recordId: string; winner: Readonly<HistoryCurrentSeat> | null }
export interface HistoryMaskRef { sha256: string; count: string }
export interface HistoryMaskDocument {
  protocol: "history-mask-v1"; orgId: string; workspaceId: string; prefix: string;
  kind: "leaf" | "node"; count: string;
  items: HistoryMaskItem[] | {digit: string; sha256: string}[];
}
export interface HistoryMaskPageStore {
  register(document: HistoryMaskDocument): Promise<HistoryMaskRef>;
  read(sha256: string): Promise<HistoryMaskDocument>;
}
const key = (id: string) => createHash("sha256").update(id, "utf8").digest("hex");
const MAX_RECORDS = 2_000_000, MAX_PAGE = 16 * 1024 ** 2;
function fail(): never { throw Error("history_mask_tree"); }
function checked(ref: HistoryMaskRef): HistoryMaskRef {
  if (!/^[a-f0-9]{64}$/.test(ref.sha256) || !/^(0|[1-9][0-9]{0,6})$/.test(ref.count) || Number(ref.count)>MAX_RECORDS) fail();
  return ref;
}
export function sqlHistoryMaskStore(db: SqlExecutor, generation: string, orgId: string, workspaceId: string): HistoryMaskPageStore {
  if(!/^[a-f0-9]{64}$/.test(generation)) fail();
  return {
    async register(document) {
      if(document.orgId!==orgId||document.workspaceId!==workspaceId||Buffer.byteLength(JSON.stringify(document))>MAX_PAGE) fail();
      const r = await db.query<{result: HistoryMaskRef & {registered: boolean}}>("SELECT sessions.ingest_history_mask_register($1::jsonb) AS result",[JSON.stringify({generation,document})]);
      if(r.rows.length!==1||r.rows[0]?.result?.registered!==true) fail();
      return checked(r.rows[0].result);
    },
    async read(sha256) {
      if(!/^[a-f0-9]{64}$/.test(sha256)) fail();
      const r=await db.query<{result:HistoryMaskDocument}>("SELECT sessions.ingest_history_mask_get($1::jsonb) AS result",[JSON.stringify({generation,orgId,workspaceId,sha256})]);
      const d=r.rows[0]?.result;
      if(r.rows.length!==1||!d||d.protocol!=="history-mask-v1"||d.orgId!==orgId||d.workspaceId!==workspaceId||Buffer.byteLength(JSON.stringify(d))>MAX_PAGE) fail();
      return d;
    },
  };
}
export async function updateHistoryMask(
  store: HistoryMaskPageStore, scope: {orgId:string;workspaceId:string},
  previous: HistoryMaskRef | null, changes: Iterable<HistoryMaskItem>,
): Promise<HistoryMaskRef> {
  const updates=new Map<string,HistoryMaskItem>();
  const hashes=new Map<string,string>();
  const hashKey=(id:string)=>{let h=hashes.get(id);if(!h){h=key(id);hashes.set(id,h)}return h};
  for(const item of changes) {
    if(!item.recordId||Buffer.byteLength(item.recordId)>4096||updates.has(item.recordId)) fail();
    updates.set(item.recordId,item);
    if(updates.size>MAX_RECORDS) fail();
  }
  async function write(prefix:string, items:HistoryMaskItem[]):Promise<HistoryMaskRef> {
    items.sort((a,b)=>hashKey(a.recordId)<hashKey(b.recordId)?-1:hashKey(a.recordId)>hashKey(b.recordId)?1:0);
    const document:HistoryMaskDocument={protocol:"history-mask-v1",...scope,prefix,kind:"leaf",count:String(items.length),items};
    if(items.length<=500&&Buffer.byteLength(JSON.stringify(document))<=MAX_PAGE) return checked(await store.register(document));
    if(prefix.length>=64) fail();
    const children:{digit:string;sha256:string}[]=[];
    for(const digit of "0123456789abcdef") {
      const group=items.filter(item=>hashKey(item.recordId)[prefix.length]===digit);
      if(group.length)children.push({digit,sha256:(await write(prefix+digit,group)).sha256});
    }
    return checked(await store.register({...document,kind:"node",items:children}));
  }
  async function visit(prefix:string, ref:HistoryMaskRef|null, patch:HistoryMaskItem[]):Promise<HistoryMaskRef> {
    if(!ref)return write(prefix,patch);
    checked(ref);
    const d=await store.read(ref.sha256);
    if(d.prefix!==prefix||d.count!==ref.count) fail();
    if(!patch.length) return ref;
    if(d.kind==="leaf") {
      const rows=new Map((d.items as HistoryMaskItem[]).map(item=>[item.recordId,item]));
      for(const item of patch) rows.set(item.recordId,item);
      return write(prefix,[...rows.values()]);
    }
    if(d.kind!=="node"||prefix.length>=64) fail();
    const children=d.items as {digit:string;sha256:string}[],next:{digit:string;sha256:string}[]=[];
    let total=BigInt(ref.count);
    for(const digit of "0123456789abcdef") {
      const changed=patch.filter(item=>hashKey(item.recordId)[prefix.length]===digit),old=children.find(item=>item.digit===digit);
      if(!changed.length) { if(old) next.push(old);continue; }
      const child=old?await store.read(old.sha256):null;
      const updated=await visit(prefix+digit,old?{sha256:old.sha256,count:child!.count}:null,changed);
      total+=BigInt(updated.count)-BigInt(child?.count??"0");
      if(total>BigInt(MAX_RECORDS)) fail();
      next.push({digit,sha256:updated.sha256});
    }
    return checked(await store.register({...d,count:String(total),items:next}));
  }
  return visit("",previous,[...updates.values()]);
}
