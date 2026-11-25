import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { publicationFleetScopeKey, runPublicationFleetTurn, type PublicationFleetOptions } from "../publication-fleet.ts";
import { RetryablePublicationError } from "../publication-retry.ts";
import { postgresPublicationRetryStore } from "../publication-retry-postgres.ts";
import type { ReceiptPosition } from "../publication-reconciliation.ts";
import type { PublicationScopeBinding } from "../publication-scoped-rpc.ts";
import type { SemanticBoundedSql, SemanticQueryContext } from "../../search-features/semantic-copy-postgres.ts";

const schema = await Bun.file(join(import.meta.dir,"fixtures/publication-retry-store.sql")).text();
const target = {provider:"s3" as const, generation:"a".repeat(64), bucket:"fixture",keyRoot:"fixture",region:"us-east-1"};
const token = (n:number) => `00000000-0000-4000-8000-${String(n).padStart(12,"0")}`;
const pos = (n:number): ReceiptPosition => ({createdAt:"2026-11-25T00:00:00.000001Z",stageToken:token(n)});

function scope(originId:string): PublicationScopeBinding {
  return {tenant:{orgId:"org",workspaceId:"workspace",userId:"user",originId}, target,
    planSha256:"3".repeat(64),frontierSha256:"4".repeat(64),admissionId:token(1)};
}
function transport(db:PGlite): SemanticBoundedSql {
  return {query:db.query.bind(db), async queryBounded<T>(sql:string,args:unknown[]|undefined,ctx:SemanticQueryContext) {
    ctx.signal.throwIfAborted();
    return db.query<T>(sql,args);
  }};
}
function options(bindings:PublicationScopeBinding[], sourceFor:PublicationFleetOptions["sourceFor"]):PublicationFleetOptions {
  return {enabled:true,fleetId:"9".repeat(64),target,bindings,receiptLimit:1,budgetMs:10_000,sourceFor};
}
function empty(b:PublicationScopeBinding) {
  return {scope:publicationFleetScopeKey(b),highest:async()=>null,page:async()=>[],reconcile:async()=>"missing" as const};
}

test("DB rotation serves hot, quiet and failing origins under arrivals and restart; durable holds stay scoped", async () => {
  const dir = await mkdtemp(join(tmpdir(),"fleet-"));
  let db = new PGlite(dir);
  try {
    await db.exec(schema);
    const bindings = [scope("hot"),scope("quiet"),scope("poison")];
    const visited:string[] = [];
    const inventory:Record<string,number[]> = {hot:[1,2],quiet:[],poison:[1,2]};
    const published = new Map<string,Set<number>>(bindings.map(b=>[b.tenant.originId,new Set()]));
    const attempts = new Map<string,number>();
    const input = options(bindings, (b,signal) => {
      const id = b.tenant.originId, rows = inventory[id]!;
      visited.push(id);
      return {scope:publicationFleetScopeKey(b),
        highest:async()=>rows.length ? pos(Math.max(...rows)) : null,
        page:async(after,through,limit)=>rows.map(pos).filter(r=>(!after || r.stageToken>after.stageToken) && r.stageToken<=through.stageToken).slice(0,limit),
        reconcile:async(t:string)=>{
          signal.throwIfAborted();
          const n=Number(t.slice(-12));
          if (n===1 && id!=="quiet") {
            attempts.set(id,(attempts.get(id)??0)+1);
            throw new RetryablePublicationError();
          }
          const accepted=published.get(id)!;
          if (accepted.has(n)) return "already_published";
          accepted.add(n);
          return "published";
        }};
    });
    let quietAt:number|undefined;
    for (let i=0;i<90;i++) {
      if (i===44) {await db.close(); db=new PGlite(dir);}
      inventory.hot!.push(3+i);
      if (i===3) inventory.quiet!.push(1);
      // Accelerate the backoff clock in this fixture only. Scheduling choice is
      // made by the fleet SQL/runtime; the caller never picks the next origin.
      await db.exec("UPDATE sessions.publication_receipt_retries SET due_ms=0 WHERE state='retry'");
      const roster = i%2===0 ? bindings : [...bindings].reverse();
      expect(await runPublicationFleetTurn(transport(db),{...input,bindings:roster})).toBe("committed");
      if (published.get("quiet")!.has(1)) quietAt??=i;
    }
    for (let start=0;start<88;start++)
      expect(new Set(visited.slice(start,start+3)).size).toBe(3);
    expect(quietAt).toBeDefined();
    expect(quietAt!-3).toBeLessThan(12);
    expect(published.get("hot")!.size).toBeGreaterThan(1);
    expect(published.get("hot")!.size).toBeLessThan(inventory.hot!.length-1);
    expect([...published.get("poison")!]).toEqual([2]);
    for (const binding of bindings) {
      const id=binding.tenant.originId, key=publicationFleetScopeKey(binding);
      const store=postgresPublicationRetryStore(db,key);
      expect((await store.load(key)).version).toBe(30);
      const retry=await store.lookup(key,token(1));
      if (id==="quiet") expect(retry).toBeNull();
      else {
        expect(attempts.get(id)).toBe(6);
        expect(retry).toMatchObject({state:"exhausted",attempts:6});
      }
    }
  } finally {await db.close(); await rm(dir,{recursive:true,force:true});}
},60000);

test("busy fleet does no source work; an expired holder cannot commit or release a replacement lease", async () => {
  const db=new PGlite();
  try {
    await db.exec(schema);
    const binding=scope("only"), key=publicationFleetScopeKey(binding);
    const first=Promise.withResolvers<void>(), second=Promise.withResolvers<void>();
    const stopFirst=Promise.withResolvers<void>(),stopSecond=Promise.withResolvers<void>();
    const block=(entered:typeof first, stop:typeof first)=>options([binding],b=>({...empty(b),highest:async()=>{
      entered.resolve(); await stop.promise; return null;
    }}));
    const a=runPublicationFleetTurn(transport(db),block(first,stopFirst));
    await first.promise;
    expect(await runPublicationFleetTurn(transport(db),options([binding],()=>{throw Error("busy selected source");}))).toBe("busy");
    await db.exec("UPDATE sessions.publication_fleet_cursors SET lease_until=clock_timestamp()-interval '1 second'");
    const b=runPublicationFleetTurn(transport(db),block(second,stopSecond));
    await second.promise;
    stopFirst.resolve();
    expect(await a).toBe("conflict");
    expect((await postgresPublicationRetryStore(db,key).load(key)).version).toBe(0);
    expect(await runPublicationFleetTurn(transport(db),options([binding],()=>{throw Error("stale unlock");}))).toBe("busy");
    stopSecond.resolve();
    expect(await b).toBe("committed");
    expect((await postgresPublicationRetryStore(db,key).load(key)).version).toBe(1);
  } finally {await db.close();}
},60000);

test("pause, expiry, roster validation, mismatched source and resource limits fail closed",async()=>{
  const db=new PGlite();
  try {
    const b=scope("a"), c=scope("b"), sql=transport(db);
    const bad=options([b],()=>{throw Error("unexpected source");});
    expect(await runPublicationFleetTurn(sql,{...bad,enabled:false})).toBe("disabled");
    expect(await runPublicationFleetTurn(sql,{...bad,bindings:[]})).toBe("empty");
    for (const extra of [{bindings:[b,b]},{bindings:Array(129).fill(b)}, {receiptLimit:21},{budgetMs:60001},
      {bindings:[{...b,target:{...target,bucket:"other"}}]}])
      await expect(runPublicationFleetTurn(sql,{...bad,...extra})).rejects.toThrow();
    await db.exec(schema);
    await expect(runPublicationFleetTurn(sql,options([b],()=>empty(c)))).rejects.toThrow("scope mismatch");
    const entered=Promise.withResolvers<void>();
    const abort=new AbortController();
    const aborted=runPublicationFleetTurn(sql,{...options([b,c],async()=>{
      entered.resolve();
      return await new Promise<never>(()=>{}); // Simulate a disconnected adapter ignoring cancellation.
    }),signal:abort.signal});
    await entered.promise;
    abort.abort();
    await expect(aborted).rejects.toThrow();
    const cp=await db.query("SELECT * FROM sessions.publication_retry_checkpoints");
    expect(cp.rows).toEqual([]);
  } finally {await db.close();}
},60000);

test("an organization with four saturated origins receives the same reservation share as a one-origin organization",async()=>{
  const db = new PGlite();
  try {
    await db.exec(schema);
    const bindings = ["a","b","c","d","quiet"].map(id=>{
      const b=scope(id);
      if(id==="quiet") b.tenant.orgId="other";
      return b;
    });
    const visits: string[]=[];
    let highest=4;
    const input=options(bindings,b=>{
      visits.push(b.tenant.originId);
      return {scope:publicationFleetScopeKey(b),highest:async()=>pos(highest),
        page:async(after,through)=>{const id=after ? Number(after.stageToken.slice(-12))+1 : 1;
          return id<=Number(through.stageToken.slice(-12)) ? [pos(id)] : [];},
        reconcile:async()=>"published"};
    });
    for(let i=0;i<64;i++) {
      highest++;
      expect(await runPublicationFleetTurn(transport(db),input)).toBe("committed");
    }
    for(let i=0;i<63;i++)
      expect(visits.slice(i,i+2).filter(x=>x==="quiet")).toHaveLength(1);
    for(let i=0;i<=56;i++) {
      const window=visits.slice(i,i+8);
      for(const id of ["a","b","c","d"]) expect(window.filter(x=>x===id)).toHaveLength(1);
    }
    // A reduced roster immediately removes retired origins from scheduling.
    const remaining = bindings.filter(b => b.tenant.orgId === "other");
    expect(await runPublicationFleetTurn(transport(db),{...input,bindings:remaining})).toBe("committed");
    expect(visits.at(-1)).toBe("quiet");
    const {rows} = await db.query<{n:number}>("SELECT count(*)::int AS n FROM sessions.publication_fleet_cursors f,LATERAL jsonb_each(f.origins)");
    expect(rows[0]!.n).toBe(1);
  } finally {await db.close();}
},60000);
