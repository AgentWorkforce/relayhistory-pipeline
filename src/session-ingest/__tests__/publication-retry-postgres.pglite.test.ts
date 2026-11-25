import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { postgresPublicationRetryStore } from "../publication-retry-postgres.ts";
import { initialRetryCheckpoint, RetryablePublicationError, runPublicationRetryTurn, type ReceiptRetry } from "../publication-retry.ts";

const scope = "a".repeat(64), otherScope = "b".repeat(64);
const token = (i: number) => `00000000-0000-4000-8000-${String(i).padStart(12,"0")}`;
const schema = await Bun.file(join(import.meta.dir,"fixtures/publication-retry-store.sql")).text();
const checkpoint = (version: number) => ({ ...initialRetryCheckpoint(), version });
const row = (i: number, due: number): ReceiptRetry => ({token: token(i), state: "retry", attempts: 1, due});

test("one CAS covers the checkpoint, holds and removals across faults, competing versions and database reopen", async () => {
  const dir = await mkdtemp(join(tmpdir(), "publication-retry-pg-"));
  let db = new PGlite(dir);
  try {
    await db.exec(schema);
    let store = postgresPublicationRetryStore(db, scope);
    const unrelated = postgresPublicationRetryStore(db, otherScope);
    await expect(store.lookup(otherScope, token(1))).rejects.toThrow("scope mismatch");
    expect(await store.commit(scope, 1, checkpoint(2), new Map())).toBe(false);
    const audit: ReceiptRetry = {...row(2,1), state:"audit"};
    const exhausted: ReceiptRetry = {...row(3,1), state:"exhausted", attempts:6};
    expect(await store.commit(scope, 0, checkpoint(1), new Map([
      [token(1), row(1,100)], [token(2),audit], [token(3),exhausted], [token(4),row(4,20)], [token(5),row(5,20)],
    ]))).toBe(true);
    expect(await store.commit(scope, 0, checkpoint(1), new Map([[token(2),null]]))).toBe(false);
    expect(await store.due(scope,99,1)).toEqual([row(4,20)]);
    expect(await store.due(scope,100,10)).toEqual([row(4,20),row(5,20),row(1,100)]);
    expect(await unrelated.commit(otherScope,0,checkpoint(1),new Map([[token(1),row(1,2)]]))).toBe(true);
    expect(await unrelated.due(otherScope,10,1)).toEqual([row(1,2)]);
    await db.exec(`ALTER TABLE sessions.publication_receipt_retries ADD CHECK(stage_token<>'${token(9)}')`);
    await expect(store.commit(scope,1,checkpoint(2),new Map([[token(1),null],[token(9),row(9,15)]]))).rejects.toMatchObject({code:"23514"});
    await db.close();
    db = new PGlite(dir);
    store = postgresPublicationRetryStore(db,scope);
    expect(await store.load(scope)).toEqual(checkpoint(1));
    expect(await store.lookup(scope,token(1))).toEqual(row(1,100));
    expect(await store.lookup(scope,token(2))).toEqual(audit);
    expect(await store.lookup(scope,token(3))).toEqual(exhausted);
    expect(await store.lookup(scope,token(9))).toBeNull();
    expect(await store.commit(scope,1,checkpoint(2),new Map([[token(1),null]]))).toBe(true);
    expect(await store.commit(scope,1,checkpoint(2),new Map([[token(1),row(1,0)]]))).toBe(false);
    expect(await store.lookup(scope,token(1))).toBeNull();
    expect(await postgresPublicationRetryStore(db,otherScope).lookup(otherScope,token(1))).toEqual(row(1,2));
  } finally {
    await db.close();
    await rm(dir,{recursive:true,force:true});
  }
}, 60000);

test("scheduler uses the async store and indexed bounded due scan; audited receipts are never retried", async () => {
  const db = new PGlite();
  try {
    await db.exec(schema);
    const store = postgresPublicationRetryStore(db,scope);
    let tries = 0;
    const pos = { stageToken:token(1), createdAt:"2026-11-01T00:00:00.000001Z" };
    const source = { scope, highest:async()=>pos, page:async(after:typeof pos|null)=>after ? [] : [pos],
      reconcile:async()=>{if (++tries===1) throw new RetryablePublicationError(); throw Error("private error");} };
    expect(await runPublicationRetryTurn(scope,source,store,1,10)).toBe("committed");
    expect(await store.lookup(scope,pos.stageToken)).toEqual(row(1,5010));
    expect(await runPublicationRetryTurn(scope,source,store,1,5010)).toBe("committed");
    expect(await store.lookup(scope,pos.stageToken)).toMatchObject({state:"audit",attempts:2});
    for(let n=0;n<10;n++) await runPublicationRetryTurn(scope,source,store,1,70000+n);
    expect(tries).toBe(2);
    expect(await store.due(scope,90000,20)).toEqual([]);

    // Check the actual adapter query, including ORDER BY. No sequential scan
    // or extra sort is necessary for the bound scope and partial retry index.
    await db.exec("SET enable_seqscan=off; SET enable_bitmapscan=off");
    let plan = "";
    const inspected = postgresPublicationRetryStore({ async query<T>(sql:string, params?:unknown[]) {
      const result = await db.query<{"QUERY PLAN":string}>("EXPLAIN (COSTS OFF) "+sql,params);
      plan = result.rows.map(r=>r["QUERY PLAN"]).join("\n");
      return db.query<T>(sql,params);
    } },scope);
    await inspected.due(scope,90000,20);
    expect(plan).toContain("publication_receipt_retries_due");
    expect(plan).not.toContain("Sort");
  } finally { await db.close(); }
},60000);
