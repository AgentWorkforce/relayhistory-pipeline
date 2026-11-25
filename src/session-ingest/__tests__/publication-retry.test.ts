import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initialRetryCheckpoint, RetryablePublicationError, runPublicationRetryTurn, type ReceiptRetry, type ReceiptRetryStore, type RetryCheckpoint } from "../publication-retry.ts";
import type { ReceiptPosition } from "../publication-reconciliation.ts";

class FixtureStore implements ReceiptRetryStore {
  db: Database;
  failBeforeCommit = false;
  constructor(path: string) {
    this.db = new Database(path);
    this.db.exec("CREATE TABLE IF NOT EXISTS cp(scope TEXT PRIMARY KEY, v INTEGER NOT NULL, json TEXT NOT NULL); CREATE TABLE IF NOT EXISTS retries(scope TEXT, token TEXT, json TEXT NOT NULL, PRIMARY KEY(scope,token))");
  }
  load(scope: string): RetryCheckpoint {
    const r = this.db.query<{json:string},[string]>("SELECT json FROM cp WHERE scope=?").get(scope);
    return r ? JSON.parse(r.json) : initialRetryCheckpoint();
  }
  lookup(scope:string,token:string): ReceiptRetry | null {
    const r=this.db.query<{json:string},[string,string]>("SELECT json FROM retries WHERE scope=? AND token=?").get(scope,token);
    return r ? JSON.parse(r.json) : null;
  }
  due(scope:string,now:number,limit:number):ReceiptRetry[] {
    return this.db.query<{json:string},[string,number,number]>("SELECT json FROM retries WHERE scope=? AND json_extract(json,'$.state')='retry' AND json_extract(json,'$.due')<=? ORDER BY json_extract(json,'$.due'),token LIMIT ?").all(scope,now,limit).map(r=>JSON.parse(r.json));
  }
  commit(scope:string,v:number,next:RetryCheckpoint,changes:ReadonlyMap<string,ReceiptRetry|null>):boolean {
    return this.db.transaction(()=>{
      this.db.query("INSERT OR IGNORE INTO cp VALUES (?,0,?)").run(scope,JSON.stringify(initialRetryCheckpoint()));
      if (!this.db.query("UPDATE cp SET v=?,json=? WHERE scope=? AND v=?").run(next.version,JSON.stringify(next),scope,v).changes) return false;
      for(const [token,r] of changes) {
        if(r) this.db.query("INSERT OR REPLACE INTO retries VALUES(?,?,?)").run(scope,token,JSON.stringify(r));
        else this.db.query("DELETE FROM retries WHERE scope=? AND token=?").run(scope,token);
      }
      if(this.failBeforeCommit) throw Error("unknown persistence");
      return true;
    }).immediate();
  }
}
const scope = "a".repeat(64);
const token=(id:number)=>`00000000-0000-0000-0000-${String(id).padStart(12,"0")}`;
const pos=(id:number):ReceiptPosition=>({createdAt:"2026-11-01T00:00:00.000000Z",stageToken:token(id)});
describe("durable recovery and fairness",()=>{
  test("same-page poison is deferred atomically, committed work replays safely, a reopened store preserves exhaustion",async()=>{
    const dir=mkdtempSync(join(tmpdir(),"receipt-retry-")), file=join(dir,"state.sqlite");
    let s=new FixtureStore(file);
    try {
      const ids=[1,2,3], committed=new Set<string>(), calls=new Map<string,number>();
      const source={scope,highest:async()=>pos(Math.max(...ids)),
        page:async(after:ReceiptPosition|null,through:ReceiptPosition,n:number)=>ids.map(pos).filter(p=>(!after||p.stageToken>after.stageToken)&&p.stageToken<=through.stageToken).slice(0,n),
        reconcile:async(t:string)=>{
          calls.set(t,(calls.get(t)??0)+1);
          if(t===token(1)) throw new RetryablePublicationError("sensitive source text");
          if(t===token(3)) throw Error("ambiguous sensitive response");
          if(committed.has(t)) return "already_published" as const;
          committed.add(t); return "published" as const;
        }};
      s.failBeforeCommit=true;
      await expect(runPublicationRetryTurn(scope,source,s,3,0)).rejects.toThrow("unknown persistence");
      expect(s.load(scope).version).toBe(0); expect(s.lookup(scope,token(1))).toBeNull();
      expect(committed.has(token(2))).toBe(true);
      s.db.close(); s=new FixtureStore(file);
      await runPublicationRetryTurn(scope,source,s,3,0);
      expect(calls.get(token(2))).toBe(2); expect(s.lookup(scope,token(1))?.due).toBe(5000);
      expect(s.lookup(scope,token(3))?.state).toBe("audit");
      expect(JSON.stringify(s.due(scope,5000,3))).not.toContain("sensitive");
      const previous=s.load(scope); ids.push(4);
      await runPublicationRetryTurn(scope,source,s,3,1);
      expect(calls.get(token(1))).toBe(2);
      expect(s.commit(scope,previous.version,previous,new Map([[token(1),null]]))).toBe(false);
      expect(s.lookup(scope,token(1))).not.toBeNull();
      for(let i=1;i<=24;i++) await runPublicationRetryTurn(scope,source,s,3,i*60001);
      expect(committed.has(token(4))).toBe(true);
      s.db.close(); s=new FixtureStore(file);
      expect(s.lookup(scope,token(1))).toMatchObject({state:"exhausted",attempts:6});
      const tries=calls.get(token(1));
      for(let i=25;i<=30;i++) await runPublicationRetryTurn(scope,source,s,3,i*60001);
      expect(calls.get(token(1))).toBe(tries);
      expect(calls.get(token(3))).toBe(2); // one pre-checkpoint replay; persisted audit holds never retry
      expect(s.load("b".repeat(64)).version).toBe(0);
      await expect(runPublicationRetryTurn("b".repeat(64),source,s,3,1)).rejects.toThrow("scope mismatch");
    } finally {s.db.close();rmSync(dir,{recursive:true,force:true});}
  });
});

test("retry revalidates authority and clears only its scoped schedule on a definitive outcome", async () => {
  const s = new FixtureStore(":memory:");
  try {
    let attempt = 0;
    const source = { scope, highest:async()=>pos(1), page:async()=>[pos(1)],
      reconcile:async()=>{
        if (++attempt === 1) throw new RetryablePublicationError();
        return "held" as const; // Fresh authority check refuses the stage.
      }};
    await runPublicationRetryTurn(scope,source,s,1,100);
    expect(s.lookup(scope,token(1))?.state).toBe("retry");
    await runPublicationRetryTurn(scope,source,s,1,5100);
    expect(attempt).toBe(2);
    expect(s.lookup(scope,token(1))).toBeNull();
  } finally { s.db.close(); }
});
