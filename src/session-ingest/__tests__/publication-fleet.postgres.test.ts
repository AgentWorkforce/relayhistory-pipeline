import { expect, test } from "bun:test";
import { join } from "node:path";
import pg from "pg";
import { SemanticPostgresExecutor } from "../../search-features/semantic-copy-postgres.ts";
import { publicationFleetScopeKey,runPublicationFleetTurn,type PublicationFleetOptions } from "../publication-fleet.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
test.skipIf(!url)("independent PG hosts reserve one turn; replacement keeps its lease after the stalled host resumes",async()=>{
  const scratch=new URL(url!);
  const name="fleet_"+crypto.randomUUID().replaceAll("-","");
  scratch.pathname="/"+name;
  const admin=new pg.Client({connectionString:url});
  const poolA=new pg.Pool({connectionString:scratch.href,max:1,connectionTimeoutMillis:1000});
  const poolB=new pg.Pool({connectionString:scratch.href,max:1,connectionTimeoutMillis:1000});
  const a=new SemanticPostgresExecutor(poolA), b=new SemanticPostgresExecutor(poolB);
  const activeA=Promise.withResolvers<void>(), activeB=Promise.withResolvers<void>();
  const finishA=Promise.withResolvers<void>(), finishB=Promise.withResolvers<void>();
  const target={provider:"s3" as const,bucket:"fixture",region:"us-east-1",keyRoot:"fixture",generation:"1".repeat(64)};
  const binding={tenant:{orgId:"org",workspaceId:"ws",userId:"u",originId:"o"},target,
    admissionId:"00000000-0000-4000-8000-000000000001",planSha256:"2".repeat(64),frontierSha256:"3".repeat(64)};
  const input=(active:typeof activeA, finish:typeof finishA): PublicationFleetOptions=>({
    enabled:true,target,bindings:[binding],fleetId:"f".repeat(64),budgetMs:20000,receiptLimit:1,
    sourceFor:(bound)=>({scope:publicationFleetScopeKey(bound),highest:async()=>{active.resolve(); await finish.promise; return null;},
      page:async()=>[],reconcile:async()=>"missing"}),
  });
  let created=false, first: Promise<unknown>|undefined, second:Promise<unknown>|undefined;
  await admin.connect();
  try {
    await admin.query("CREATE DATABASE "+name); created=true;
    await poolA.query(await Bun.file(join(import.meta.dir,"fixtures/publication-retry-store.sql")).text());
    first=runPublicationFleetTurn(a,input(activeA,finishA));
    await Promise.race([activeA.promise,first.then(()=>{throw Error("first reservation ended early");})]);
    expect(await runPublicationFleetTurn(b,input(activeB,finishB))).toBe("busy");
    // Simulate the old lease expiring. Separate live PG sessions exercise real row locks and CAS.
    await poolB.query("UPDATE sessions.publication_fleet_cursors SET lease_until=clock_timestamp()-interval '1 second'");
    second=runPublicationFleetTurn(b,input(activeB,finishB));
    await Promise.race([activeB.promise,second.then(()=>{throw Error("replacement reservation ended early");})]);
    finishA.resolve();
    expect(await first).toBe("conflict");
    expect(await runPublicationFleetTurn(a,input(activeA,finishA))).toBe("busy");
    finishB.resolve();
    expect(await second).toBe("committed");
    const r=await poolA.query("SELECT version::text FROM sessions.publication_retry_checkpoints");
    expect(r.rows).toEqual([{version:"1"}]);
  } finally {
    finishA.resolve(); finishB.resolve();
    await Promise.allSettled([first,second]);
    await Promise.allSettled([poolA.end(),poolB.end()]);
    try {if(created) await admin.query("DROP DATABASE "+name);} finally {await admin.end();}
  }
},30000);
