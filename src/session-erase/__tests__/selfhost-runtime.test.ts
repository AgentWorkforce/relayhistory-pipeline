import { expect, test } from "bun:test";
import { buildSelfhostEraseRuntime, decisionRecordsErasePort, eraseStoreIdentity } from "../selfhost-runtime.ts";
test("dormant composition opens no pool, reads no credentials and invokes no SQL", async () => {
 let sql = 0;
 expect(await buildSelfhostEraseRuntime({ env: new Proxy({}, { get() { throw Error("dormant env read"); } }), keyRoot: "96bulls", api: { enabled: false, reason: "flag_off" },
  run: async () => { sql++; throw Error("SQL forbidden"); }, pool: {} as never, metadata: {} })).toBeUndefined();
 expect(sql).toBe(0);
});
test("store identity binds endpoint, bucket, region and root without exposing credentials", () => {
 const base = { LOCKER_BUCKET: "bucket", LOCKER_ENDPOINT: "http://127.0.0.1:9000", LOCKER_SECRET_ACCESS_KEY: "synthetic-secret" };
 const first = eraseStoreIdentity(base, "root");
 expect(eraseStoreIdentity({ ...base, LOCKER_ENDPOINT: "http://127.0.0.1:9000/", LOCKER_SECRET_ACCESS_KEY: "rotated" }, "root")).toEqual(first);
 for(const changed of [{LOCKER_BUCKET:"other"},{LOCKER_REGION:"eu-west-1"},{LOCKER_ENDPOINT:"http://127.0.0.1:9001"}])
  expect(eraseStoreIdentity({...base,...changed},"root").storeRef).not.toBe(first.storeRef);
 expect(eraseStoreIdentity(base,"other").storeRef).not.toBe(first.storeRef);
 expect(JSON.stringify(first)).not.toContain("synthetic-secret");
 expect(eraseStoreIdentity({...base,SEARCH_INDEX_BACKEND:"opensearch",SEARCH_INDEX_OPENSEARCH_URL:"http://127.0.0.1:9200"},"root").storeRef).not.toBe(first.storeRef);
 const search = {...base,SEARCH_INDEX_BACKEND:"opensearch",SEARCH_INDEX_OPENSEARCH_URL:"http://127.0.0.1:9200"};
 expect(eraseStoreIdentity({...search,SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX:"other"},"root").storeRef).not.toBe(eraseStoreIdentity(search,"root").storeRef);
 expect(() => eraseStoreIdentity({...base,LOCKER_PROVIDER:"gcs"},"root")).toThrow(/versioned S3/);
});
test("enforcement refuses startup without installed narrow SQL capabilities", async () => {
 await expect(buildSelfhostEraseRuntime({ env:{}, keyRoot:"root",api:{enabled:false,reason:"flag_off"},enforced:true,
  run:async()=>({rows:[{ready:false}]}),pool:{} as never,metadata:{} })).rejects.toThrow(/migration and grants required/);
});

test("automatic deletion refuses a one-connection pool before external I/O", async () => {
 await expect(buildSelfhostEraseRuntime({env:{LOCKER_BUCKET:"bucket"},keyRoot:"root",api:{enabled:false,reason:"flag_off"},enforced:true,bytesEnabled:true,
 run:async()=>({rows:[{ready:true}]}),pool:{} as never,metadata:{max:1}})).rejects.toThrow(/at least two/);
});

test("decision records erase is wired only when SUMMARY_DECISION_RECORDS_ENABLED is exactly true, and then its function is a startup requirement", async () => {
 const run = async () => { throw Error("SQL forbidden"); };
 for (const value of [undefined, "", "1", "TRUE", "yes", " true"])
  expect(decisionRecordsErasePort(value === undefined ? {} : { SUMMARY_DECISION_RECORDS_ENABLED: value }, run)).toBeUndefined();
 expect(decisionRecordsErasePort({ SUMMARY_DECISION_RECORDS_ENABLED: "true" }, run)).toBeDefined();
 const required = async (env: NodeJS.ProcessEnv) => {
  let fns: string[] = [];
  await buildSelfhostEraseRuntime({ env, keyRoot: "root", api: { enabled: false, reason: "flag_off" }, enforced: true,
   run: async (_text, values) => { fns = (values?.[0] ?? []) as string[]; return { rows: [{ ready: false }] }; }, pool: {} as never, metadata: {} }).catch(() => {});
  return fns;
 };
 const off = await required({}), on = await required({ SUMMARY_DECISION_RECORDS_ENABLED: "true" });
 expect(off.some(fn => fn.includes("decision_evidence"))).toBe(false);
 expect(on).toEqual([...off, "sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid)"]);
});
