import { expect, test } from "bun:test";
import { buildSelfhostEraseRuntime, eraseStoreIdentity } from "../selfhost-runtime.ts";
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
