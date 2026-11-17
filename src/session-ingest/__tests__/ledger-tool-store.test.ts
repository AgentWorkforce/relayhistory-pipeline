/**
 * The operator tools' stores for a relocated ledger (ledger-tool-store.ts) and their use by the fence backfill and
 * the buffered drain CLIs: the edge's variables and parser, the routed store (fences in the Locker bucket, ledger/** in
 * the ledger bucket, no encryption header there), and the fork guard (ledger bucket: always, required; without:
 * INGEST_LEDGER_HOME_GUARD). Synthetic bucket names only (the repository is public).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { s3LedgerKeys } from "../ledger-s3.ts";
import { LedgerHomeRefused, isLedgerKey, ledgerHomeKey } from "../ledger-store.ts";
import { TOOL_HOME_RECHECK_MS, assertToolLedgerHome, homeRecheckingStore, toolLedgerBucket, toolStores } from "../ledger-tool-store.ts";
import { assertBackfillLedgerHome, backfillStores, runBackfill, type BackfillArgs } from "../../../scripts/upload-fence-backfill.ts";
import { drainStores, parseDrainArgs, runDrainTool } from "../../../scripts/buffered-drain.ts";
import { scratchStore, TENANT } from "./support.ts";

const ROOT = "96bulls";
const LOCKER = "example-locker-bucket", LEDGER = "example-ledger-bucket";
const KMS = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000";
const CREDS = { accessKeyId: "AKIAEXAMPLE", secretAccessKey: "secret" };
const keys = s3LedgerKeys(ROOT, TENANT);
const enc = new TextEncoder();

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
function scratch(): ObjectStore {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}

/** A fake createS3Store: one scratch store per bucket, every config recorded, every request through it logged. */
function fakeS3() {
  const buckets = new Map<string, ObjectStore>();
  const configs: Record<string, unknown>[] = [];
  const requests: { bucket: string; op: string; key: string }[] = [];
  const raw = (b: string) => { if (!buckets.has(b)) buckets.set(b, scratch()); return buckets.get(b)!; };
  const logged = (bucket: string): ObjectStore => {
    const s = raw(bucket);
    const log = (op: string, key: unknown) => requests.push({ bucket, op, key: String(key) });
    return {
      backend: s.backend,
      head: (k) => { log("head", k); return s.head(k); },
      get: (k, r) => { log("get", k); return s.get(k, r); },
      getObject: (k, r) => { log("get", k); return s.getObject(k, r); },
      list: (p, c) => { log("list", p); return s.list(p, c); },
      presignGet: (k, t) => { log("presign", k); return s.presignGet(k, t); },
      put: (k, b, o) => { log("put", k); return s.put(k, b, o); },
      delete: (k) => { log("delete", k); return s.delete(k); },
    };
  };
  const create = (config: { bucket: string }) => {
    configs.push(config as unknown as Record<string, unknown>);
    return logged(config.bucket);
  };
  return { create, configs, requests, bucket: raw, writes: () => requests.filter((r) => r.op === "put" || r.op === "delete") };
}
/** A store that fails the test on any request. */
const noRequests = new Proxy({} as ObjectStore, { get: (_t, p) => (p === "then" ? undefined : () => { throw new Error(`unexpected_request_${String(p)}`); }) });

const home = (s: ObjectStore, bucket: string) =>
  s.put(objectKey(ledgerHomeKey(ROOT)), enc.encode(JSON.stringify({ v: 1, home: bucket, writtenAtMs: 1 })), { contentType: "application/json" });
async function has(s: ObjectStore, key: string): Promise<boolean> {
  return (await s.head(objectKey(key))) !== undefined;
}
const refusal = async (p: Promise<unknown>) => {
  try { await p; return null; } catch (e) { return (e as Error).message; }
};
const ledgerEnv = { INGEST_LEDGER_BUCKET: LEDGER, INGEST_LEDGER_REGION: "eu-west-1" };

describe("toolLedgerBucket: the edge's variables and parser", () => {
  const locker = { bucket: LOCKER, region: "us-east-1" };
  test("unset or empty: null (one bucket, as before)", () => {
    expect(toolLedgerBucket({}, locker)).toBeNull();
    expect(toolLedgerBucket({ INGEST_LEDGER_BUCKET: "" }, locker)).toBeNull();
  });
  test("set: the bucket, its region (default the tool's), an optional endpoint", () => {
    expect(toolLedgerBucket(ledgerEnv, locker)).toEqual({ bucket: LEDGER, region: "eu-west-1" });
    expect(toolLedgerBucket({ INGEST_LEDGER_BUCKET: LEDGER }, locker)).toEqual({ bucket: LEDGER, region: "us-east-1" });
    expect(toolLedgerBucket({ ...ledgerEnv, INGEST_LEDGER_ENDPOINT: "http://127.0.0.1:9000" }, locker)).toEqual({ bucket: LEDGER, region: "eu-west-1", endpoint: "http://127.0.0.1:9000" });
  });
  test("every invalid combination refuses with one closed code, never the value", () => {
    for (const env of [
      { INGEST_LEDGER_REGION: "eu-west-1" },
      { INGEST_LEDGER_ENDPOINT: "http://127.0.0.1:9000" },
      { INGEST_LEDGER_BUCKET: LOCKER },
      { INGEST_LEDGER_BUCKET: "Not_A_Bucket" },
      { INGEST_LEDGER_BUCKET: LEDGER, INGEST_LEDGER_REGION: "eu west 1" },
      { INGEST_LEDGER_BUCKET: LEDGER, INGEST_LEDGER_ENDPOINT: "http://host/path" },
    ]) expect(() => toolLedgerBucket(env, locker)).toThrow(/^ledger_bucket_invalid$/);
  });
});

describe("toolStores: the Locker store and the routed ledger store", () => {
  test("without a ledger bucket: one store, SSE-KMS only on execute (unchanged)", () => {
    const s3 = fakeS3();
    const dry = toolStores({ env: {}, bucket: LOCKER, region: "us-east-1", keyRoot: ROOT, credentials: CREDS, create: s3.create });
    expect(dry.ledger).toBeUndefined();
    expect(dry.store).toBe(dry.locker);
    toolStores({ env: {}, bucket: LOCKER, region: "us-east-1", keyRoot: ROOT, credentials: CREDS, kmsKeyArn: KMS, create: s3.create });
    expect(s3.configs).toEqual([
      { bucket: LOCKER, region: "us-east-1", credentials: CREDS },
      { bucket: LOCKER, region: "us-east-1", credentials: CREDS, serverSideEncryption: "aws:kms", sseKmsKeyId: KMS },
    ]);
  });

  test("with a ledger bucket: the ledger store sends no encryption header, same credentials; keys route by kind", async () => {
    const s3 = fakeS3();
    const t = toolStores({ env: ledgerEnv, bucket: LOCKER, region: "us-east-1", keyRoot: ROOT, credentials: CREDS, kmsKeyArn: KMS, create: s3.create });
    expect(s3.configs).toEqual([
      { bucket: LOCKER, region: "us-east-1", credentials: CREDS, serverSideEncryption: "aws:kms", sseKmsKeyId: KMS },
      { bucket: LEDGER, region: "eu-west-1", credentials: CREDS },
    ]);
    expect(t.ledger?.bucket).toBe(LEDGER);
    const ledgerKeys = [keys.owner("o1", "claude", "s1"), keys.revisions("o1", "rec-1"), keys.rearm("o1", "claude", "s1"), ledgerHomeKey(ROOT)];
    const lockerKeys = [keys.fence, `${ROOT}/.drain/lock.json`];
    for (const k of [...ledgerKeys, ...lockerKeys]) await t.store.put(objectKey(k), enc.encode("{}"), { contentType: "application/json" });
    for (const k of ledgerKeys) expect([k, await has(s3.bucket(LEDGER), k), await has(s3.bucket(LOCKER), k)]).toEqual([k, true, false]);
    for (const k of lockerKeys) expect([k, await has(s3.bucket(LOCKER), k), await has(s3.bucket(LEDGER), k)]).toEqual([k, true, false]);
  });

  test("the endpoint is passed only to the ledger store", () => {
    const s3 = fakeS3();
    toolStores({ env: { ...ledgerEnv, INGEST_LEDGER_ENDPOINT: "http://127.0.0.1:9000" }, bucket: LOCKER, region: "us-east-1", keyRoot: ROOT, credentials: CREDS, create: s3.create });
    expect(s3.configs[0]).not.toHaveProperty("endpoint");
    expect(s3.configs[1]).toMatchObject({ bucket: LEDGER, endpoint: "http://127.0.0.1:9000" });
  });
});

describe("assertToolLedgerHome: the fork guard", () => {
  test("with a ledger bucket: always read there, whatever the guard says; missing or another bucket refuses", async () => {
    for (const guard of [{}, { INGEST_LEDGER_HOME_GUARD: "off" }, { INGEST_LEDGER_HOME_GUARD: "on" }]) {
      const s3 = fakeS3();
      const t = toolStores({ env: ledgerEnv, bucket: LOCKER, region: "us-east-1", keyRoot: ROOT, credentials: CREDS, create: s3.create });
      // The Locker bucket's marker ("moved" to the ledger bucket, as a relocation leaves it) is not what decides.
      await home(s3.bucket(LOCKER), LEDGER);
      expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, guard))).toBe("ledger_home_missing");
      await home(s3.bucket(LEDGER), "example-other-bucket");
      expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, guard))).toBe("ledger_home_moved");
      await home(s3.bucket(LEDGER), LOCKER);
      expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, guard))).toBe("ledger_home_moved");
      await home(s3.bucket(LEDGER), LEDGER);
      expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, guard))).toBeNull();
    }
  });

  test("with a ledger bucket: the Locker bucket's own marker must exist and name exactly that ledger bucket (ledger_home_mismatch)", async () => {
    const s3 = fakeS3();
    const t = toolStores({ env: ledgerEnv, bucket: LOCKER, region: "us-east-1", keyRoot: ROOT, credentials: CREDS, create: s3.create });
    // The ledger bucket's own marker names itself throughout: a wrong setting cannot pass on that alone.
    await home(s3.bucket(LEDGER), LEDGER);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, {}))).toBe("ledger_home_mismatch"); // absent: not relocated
    await home(s3.bucket(LOCKER), LOCKER);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, {}))).toBe("ledger_home_mismatch"); // still home in the Locker
    await home(s3.bucket(LOCKER), "example-other-env-ledger");
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, {}))).toBe("ledger_home_mismatch"); // moved elsewhere
    await home(s3.bucket(LOCKER), LEDGER);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, {}))).toBeNull();
    // The Locker side is read from the Locker bucket itself (the routed store would send the marker key to the ledger).
    expect(s3.requests.filter((r) => r.key === ledgerHomeKey(ROOT)).map((r) => r.bucket)).toContain(LOCKER);
    await s3.bucket(LOCKER).put(objectKey(ledgerHomeKey(ROOT)), enc.encode("not json"));
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, {}))).toBe("ledger_home_invalid");
  });

  test("without a ledger bucket: guard on refuses a Locker marker naming another bucket; absent or naming it passes", async () => {
    const s3 = fakeS3();
    const t = toolStores({ env: {}, bucket: LOCKER, region: "us-east-1", keyRoot: ROOT, credentials: CREDS, create: s3.create });
    const on = { INGEST_LEDGER_HOME_GUARD: "on" };
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, on))).toBeNull();
    await home(s3.bucket(LOCKER), LOCKER);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, on))).toBeNull();
    await home(s3.bucket(LOCKER), LEDGER);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, on))).toBe("ledger_home_moved");
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, { INGEST_LEDGER_HOME_GUARD: "yes" }))).toBe("ledger_home_guard_invalid");
  });

  test("without a ledger bucket and the guard off: no request (unchanged)", async () => {
    expect(await refusal(assertToolLedgerHome({ store: noRequests, locker: noRequests }, ROOT, LOCKER, {}))).toBeNull();
  });
});

describe("the fence backfill CLI", () => {
  const args = (mode: "dry_run" | "execute", sections: BackfillArgs["sections"]): BackfillArgs => ({
    mode, bucket: LOCKER, region: "us-east-1", orgs: [], sections, pageSize: 1, rowPageSize: 1, concurrency: 1, statementTimeoutMs: 1_000,
    ...(mode === "execute" ? { kmsKeyArn: KMS } : {}),
  });

  test("backfillStores: KMS on the Locker store only on execute; the ledger store never", () => {
    const s3 = fakeS3();
    expect(backfillStores(args("execute", ["fences", "ownership", "revisions"]), ledgerEnv, CREDS, s3.create).ledger?.bucket).toBe(LEDGER);
    backfillStores(args("dry_run", ["fences"]), ledgerEnv, CREDS, s3.create);
    expect(s3.configs.map((c) => [c.bucket, c.serverSideEncryption ?? null])).toEqual([[LOCKER, "aws:kms"], [LEDGER, null], [LOCKER, null], [LEDGER, null]]);
  });

  test("a fences-only run makes no marker request, with or without a ledger bucket and with the guard on", async () => {
    const env = { ...ledgerEnv, INGEST_LEDGER_HOME_GUARD: "on" };
    expect(await refusal(assertBackfillLedgerHome({ store: noRequests, locker: noRequests, ledger: { store: noRequests, bucket: LEDGER } }, args("execute", ["fences"]), env))).toBeNull();
    expect(await refusal(assertBackfillLedgerHome({ store: noRequests, locker: noRequests }, args("execute", ["fences"]), { INGEST_LEDGER_HOME_GUARD: "on" }))).toBeNull();
  });

  test("an empty section list is never treated as fences-only: the check runs", async () => {
    expect(await refusal(assertBackfillLedgerHome({ store: noRequests, locker: noRequests }, args("execute", []), { INGEST_LEDGER_HOME_GUARD: "on" })))
      .toBe("ledger_home_store_error");
    const s3 = fakeS3();
    const routed = backfillStores(args("execute", []), ledgerEnv, CREDS, s3.create);
    expect(await refusal(assertBackfillLedgerHome(routed, args("execute", []), ledgerEnv))).toBe("ledger_home_mismatch");
  });

  test("an ownership or revision section: the ledger bucket's marker must name it; without one, a moved Locker marker refuses", async () => {
    for (const sections of [["ownership"], ["revisions"], ["fences", "ownership", "revisions"]] as BackfillArgs["sections"][]) {
      const s3 = fakeS3();
      const routed = backfillStores(args("execute", sections), ledgerEnv, CREDS, s3.create);
      expect(await refusal(assertBackfillLedgerHome(routed, args("execute", sections), ledgerEnv))).toBe("ledger_home_mismatch");
      await home(s3.bucket(LOCKER), LEDGER);
      expect(await refusal(assertBackfillLedgerHome(routed, args("execute", sections), ledgerEnv))).toBe("ledger_home_missing");
      await home(s3.bucket(LEDGER), "example-other-bucket");
      expect(await refusal(assertBackfillLedgerHome(routed, args("execute", sections), ledgerEnv))).toBe("ledger_home_moved");
      await home(s3.bucket(LEDGER), LEDGER);
      expect(await refusal(assertBackfillLedgerHome(routed, args("execute", sections), ledgerEnv))).toBeNull();

      const single = fakeS3();
      const one = backfillStores(args("execute", sections), {}, CREDS, single.create);
      await home(single.bucket(LOCKER), LEDGER);
      expect(await refusal(assertBackfillLedgerHome(one, args("execute", sections), { INGEST_LEDGER_HOME_GUARD: "on" }))).toBe("ledger_home_moved");
    }
  });
});

describe("the buffered drain CLI", () => {
  test("drainStores: the revision index, re-arm markers and owner stubs in the ledger bucket; fences, drained markers and the lease in the Locker", async () => {
    const s3 = fakeS3();
    const t = drainStores({ mode: "execute", bucket: LOCKER, region: "us-east-1", kmsKeyArn: KMS }, ledgerEnv, CREDS, s3.create);
    expect(s3.configs.map((c) => [c.bucket, c.serverSideEncryption ?? null])).toEqual([[LOCKER, "aws:kms"], [LEDGER, null]]);
    const toLedger = [keys.revisions("o1", "rec-1"), keys.rearm("o1", "claude", "s1"), keys.owner("o1", "claude", "s1")];
    const toLocker = [keys.fence, `${ROOT}/.drain/lock.json`, `${ROOT}/org-a/buffered-batches/v1/ws=ws-a/m=srv_${"0".repeat(32)}/b=x.drained.json`];
    for (const k of [...toLedger, ...toLocker]) await t.store.put(objectKey(k), enc.encode("{}"), { contentType: "application/json" });
    for (const k of toLedger) expect([k, await has(s3.bucket(LEDGER), k), await has(s3.bucket(LOCKER), k)]).toEqual([k, true, false]);
    for (const k of toLocker) expect([k, await has(s3.bucket(LOCKER), k), await has(s3.bucket(LEDGER), k)]).toEqual([k, true, false]);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, {}))).toBe("ledger_home_mismatch");
    await home(s3.bucket(LOCKER), LEDGER);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, {}))).toBe("ledger_home_missing");
  });

  test("without a ledger bucket: one store; guard on refuses a moved Locker marker", async () => {
    const s3 = fakeS3();
    const t = drainStores({ mode: "dry_run", bucket: LOCKER, region: "us-east-1" }, {}, CREDS, s3.create);
    expect(t.ledger).toBeUndefined();
    await home(s3.bucket(LOCKER), LEDGER);
    expect(await refusal(assertToolLedgerHome(t, ROOT, LOCKER, { INGEST_LEDGER_HOME_GUARD: "on" }))).toBe("ledger_home_moved");
  });
});

describe("homeRecheckingStore: the home check re-run during a long run", () => {
  const put = (s: ObjectStore, k = `${ROOT}/x.json`) => s.put(objectKey(k), enc.encode("{}"), { contentType: "application/json" });
  test("no re-check inside the interval; one before the first write after it; reads never re-check", async () => {
    let t = 0, checks = 0;
    const s = homeRecheckingStore(scratch(), async () => { checks++; }, { now: () => t });
    await put(s);
    t = TOOL_HOME_RECHECK_MS - 1;
    await put(s);
    await s.head(objectKey(`${ROOT}/x.json`));
    expect(checks).toBe(0);
    t = TOOL_HOME_RECHECK_MS;
    await s.head(objectKey(`${ROOT}/x.json`));
    expect(checks).toBe(0);
    await Promise.all([put(s), put(s), put(s)]);
    expect(checks).toBe(1); // concurrent writes share one re-check
    t += TOOL_HOME_RECHECK_MS - 1;
    await put(s);
    expect(checks).toBe(1);
  });

  test("a refusal is sticky (no write and no further request); a transient one is retried by the next write", async () => {
    let t = 0, checks = 0, verdict: LedgerHomeRefused | null = new LedgerHomeRefused("ledger_home_unavailable");
    const inner = scratch();
    const s = homeRecheckingStore(inner, async () => { checks++; if (verdict) throw verdict; }, { now: () => t });
    t = TOOL_HOME_RECHECK_MS;
    expect(await refusal(put(s, `${ROOT}/a.json`))).toBe("ledger_home_unavailable");
    verdict = new LedgerHomeRefused("ledger_home_moved");
    expect(await refusal(put(s, `${ROOT}/a.json`))).toBe("ledger_home_moved");
    expect(checks).toBe(2);
    verdict = null;
    expect(await refusal(put(s, `${ROOT}/a.json`))).toBe("ledger_home_moved");
    expect(await refusal(s.delete(objectKey(`${ROOT}/a.json`)))).toBe("ledger_home_moved");
    expect(checks).toBe(2);
    expect(await has(inner, `${ROOT}/a.json`)).toBe(false);
  });

  test("with keyRoot: only ledger-key writes are guarded; the Locker's own objects (the drain's lease) still go through", async () => {
    let t = 0, checks = 0;
    const inner = scratch();
    const s = homeRecheckingStore(inner, async () => { checks++; throw new LedgerHomeRefused("ledger_home_moved"); }, { now: () => t, keyRoot: ROOT });
    t = TOOL_HOME_RECHECK_MS;
    // A Locker write never triggers the re-check, before or after a refusal.
    await put(s, `${ROOT}/.drain/lock.json`);
    expect(checks).toBe(0);
    expect(s.homeRefusal()).toBeUndefined();
    expect(await refusal(put(s, keys.owner("o1", "claude", "s1")))).toBe("ledger_home_moved");
    expect(s.homeRefusal()?.code).toBe("ledger_home_moved");
    expect(await refusal(put(s, keys.revisions("o1", "rec-1")))).toBe("ledger_home_moved");
    expect(await refusal(put(s, ledgerHomeKey(ROOT)))).toBe("ledger_home_moved");
    expect(await refusal(put(s, `${ROOT}/.drain/lock.json`))).toBeNull();
    expect(await refusal(put(s, keys.fence))).toBeNull();
    expect(checks).toBe(1);
    expect(await has(inner, keys.owner("o1", "claude", "s1"))).toBe(false);
  });

  test("the drain stops writing once the Locker marker is moved away mid-run", async () => {
    const s3 = fakeS3();
    await home(s3.bucket(LOCKER), LEDGER);
    await home(s3.bucket(LEDGER), LEDGER);
    let t = 0;
    const stores = drainStores({ mode: "execute", bucket: LOCKER, region: "us-east-1", kmsKeyArn: KMS }, ledgerEnv, CREDS, s3.create);
    const { homeGuardedToolStore } = await import("../ledger-tool-store.ts");
    const guarded = homeGuardedToolStore(stores, ROOT, LOCKER, ledgerEnv, { now: () => t });
    await put(guarded, keys.owner("o1", "claude", "s1"));
    await home(s3.bucket(LOCKER), "example-other-env-ledger");
    t = TOOL_HOME_RECHECK_MS;
    expect(await refusal(put(guarded, keys.owner("o1", "claude", "s2")))).toBe("ledger_home_mismatch");
    expect(await has(s3.bucket(LEDGER), keys.owner("o1", "claude", "s2"))).toBe(false);
  });
});

describe("CLI order: every refusal comes before the first write (and the backfill's first SQL read)", () => {
  const drainArgs = parseDrainArgs(["--execute", "--bucket", LOCKER, "--kms-key-arn", KMS, "--org", "org-a", "--concurrency", "1"]);
  const backfillArgs: BackfillArgs = { mode: "execute", bucket: LOCKER, region: "us-east-1", orgs: [], sections: ["fences", "ownership", "revisions"],
    pageSize: 1, rowPageSize: 1, concurrency: 1, statementTimeoutMs: 1_000, kmsKeyArn: KMS };
  const noSql = { query: async () => { throw new Error("unexpected_sql"); } };
  const drain = (s3: ReturnType<typeof fakeS3>, env: Record<string, string>) =>
    refusal(runDrainTool(drainArgs, { env, credentials: CREDS, create: s3.create, orgs: async () => { throw new Error("unexpected_org_discovery"); }, stderr: () => {} }));
  const backfill = (s3: ReturnType<typeof fakeS3>, env: Record<string, string>) =>
    refusal(runBackfill(backfillArgs, { sql: noSql, env, credentials: CREDS, create: s3.create, ledgerBucket: toolLedgerBucket(env, backfillArgs) }));

  test("vars unset (one bucket, guard on) and the Locker marker moved: ledger_home_moved, nothing written, nothing listed", async () => {
    for (const tool of [drain, backfill]) {
      const s3 = fakeS3();
      await home(s3.bucket(LOCKER), LEDGER);
      expect(await tool(s3, { INGEST_LEDGER_HOME_GUARD: "on" })).toBe("ledger_home_moved");
      expect(s3.writes()).toEqual([]);
      expect(s3.requests).toEqual([{ bucket: LOCKER, op: "get", key: ledgerHomeKey(ROOT) }]);
    }
  });

  test("a ledger bucket the Locker marker does not name (another environment's): ledger_home_mismatch before any write", async () => {
    for (const tool of [drain, backfill]) {
      const s3 = fakeS3();
      await home(s3.bucket(LOCKER), "example-other-env-ledger");
      await home(s3.bucket(LEDGER), LEDGER);
      expect(await tool(s3, { ...ledgerEnv, INGEST_LEDGER_HOME_GUARD: "on" })).toBe("ledger_home_mismatch");
      expect(s3.writes()).toEqual([]);
      expect(s3.requests.every((r) => r.op === "get" && r.key === ledgerHomeKey(ROOT))).toBe(true);
    }
  });

  test("a ledger bucket whose own marker names another bucket: ledger_home_moved before any write", async () => {
    for (const tool of [drain, backfill]) {
      const s3 = fakeS3();
      await home(s3.bucket(LOCKER), LEDGER);
      await home(s3.bucket(LEDGER), "example-other-env-ledger");
      expect(await tool(s3, { ...ledgerEnv, INGEST_LEDGER_HOME_GUARD: "on" })).toBe("ledger_home_moved");
      expect(s3.writes()).toEqual([]);
    }
  });

  test("routed: no ledger key but the Locker's own marker is ever read from the Locker bucket", async () => {
    const s3 = fakeS3();
    await home(s3.bucket(LOCKER), LEDGER);
    await home(s3.bucket(LEDGER), LEDGER);
    // A stale pre-relocation copy of an owner stub and a revision shard left in the Locker bucket.
    for (const k of [keys.owner("org-a", "claude", "s1"), keys.revisions("org-a", "rec-1")])
      await s3.bucket(LOCKER).put(objectKey(k), enc.encode("{\"stale\":true}"), { contentType: "application/json" });
    const stores = drainStores({ mode: "execute", bucket: LOCKER, region: "us-east-1", kmsKeyArn: KMS }, ledgerEnv, CREDS, s3.create);
    await assertToolLedgerHome(stores, ROOT, LOCKER, ledgerEnv);
    await stores.store.head(objectKey(keys.owner("org-a", "claude", "s1")));
    await stores.store.getObject(objectKey(keys.revisions("org-a", "rec-1"))).catch(() => undefined);
    await stores.store.list(objectKey(`${ROOT}/.admission-inbox`));
    const fromLocker = s3.requests.filter((r) => r.bucket === LOCKER && r.op !== "list" && isLedgerKey(ROOT, r.key));
    expect(fromLocker.map((r) => r.key)).toEqual([ledgerHomeKey(ROOT)]);
  });
});
