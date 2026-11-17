/**
 * The operator tools' stores for a relocated ledger (ledger-tool-store.ts) and their use by the fence backfill and
 * the buffered drain CLIs: the edge's variables and parser, the routed store (fences in the Locker bucket, ledger/** in
 * the ledger bucket, no encryption header there), and the fork guard (ledger bucket: always, required; without:
 * INGEST_LEDGER_HOME_GUARD). Synthetic bucket names only (the repository is public).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { s3LedgerKeys } from "../ledger-s3.ts";
import { ledgerHomeKey } from "../ledger-store.ts";
import { assertToolLedgerHome, toolLedgerBucket, toolStores } from "../ledger-tool-store.ts";
import { assertBackfillLedgerHome, backfillStores, type BackfillArgs } from "../../../scripts/upload-fence-backfill.ts";
import { drainStores } from "../../../scripts/buffered-drain.ts";
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

/** A fake createS3Store: one scratch store per bucket, every config recorded. */
function fakeS3() {
  const buckets = new Map<string, ObjectStore>();
  const configs: Record<string, unknown>[] = [];
  const create = (config: { bucket: string }) => {
    configs.push(config as unknown as Record<string, unknown>);
    if (!buckets.has(config.bucket)) buckets.set(config.bucket, scratch());
    return buckets.get(config.bucket)!;
  };
  return { create, configs, bucket: (b: string) => { if (!buckets.has(b)) buckets.set(b, scratch()); return buckets.get(b)!; } };
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

  test("an ownership or revision section: the ledger bucket's marker must name it; without one, a moved Locker marker refuses", async () => {
    for (const sections of [["ownership"], ["revisions"], ["fences", "ownership", "revisions"]] as BackfillArgs["sections"][]) {
      const s3 = fakeS3();
      const routed = backfillStores(args("execute", sections), ledgerEnv, CREDS, s3.create);
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
