/**
 * HARNESS (run as its own process by sweeper-pass-verify.test.ts: module mocks are process-wide). The write-factory
 * trap for sweeper-pass-verify's production entrypoint (scripts/backlog-sweeper-verify.ts): after the fixture is
 * seeded and before the CLI is loaded, every write-capable store factory is replaced by a trap and the authority
 * modules by ones that fail to load. The CLI then runs end to end against a fake S3 endpoint; it prints what fired.
 */
import { mock } from "bun:test";
import { createHash } from "node:crypto";
import { join } from "node:path";
import * as real from "@rhc-object-store";

const hits: string[] = [];
const trap = (name: string) => (..._a: unknown[]) => { hits.push(name); throw new Error(`trap: ${name}`); };
class TrapStore { constructor() { hits.push("BaseStore"); throw new Error("trap: BaseStore"); } }
// The scratch store that seeds the fixture is built BEFORE the traps (createFsStore is trapped afterwards too).
const { scratchStore } = await import("../support.ts");
const { writeRearmPassReceipt, rearmBucketHash } = await import("../../rearm-sweeper-receipt.ts");
const { rearmScopeHash, rearmScopes } = await import("../../rearm-sweeper.ts");
const scratch = scratchStore();
const fs = scratch.store;

mock.module("@rhc-object-store", () => ({ ...real, createS3Store: trap("createS3Store"), createGcsStore: trap("createGcsStore"), createAzureBlobStore: trap("createAzureBlobStore"), BaseStore: TrapStore }));
const here = join(import.meta.dir, "..");
// And the package's own factory modules, for anything that imports them directly (as test support does).
const pkg = join(here, "../../../packages/object-store/src");
const realS3 = await import(join(pkg, "s3.ts")), realFs = await import(join(pkg, "fs.ts"));
mock.module(join(pkg, "s3.ts"), () => ({ ...realS3, createS3Store: trap("s3.ts createS3Store") }));
mock.module(join(pkg, "fs.ts"), () => ({ ...realFs, createFsStore: trap("fs.ts createFsStore") }));
for (const m of ["../backlog-authority.ts", "../authority-evidence.ts", "../ledger-tool-store.ts", "../backlog-ops.ts", "../revision-reshard.ts"]) {
  mock.module(join(here, m), () => { hits.push(`load ${m}`); throw new Error(`trap: load ${m}`); });
}

const T0 = Date.now() + 10 * 60_000, FIN = T0 + 5000, ROOT = "96bulls";
const counts = Object.fromEntries(["listed", "erased", "superseded", "alreadyDone", "pending", "resetRunning", "reenabled", "fenceAbsent", "fenceUnreadable",
  "corrupt", "ownerMissing", "machineMismatch", "machineUnbound", "layoutCollision", "layoutScanTooLarge", "machineDerived", "emptyUnbound", "deferred", "gone",
  "failed", "wouldErase", "wouldSupersede", "versions"].map((k) => [k, 0]));
const seededOk = await writeRearmPassReceipt(fs, { v: 1, kind: "rearm-sweeper-pass", org: "org-a", enabled: true, dryRun: false, complete: true, fromStart: true,
  failures: 0, startedAtMs: T0, finishedAtMs: FIN, scopeHash: rearmScopeHash(rearmScopes(ROOT, ["org-a"])), scopes: 1, keyRoot: ROOT,
  bucketHash: rearmBucketHash("transcripts"), revision: { imageDigest: `sha256:${"b".repeat(64)}` },
  workload: { namespace: "ingest", cronJobName: "rearm-sweeper", jobName: "rearm-sweeper-1", jobUid: "0f0e0d0c-0b0a-4908-8706-050403020100",
    podName: "rearm-sweeper-1-abcde", podUid: "1a2b3c4d-5e6f-4a0b-9c1d-2e3f4a5b6c7d" }, counts: counts as never });
const hitsBeforeCli = [...hits];

const methods: string[] = [];
const fetcher = (async (input: URL | RequestInfo, init?: RequestInit) => {
  methods.push(init?.method ?? "GET");
  const key = decodeURIComponent(new URL(String(input)).pathname.slice(1));
  const bytes = await real.readAll((await fs.getObject(real.objectKey(key))).body);
  return new Response(bytes, { headers: { etag: `"${createHash("md5").update(bytes).digest("hex")}"`, "x-amz-version-id": "v1", "last-modified": new Date(FIN).toUTCString() } });
}) as typeof fetch;
let summary: unknown = null;
try {
  const { main } = await import("../../../../scripts/backlog-sweeper-verify.ts");
  summary = await main(["--bucket", "transcripts", "--region", "us-east-1"], { AWS_ACCESS_KEY_ID: "AKIATESTONLY0000", AWS_SECRET_ACCESS_KEY: "test-only-secret" },
    () => JSON.stringify({ org: "org-a", orgs: ["org-a"], bucket: "transcripts", ledgerBucket: null, namespace: "ingest", cronJobName: "rearm-sweeper",
      imageDigest: `sha256:${"b".repeat(64)}` }), { fetch: fetcher, now: () => FIN + 1000 });
} finally { scratch.done(); }
const hitsByCli = hits.slice(hitsBeforeCli.length);
// The traps are live: anything that did build a store would have hit one.
const trapped = await import("@rhc-object-store") as any;
const armed = [() => trapped.createS3Store({}), () => new trapped.BaseStore(), () => trapped.createGcsStore({}), () => trapped.createAzureBlobStore({})]
  .map((f) => { try { f(); return false; } catch (e) { return String((e as Error).message).startsWith("trap:"); } });
let authorityArmed = false;
try { await import("../../backlog-authority.ts"); } catch (e) { authorityArmed = String((e as Error).message).startsWith("trap:"); }
process.stdout.write(JSON.stringify({ seededOk, hitsBeforeCli, hitsByCli, methods, summary, armed, authorityArmed }) + "\n");
