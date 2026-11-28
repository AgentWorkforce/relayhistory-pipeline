/**
 * The S3 writer fence of a per-org cutover through a committed bridge_complete (org-bridge-gate.ts,
 * docs/design/org-bridge-gate.md). The direct writer is the S3 consumer (ledger-s3 claim with `orgBridge: "required"`).
 * It must stay fenced at the marker and open only at the bridge-complete record. completeOrgBridge writes that record
 * only after the SQL release is committed AND the marker has landed, in either commit order, and it survives a crash
 * between the two. The drain's side (the fallback writer) is in buffered-drain.test.ts. Synthetic ids only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { acquireDrainLock, drainLockKey, exportLockKey } from "../buffered-drain.ts";
import type { ClaimInput, LedgerRecord } from "../ledger.ts";
import { fenceAuthorityKey, orgBridgeEpochHash, orgBridgeKey, orgFenceAuthorityKey, s3Ledger, validOrgBridge } from "../ledger-s3.ts";
import { completeOrgBridge, OrgBridgeRefusal, type BridgeSql } from "../org-bridge-gate.ts";
import { TENANT, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORIGIN = "origin-1";
const ORG = TENANT.orgId;
const EPOCH = "00000000-0000-4000-8000-000000000001";
const EVIDENCE = "e".repeat(64);
const hex = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const enc = new TextEncoder();
const FIELDS = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1 };
const MARKER = enc.encode(JSON.stringify({ ...FIELDS, orgId: ORG }));

const cleanups: (() => void)[] = [];
afterEach(() => { cleanups.splice(0).forEach((c) => c()); });
function fresh(): ObjectStore {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}
const putMarker = (store: ObjectStore, bytes = MARKER) => store.put(objectKey(orgFenceAuthorityKey(ROOT, ORG)), bytes, { contentType: "application/json" });
const putJson = (store: ObjectStore, key: string, v: unknown) => store.put(objectKey(key), enc.encode(typeof v === "string" ? v : JSON.stringify(v)), { contentType: "application/json" });
const record = (o: Record<string, unknown> = {}) => ({ v: 1, kind: "org-bridge-complete", orgId: ORG, holdEpochSha256: orgBridgeEpochHash(EPOCH), markerSha256: hex(MARKER),
  releaseEvidenceSha256: EVIDENCE, writtenAtMs: 5, ...o });
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;

function rec(recordId: string, revision: number): LedgerRecord {
  return { source: "claude", sessionId: "sess-1", recordId, revision, revisionId: `${recordId}@${revision}`, digest: hex(`${recordId}:${revision}`) };
}
function claimInput(batchId: string, tenant = TENANT): ClaimInput {
  const records = [rec("r1", 1)];
  return { tenant, originId: ORIGIN, batchId, batchDigest: hex(batchId), receiptId: `rhr_${hex(`receipt:${batchId}`)}`, sessions: [["claude", "sess-1"]], records } as ClaimInput;
}
const consumer = (store: ObjectStore, now: () => number = () => 0) =>
  s3Ledger(store, ROOT, { fenceAuthority: "required", orgBridge: "required", nowMs: now, sleep: async () => {} });
/** The direct writer's verdict for one new batch: "claimed" or the refusal code. */
async function direct(store: ObjectStore, batchId = `b-${randomUUID()}`, now?: () => number): Promise<string> {
  try { return (await consumer(store, now).claim(claimInput(batchId))).kind; } catch (e) { return String((e as { code?: unknown }).code); }
}

/** slot 062320's status function, as completeOrgBridge reads it: the latest hold of the org. */
function sqlWith(hold: Record<string, unknown> | null | (() => Record<string, unknown> | null)): BridgeSql & { calls: number } {
  const s = {
    calls: 0,
    async query(text: string, params: unknown[]) {
      s.calls++;
      expect(text).toContain("sessions.ingest_org_admission_hold_status");
      expect(params).toEqual([ORG]);
      const h = typeof hold === "function" ? hold() : hold;
      return { rows: [{ status: { hold: h, erasures: 0 } }] };
    },
  };
  return s;
}
const held = (state: string, o: Record<string, unknown> = {}) => ({ epoch: EPOCH, held_org: ORG, state, release_reason: null, release_evidence_sha256: null, ...o });
const released = (o: Record<string, unknown> = {}) => held("released", { release_reason: "bridge_complete", release_evidence_sha256: EVIDENCE, ...o });
const complete = (store: ObjectStore, sql: BridgeSql, o: Partial<Parameters<typeof completeOrgBridge>[0]> = {}) =>
  completeOrgBridge({ sql, store, keyRoot: ROOT, orgId: ORG, epoch: EPOCH, holder: "bridge-test", now: () => 10, ...o });
const refusal = async (p: Promise<unknown>) => { try { await p; return "ok"; } catch (e) { return e instanceof OrgBridgeRefusal ? e.code : `other:${String(e)}`; } };

describe("ledger-s3 orgBridge: the direct writer stays fenced at the marker, until the bridge-complete record", () => {
  test("a valid per-org marker alone is a retry (bridge_pending), never an admit; the bound record admits", async () => {
    const store = fresh();
    await putMarker(store);
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    await putJson(store, orgBridgeKey(ROOT, ORG), record());
    expect(await direct(store)).toBe("claimed");
  });

  test("a record that is invalid, names another org or binds other marker bytes refuses (bridge_invalid)", async () => {
    for (const bad of ["not json", { ...record(), extra: 1 }, record({ orgId: "org-other" }), record({ markerSha256: "0".repeat(64) }), record({ kind: "x" })]) {
      const store = fresh();
      await putMarker(store);
      await putJson(store, orgBridgeKey(ROOT, ORG), bad);
      expect(await direct(store)).toBe("ledger_fence_authority_bridge_invalid");
    }
    // The marker replaced by other bytes after the record: the record no longer binds it.
    const store = fresh();
    await putJson(store, orgBridgeKey(ROOT, ORG), record());
    await putMarker(store, enc.encode(JSON.stringify({ ...FIELDS, orgId: ORG, writtenAtMs: 2 })));
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_invalid");
  });

  test("a paused direct writer resuming across the fence: a refusal is never cached, and nothing positive is cached before the record", async () => {
    const store = fresh();
    let now = 0;
    await putMarker(store);
    const ledger = consumer(store, () => now);
    const call = async (id: string) => { try { return (await ledger.claim(claimInput(id))).kind; } catch (e) { return String((e as { code?: unknown }).code); } };
    // The same ledger instance (one warm consumer) through the whole window: held across 60 s cache windows.
    for (const t of [0, 30_000, 61_000, 3_600_000]) { now = t; expect(await call(`p${t}`)).toBe("ledger_fence_authority_bridge_pending"); }
    await putJson(store, orgBridgeKey(ROOT, ORG), record());
    expect(await call("after")).toBe("claimed");
  });

  test("the deployment-wide marker does not bypass a per-org marker's bridge; an org with no per-org marker is unchanged", async () => {
    const store = fresh();
    await putJson(store, fenceAuthorityKey(ROOT), FIELDS);
    await putMarker(store);
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    expect(await (async () => { try { return (await consumer(store).claim(claimInput("o1", { ...TENANT, orgId: "org-other" }))).kind; } catch (e) { return String((e as { code?: unknown }).code); } })()).toBe("claimed");
  });

  test("unset (self-host): a per-org marker alone admits, exactly as before; orgBridge without fenceAuthority refuses construction", async () => {
    const store = fresh();
    await putMarker(store);
    expect((await s3Ledger(store, ROOT, { fenceAuthority: "required", sleep: async () => {} }).claim(claimInput("s1"))).kind).toBe("claimed");
    expect(() => s3Ledger(store, ROOT, { orgBridge: "required" })).toThrow();
    expect(() => s3Ledger(store, ROOT, { fenceAuthority: "required", orgBridge: "on" as never })).toThrow();
  });
});

describe("completeOrgBridge: the switch is written only after a committed bridge_complete and a landed marker", () => {
  test("marker first, then bridge_complete: no record while the hold is live; the release writes it; the consumer opens only then", async () => {
    const store = fresh();
    await putMarker(store);
    for (const state of ["held", "sealed", "marker_attempted"]) {
      expect(await refusal(complete(store, sqlWith(held(state))))).toBe("bridge_hold_not_released");
      expect(await exists(store, orgBridgeKey(ROOT, ORG))).toBe(false);
      expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    }
    const r = await complete(store, sqlWith(released()));
    expect(r.outcome).toBe("written");
    const stored = JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(orgBridgeKey(ROOT, ORG))))));
    expect(validOrgBridge(stored)).toBe(true);
    expect(stored).toEqual(record({ writtenAtMs: 10 }));
    expect(JSON.stringify(stored)).not.toContain(EPOCH); // hashes only
    expect(await direct(store)).toBe("claimed");
    // Idempotent: a rerun adopts the same bytes.
    expect(await complete(store, sqlWith(released()), { now: () => 99 })).toEqual({ outcome: "adopted", recordSha256: r.recordSha256 });
  });

  test("bridge_complete committed before the marker PUT lands: refused (bridge_marker_absent), fenced; the late marker alone does not open; a rerun does", async () => {
    const store = fresh();
    expect(await refusal(complete(store, sqlWith(released())))).toBe("bridge_marker_absent");
    expect(await exists(store, orgBridgeKey(ROOT, ORG))).toBe(false);
    expect(await direct(store)).toBe("ledger_fence_authority_missing");
    await putMarker(store); // the paused marker PUT is delivered late
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    expect((await complete(store, sqlWith(released()))).outcome).toBe("written");
    expect(await direct(store)).toBe("claimed");
  });

  test("a crash between the SQL release and the record: fenced (fail closed), and the rerun completes it", async () => {
    const base = fresh();
    await putMarker(base);
    // The record PUT never lands (the process died on it).
    const crashing = new Proxy(base, {
      get(target, prop) {
        if (prop === "put") return async (...a: Parameters<ObjectStore["put"]>) => {
          if (String(a[0]) === orgBridgeKey(ROOT, ORG)) throw new Error("killed");
          return target.put(...a);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
      },
    });
    expect(await refusal(complete(crashing, sqlWith(released())))).toBe("bridge_put_unknown");
    expect(await exists(base, orgBridgeKey(ROOT, ORG))).toBe(false);
    expect(await direct(base)).toBe("ledger_fence_authority_bridge_pending");
    // The lease it took was released (or runs out): the rerun is not blocked by its own crash.
    expect((await complete(base, sqlWith(released()))).outcome).toBe("written");
    expect(await direct(base)).toBe("claimed");
  });

  test("an ambiguous record PUT that did land is adopted by its read-back", async () => {
    const base = fresh();
    await putMarker(base);
    const lossy = new Proxy(base, {
      get(target, prop) {
        if (prop === "put") return async (...a: Parameters<ObjectStore["put"]>) => {
          const r = await target.put(...a);
          if (String(a[0]) === orgBridgeKey(ROOT, ORG)) throw new Error("response lost");
          return r;
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
      },
    });
    expect((await complete(lossy, sqlWith(released()))).outcome).toBe("adopted");
    expect(await direct(base)).toBe("claimed");
  });

  test("anything but this epoch released with bridge_complete refuses, with nothing written", async () => {
    const cases: [Record<string, unknown> | null, string][] = [
      [null, "bridge_hold_absent"],
      [released({ epoch: "00000000-0000-4000-8000-000000000002" }), "bridge_hold_epoch_mismatch"],
      [held("released", { release_reason: "verified_rollback", release_evidence_sha256: EVIDENCE }), "bridge_release_not_bridge_complete"],
      [released({ release_evidence_sha256: "nope" }), "bridge_hold_unreadable"],
    ];
    for (const [h, code] of cases) {
      const store = fresh();
      await putMarker(store);
      expect(await refusal(complete(store, sqlWith(h)))).toBe(code);
      expect(await exists(store, orgBridgeKey(ROOT, ORG))).toBe(false);
    }
    const store = fresh();
    await putMarker(store);
    expect(await refusal(complete(store, { query: async () => { throw new Error("db down"); } }))).toBe("bridge_hold_unreadable");
    expect(await refusal(complete(store, sqlWith(released()), { epoch: "not-a-uuid" }))).toBe("bridge_input");
    await putMarker(store, enc.encode(JSON.stringify({ ...FIELDS, orgId: "org-other" })));
    expect(await refusal(complete(store, sqlWith(released())))).toBe("bridge_marker_invalid");
  });

  test("a stored record of another release is a conflict and is never overwritten", async () => {
    const store = fresh();
    await putMarker(store);
    const other = record({ releaseEvidenceSha256: "f".repeat(64) });
    await putJson(store, orgBridgeKey(ROOT, ORG), other);
    expect(await refusal(complete(store, sqlWith(released())))).toBe("bridge_record_conflict");
    expect(JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(orgBridgeKey(ROOT, ORG))))))).toEqual(other);
  });

  test("no drain beside the switch: a live drain (or export) lease refuses; a stalled drain whose lease expired loses it to the bridge", async () => {
    const store = fresh();
    await putMarker(store);
    let now = 1_000;
    const drain = await acquireDrainLock(store, ROOT, { holder: "drain-1", ttlMs: 60_000, now: () => now });
    expect(await refusal(complete(store, sqlWith(released()), { now: () => now }))).toBe("bridge_drain_running");
    expect(await exists(store, orgBridgeKey(ROOT, ORG))).toBe(false);
    // The drain stalls past its lease; the bridge takes the lease (a higher fencing token) and writes the switch.
    now = 1_000 + 61_000;
    expect((await complete(store, sqlWith(released()), { now: () => now })).outcome).toBe("written");
    expect(drain.alive()).toBe(false);
    await expect(drain.renew()).rejects.toMatchObject({ code: "drain_lock_lost" });
    // An export's live lease refuses too (the export rewrites what a drain acts on).
    const s2 = fresh();
    await putMarker(s2);
    const exp = await acquireDrainLock(s2, ROOT, { holder: "export-1", ttlMs: 60_000, now: () => 5, key: exportLockKey(ROOT) });
    expect(await refusal(complete(s2, sqlWith(released()), { now: () => 5 }))).toBe("bridge_drain_running");
    expect(await exists(s2, drainLockKey(ROOT))).toBe(true); // its own lease, expired in place
    await exp.release();
    expect((await complete(s2, sqlWith(released()), { now: () => 5 })).outcome).toBe("written");
    // A clock at zero would write a record the consumer refuses: refused before any PUT instead.
    const s3 = fresh();
    await putMarker(s3);
    expect(await refusal(complete(s3, sqlWith(released()), { now: () => 0 }))).toBe("bridge_input");
    expect(await exists(s3, orgBridgeKey(ROOT, ORG))).toBe(false);
  });

  test("store errors are codes, never an admit", async () => {
    const base = fresh();
    await putMarker(base);
    const broken = new Proxy(base, {
      get(target, prop) {
        if (prop === "getObject") return async (k: Parameters<ObjectStore["getObject"]>[0]) => {
          if (String(k) === orgBridgeKey(ROOT, ORG)) throw new ObjectStoreError("unavailable", "down");
          return target.getObject(k);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
      },
    });
    expect(await refusal(complete(broken, sqlWith(released())))).toBe("bridge_record_unreadable");
    expect(await direct(broken)).toMatch(/^ledger_store_/);
  });
});
