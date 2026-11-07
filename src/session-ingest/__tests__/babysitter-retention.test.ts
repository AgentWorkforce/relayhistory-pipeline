/**
 * Babysitter retention: bucket derivation (reset epoch in the key), the uniform
 * write path (register -> bucket -> stub in every acceptance mode; flag off =
 * today's layout), and the sweep tick's refusals, lease guard, budget, dry run and
 * idempotency. The SQL lane is proven against real PG17 in
 * babysitter-retention.postgres.test.ts.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import {
  bucketRefs,
  BABYSITTER_HARNESS,
  MIN_LEASE_LEFT_MS,
  babysitterBucket,
  babysitterRetentionTick,
  planBabysitterBuckets,
  retentionTargets,
  routingFlag,
  sweepMode,
  worstCaseSubrequests,
  type BabysitterRetentionLedger,
  type ClaimedObject,
} from "../babysitter-retention.ts";
import { babysitterRoutingActive, deliverBatch, DeliveryBudgetExceeded, IngestUnavailable } from "../deliver.ts";
import type { BabysitterRetentionRegistration } from "../ledger.ts";
import { liveMachineId } from "../writer.ts";
import { readBufferedStage, readStaged } from "../staging.ts";
import { ALLOWED_SQL, allowlistedQuery, phaseSettings, poolConfig, startBabysitterPhase } from "../../migration/neon-s3-erasure/babysitter-phase.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const HOUR = 3_600_000;
const GEN = "e".repeat(64);
const EXPIRES = Date.UTC(2026, 11, 6, 13, 20, 0); // 2026-12-06T13:20Z -> bucket hour 14:00Z
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const SECRET_FIELDS = ["lin-1", "del-1"];

function receipt(id: string, over: Record<string, unknown> = {}, session = "sess-1") {
  return record({
    record_id: id,
    kind: "turn_receipt",
    session_id: session,
    payload: {
      id: `tr-${id}`, lineage_id: "lin-1", delivery_id: "del-1", status: "delivered", head_sha: "a".repeat(40), sequence: 1,
      queued_at_ms: 1, delivered_at_ms: 2, completed_at_ms: 3, coding_session_harness: "claude", coding_session_id: "sess-1",
      retention_class: "standard_30d", expires_at_ms: EXPIRES, ...over,
    },
  });
}

async function listAll(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}

/** Every raw chunk line under a prefix, decoded. */
async function chunkText(store: ObjectStore, prefix: string): Promise<string> {
  let text = "";
  for (const key of (await listAll(store, prefix)).filter((k) => k.includes("/raw/")))
    text += new TextDecoder().decode(zstd(await readAll((await store.getObject(objectKey(key))).body)));
  return text;
}

class RecordingLedger extends MemoryLedger {
  registrations: BabysitterRetentionRegistration[] = [];
  order: string[] = [];
  async registerBabysitterRetention(input: BabysitterRetentionRegistration) {
    this.registrations.push(input);
    this.order.push("register");
    // As the SQL lane: the ref names the hour and sha16(source, session, reset epoch, fence epoch 0).
    const objects = input.buckets.map((b) => {
      const d = new Date(b.eraseAfterMs), p2 = (n: number) => String(n).padStart(2, "0");
      const tag = createHash("sha256").update(`${b.source}\u001f${b.sessionId ?? ""}\u001f${input.resetEpoch}\u001f0`).digest("hex").slice(0, 16);
      return { ...b, ref: `_bx${d.getUTCFullYear()}${p2(d.getUTCMonth() + 1)}${p2(d.getUTCDate())}${p2(d.getUTCHours())}-${tag}` };
    });
    return { objects, reopened: 0 };
  }
}

function setup(over: { routing?: boolean; ledger?: MemoryLedger; inlineMaxSessions?: number } = {}) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = over.ledger ?? new RecordingLedger();
  const fanouts: { key: string }[] = [];
  const deps = {
    ledger,
    target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} },
    ...(over.routing ? { babysitterRetention: true, subrequestCap: 10_000, subrequestRedirects: 0, retentionGeneration: GEN } : {}),
    ...(over.inlineMaxSessions !== undefined ? { inlineMaxSessions: over.inlineMaxSessions, fanout: async (m: { key: string }[]) => { fanouts.push(...m); } } : {}),
  };
  return { store: s.store, ledger, deps, fanouts };
}

const machine = liveMachineId(TENANT.workspaceId, "origin-1", TENANT.userId);
const bucketPrefix = `96bulls/org-a/v1/ws=ws-a/m=${machine}/h=${BABYSITTER_HARNESS}`;

describe("bucket derivation", () => {
  test("rounds the expiry UP to the UTC hour; the ref (with the claim's epochs) is SQL's", () => {
    const r = { kind: "turn_receipt", operation: "upsert", source: "claude", session_id: "sess-1", payload: { expires_at_ms: EXPIRES } } as const;
    expect(babysitterBucket(r)).toEqual({ eraseAfterMs: Date.UTC(2026, 11, 6, 14), source: "claude", sessionId: "sess-1" });
    expect(babysitterBucket({ ...r, payload: { expires_at_ms: Date.UTC(2026, 11, 6, 14) } })!.eraseAfterMs).toBe(Date.UTC(2026, 11, 6, 14));
  });
  test("other kinds, tombstones and non-numeric expiry are not routed; sessionless evidence has a null session", () => {
    const base = { source: "claude", session_id: "s" };
    expect(babysitterBucket({ ...base, kind: "turn_receipt", operation: "upsert", session_id: "", payload: { expires_at_ms: EXPIRES } })!.sessionId).toBeNull();
    expect(babysitterBucket({ ...base, kind: "session_event", operation: "upsert", payload: { expires_at_ms: EXPIRES } })).toBeNull();
    expect(babysitterBucket({ ...base, kind: "turn_receipt", operation: "delete", payload: null })).toBeNull();
    expect(babysitterBucket({ ...base, kind: "turn_receipt", operation: "upsert", payload: { expires_at_ms: "1" } })).toBeNull();
  });
  test("plan: one request per (source, session, hour); a partial or malformed registry answer is refused", () => {
    const three = planBabysitterBuckets([receipt("a"), receipt("b"), receipt("c", { expires_at_ms: EXPIRES + 2 * HOUR })] as never);
    expect(three.requests).toHaveLength(2);
    expect(() => bucketRefs(three, [])).toThrow();
    expect(() => bucketRefs(three, three.requests.map((r) => ({ ...r, ref: "../x" })))).toThrow();
    const ok = bucketRefs(three, three.requests.map((r, i) => ({ ...r, ref: `_bx2026120614-${String(i).repeat(16)}` })));
    expect(ok.size).toBe(2);
  });
  test("routing flag is case-insensitive like the sweep flag; anything else is off", () => {
    expect(routingFlag("on")).toBe(true);
    expect(routingFlag(" ON ")).toBe(true);
    expect(routingFlag("On")).toBe(true);
    expect(routingFlag("true")).toBe(false);
    expect(routingFlag(undefined)).toBe(false);
  });
});

describe("write path (canonical inline and staged)", () => {
  test("flag off: today's layout, nothing registered", async () => {
    const { store, ledger, deps } = setup();
    expect((await deliverBatch(deps as never, TENANT, await batch([receipt("off-1"), record({ record_id: "ev-1" })]))).outcome).toBe("accepted");
    expect((await listAll(store, "96bulls/org-a/v1")).some((k) => k.includes(`/h=${BABYSITTER_HARNESS}/`))).toBe(false);
    expect((ledger as RecordingLedger).registrations).toEqual([]);
    expect(await chunkText(store, "96bulls/org-a/v1")).toContain("lin-1");
  });

  test("flag on: register first, full evidence only in its bucket, a payload-free stub in its own session", async () => {
    const { store, ledger, deps } = setup({ routing: true });
    const rec = ledger as RecordingLedger;
    const put = store.put.bind(store);
    store.put = (async (...args: Parameters<ObjectStore["put"]>) => { rec.order.push("put"); return put(...args); }) as ObjectStore["put"];
    expect((await deliverBatch(deps as never, TENANT, await batch([receipt("on-1"), record({ record_id: "ev-2" })]))).outcome).toBe("accepted");
    expect(rec.order[0]).toBe("register");
    const reg = rec.registrations[0]!;
    expect(reg).toMatchObject({ resetEpoch: 0, generation: GEN, batchId: "batch-1" });
    expect(reg.claimToken).toMatch(/^[0-9a-f-]{36}$/);
    expect(reg.buckets).toEqual([{ source: "claude", sessionId: "sess-1", eraseAfterMs: Date.UTC(2026, 11, 6, 14) }]);
    const ref = (await rec.registerBabysitterRetention(reg)).objects[0]!.ref;
    // One create-only object per (bucket, batch): no manifest and no live-session pointer.
    const bucketKeys = await listAll(store, bucketPrefix);
    expect(bucketKeys).toEqual([expect.stringMatching(new RegExp(`^${bucketPrefix}/d=2026/12/06/s=${ref}/raw/b=[0-9a-f]{32}\\.ndjson\\.zst$`))]);
    expect((await listAll(store, "96bulls/org-a/live-sessions")).some((k) => k.includes(`/h=${BABYSITTER_HARNESS}/`))).toBe(false);
    const bucketText = await chunkText(store, bucketPrefix);
    for (const f of SECRET_FIELDS) expect(bucketText).toContain(f);
    const sessionText = await chunkText(store, `96bulls/org-a/v1/ws=ws-a/m=${machine}/h=claude`);
    for (const f of SECRET_FIELDS) expect(sessionText).not.toContain(f);
    expect(sessionText).toContain(`"retention_bucket":"${ref}"`);
    expect(sessionText).toContain("ev-2");
  });

  test("P1-a: a too-wide batch registers buckets only, and its staged object holds no Babysitter payload", async () => {
    const { store, ledger, deps, fanouts } = setup({ routing: true, inlineMaxSessions: 1 });
    const r = await deliverBatch(deps as never, TENANT, await batch([receipt("st-1"), record({ record_id: "ev-3", session_id: "sess-2" })]));
    expect(r.outcome).toBe("accepted_staged");
    const regs = (ledger as RecordingLedger).registrations;
    expect(regs).toHaveLength(1);
    expect(regs[0]!.buckets.every((o) => typeof o.source === "string")).toBe(true);
    // The staged key is never registered for erasure.
    expect(JSON.stringify(regs)).not.toContain("live-batches");
    const staged = await readStaged(store, fanouts[0]!.key, zstd);
    const text = JSON.stringify(staged);
    for (const f of SECRET_FIELDS) expect(text).not.toContain(f);
    expect(text).toContain("ev-3");
    expect(staged.groups.map((g) => g.source)).not.toContain(BABYSITTER_HARNESS);
    // The bucket already holds the evidence before the fan-out runs.
    for (const f of SECRET_FIELDS) expect(await chunkText(store, bucketPrefix)).toContain(f);
  });

  test("disposition: a 60-session batch keeps 60 per-session buckets (never collapsed), one PUT each", async () => {
    const { store, ledger, deps } = setup({ routing: true, inlineMaxSessions: 40 });
    const recs = Array.from({ length: 60 }, (_, i) => receipt(`wide-${i}`, {}, `wide-sess-${i}`));
    const puts: string[] = [];
    const put = store.put.bind(store);
    store.put = (async (...args: Parameters<ObjectStore["put"]>) => { if (String(args[0]).includes("/h=_babysitter/")) puts.push(String(args[0])); return put(...args); }) as ObjectStore["put"];
    await deliverBatch(deps as never, TENANT, await batch(recs));
    const objs = (ledger as RecordingLedger).registrations[0]!.buckets;
    expect(objs).toHaveLength(60);
    expect(new Set(objs.map((o) => o.sessionId))).toEqual(new Set(recs.map((r) => r.session_id)));
    expect(objs.every((o) => o.source === "claude")).toBe(true);
    expect(puts).toHaveLength(60);
  });

  test("budget: an over-cap routed batch is refused with a typed 422 BEFORE the claim (no ledger row, no S3)", async () => {
    const { store, ledger, deps } = setup({ routing: true, inlineMaxSessions: 1000 });
    const recs = Array.from({ length: 200 }, (_, i) => receipt(`budget-${i}`, {}, `budget-sess-${i}`));
    const refused = deliverBatch(deps as never, TENANT, await batch(recs));
    await expect(refused).rejects.toBeInstanceOf(DeliveryBudgetExceeded);
    await expect(refused).rejects.toMatchObject({ status: 422, code: "delivery_budget_exceeded" });
    expect(ledger.claims).toEqual([]);
    expect((ledger as RecordingLedger).registrations).toEqual([]);
    expect(await listAll(store, "96bulls")).toEqual([]);
    // Client reduction: the same records split (as Desktop's local isolation halves a 422'd batch) fit.
    const half = setup({ routing: true, inlineMaxSessions: 1000 });
    const small = await batch(recs.slice(0, 40), { batch_id: "budget-child-1" });
    expect((await deliverBatch(half.deps as never, TENANT, small)).outcome).toBe("accepted");
    // Through the default staging bound the full batch fits (200 PUTs + one staged object).
    const ok = setup({ routing: true, inlineMaxSessions: 40 });
    expect((await deliverBatch(ok.deps as never, TENANT, await batch(recs))).outcome).toBe("accepted_staged");
  });

  test("budget: with the pessimistic default (20 redirects per fetch) a cap of 10,000 refuses a 500-bucket buffered batch", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const ledger = new RecordingLedger();
    const recs = Array.from({ length: 500 }, (_, i) => receipt(`wide-r-${i}`, {}, `wide-r-sess-${i}`));
    const deps = { ledger, acceptanceMode: "buffered" as const, babysitterRetention: true, subrequestCap: 10_000, retentionGeneration: GEN, target: { store: s.store, keyRoot: "96bulls" } };
    await expect(deliverBatch(deps as never, TENANT, await batch(recs))).rejects.toBeInstanceOf(DeliveryBudgetExceeded);
    expect(ledger.claims).toEqual([]);
  });

  test("a registration failure is a retryable 503 and nothing is written", async () => {
    const ledger = new RecordingLedger();
    ledger.registerBabysitterRetention = async () => { throw new Error("down"); };
    const { store, deps } = setup({ routing: true, ledger });
    await expect(deliverBatch(deps as never, TENANT, await batch([receipt("fail-1")]))).rejects.toBeInstanceOf(IngestUnavailable);
    expect(await listAll(store, "96bulls")).toEqual([]);
  });

  test("a ledger without the RPC refuses a routed batch (never writes an unregistered bucket)", async () => {
    const { store, deps } = setup({ routing: true, ledger: new MemoryLedger() });
    await expect(deliverBatch(deps as never, TENANT, await batch([receipt("noreg-1")]))).rejects.toMatchObject({ stage: "babysitter_retention" });
    expect(await listAll(store, "96bulls")).toEqual([]);
  });

  test("routing needs the Neon replica off, a proven subrequest cap and the current generation (never assumed)", () => {
    const on = { babysitterRetention: true, subrequestCap: 10_000, retentionGeneration: GEN };
    expect(babysitterRoutingActive(on)).toBe(true);
    expect(babysitterRoutingActive({ ...on, subrequestCap: undefined })).toBe(false);
    expect(babysitterRoutingActive({ ...on, subrequestCap: 0 })).toBe(false);
    expect(babysitterRoutingActive({ ...on, retentionGeneration: undefined })).toBe(false);
    expect(babysitterRoutingActive({ ...on, retentionGeneration: "stale" })).toBe(false);
    expect(babysitterRoutingActive({ ...on, replica: { mode: "required" } as never })).toBe(false);
    expect(babysitterRoutingActive({ ...on, replica: { mode: "off" } as never })).toBe(true);
    expect(babysitterRoutingActive({})).toBe(false);
  });

  test("budget proof: every S3 call x (1 + redirects), the writer's retry loops at their maxima", () => {
    // per inline session: GROUP_ATTEMPTS 3 x (4 pointer + CAS_ATTEMPTS 8 x 4) = 108 S3 calls
    expect(worstCaseSubrequests({ buckets: 0, sessions: 1, mode: "inline", redirects: 0 }, 3, 8)).toBe(1 + 108);
    expect(worstCaseSubrequests({ buckets: 10, sessions: 2, mode: "inline", redirects: 0 }, 3, 8)).toBe(1 + 10 + 216);
    expect(worstCaseSubrequests({ buckets: 500, sessions: 500, mode: "staged", redirects: 0 }, 3, 8)).toBe(1 + 1 + 501);
    expect(worstCaseSubrequests({ buckets: 500, sessions: 500, mode: "buffered", redirects: 20 }, 3, 8)).toBe(1 + 502 * 21);
    expect(() => worstCaseSubrequests({ buckets: 1, sessions: 1, mode: "inline", redirects: 21 }, 3, 8)).toThrow();
  });
});

class BufferedLedger extends RecordingLedger {
  specs: { key: string; sha256: string; bytes: number; groupOrdinals: number[][] }[] = [];
  async prepareBufferStage(input: { key: string; sha256: string; bytes: number; groupOrdinals: number[][] }) {
    this.order.push("buffer_intent");
    this.specs.push({ key: input.key, sha256: input.sha256, bytes: input.bytes, groupOrdinals: input.groupOrdinals });
    return { stageToken: crypto.randomUUID(), expiresAt: new Date(Date.now() + 3600000).toISOString() };
  }
  async commitBuffered() { this.order.push("buffer_receipt"); }
}

describe("buffered acceptance (the Neon-exit path)", () => {
  function bufferedSetup(routing = true) {
    const s = scratchStore();
    cleanups.push(s.done);
    const ledger = new BufferedLedger();
    const put = s.store.put.bind(s.store);
    s.store.put = (async (...args: Parameters<ObjectStore["put"]>) => { ledger.order.push(String(args[0]).includes("/buffered-batches/") ? "stage_put" : "put"); return put(...args); }) as ObjectStore["put"];
    const deps = { ledger, acceptanceMode: "buffered" as const, babysitterRetention: routing, subrequestCap: 10_000, subrequestRedirects: 0, retentionGeneration: GEN, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} } };
    return { store: s.store, ledger, deps };
  }

  test("register -> bucket -> intent -> stage -> receipt; the stage keeps every record in its own session as a stub", async () => {
    const { store, ledger, deps } = bufferedSetup();
    expect((await deliverBatch(deps as never, TENANT, await batch([receipt("bf-1"), record({ record_id: "ev-b1" })]))).outcome).toBe("accepted_buffered");
    expect(ledger.order[0]).toBe("register");
    expect(ledger.order.indexOf("put")).toBeGreaterThan(0);
    expect(ledger.order.lastIndexOf("put")).toBeLessThan(ledger.order.indexOf("buffer_intent"));
    expect(ledger.order.slice(-2)).toEqual(["stage_put", "buffer_receipt"]);
    const ref = (await ledger.registerBabysitterRetention(ledger.registrations[0]!)).objects[0]!.ref;
    expect((await listAll(store, "96bulls/org-a/v1")).every((k) => k.startsWith(bucketPrefix))).toBe(true);
    const decoded = await readBufferedStage(store, ledger.specs[0]!, zstd);
    expect(decoded.groups.map((g) => [g.source, g.sessionId])).toEqual([["claude", "sess-1"]]);
    const stub = decoded.groups[0]!.lines.map((l) => JSON.parse(l)).find((l) => l.kind === "turn_receipt");
    expect(stub.payload).toEqual({ retention_class: "standard_30d", expires_at_ms: EXPIRES, retention_bucket: ref });
    for (const f of SECRET_FIELDS) expect(JSON.stringify(decoded)).not.toContain(f);
    expect(ledger.specs[0]!.groupOrdinals.flat().sort()).toEqual([0, 1]);
  });

  test("a retry writes no second bucket chunk (idempotent by batch digest)", async () => {
    const { store, ledger, deps } = bufferedSetup();
    const b = await batch([receipt("bf-2")]);
    await deliverBatch(deps as never, TENANT, b);
    const before = (await listAll(store, bucketPrefix)).filter((k) => k.includes("/raw/")).length;
    ledger.batches.clear();
    await deliverBatch(deps as never, TENANT, b);
    expect((await listAll(store, bucketPrefix)).filter((k) => k.includes("/raw/")).length).toBe(before);
  });

  test("a registration failure writes nothing at all", async () => {
    const { store, ledger, deps } = bufferedSetup();
    ledger.registerBabysitterRetention = async () => { throw new Error("down"); };
    await expect(deliverBatch(deps as never, TENANT, await batch([receipt("bf-3")]))).rejects.toBeInstanceOf(IngestUnavailable);
    expect(await listAll(store, "96bulls")).toEqual([]);
  });

  test("batches without Babysitter records are unchanged", async () => {
    const on = bufferedSetup(true), off = bufferedSetup(false);
    const b = await batch([record({ record_id: "plain-1" })]);
    await deliverBatch(on.deps as never, TENANT, b);
    await deliverBatch(off.deps as never, TENANT, b);
    expect(on.ledger.registrations).toEqual([]);
    expect(on.ledger.specs[0]!.groupOrdinals).toEqual(off.ledger.specs[0]!.groupOrdinals);
    const norm = async (x: ReturnType<typeof bufferedSetup>) =>
      (await readBufferedStage(x.store, x.ledger.specs[0]!, zstd)).groups.map((g) => ({ ...g, lines: g.lines.map((l) => { const o = JSON.parse(l); delete o.received_at; return o; }) }));
    expect(await norm(on)).toEqual(await norm(off));
  });
});

const machineOf = (ws: string, origin: string, user: string) => liveMachineId(ws, origin, user);
function claimed(over: Partial<ClaimedObject> = {}): ClaimedObject {
  const base = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1", originId: "origin-1" };
  return { leaseToken: crypto.randomUUID(), leaseUntil: new Date(Date.now() + 300_000).toISOString(), ...base, machineId: machineOf(base.workspaceId, base.originId, base.userId), ref: `_bx2026120614-${"a".repeat(16)}`, reason: "expired", ...over };
}

function fakeLedger(objects: ClaimedObject[], settings = { enabled: true, dryRun: false }, fenceOk = true) {
  const completions: { token: string; outcome: string; versions: number }[] = [];
  const ledger: BabysitterRetentionLedger = {
    claim: async () => ({ ...settings, objects }),
    fence: async () => fenceOk,
    complete: async (token, outcome, versions) => { completions.push({ token, outcome, versions }); return { ok: true, state: outcome === "erased" ? "erased" : "pending" }; },
  };
  return { ledger, completions };
}

function fakeEraser() {
  const calls: string[] = [];
  return {
    calls,
    eraser: {
      listVersions: async (prefix: string) => { calls.push(`list:${prefix}`); return [1, 2, 3].map((i) => ({ key: `${prefix}raw/b=${i}.ndjson.zst`, versionId: `v${i}` })); },
      removeVersions: async (list: readonly { key: string; versionId: string }[]) => { calls.push(`remove:${list.length}`); return list.length; },
    },
  };
}

describe("sweep tick", () => {
  test("off does nothing at all", async () => {
    let claims = 0;
    await babysitterRetentionTick({ ledger: { claim: async () => { claims++; return { enabled: true, dryRun: false, objects: [] }; }, fence: async () => true, complete: async () => ({ ok: true }) }, eraser: fakeEraser().eraser, keyRoot: "96bulls", mode: "off" });
    expect(claims).toBe(0);
  });

  test("on: erases only the bucket directory it derived, then records the erase", async () => {
    const o = claimed();
    const { ledger, completions } = fakeLedger([o]);
    const { eraser, calls } = fakeEraser();
    const r = await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode: "on" });
    // Exactly this bucket's own directory (its date from the ref's hour): never a machine-wide listing.
    expect(calls).toEqual([`list:96bulls/org-a/v1/ws=ws-a/m=${o.machineId}/h=_babysitter/d=2026/12/06/s=${o.ref}/`, "remove:3"]);
    expect(completions).toEqual([{ token: o.leaseToken, outcome: "erased", versions: 3 }]);
    expect(r).toMatchObject({ claimed: 1, erased: 1, erased_versions: 3, failed: 0 });
  });

  test("P1-c: never starts an erase with too little lease left (a registration could follow)", async () => {
    const o = claimed({ leaseUntil: new Date(Date.now() + MIN_LEASE_LEFT_MS - 1_000).toISOString() });
    const { ledger, completions } = fakeLedger([o]);
    const { eraser, calls } = fakeEraser();
    const r = await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode: "on" });
    expect(calls).toEqual([]);
    expect(completions).toEqual([{ token: o.leaseToken, outcome: "released", versions: 0 }]);
    expect(r.codes.lease_short).toBe(1);
  });

  test("P2: a slow eraser never outlives the lease: it is cut at the deadline, released, and re-claimed later", async () => {
    const o = claimed({ leaseUntil: new Date(Date.now() + 300).toISOString() });
    const { ledger, completions } = fakeLedger([o]);
    let resolve!: (n: number) => void;
    const slow = { listVersions: () => new Promise<never[]>(() => { resolve = () => {}; }), removeVersions: async () => 0 };
    const started = Date.now();
    const r = await babysitterRetentionTick({ ledger, eraser: slow, keyRoot: "96bulls", mode: "on", minLeaseLeftMs: 100 });
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(r).toMatchObject({ erased: 0, failed: 1, codes: { lease_deadline: 1 } });
    expect(completions).toEqual([{ token: o.leaseToken, outcome: "released", versions: 0 }]);
    resolve(0);
  });

  test("generation fence: a stale generation (fence refused) deletes nothing even with a valid lease", async () => {
    const { ledger, completions } = fakeLedger([claimed()], { enabled: true, dryRun: false }, false);
    const { eraser, calls } = fakeEraser();
    const r = await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode: "on" });
    expect(calls.some((c) => c.startsWith("remove"))).toBe(false);
    expect(completions).toEqual([]);
    expect(r).toMatchObject({ erased: 0, codes: { fence_refused: 1 } });
  });

  test("mixed objects: with live siblings beside the bucket (canonical chunk, staged batch, another bucket), only the claimed bucket is erased", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const { storeEraser } = await import("../../migration/neon-s3-erasure/eraser.ts");
    const o = claimed();
    const m = o.machineId;
    const other = `_bx2026120614-${"b".repeat(16)}`, later = `_bx2026120615-${"a".repeat(16)}`;
    const keys = {
      target: `96bulls/org-a/v1/ws=ws-a/m=${m}/h=_babysitter/d=2026/12/06/s=${o.ref}/raw/b=1.ndjson.zst`,
      sibling: `96bulls/org-a/v1/ws=ws-a/m=${m}/h=_babysitter/d=2026/12/06/s=${other}/raw/b=1.ndjson.zst`,
      laterHour: `96bulls/org-a/v1/ws=ws-a/m=${m}/h=_babysitter/d=2026/12/06/s=${later}/raw/b=1.ndjson.zst`,
      canonical: `96bulls/org-a/v1/ws=ws-a/m=${m}/h=claude/d=2026/12/06/s=sess-1/raw/0000000000-0-abcdef01.ndjson.zst`,
      manifest: `96bulls/org-a/v1/ws=ws-a/m=${m}/h=claude/d=2026/12/06/s=sess-1/manifest.json`,
      staged: `96bulls/org-a/live-batches/v1/ws=ws-a/m=${m}/b=${"a".repeat(32)}-${"b".repeat(16)}-${"c".repeat(32)}.staged.ndjson.zst`,
      otherMachine: `96bulls/org-a/v1/ws=ws-a/m=srv_${"9".repeat(32)}/h=_babysitter/d=2026/12/06/s=${o.ref}/raw/b=1.ndjson.zst`,
    };
    for (const k of Object.values(keys)) await s.store.put(objectKey(k), new Uint8Array([1]), { ifNoneMatch: true });
    const { ledger } = fakeLedger([o]);
    expect((await babysitterRetentionTick({ ledger, eraser: storeEraser(s.store), keyRoot: "96bulls", mode: "on" })).erased).toBe(1);
    const left = await listAll(s.store, "96bulls");
    expect(left).not.toContain(keys.target);
    for (const k of [keys.sibling, keys.laterHour, keys.canonical, keys.manifest, keys.staged, keys.otherMachine]) expect(left).toContain(k);
  });

  test("late writer: a version written after the snapshot is never deleted, however late the deletes run", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const { storeEraser } = await import("../../migration/neon-s3-erasure/eraser.ts");
    const real = storeEraser(s.store);
    const o = claimed();
    const prefix = `96bulls/org-a/v1/ws=ws-a/m=${o.machineId}/h=_babysitter/d=2026/12/06/s=${o.ref}/`;
    await s.store.put(objectKey(`${prefix}raw/b=old.ndjson.zst`), new Uint8Array([1]), { ifNoneMatch: true });
    const eraser = {
      listVersions: (p: string) => real.listVersions(p),
      removeVersions: async (list: readonly { key: string; versionId: string }[]) => {
        // A re-enabled session's new evidence lands between the snapshot and the deletes.
        await s.store.put(objectKey(`${prefix}raw/b=new.ndjson.zst`), new Uint8Array([2]), { ifNoneMatch: true });
        return real.removeVersions(list);
      },
    };
    const { ledger } = fakeLedger([o]);
    expect((await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode: "on" })).erased).toBe(1);
    expect(await listAll(s.store, prefix.slice(0, -1))).toEqual([`${prefix}raw/b=new.ndjson.zst`]);
  });

  test("dry run (flag or DB setting) never calls the eraser and releases every lease", async () => {
    for (const [mode, dbDry] of [["dry_run", false], ["on", true]] as const) {
      const { ledger, completions } = fakeLedger([claimed(), claimed()], { enabled: true, dryRun: dbDry });
      const { eraser, calls } = fakeEraser();
      expect((await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode })).dry_run).toBe(true);
      expect(calls).toEqual([]);
      expect(completions.every((c) => c.outcome === "released")).toBe(true);
    }
  });

  test("refuses refs and machines it did not derive, without erasing", async () => {
    const bad = [claimed({ ref: "../escape" }), claimed({ machineId: "srv_" + "0".repeat(32) }), claimed({ ref: "96bulls/org-a/live-batches/v1/x.staged.ndjson.zst" }), claimed({ orgId: "../org" })];
    const { ledger, completions } = fakeLedger(bad);
    const { eraser, calls } = fakeEraser();
    const r = await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode: "on" });
    expect(calls).toEqual([]);
    expect(r.refused).toBe(4);
    expect(completions.every((c) => c.outcome === "released")).toBe(true);
    expect(() => retentionTargets("96bulls", claimed({ ref: "_bx1-x" }))).toThrow();
  });

  test("an eraser failure releases the lease and counts a code; the next bucket still runs", async () => {
    const { ledger, completions } = fakeLedger([claimed(), claimed()]);
    let first = true;
    const eraser = {
      listVersions: async () => { if (first) { first = false; throw Object.assign(new Error("s3 quoted key"), { code: "AccessDenied" }); } return [{ key: "k", versionId: "v" }]; },
      removeVersions: async (l: readonly unknown[]) => l.length,
    };
    const r = await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode: "on" });
    expect(r).toMatchObject({ failed: 1, erased: 1, codes: { AccessDenied: 1 } });
    expect(completions.map((c) => c.outcome)).toEqual(["released", "erased"]);
  });

  test("past the budget, remaining leases are released, not erased", async () => {
    let t = Date.now();
    const { ledger, completions } = fakeLedger([claimed(), claimed()]);
    const { eraser, calls } = fakeEraser();
    const r = await babysitterRetentionTick({ ledger, eraser, keyRoot: "96bulls", mode: "on", budgetMs: 10, clock: () => (t += 20) });
    expect(calls).toEqual([]);
    expect(r.codes.budget).toBe(2);
    expect(completions.every((c) => c.outcome === "released")).toBe(true);
  });

  test("a raced erase is not counted as erased", async () => {
    const ledger: BabysitterRetentionLedger = { claim: async () => ({ enabled: true, dryRun: false, objects: [claimed()] }), fence: async () => true, complete: async () => ({ ok: true, state: "pending" }) };
    expect(await babysitterRetentionTick({ ledger, eraser: fakeEraser().eraser, keyRoot: "96bulls", mode: "on" })).toMatchObject({ erased: 0, raced: 1 });
  });

  test("the result holds counts and codes only", async () => {
    const { ledger } = fakeLedger([claimed()]);
    const text = JSON.stringify(await babysitterRetentionTick({ ledger, eraser: fakeEraser().eraser, keyRoot: "96bulls", mode: "on" }));
    for (const s of ["org-a", "_bx", "srv_"]) expect(text).not.toContain(s);
  });
});

describe("E3 erasure phase (no new process, Lambda or S3 key)", () => {
  const env = (m: Record<string, string>) => (k: string) => m[k];
  const gen = "a".repeat(64);
  test("defaults off and needs nothing; without --execute it is OFF (no connection, no write); anything else refuses", () => {
    expect(phaseSettings(env({}), true, "96bulls").mode).toBe("off");
    expect(phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "on", INGEST_EXPECT_GENERATION: gen }), false, "96bulls").mode).toBe("off");
    expect(phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "dry_run" }), false, "96bulls").mode).toBe("off");
    expect(phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "dry_run", INGEST_EXPECT_GENERATION: gen }), true, "96bulls").mode).toBe("dry_run");
    expect(phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "on", INGEST_EXPECT_GENERATION: gen }), true, "96bulls").mode).toBe("on");
    expect(() => phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "on" }), true, "96bulls")).toThrow("INGEST_EXPECT_GENERATION");
    expect(() => phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "true", INGEST_EXPECT_GENERATION: gen }), true, "96bulls")).toThrow();
    expect(() => phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "on", INGEST_EXPECT_GENERATION: gen, BABYSITTER_RETENTION_INTERVAL_MS: "1000" }), true, "96bulls")).toThrow();
  });
  test("P1: registry login is the least-privilege retention role only; the full ingest login is refused", () => {
    const pooler = "postgresql://bulls96_babysitter_retention.proj:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres";
    expect(poolConfig(pooler, false).ssl).toMatchObject({ rejectUnauthorized: true, servername: "aws-0-us-east-1.pooler.supabase.com" });
    expect(() => poolConfig(undefined, false)).toThrow("stdin");
    expect(() => poolConfig("postgresql://bulls96_session_ingest.proj:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres", false)).toThrow("cross-tenant");
    expect(() => poolConfig("postgresql://bulls96_session_ingest@127.0.0.1:5432/postgres", true)).toThrow("cross-tenant");
    expect(() => poolConfig("postgresql://postgres.proj:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres", false)).toThrow("retention role");
    expect(() => poolConfig("postgresql://bulls96_babysitter_retentionx.proj:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres", false)).toThrow("retention role");
    expect(() => poolConfig("postgresql://bulls96_babysitter_retention:pw@db.example.com:5432/postgres", false)).toThrow("pooler");
    expect(() => poolConfig("postgresql://bulls96_babysitter_retention@127.0.0.1:5432/postgres", false)).toThrow("pooler");
    expect(poolConfig("postgresql://bulls96_babysitter_retention@127.0.0.1:5432/postgres", true).ssl).toBeUndefined();
  });
  test("fixed call allowlist: the phase sends only its four statements; anything else is refused unsent", async () => {
    const sent: string[] = [];
    const pool = { end: async () => {}, query: async (sql: string) => {
      sent.push(sql);
      if (sql.startsWith("SELECT current_user")) return { rows: [{ c: "bulls96_babysitter_retention" }] };
      if (sql.includes("_claim(")) return { rows: [{ result: { enabled: true, dryRun: false, objects: [claimed()] } }] };
      if (sql.includes("_fence(")) return { rows: [{ result: { ok: true } }] };
      return { rows: [{ result: { ok: true, state: "erased" } }] };
    } };
    const settings = phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "on", INGEST_EXPECT_GENERATION: gen }), true, "96bulls");
    const phase = await startBabysitterPhase(settings, pool as never, fakeEraser().eraser, () => {});
    expect((await phase.tick()).erased).toBe(1);
    expect(sent.every((t) => ALLOWED_SQL.has(t))).toBe(true);
    expect(new Set(sent)).toEqual(ALLOWED_SQL);
    const q = allowlistedQuery(pool as never);
    for (const bad of ["SELECT sessions.ingest_babysitter_retention_register($1::jsonb) AS result", "SELECT sessions.ingest_claim_batch($1::jsonb) AS result", "DELETE FROM sessions.babysitter_retention_objects"])
      await expect(q(bad)).rejects.toMatchObject({ code: "call_not_allowed" });
    expect(sent.some((t) => t.includes("register") || t.includes("claim_batch") || t.startsWith("DELETE"))).toBe(false);
  });

  test("refuses a pool connected as another role; the loop logs codes only and stops on the process signal", async () => {
    const wrong = { query: async () => ({ rows: [{ c: "bulls96_session_ingest" }] }), end: async () => {} };
    const settings = phaseSettings(env({ BABYSITTER_RETENTION_SWEEP: "dry_run", INGEST_EXPECT_GENERATION: gen }), true, "96bulls");
    await expect(startBabysitterPhase(settings, wrong as never, fakeEraser().eraser, () => {})).rejects.toThrow("wrong role");
    const logs: Record<string, unknown>[] = [];
    let calls = 0;
    const pool = { end: async () => {}, query: async (sql: string) => {
      calls++;
      if (sql.startsWith("SELECT current_user")) return { rows: [{ c: "bulls96_babysitter_retention" }] };
      throw Object.assign(new Error("password=hunter2 host=db"), { code: "57014" });
    } };
    let stop = false;
    const phase = await startBabysitterPhase(settings, pool as never, fakeEraser().eraser, (f) => logs.push(f), async () => { stop = true; });
    await phase.loop(() => stop);
    expect(calls).toBe(2);
    expect(logs).toEqual([{ message: "babysitter retention tick failed", error_code: "ledger_timeout" }]);
    expect(JSON.stringify(logs)).not.toContain("hunter2");
  });
});

describe("retention buckets are never indexed as sessions", () => {
  const key = `96bulls/org-a/v1/ws=ws-a/m=${machine}/h=_babysitter/d=2026/11/06/s=_bx2026120614-${"a".repeat(16)}/manifest.json`;
  test("the pipeline adapter ignores put and delete events under h=_babysitter", async () => {
    const { buildObjectLandedMessage, buildObjectDeletedMessage } = await import("@rhc/lib/pipeline/adapter.ts");
    expect(buildObjectLandedMessage({ landed: { key } as never, orgId: "org-a" })).toEqual({ status: "ignored", reason: "retention_bucket" });
    expect(buildObjectDeletedMessage({ landed: { key } as never, orgId: "org-a" })).toEqual({ status: "ignored", reason: "retention_bucket" });
  });
  test("the pointer indexer ignores them before any lookup", async () => {
    const { indexPointer } = await import("../../session-pipeline/pointer.ts");
    const deps = new Proxy({}, { get: () => { throw new Error("no dependency may be touched"); } });
    expect(await indexPointer({ key } as never, deps as never)).toEqual({ status: "ignored", reason: "retention_bucket" });
  });
});
