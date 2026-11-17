/**
 * SELFHOST_ADMISSION_SPREAD (selfhost-admission.ts "Spread"): replicas that share one inbox start on different
 * machines' work below the grace, take work older than the grace oldest-first exactly as with spread off, and with
 * spread off the order is unchanged. Plus the settings' validation and the promotion pass's spread choice.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { selfhostAdmissionSettings } from "../selfhost-config.ts";
import { admissionSpreadGroup, selectUploadsFairly, selectUploadsSpread, selfhostAdmissionConsumer, uploadSpreadGroup, type UploadCandidate } from "../selfhost-admission.ts";
import { enqueueAdmission } from "../async-admission.ts";
import { LedgerUnavailableError, type ClaimInput } from "../ledger.ts";
import { uploadInboxRoot } from "../upload-admission.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

describe("SELFHOST_ADMISSION_SPREAD settings", () => {
  const base = { SELFHOST_INGEST_ADMISSION: "async" };
  test("unset, empty and off: no spread field (the settings exactly as before)", () => {
    expect(selfhostAdmissionSettings(base)).toEqual({ mode: "async", concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 20 });
    for (const v of ["", "off"]) expect(selfhostAdmissionSettings({ ...base, SELFHOST_ADMISSION_SPREAD: v })).not.toHaveProperty("spread");
  });
  test("on: the grace defaults to max(120000, 4 x scan interval) and is bounded", () => {
    expect(selfhostAdmissionSettings({ ...base, SELFHOST_ADMISSION_SPREAD: "on" })).toMatchObject({ spread: { graceMs: 120_000 } });
    expect(selfhostAdmissionSettings({ ...base, SELFHOST_ADMISSION_SPREAD: "on", SELFHOST_ADMISSION_SCAN_MS: "60000" })).toMatchObject({ spread: { graceMs: 240_000 } });
    expect(selfhostAdmissionSettings({ ...base, SELFHOST_ADMISSION_SPREAD: "on", SELFHOST_ADMISSION_SPREAD_GRACE_MS: "45000" })).toMatchObject({ spread: { graceMs: 45_000 } });
    for (const bad of ["999", "86400001", "1000.5", "-1", "abc"])
      expect(() => selfhostAdmissionSettings({ ...base, SELFHOST_ADMISSION_SPREAD: "on", SELFHOST_ADMISSION_SPREAD_GRACE_MS: bad })).toThrow(/^96bulls: SELFHOST_ADMISSION_SPREAD_GRACE_MS must be an integer from 1000 to 86400000$/);
  });
  test("anything but on/off refuses; a grace without spread refuses; spread without async/drain refuses; errors never echo the value", () => {
    for (const bad of ["true", "1", "ON", " on"]) expect(() => selfhostAdmissionSettings({ ...base, SELFHOST_ADMISSION_SPREAD: bad })).toThrow(/^96bulls: SELFHOST_ADMISSION_SPREAD must be on or off$/);
    expect(() => selfhostAdmissionSettings({ ...base, SELFHOST_ADMISSION_SPREAD_GRACE_MS: "60000" })).toThrow(/^96bulls: SELFHOST_ADMISSION_SPREAD_GRACE_MS needs SELFHOST_ADMISSION_SPREAD=on$/);
    expect(() => selfhostAdmissionSettings({ SELFHOST_ADMISSION_SPREAD: "on" })).toThrow(/^96bulls: SELFHOST_ADMISSION_SPREAD needs SELFHOST_INGEST_ADMISSION=async or drain$/);
    expect(() => selfhostAdmissionSettings({ SELFHOST_INGEST_ADMISSION: "sync", SELFHOST_ADMISSION_SPREAD_GRACE_MS: "5000" })).toThrow(/SELFHOST_ADMISSION_SPREAD_GRACE_MS needs/);
    // Off with sync is still the plain sync default.
    expect(selfhostAdmissionSettings({ SELFHOST_ADMISSION_SPREAD: "off" })).toBeUndefined();
  });
});

const USERS = 12, PER_USER = 3;

/**
 * Two consumers over ONE store, each with a ledger whose every claim is a retry: one settle() then starts every due
 * entry exactly once, in the consumer's own order, which is recorded as user ids (the order is the thing under test).
 */
async function shared(ageMs: number, spread: ((i: number) => { graceMs: number; salt: string }) | undefined) {
  const fs = scratchStore();
  cleanups.push(fs.done);
  const t0 = 10_000_000;
  const clock = t0 + ageMs;
  let n = 0;
  for (let b = 0; b < PER_USER; b++) {
    for (let u = 0; u < USERS; u++) {
      const tenant = { ...TENANT, userId: `user-${u}` };
      const prepared = await prepareDelivery(tenant, await batch([record({ record_id: `r-${u}-${b}` })], { account_id: await deliveryAccount(tenant), batch_id: `b-${u}-${b}` }));
      // Interleaved receive times across users, so oldest-first alternates users.
      await enqueueAdmission({ store: fs.store, keyRoot: "96bulls", enqueue: async () => {} }, tenant, prepared, t0 + (n++) * 10, 0);
    }
  }
  const run = async (i: number) => {
    const ledger = new MemoryLedger();
    const order: string[] = [];
    ledger.claim = (async (input: ClaimInput) => { order.push(input.tenant.userId); throw new LedgerUnavailableError("ledger_test_retry"); }) as typeof ledger.claim;
    const quiet = () => {};
    const consumer = selfhostAdmissionConsumer({
      mode: "on", concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 20, nowMs: () => clock, log: { info: quiet, warn: quiet, error: quiet },
      deliver: { ledger, acceptanceMode: "canonical", target: { store: fs.store, keyRoot: "96bulls" } },
      ...(spread ? { spread: spread(i) } : {}),
    });
    await consumer.scan();
    await consumer.settle();
    await consumer.stop();
    return order;
  };
  return [await run(0), await run(1)] as const;
}

const firstUsers = (order: string[], k: number) => new Set(order.slice(0, k));
const grouped = (order: string[]) => {
  // Every user's entries are contiguous: the number of user changes is USERS - 1.
  let changes = 0;
  for (let i = 1; i < order.length; i++) if (order[i] !== order[i - 1]) changes++;
  return changes === USERS - 1;
};

describe("spread over a shared inbox", () => {
  test("off: both consumers start in the same strict oldest-first order (unchanged)", async () => {
    const [a, b] = await shared(1_000, undefined);
    expect(a).toHaveLength(USERS * PER_USER);
    expect(a).toEqual(b);
    expect(a.slice(0, USERS)).toEqual(Array.from({ length: USERS }, (_, u) => `user-${u}`));
  });

  test("on, young entries: different salts start on different users, each user's entries kept together", async () => {
    const [a, b] = await shared(1_000, (i) => ({ graceMs: 120_000, salt: `salt-${i}` }));
    expect(a).toHaveLength(USERS * PER_USER);
    expect(b).toHaveLength(USERS * PER_USER);
    expect(new Set(a)).toEqual(new Set(b));
    expect(grouped(a)).toBe(true);
    expect(grouped(b)).toBe(true);
    // The first user each picks differs, and their first quarter of users barely overlap.
    expect(a[0]).not.toBe(b[0]);
    const qa = firstUsers(a, PER_USER * 3), qb = firstUsers(b, PER_USER * 3);
    expect([...qa].filter((u) => qb.has(u)).length).toBeLessThanOrEqual(1);
  });

  test("on, entries past the grace (owner dead, replicas disagreeing, hot backlog): every consumer goes oldest-first, as off", async () => {
    const [off] = await shared(600_000, undefined);
    const [a, b] = await shared(600_000, (i) => ({ graceMs: 120_000, salt: `salt-${i}` }));
    expect(a).toEqual(off);
    expect(b).toEqual(off);
  });

  test("on, mixed ages: the old tier goes first oldest-first, then the young tier by salt", async () => {
    // Grace sits between the oldest and newest receive time: the first entries (by receive time) are old.
    const span = (USERS * PER_USER - 1) * 10;
    const [a, b] = await shared(1_000 + span, (i) => ({ graceMs: 1_000 + span / 2, salt: `salt-${i}` }));
    const [off] = await shared(1_000 + span, undefined);
    const oldCount = off.findIndex((_, idx) => idx * 10 > span / 2);
    expect(a.slice(0, oldCount)).toEqual(off.slice(0, oldCount));
    expect(b.slice(0, oldCount)).toEqual(off.slice(0, oldCount));
    expect(a.slice(oldCount)).not.toEqual(b.slice(oldCount));
  });

  test("the admission group is the machine directory (everything before /r=)", () => {
    expect(admissionSpreadGroup("96bulls/.admission-inbox/org/v1/ws=w/m=srv_x/r=ab/a=1.ndjson.zst")).toBe("96bulls/.admission-inbox/org/v1/ws=w/m=srv_x");
    expect(admissionSpreadGroup("no-receipt")).toBe("no-receipt");
  });
});

describe("selectUploadsSpread", () => {
  const ROOT = "96bulls";
  const key = (user: string, origin: string, i: number) => `${uploadInboxRoot(ROOT)}org/ws=w/u=${user}/o=${origin}/b=${String(i).padStart(32, "0")}/d=${"a".repeat(32)}-w0.json.zst`;
  const cand = (user: string, origin: string, i: number, at: number): UploadCandidate => ({ key: key(user, origin, i), lastModifiedMs: at, origin });
  const now = 1_000_000;
  const young = Array.from({ length: 10 }, (_, o) => Array.from({ length: 5 }, (_, i) => cand(`u${o}`, `o${o}`, i, now - 1_000 + o * 10 + i))).flat();

  test("young candidates: whole origins in salted order, oldest first in each; two salts choose mostly disjoint batches", () => {
    const a = selectUploadsSpread(young, 10, "s-a", 60_000, now);
    const b = selectUploadsSpread(young, 10, "s-b", 60_000, now);
    expect(a).toHaveLength(10);
    const groupsA = new Set(a.map(uploadSpreadGroup)), groupsB = new Set(b.map(uploadSpreadGroup));
    expect(groupsA.size).toBe(2);
    expect([...groupsA].filter((g) => groupsB.has(g)).length).toBeLessThanOrEqual(1);
    // Within a group, oldest first.
    const first = a.filter((k) => uploadSpreadGroup(k) === uploadSpreadGroup(a[0]!));
    expect(first).toEqual([...first].sort());
  });

  test("candidates past the grace go first, chosen exactly as selectUploadsFairly", () => {
    const old = [cand("x", "ox", 0, now - 500_000), cand("y", "oy", 0, now - 400_000), cand("x", "ox", 1, now - 300_000)];
    const out = selectUploadsSpread([...young, ...old], 4, "s-a", 60_000, now);
    expect(out.slice(0, 3)).toEqual(selectUploadsFairly(old, 3));
    expect(selectUploadsSpread([...young, ...old], 2, "s-b", 60_000, now)).toEqual(selectUploadsFairly(old, 2));
    // All past the grace: identical to the fair choice for any salt.
    expect(selectUploadsSpread(young, 17, "s-a", 0, now)).toEqual(selectUploadsFairly(young, 17));
  });

  test("the upload group ends at the o= segment", () => {
    expect(uploadSpreadGroup(key("u1", "o1", 3))).toBe(`${uploadInboxRoot(ROOT)}org/ws=w/u=u1/o=o1/`);
  });
});
