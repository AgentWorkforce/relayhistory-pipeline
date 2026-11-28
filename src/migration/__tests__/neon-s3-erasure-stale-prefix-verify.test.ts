/**
 * The READ-ONLY stale-prefix verifier (stale-prefix-verify.ts, ErasureJob#staleprefixVerify) and the receipt fence on
 * `--stale-prefixes-only --execute` (ErasureJob#receiptItem): record multisets over every old version, survivor
 * pins, the receipt, drift refusals, per-delete survivor checks with partial accounting, the survivor-prefix lease
 * and the protection preflight (lifecycle, versioning, attestation). Real SyncState on a temp file (S3X_OBLIGATIONS
 * off) and the offline version provider with bodies. Synthetic identifiers only.
 */
import { afterAll as __modeAfterAll, beforeEach as __modeBeforeEach } from "bun:test";
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob, STALE_RUN_LEASE, staleprefixIdentityHash, type StalePrefixPassOptions } from "../neon-s3-erasure/job.ts";
import { boundOperatorFile, exitStatusForError, staleprefixOptions, stalePassStatus } from "../neon-s3-erasure/main.ts";
import {
  attestationProblem, chunkRecords, compareMultisets, evaluateLifecycle, parseReceipt, type AttestedActor, type ProtectionAttestation, type VerificationReceipt,
} from "../neon-s3-erasure/stale-prefix-verify.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { ShortLivedCredentialError } from "../short-lived-credentials.ts";
import { signRequest } from "../../../packages/object-store/src/sigv4.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";
import { approvedFor, attestation, BUCKET_SHA, buildCopy, line, seedBodies, TARGET } from "./stale-prefix-fixture.ts";

const OBLIGATIONS_MODE = "off";
const __modeBefore = process.env.S3X_OBLIGATIONS;
process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE;
__modeBeforeEach(() => void (process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE));
__modeAfterAll(() => { if (__modeBefore === undefined) delete process.env.S3X_OBLIGATIONS; else process.env.S3X_OBLIGATIONS = __modeBefore; });

const ORG = "org-1";
const OLD = Date.parse("2026-11-01T00:00:00Z");
const NOW = Date.parse("2026-11-07T00:00:00Z");
const pfx = (d: string, s = "sess-1") => `96bulls/${ORG}/v1/ws=ws-1/m=m-1/h=claude/d=${d}/s=${s}/`;
const sha = (v: string) => createHash("sha256").update(v).digest("hex");
const ID = (s = "sess-1") => staleprefixIdentityHash("96bulls", ORG, pfx("2026/11/01", s))!;
const NO_ID = "0".repeat(64);
type Logged = Record<string, unknown>;

function authority(): ErasureAuthority {
  const forbidden = async () => { throw new Error("not in this mode"); };
  return {
    kind: "neon", ready: async () => {}, beginSweep: () => {},
    lockWaiters: forbidden as ErasureAuthority["lockWaiters"], fencedGroups: forbidden as ErasureAuthority["fencedGroups"],
    fencedLivePage: forbidden as ErasureAuthority["fencedLivePage"], stillFenced: forbidden as ErasureAuthority["stillFenced"],
    presence: new Proxy({}, { get: () => forbidden }) as ErasureAuthority["presence"],
  };
}

async function fixture() {
  const path = join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-verify-")), "s.sqlite");
  const st = new SyncState(path, { obligations: false });
  const s = new FakeS3Versions();
  const logs: Logged[] = [];
  const clock = { now: NOW };
  const job = () => new ErasureJob(authority(), st, s.eraser(), (f) => void logs.push(f as Logged), DEFAULT_ERASURE_CONFIG, () => clock.now, async () => {});
  /** A date move: old copy at d=11/01 (queued stale), survivor at d=11/02 (exported). Chunks are ndjson line lists. */
  const dateMove = (old: string[][] = [[line("r1"), line("r2")], [line("r3")]], survivor: string[][] = old, session = "sess-1", at = NOW - 60_000) => {
    const oldP = pfx("2026/11/01", session), newP = pfx("2026/11/02", session);
    const oldCopy = seedBodies(s, oldP, OLD, old);
    const newCopy = seedBodies(s, newP, OLD + 1000, survivor);
    st.markExported(ORG, `g-${session}`, 1, [newCopy.manifestKey], at);
    st.enqueueErasure([{ key: oldP, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], at);
    return { oldP, newP, oldCopy, newCopy };
  };
  const verify = (ids: string[] = [ID()]) => job().staleprefixVerify({ onlyIdentities: ids, reader: s.reader(), bucketSha256: BUCKET_SHA, target: TARGET });
  /** The execute exactly as the owner entry approves it (allowlist digest and planned count from the receipt). */
  const execute = (receipt: VerificationReceipt, over: Partial<StalePrefixPassOptions> = {}) => {
    s.deletes = [];
    return job().staleprefixPass({
      execute: true, onlyIdentities: receipt.allowlist, probe: s.eraser(), receipt, fence: s.eraser(), reader: s.reader(), target: TARGET,
      bucketSha256: BUCKET_SHA, attestation: attestation(clock.now), ...approvedFor(receipt), ...over,
    });
  };
  const row = (key: string) => st.db.prepare("SELECT * FROM erasures WHERE key = ?").get(key) as Record<string, unknown> | undefined;
  const leases = () => st.db.prepare("SELECT prefix, holder FROM leases").all() as { prefix: string; holder: string }[];
  return { path, st, s, logs, clock, job, dateMove, verify, execute, row, leases };
}
const item = (r: VerificationReceipt, i = 0, j = 0) => r.identities[i]!.items[j]!;
const versionIds = (r: VerificationReceipt) => item(r).old_versions.map((v) => `${v.key_sha256}@${v.version_id}`).sort();
const deletedIds = (deletes: string[]) => deletes.map((d) => { const at = d.lastIndexOf("@"); return `${sha(d.slice(0, at))}@${d.slice(at + 1)}`; }).sort();

describe("verifier: record multisets", () => {
  test("PASS when the survivor is a byte-identical superset; every old version listed, survivor pinned at its current versions", async () => {
    const f = await fixture();
    const m = f.dateMove([[line("r1"), line("r2")], [line("r3")]], [[line("r1"), line("r2")], [line("r3"), line("r4")]]);
    const r = await f.verify();
    expect(r.status).toBe("PASS");
    const it = item(r);
    expect(it).toMatchObject({ status: "PASS", reasons: {}, counts: { versions: 3, delete_markers: 0, manifest_versions: 1, chunk_versions: 2, other_versions: 0, records_old: 3, records_survivor: 4 } });
    expect(it.old_prefix_sha256).toBe(sha(m.oldP));
    expect(it.survivor!.manifest).toEqual({ key_sha256: sha(m.newCopy.manifestKey), version_id: f.s.keys.get(m.newCopy.manifestKey)![0]!.id });
    expect(it.survivor!.chunks.map((c) => c.key_sha256).sort()).toEqual(m.newCopy.chunks.map((c) => sha(c.key)).sort());
    // Every chunk version was fetched BY VERSION ID.
    for (const c of m.oldCopy.chunks) expect(f.s.gets).toContain(`${c.key}@${f.s.keys.get(c.key)![0]!.id}`);
    // Receipt and logs: hashes and counts only.
    const text = JSON.stringify([r, f.logs]);
    for (const leak of [ORG, "sess-1", "ws-1", "96bulls/", "r1", "text of"]) expect(text).not.toContain(leak);
  });

  test("FAIL missing_identity: a record the survivor lacks (never assumed intentionally deleted)", async () => {
    const f = await fixture();
    f.dateMove([[line("r1"), line("r2")], [line("r5")]], [[line("r1"), line("r2")]]);
    const it = item(await f.verify());
    expect(it).toMatchObject({ status: "FAIL", reasons: { missing_identity: 1 } });
  });

  test("FAIL digest_mismatch: the same identity with different line bytes (a truncated string)", async () => {
    const f = await fixture();
    f.dateMove([[line("r1", { text: "a full sentence" })]], [[line("r1", { text: "a full sent" })]]);
    expect(item(await f.verify()).reasons).toEqual({ digest_mismatch: 1 });
  });

  test("FAIL multiplicity_short: two old copies of one record, one survivor copy", async () => {
    const f = await fixture();
    f.dateMove([[line("r1"), line("r2")], [line("r1")]], [[line("r1"), line("r2")]]);
    expect(item(await f.verify()).reasons).toEqual({ multiplicity_short: 1 });
  });

  test("noncurrent old versions and delete markers: a noncurrent chunk version is compared (its record missing -> FAIL); the marker is listed, never fetched", async () => {
    const f = await fixture();
    const m = f.dateMove();
    // A chunk the exporter later dropped: one version with record r9, now covered by a delete marker (noncurrent).
    const extra = buildCopy(m.oldP, [[line("r1"), line("r2")], [line("r3")], [line("r9")]]).chunks[2]!;
    f.s.put(extra.key, OLD, undefined, extra.body);
    const marker = f.s.mark(extra.key, OLD + 5);
    const it = item(await f.verify());
    expect(it).toMatchObject({ status: "FAIL", reasons: { missing_identity: 1 }, counts: { versions: 5, delete_markers: 1, chunk_versions: 3 } });
    expect(it.old_versions).toContainEqual({ key_sha256: sha(extra.key), version_id: marker, delete_marker: true });
    expect(f.s.gets.some((g) => g.endsWith(`@${marker}`))).toBe(false);
    // Control: the same noncurrent version whose record the survivor has passes.
    const g = await fixture();
    const n = g.dateMove(undefined, [[line("r1"), line("r2")], [line("r3"), line("r9")]]);
    const extra2 = buildCopy(n.oldP, [[line("r1"), line("r2")], [line("r3")], [line("r9")]]).chunks[2]!;
    g.s.put(extra2.key, OLD, undefined, extra2.body);
    g.s.mark(extra2.key, OLD + 5);
    expect(item(await g.verify())).toMatchObject({ status: "PASS", counts: { delete_markers: 1, chunk_versions: 3, records_old: 4 } });
  });

  test("every old manifest version is checked: a referenced chunk that is not among the compared versions is manifest_ref_unverified", async () => {
    const f = await fixture();
    const m = f.dateMove();
    // An older manifest version referencing a third chunk whose versions are gone from the old prefix.
    const older = buildCopy(m.oldP, [[line("r1"), line("r2")], [line("r3")], [line("r4")]]);
    f.s.keys.set(m.oldCopy.manifestKey, [...f.s.keys.get(m.oldCopy.manifestKey)!, { id: "v-old-manifest", at: OLD - 10, body: older.manifest }]);
    expect(item(await f.verify())).toMatchObject({ status: "FAIL", reasons: { manifest_ref_unverified: 1 }, counts: { manifest_versions: 2 } });
  });

  test("fail closed: an undecodable chunk version is unreadable_version; an object that is neither manifest nor raw chunk is unverified_object", async () => {
    const f = await fixture();
    const m = f.dateMove();
    f.s.put(m.oldCopy.chunks[0]!.key, OLD + 2, undefined, new TextEncoder().encode("not zstd"));
    f.s.put(`${m.oldP}normalized/v1/events.json`, OLD, undefined, new TextEncoder().encode("{}"));
    expect(item(await f.verify()).reasons).toEqual({ unreadable_version: 1, unverified_object: 1 });
  });

  test("survivor: none current (delete marker) -> survivor_missing; a referenced survivor chunk missing -> unreadable_version", async () => {
    const f = await fixture();
    const m = f.dateMove();
    f.s.mark(m.newCopy.manifestKey, NOW - 10);
    expect(item(await f.verify())).toMatchObject({ status: "FAIL", reasons: { survivor_missing: 1 }, survivor: null });
    const g = await fixture();
    const n = g.dateMove();
    g.s.keys.delete(n.newCopy.chunks[1]!.key);
    expect(item(await g.verify()).reasons).toMatchObject({ unreadable_version: 1, missing_identity: 1 });
  });

  test("the receipt reports against the ORIGINAL allowlist and is deterministic; parseReceipt recomputes every digest", async () => {
    const f = await fixture();
    f.dateMove();
    const ids = [NO_ID, ID()];
    const a = await f.verify(ids), b = await f.verify(ids);
    expect(JSON.stringify(a)).toBe(JSON.stringify(b));
    expect(a.allowlist).toEqual(ids);
    expect(a.identities.map((i) => [i.identity_hash, i.status, i.reasons])).toEqual([[NO_ID, "FAIL", { no_stale_prefix_entry: 1 }], [ID(), "PASS", {}]]);
    expect(a.status).toBe("FAIL");
    expect(item(a, 1).old_versions).toEqual([...item(a, 1).old_versions].sort((x, y) => (x.key_sha256 < y.key_sha256 ? -1 : 1)));
    expect(parseReceipt(JSON.stringify(a))).toEqual(a);
    // Any edit is refused: the overall digest, a version set, a pin, or a FAIL turned PASS.
    const edits: ((r: VerificationReceipt) => void)[] = [
      (r) => { r.allowlist.pop(); },
      (r) => { item(r, 1).old_versions[0]!.version_id = "v-other"; },
      (r) => { item(r, 1).survivor!.chunks[0]!.version_id = "v-other"; },
      (r) => { r.identities[0]!.status = "PASS"; },
    ];
    for (const edit of edits) {
      const r = JSON.parse(JSON.stringify(a)) as VerificationReceipt;
      edit(r);
      expect(() => parseReceipt(JSON.stringify(r))).toThrow();
    }
  });

  test("the verifier is read-only: no lease, no state write, no delete", async () => {
    const f = await fixture();
    f.dateMove();
    const before = JSON.stringify([f.leases(), f.st.staleprefixErasures()]);
    await f.verify();
    expect(f.s.deletes).toEqual([]);
    expect(JSON.stringify([f.leases(), f.st.staleprefixErasures()])).toBe(before);
  });

  test("units: chunk records need line-terminated JSON with identity fields; compare counts records", () => {
    const enc = new TextEncoder();
    expect(chunkRecords(enc.encode(`${line("r1")}\n${line("r2")}\n`))).toHaveLength(2);
    for (const bad of [`${line("r1")}`, `${line("r1")}\n\n`, `{"kind":"x"}\n`, `[1]\n`, `not json\n`])
      expect(() => chunkRecords(enc.encode(bad))).toThrow();
    const ms = (pairs: [string, string, number][]) => { const m = new Map<string, Map<string, number>>(); for (const [i, d, n] of pairs) (m.get(i) ?? m.set(i, new Map()).get(i)!).set(d, n); return m; };
    expect(compareMultisets(ms([["a", "1", 2], ["b", "2", 1], ["c", "3", 1]]), ms([["a", "1", 1], ["b", "9", 1]]))).toEqual({ multiplicity_short: 1, digest_mismatch: 1, missing_identity: 1 });
    expect(compareMultisets(ms([["a", "1", 1]]), ms([["a", "1", 3], ["z", "0", 1]]))).toEqual({});
  });
});

describe("receipt fence on execute", () => {
  test("PASS receipt: erases exactly the receipt's version set (manifest first), survivor untouched, item done, leases released", async () => {
    const f = await fixture();
    const m = f.dateMove();
    f.s.put(m.oldCopy.manifestKey, OLD + 5, undefined, m.oldCopy.manifest); // a noncurrent old manifest version too
    const receipt = await f.verify();
    const survivorBefore = JSON.stringify([...f.s.keys].filter(([k]) => k.startsWith(m.newP)));
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: true, erased: 1, versionsDeleted: 4 });
    expect(r.fenced).toHaveLength(1);
    expect(r.fenced![0]).toMatchObject({ outcome: "completed", deleted: 4 });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "deleted")).toBe(true);
    expect(deletedIds(f.s.deletes)).toEqual(versionIds(receipt));
    expect(f.s.deletes.slice(0, 2).every((d) => d.startsWith(m.oldCopy.manifestKey))).toBe(true);
    expect(f.s.count(m.oldP)).toBe(0);
    expect(JSON.stringify([...f.s.keys].filter(([k]) => k.startsWith(m.newP)))).toBe(survivorBefore);
    expect(f.row(m.oldP)).toBeUndefined();
    expect(f.leases()).toEqual([]);
    expect(JSON.stringify(f.logs)).not.toContain("sess-1");
  });

  test("drift in the old set (a version added after verification): stale_prefix_receipt_drift, refused before delete, nothing deleted, row untouched", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    f.s.put(m.oldCopy.chunks[0]!.key, NOW - 5, undefined, m.oldCopy.chunks[0]!.body);
    const row = JSON.stringify(f.row(m.oldP));
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, erased: 0, versionsDeleted: 0, refused: "stale_prefix_receipt_drift" });
    expect(r.fenced![0]).toMatchObject({ outcome: "refused_before_delete", reason: "stale_prefix_receipt_drift", deleted: 0 });
    expect(f.s.deletes).toEqual([]);
    expect(JSON.stringify(f.row(m.oldP))).toBe(row);
    expect(f.leases()).toEqual([]);
    expect(() => stalePassStatus(r, false)).toThrow();
    // A version gone since verification is drift too.
    const g = await fixture();
    const n = g.dateMove();
    const rec = await g.verify();
    g.s.remove(n.oldCopy.chunks[1]!.key, g.s.keys.get(n.oldCopy.chunks[1]!.key)![0]!.id);
    expect((await g.execute(rec)).fenced![0]).toMatchObject({ outcome: "refused_before_delete", reason: "stale_prefix_receipt_drift" });
    expect(g.s.deletes).toEqual([]);
  });

  test("survivor changed after verification (a new current manifest or chunk version, or a delete marker): stale_prefix_survivor_drift, nothing deleted", async () => {
    for (const change of ["manifest", "chunk", "marker"] as const) {
      const f = await fixture();
      const m = f.dateMove();
      const receipt = await f.verify();
      if (change === "manifest") f.s.put(m.newCopy.manifestKey, NOW - 5, undefined, m.newCopy.manifest);
      else if (change === "chunk") f.s.put(m.newCopy.chunks[0]!.key, NOW - 5, undefined, m.newCopy.chunks[0]!.body);
      else f.s.mark(m.newCopy.chunks[1]!.key, NOW - 5);
      const r = await f.execute(receipt);
      expect(r.fenced![0]).toMatchObject({ outcome: "refused_before_delete", reason: "stale_prefix_survivor_drift", deleted: 0 });
      expect(f.s.deletes).toEqual([]);
      expect(f.s.count(m.oldP)).toBe(3);
      expect(f.row(m.oldP)).toMatchObject({ attempts: 0 });
    }
  });

  test("whole-receipt gate: [PASS, FAIL] and [FAIL, PASS] receipts refuse before any delete (zero deleteVersion calls, non-zero exit, no state write)", async () => {
    for (const order of [["sess-1", "sess-2"], ["sess-2", "sess-1"]]) {
      const f = await fixture();
      const bad = f.dateMove([[line("r1")], [line("r7")]], [[line("r1")]], "sess-1", NOW - 60_000);
      const good = f.dateMove(undefined, undefined, "sess-2", NOW - 50_000);
      const receipt = await f.verify(order.map((x) => ID(x)));
      expect(receipt.identities.map((i) => i.status)).toEqual(order.map((x) => (x === "sess-1" ? "FAIL" : "PASS")));
      const fence = f.s.eraser();
      let calls = 0;
      const del = fence.deleteVersion;
      fence.deleteVersion = (k, v) => (calls++, del(k, v));
      const rows = JSON.stringify([f.row(bad.oldP), f.row(good.oldP), f.leases()]);
      let err: unknown;
      try { await f.execute(receipt, { fence }); } catch (e) { err = e; }
      expect((err as { code?: string }).code).toBe("stale_prefix_receipt_not_all_pass");
      expect(exitStatusForError(err)).toBe(1);
      expect([calls, f.s.deletes.length]).toEqual([0, 0]);
      expect(JSON.stringify([f.row(bad.oldP), f.row(good.oldP), f.leases()])).toBe(rows);
    }
  });

  test("whole-receipt gate: an allowlist that is a strict subset or superset of the receipt's identities refuses; so does a missing approval", async () => {
    const f = await fixture();
    f.dateMove(undefined, undefined, "sess-1", NOW - 60_000);
    f.dateMove(undefined, undefined, "sess-2", NOW - 50_000);
    const both = await f.verify([ID("sess-1"), ID("sess-2")]);
    const one = await f.verify([ID("sess-1")]);
    await expect(f.execute(both, { onlyIdentities: [ID("sess-1")] })).rejects.toMatchObject({ code: "stale_prefix_receipt_not_all_pass" });
    await expect(f.execute(one, { onlyIdentities: [ID("sess-1"), ID("sess-2")] })).rejects.toMatchObject({ code: "stale_prefix_receipt_not_all_pass" });
    await expect(f.execute(both, { approvedAllowlistSha256: NO_ID })).rejects.toMatchObject({ code: "stale_prefix_approved_allowlist_mismatch" });
    await expect(f.execute(both, { approvedMax: undefined })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_approval" });
    await expect(f.execute(both, { max: 1, approvedMax: 1 })).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    await expect(f.execute(both, { approvedMax: 3 })).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    expect(f.s.deletes).toEqual([]);
  });

  test("batch inventory: an identity with TWO old prefixes is planned as two items; a max covering one refuses with zero deletes; the exact count erases both", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const older = pfx("2026/10/31");
    seedBodies(f.s, older, OLD - 1000);
    f.st.enqueueErasure([{ key: older, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 30_000);
    const receipt = await f.verify();
    expect(receipt.identities[0]!.items.map((i) => i.old_prefix_sha256)).toEqual([sha(older), sha(m.oldP)]);
    await expect(f.execute(receipt, { max: 1, approvedMax: 1 })).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    expect(f.s.deletes).toEqual([]);
    // SYNTHETIC: the multi-prefix mode under its own (test-supplied) approval.
    const r = await f.execute(receipt, { approvedPrefixCount: 2 });
    expect(r).toMatchObject({ ok: true, erased: 2, versionsDeleted: 6 });
    expect(r.fenced!.map((x) => x.outcome)).toEqual(["completed", "completed"]);
  });

  test("batch inventory: a queued item the receipt does not name, a receipt item no longer queued, or a not-due item refuses the WHOLE batch with zero deletes", async () => {
    // A second prefix queued for the identity after verification.
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    const later = pfx("2026/10/30");
    seedBodies(f.s, later, OLD - 2000);
    f.st.enqueueErasure([{ key: later, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 10_000);
    expect(await f.execute(receipt)).toMatchObject({ ok: false, refused: "stale_prefix_receipt_missing", erased: 0 });
    expect(f.s.deletes).toEqual([]);
    // The receipt's item is gone from the queue.
    const g = await fixture();
    const n = g.dateMove();
    const rec = await g.verify();
    g.st.erasureDone(n.oldP, Number(g.row(n.oldP)!.enqueued_at));
    expect(await g.execute(rec)).toMatchObject({ ok: false, refused: "stale_prefix_receipt_missing" });
    expect(g.s.deletes).toEqual([]);
    // Backing off (not due).
    const h = await fixture();
    const o = h.dateMove(undefined, undefined, "sess-1", NOW - 60_000);
    h.dateMove(undefined, undefined, "sess-2", NOW - 50_000);
    const rh = await h.verify([ID("sess-1"), ID("sess-2")]);
    h.st.erasureFailed(pfx("2026/11/01", "sess-2"), "x", NOW);
    const res = await h.execute(rh);
    expect(res).toMatchObject({ ok: false, refused: "stale_prefix_batch_not_due", erased: 0 });
    expect(h.s.deletes).toEqual([]);
    expect(h.s.count(o.oldP)).toBe(3);
    expect(() => stalePassStatus(res, false)).toThrow();
  });

  test("a guard skip on a LATER item refuses the whole batch before delete 1 (the earlier item is untouched)", async () => {
    const f = await fixture();
    const a = f.dateMove(undefined, undefined, "sess-1", NOW - 60_000);
    const b = f.dateMove(undefined, undefined, "sess-2", NOW - 50_000);
    const receipt = await f.verify([ID("sess-1"), ID("sess-2")]);
    f.st.recordPendingKey(ORG, "g-other", `${b.oldP}raw/x.ndjson.zst`);
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_batch_guard_skip", erased: 0, skipped: { current_key: 1 } });
    expect(f.s.deletes).toEqual([]);
    expect(f.s.count(a.oldP)).toBe(3);
  });

  test("an entry the receipt does not name (re-queued after verification) is refused stale_prefix_receipt_missing", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    f.st.db.prepare("UPDATE erasures SET enqueued_at = enqueued_at + 1 WHERE key = ?").run(m.oldP);
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_receipt_missing" });
    expect(f.s.deletes).toEqual([]);
  });

  test("survivor protection lost after delete 1 (a survivor chunk superseded) and before the last: partial with exactly 1 deleted and the rest not_attempted, coded, non-zero exit", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let n = 0;
    f.s.onDelete = () => { if (++n === 1) f.s.put(m.newCopy.chunks[0]!.key, NOW - 1, undefined, m.newCopy.chunks[0]!.body); };
    const r = await f.execute(receipt);
    expect(f.s.deletes).toHaveLength(1);
    expect(r).toMatchObject({ ok: false, erased: 0, versionsDeleted: 1, stoppedBy: "failed" });
    const rep = r.fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_survivor_drift", deleted: 1 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
    expect(deletedIds(f.s.deletes)).toEqual([`${rep.versions[0]!.key_sha256}@${rep.versions[0]!.version_id}`]);
    expect(f.row(m.oldP)).toMatchObject({ attempts: 1, error: "stale_prefix_survivor_drift" });
    expect(f.leases()).toEqual([]);
    let err: unknown; try { stalePassStatus(r, false); } catch (e) { err = e; }
    expect((err as { code?: string }).code).toBe("stale_prefix_pass_stopped");
    expect(f.logs.find((l) => l.message === "stale prefix fence")).toMatchObject({ outcome: "partial", deleted: 1, not_attempted: 2, unknown: 0, absent_observed: 0, error_code: "stale_prefix_survivor_drift" });
  });

  test("survivor protection lost after delete 2 (a survivor chunk version removed) and before the last: partial with exactly 2 deleted", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let n = 0;
    f.s.onDelete = () => { if (++n === 2) { const k = m.newCopy.chunks[1]!.key; f.s.remove(k, f.s.keys.get(k)![0]!.id); } };
    const r = await f.execute(receipt);
    const rep = r.fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_survivor_drift", deleted: 2 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "deleted", "not_attempted"]);
    expect(f.s.deletes).toHaveLength(2);
    expect(() => stalePassStatus(r, false)).toThrow();
  });

  test("a version that appears in the old prefix after delete 1 (not in the receipt) is never deleted: partial, stale_prefix_receipt_drift, exact accounting", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let n = 0, intruder = "";
    f.s.onDelete = () => { if (++n === 1) intruder = f.s.put(m.oldCopy.chunks[1]!.key, NOW - 1, undefined, m.oldCopy.chunks[1]!.body); };
    const r = await f.execute(receipt);
    expect(f.s.deletes).toHaveLength(1);
    expect(f.s.deletes.some((d) => d.endsWith(`@${intruder}`))).toBe(false);
    expect(f.s.keys.get(m.oldCopy.chunks[1]!.key)!.some((v) => v.id === intruder)).toBe(true);
    const rep = r.fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_receipt_drift", deleted: 1 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
    expect(rep.versions.map((v) => `${v.key_sha256}@${v.version_id}`).sort()).toEqual(versionIds(receipt));
    expect(rep.versions.some((v) => v.version_id === intruder)).toBe(false);
    expect(r).toMatchObject({ ok: false, stoppedBy: "failed" });
    expect(f.row(m.oldP)).toMatchObject({ attempts: 1, error: "stale_prefix_receipt_drift" });
    expect(() => stalePassStatus(r, false)).toThrow();
  });

  test("ambiguous DELETE (applied, then a transport error): resolved by the exact relist as absent_observed, never not_attempted; partial, exact accounting", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let n = 0;
    f.s.onDelete = () => (++n === 2 ? "transport" : undefined);
    const r = await f.execute(receipt);
    const rep = r.fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_delete_ambiguous", deleted: 1 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "absent_observed", "not_attempted"]);
    expect(rep.versions[1]!.error_code).toBe("TypeError");
    expect(f.s.deletes).toHaveLength(2);
    expect(f.row(m.oldP)).toMatchObject({ attempts: 1, error: "stale_prefix_delete_ambiguous" });
    expect(r).toMatchObject({ ok: false, stoppedBy: "failed", versionsDeleted: 1 });
    expect(() => stalePassStatus(r, false)).toThrow();
  });

  test("ambiguous DELETE: a failed exact relist records unknown; a version still present records present_after_error; both stop the batch", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    let failing = false;
    f.s.rewrite = (_c, xml) => (failing ? "<ListVersionsResult><IsTruncated>" : xml);
    f.s.onDelete = () => { failing = true; return "transport"; };
    const rep = (await f.execute(receipt)).fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_delete_ambiguous", deleted: 0 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["unknown", "not_attempted", "not_attempted"]);
    const g = await fixture();
    g.dateMove();
    const rec = await g.verify();
    g.s.onDelete = () => "transport-before";
    const rg = (await g.execute(rec)).fenced![0]!;
    expect(rg).toMatchObject({ outcome: "partial", deleted: 0 });
    expect(rg.versions.map((v) => v.outcome)).toEqual(["present_after_error", "not_attempted", "not_attempted"]);
  });

  test("a competing lease on the SURVIVOR prefix (the exporter writing it): refused_before_delete, zero deletes", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    f.st.acquireLease(m.newP, "exporter:31337", undefined, NOW - 1000);
    const r = await f.execute(receipt);
    expect(r.fenced![0]).toMatchObject({ outcome: "refused_before_delete", reason: "stale_prefix_survivor_lease_busy", deleted: 0 });
    expect(f.s.deletes).toEqual([]);
    expect(f.leases()).toEqual([{ prefix: m.newP, holder: "exporter:31337" }]);
  });

  test("a competing eraser: another eraser's live lease (including its run lease) refuses the pass before any delete", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    f.st.acquireLease(STALE_RUN_LEASE, "eraser:424242", undefined, NOW - 1000);
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_mode_other_eraser_lease", erased: 0 });
    expect(f.s.deletes).toEqual([]);
  });

  test("the run lease is held for the interval (seen at every delete) and an eraser lease appearing mid-run stops it", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const held: boolean[] = [];
    let n = 0;
    f.s.onDelete = () => {
      held.push(f.leases().some((l) => l.prefix === STALE_RUN_LEASE && l.holder === `eraser:${process.pid}`));
      if (++n === 1) f.st.acquireLease(`${m.oldP.slice(0, -1)}-x/`, "eraser:999", undefined, NOW);
    };
    const rep = (await f.execute(receipt)).fenced![0]!;
    expect(held).toEqual([true]);
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_mode_other_eraser_lease", deleted: 1 });
  });

  test("execute without a verified receipt is refused at the job and at the CLI; a receipt for another bucket is refused", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    await expect(f.job().staleprefixPass({ execute: true, onlyIdentities: [ID()], probe: f.s.eraser() })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_verified_receipt" });
    await expect(f.job().staleprefixPass({ execute: true, onlyIdentities: [ID()], probe: f.s.eraser(), receipt })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_verified_receipt" });
    await expect(f.execute(receipt, { reader: undefined })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_verified_receipt" });
    await expect(f.execute(receipt, { bucketSha256: sha("other") })).rejects.toMatchObject({ code: "stale_prefix_receipt_invalid" });
    expect(f.s.deletes).toEqual([]);
    const code = (args: string[]) => { try { staleprefixOptions(args, () => undefined); return null; } catch (e) { return (e as { code?: string }).code; } };
    const mode = ["--stale-prefixes-only", "--only-identity", ID()];
    expect(code([...mode, "--execute"])).toBe("stale_prefix_execute_requires_verified_receipt");
    expect(code([...mode, "--execute", "--verified-receipt", "r.json"])).toBe("stale_prefix_execute_requires_attestation");
    const files = [...mode, "--execute", "--verified-receipt", "r.json", "--protection-attestation", "a.json"];
    expect(code(files)).toBe("stale_prefix_execute_requires_approval");
    const approved = [...files, "--receipt-sha256", NO_ID, "--attestation-sha256", NO_ID, "--approved-allowlist-sha256", NO_ID, "--approved-max", "1", "--approved-identity-count", "1"];
    expect(code(approved)).toBe("stale_prefix_execute_requires_approval"); // --max too
    expect(code([...approved, "--max", "1"])).toBeNull();
    expect(code([...approved.slice(0, -2), "--max", "1"])).toBe("stale_prefix_execute_requires_approval");
    expect(code([...approved.slice(0, -4), "--approved-identity-count", "1", "--max", "1"])).toBe("stale_prefix_execute_requires_approval");
    expect(code([...approved, "--max", "1", "--approved-prefix-count", "0"])).toBe("stale_prefix_max_invalid");
    expect(code([...files, "--receipt-sha256", "ABC"])).toBe("stale_prefix_sha256_invalid");
    expect(code([...mode, "--receipt-sha256", NO_ID])).toBe("stale_prefix_receipt_without_execute");
    expect(code([...mode, "--verified-receipt", "r.json"])).toBe("stale_prefix_receipt_without_execute");
    expect(code([...mode, "--execute", "--verified-receipt"])).toBe("stale_prefix_path_invalid");
    expect(code([...mode, "--verify-only", "--execute"])).toBe("stale_prefix_verify_only_flag_refused");
    expect(code([...mode, "--verify-only", "--max", "2"])).toBe("stale_prefix_verify_only_flag_refused");
    expect(code([...mode, "--receipt-out", "o.json"])).toBe("stale_prefix_receipt_out_without_verify");
    expect(code([...mode, "--verify"])).toBe("stale_prefix_mode_flag_refused");
    for (const near of ["--verify-only", "--verified-receipt", "--receipt-out", "--protection-attestation"]) expect(code(["--execute", near, "x"])).toBe("stale_prefix_mode_flag_refused");
    expect(staleprefixOptions([...mode, "--verify-only", "--receipt-out", "o.json"], () => undefined)).toEqual({ execute: false, verifyOnly: true, onlyIdentities: [ID()], receiptOut: "o.json" });
  });
});

describe("protection preflight", () => {
  const rule = (body: string, prefix = "96bulls/") => `<Rule><ID>r</ID><Filter><Prefix>${prefix}</Prefix></Filter><Status>Enabled</Status>${body}</Rule>`;
  const lc = (...rules: string[]) => ({ status: 200, body: `<?xml version="1.0" encoding="UTF-8"?><LifecycleConfiguration>${rules.join("")}</LifecycleConfiguration>` });
  const NONCURRENT = rule("<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration>");

  test("a current-version Expiration rule refuses the pass with zero deletes and zero lists", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    f.s.lifecycle = lc(NONCURRENT, rule("<Expiration><Days>30</Days></Expiration>"));
    f.s.lists = 0;
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_protection_unproven", erased: 0 });
    expect([f.s.deletes.length, f.s.lists]).toEqual([0, 0]);
    expect(f.logs.find((l) => l.message === "stale prefix pass refused")).toMatchObject({ problems: ["lifecycle_current_expiration"] });
    expect(f.leases()).toEqual([]);
  });

  test("a noncurrent-only rule (and ExpiredObjectDeleteMarker, noted) passes the preflight and the pass erases", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    f.s.lifecycle = lc(NONCURRENT, rule("<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>"), rule("<Expiration><Days>1</Days></Expiration>", "other-root/"));
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: true, erased: 1 });
    expect(f.s.count(m.oldP)).toBe(0);
    expect(f.logs.find((l) => l.message === "stale prefix protection preflight")).toMatchObject({ proven: true, attestation_used: true, notes: ["noncurrent_version_expiration_days:1", "expired_object_delete_marker"] });
  });

  test("lifecycle unreadable: refused without an attestation; with a valid one the attestation's facts are used (and logged)", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    f.s.lifecycle = { status: 403, body: "<Error><Code>AccessDenied</Code></Error>" };
    const r = await f.execute(receipt, { attestation: undefined });
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_protection_unproven" });
    expect(f.logs.find((l) => l.message === "stale prefix pass refused")).toMatchObject({ problems: ["attestation_missing"] });
    expect(f.s.deletes).toEqual([]);
    const ok = await f.execute(receipt);
    expect(ok).toMatchObject({ ok: true, erased: 1 });
    expect(f.logs.find((l) => l.message === "stale prefix protection preflight")).toMatchObject({ attestation_used: true, notes: ["lifecycle_from_attestation"] });
    expect(f.s.count(m.oldP)).toBe(0);
  });

  const withActor = (actor: AttestedActor, replaceSelf = false) => {
    const a = attestation(NOW);
    return { ...a, actors: [...a.actors.filter((x) => !replaceSelf || !x.self), actor] };
  };
  test("versioning not Enabled, and any attestation that is stale, incomplete, or leaves an actor without a preventive control, refuses with zero deletes", async () => {
    const cases: [string, (f: Awaited<ReturnType<typeof fixture>>) => Partial<StalePrefixPassOptions>][] = [
      ["versioning_not_enabled", (f) => { f.s.versioning = { status: 200, body: "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>" }; return {}; }],
      ["attestation_stale", () => ({ attestation: attestation(NOW - 61 * 60_000) })],
      ["attestation_actors", () => ({ attestation: attestation(NOW, { actors: undefined } as unknown as Partial<ProtectionAttestation>) })],
      ["attestation_actors", () => ({ attestation: attestation(NOW, { actors_complete: false } as unknown as Partial<ProtectionAttestation>) })],
      ["attestation_lifecycle", () => ({ attestation: attestation(NOW, { lifecycle: { current_version_expiration: true } } as unknown as Partial<ProtectionAttestation>) })],
      // A principal that can write a survivor key or change lifecycle with no preventive control (an alarm is not one).
      ["attestation_actor_unprevented", () => ({ attestation: withActor({ principal: "role/ops", actions: ["s3:PutLifecycleConfiguration"], preventive_control: null }) })],
      ["attestation_actor_unprevented", () => ({ attestation: withActor({ principal: "role/ops", actions: ["s3:PutObject"], preventive_control: { name: "alarm-on-put", type: "alarm" } } as unknown as AttestedActor) })],
      ["attestation_actor_unprevented", () => ({ attestation: withActor({ principal: "role/eraser-2", actions: ["s3:DeleteObjectVersion", "s3:PutObject"], self: true, preventive_control: null }, true) })],
      ["attestation_actors_incomplete", () => ({ attestation: attestation(NOW, { actors: [{ principal: "role/admin", actions: ["s3:PutObject"], preventive_control: { name: "scp", type: "scp" } }] }) })],
      ["attestation_scope", () => ({ attestation: attestation(NOW, {}, "other-root") })],
      ["attestation_change_control_ref", () => ({ attestation: attestation(NOW, { change_control_ref: "" }) })],
    ];
    for (const [problem, arrange] of cases) {
      const f = await fixture();
      f.dateMove();
      const receipt = await f.verify();
      const r = await f.execute(receipt, arrange(f));
      expect(r).toMatchObject({ ok: false, refused: "stale_prefix_protection_unproven" });
      expect((f.logs.find((l) => l.message === "stale prefix pass refused")!.problems as string[])).toContain(problem);
      expect(f.s.deletes).toEqual([]);
    }
  });

  test("evaluateLifecycle: fail closed on transitions, unknown actions and unscoped filters; disabled and foreign-prefix rules ignored", () => {
    const xml = (...rules: string[]) => ({ status: "rules" as const, xml: `<LifecycleConfiguration>${rules.join("")}</LifecycleConfiguration>` });
    expect(evaluateLifecycle({ status: "none" }, "96bulls")).toMatchObject({ proven: true });
    expect(evaluateLifecycle(xml(rule("<Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition>")), "96bulls").violations).toEqual(["lifecycle_current_transition"]);
    expect(evaluateLifecycle(xml(rule("<Expiration><Date>2027-01-01T00:00:00Z</Date></Expiration>", "")), "96bulls").violations).toEqual(["lifecycle_current_expiration"]);
    expect(evaluateLifecycle(xml(rule("<Expiration><Days>1</Days></Expiration>", "96bulls/org-1/")), "96bulls").proven).toBe(false);
    expect(evaluateLifecycle(xml(`<Rule><Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>`), "96bulls").proven).toBe(false);
    expect(evaluateLifecycle(xml(rule("<SomethingNew/>")), "96bulls").violations).toEqual(["lifecycle_unknown_action"]);
    expect(evaluateLifecycle(xml(`<Rule><Filter><Prefix>96bulls/</Prefix></Filter><Status>Disabled</Status><Expiration><Days>1</Days></Expiration></Rule>`), "96bulls").proven).toBe(true);
    expect(evaluateLifecycle({ status: "rules", xml: "<LifecycleConfiguration><Rule>" }, "96bulls").proven).toBe(false);
    expect(evaluateLifecycle({ status: "unreadable", code: "s3_403" }, "96bulls").proven).toBe(false);
    expect(attestationProblem(attestation(NOW), { bucketSha256: BUCKET_SHA, keyRoot: "96bulls" })).toBeUndefined();
  });
});

describe("provenance and target binding", () => {
  test("--receipt-sha256 / --attestation-sha256: the exact file bytes must hash to the approved value (one read; mismatch refused)", async () => {
    const dir = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-bind-"));
    const path = join(dir, "receipt.json");
    writeFileSync(path, '{"v":1}\n');
    expect(boundOperatorFile(path, sha('{"v":1}\n'), "stale_prefix_receipt")).toBe('{"v":1}\n');
    expect(() => boundOperatorFile(path, sha('{"v":1}'), "stale_prefix_receipt")).toThrow(expect.objectContaining({ code: "stale_prefix_receipt_hash_mismatch" }));
    expect(() => boundOperatorFile(path, NO_ID, "stale_prefix_attestation")).toThrow(expect.objectContaining({ code: "stale_prefix_attestation_hash_mismatch" }));
    expect(() => boundOperatorFile(join(dir, "missing.json"), NO_ID, "stale_prefix_receipt")).toThrow(expect.objectContaining({ code: "stale_prefix_receipt" }));
  });

  test("the receipt records its target; execute refuses any mismatch (state file, verifier commit, bucket, key root) with zero deletes", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    expect(receipt.target).toMatchObject({ bucket_sha256: BUCKET_SHA, key_root: "96bulls", state_identity: TARGET.stateIdentity, verifier_commit: TARGET.verifierCommit, produced_at: new Date(NOW).toISOString() });
    for (const target of [{ ...TARGET, stateIdentity: "other-state" }, { ...TARGET, verifierCommit: "f".repeat(40) }])
      await expect(f.execute(receipt, { target })).rejects.toMatchObject({ code: "stale_prefix_receipt_target_mismatch" });
    const moved = JSON.parse(JSON.stringify(receipt)) as VerificationReceipt;
    moved.target.key_root = "other-root";
    await expect(f.execute(moved)).rejects.toMatchObject({ code: "stale_prefix_receipt_target_mismatch" });
    const otherBucket = JSON.parse(JSON.stringify(receipt)) as VerificationReceipt;
    otherBucket.target.bucket_sha256 = sha("other");
    await expect(f.execute(otherBucket)).rejects.toMatchObject({ code: "stale_prefix_receipt_target_mismatch" });
    expect(f.s.deletes).toEqual([]);
  });

  test("a receipt older than 6 h (or from the future) is refused stale with zero deletes", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    f.clock.now = NOW + 6 * 3_600_000 + 1;
    await expect(f.execute(receipt)).rejects.toMatchObject({ code: "stale_prefix_receipt_stale" });
    f.clock.now = NOW - 5 * 60_000;
    await expect(f.execute(receipt)).rejects.toMatchObject({ code: "stale_prefix_receipt_stale" });
    expect(f.s.deletes).toEqual([]);
    f.clock.now = NOW + 6 * 3_600_000;
    expect(await f.execute(receipt)).toMatchObject({ ok: true, erased: 1 });
    expect(f.s.count(m.oldP)).toBe(0);
  });

  test("mandatory inline re-verification: a receipt that does not reproduce exactly (forged counts, or content changed in place) refuses with zero deletes", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const forged = JSON.parse(JSON.stringify(receipt)) as VerificationReceipt;
    forged.identities[0]!.items[0]!.counts.records_old = 2;
    const r = await f.execute(forged);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_reverify_mismatch", erased: 0 });
    expect(f.s.deletes).toEqual([]);
    // Same version ids, different bytes (a provider-side rewrite the version set cannot show).
    const v = f.s.keys.get(m.oldCopy.chunks[1]!.key)![0]!;
    v.body = buildCopy(m.oldP, [[line("r1"), line("r2")], [line("r3", { text: "changed" })]]).chunks[1]!.body;
    expect(await f.execute(receipt)).toMatchObject({ ok: false, refused: "stale_prefix_reverify_mismatch" });
    expect(f.s.deletes).toEqual([]);
    expect(f.row(m.oldP)).toMatchObject({ attempts: 0 });
  });

  test("attestation validity bounds the hard deadline: once consumed after delete 1, the next delete is not issued (partial, stale_prefix_deadline)", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    let n = 0;
    // Verified 50 min before the run: protected until NOW + 10 min - 2 x 30 s. Delete 1 takes 9 min.
    f.s.onDelete = () => { if (++n === 1) f.clock.now = NOW + 9 * 60_000; };
    const rep = (await f.execute(receipt, { attestation: attestation(NOW - 50 * 60_000) })).fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_deadline", deleted: 1 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
    expect(f.s.deletes).toHaveLength(1);
  });

  test("lease TTL bound from the request deadline x attempts through the LAST delete: below it, the batch stops before that delete", async () => {
    // Initial relist + 3 deletes x (old page + survivor page + delete) + 2 post-checks = 12 requests; at an 11-minute
    // request deadline that is 132 min, beyond the 2 h lease TTL.
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const slow = f.s.eraser({ requestDeadlineMs: 11 * 60_000 });
    const r = await f.execute(receipt, { fence: slow });
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_lease_ttl_insufficient", erased: 0 });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
    expect(f.s.deletes).toEqual([]);
    expect(f.s.count(m.oldP)).toBe(3);
    // The same batch at a 1-minute deadline fits and erases.
    const ok = await f.execute(receipt, { fence: f.s.eraser({ requestDeadlineMs: 60_000 }) });
    expect(ok).toMatchObject({ ok: true, erased: 1 });
  });

  test("lease TTL bound with a fake clock: a lease whose remaining TTL fell below the bound after delete 1 (renewal refused) stops before delete 2", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let n = 0;
    // After delete 1 the survivor lease is taken over by an exporter whose own clock is ahead (renewal by us fails).
    f.s.onDelete = () => { if (++n === 1) f.st.db.prepare("UPDATE leases SET holder = 'exporter:777' WHERE prefix = ?").run(m.newP); };
    const rep = (await f.execute(receipt)).fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "erasure_lease_lost", deleted: 1 });
    const g = await fixture();
    g.dateMove();
    const rec = await g.verify();
    // Renewal succeeds but the clock moved backwards relative to the lease row: remaining TTL is below the bound.
    let k = 0;
    const realRenew = g.st.renewLease.bind(g.st);
    g.st.renewLease = (p, h, now) => realRenew(p, h, k >= 1 ? (now ?? NOW) - 2 * 3_600_000 + 1000 : now);
    g.s.onDelete = () => void k++;
    const rg = (await g.execute(rec)).fenced![0]!;
    expect(rg).toMatchObject({ outcome: "partial", reason: "stale_prefix_lease_ttl_insufficient", deleted: 1 });
    expect(rg.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
  });
});

describe("cap units (identities vs prefixes)", () => {
  const twoPrefixes = async () => {
    const f = await fixture();
    f.dateMove();
    const older = pfx("2026/10/31");
    seedBodies(f.s, older, OLD - 1000);
    f.st.enqueueErasure([{ key: older, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 30_000);
    return { f, receipt: await f.verify() };
  };

  test("DEFAULT rule: 1 identity with 2 prefixes, every CLI value matching the receipt, no separate multi-prefix approval: refused, zero deletes", async () => {
    const { f, receipt } = await twoPrefixes();
    expect(receipt.identities[0]!.item_count).toBe(2);
    await expect(f.execute(receipt, { max: 2, approvedMax: 2, approvedIdentityCount: 1 })).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    expect(f.s.deletes).toEqual([]);
  });

  test("approved max equal to the IDENTITY count (1, not 2) for 2 prefixes: refused, zero deletes (never raised from the receipt)", async () => {
    const { f, receipt } = await twoPrefixes();
    await expect(f.execute(receipt, { max: 1, approvedMax: 1, approvedIdentityCount: 1 })).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    await expect(f.execute(receipt, { max: 1, approvedMax: 1, approvedIdentityCount: 1, approvedPrefixCount: 2 })).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    expect(f.s.deletes).toEqual([]);
  });

  test("SYNTHETIC multi-prefix case under its own separate approval: approved max 2, identity count 1, --approved-prefix-count 2 passes the gate and erases both", async () => {
    const { f, receipt } = await twoPrefixes();
    expect(await f.execute(receipt, { max: 2, approvedMax: 2, approvedIdentityCount: 1, approvedPrefixCount: 2 })).toMatchObject({ ok: true, erased: 2 });
  });

  test("identity-count and per-identity item-count drift refuse with zero deletes", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    await expect(f.execute(receipt, { approvedIdentityCount: 2 })).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    const drifted = JSON.parse(JSON.stringify(receipt)) as VerificationReceipt;
    drifted.identities[0]!.item_count = 2;
    await expect(f.execute(drifted)).rejects.toMatchObject({ code: "stale_prefix_cap_units_mismatch" });
    expect(() => parseReceipt(JSON.stringify(drifted))).toThrow();
    expect(f.s.deletes).toEqual([]);
  });
});

describe("two identities: ordering and isolation", () => {
  const spy = (f: Awaited<ReturnType<typeof fixture>>) => {
    const fence = f.s.eraser();
    const calls: string[] = [];
    const del = fence.deleteVersion;
    fence.deleteVersion = (k, v) => (calls.push(k), del(k, v));
    return { fence, calls };
  };
  for (const missing of ["first", "last"] as const)
    test(`a missing receipt entry for the ${missing} identity refuses the whole batch: zero deleteVersion calls`, async () => {
      const f = await fixture();
      const a = f.dateMove(undefined, undefined, "sess-1", NOW - 60_000);
      const b = f.dateMove(undefined, undefined, "sess-2", NOW - 50_000);
      const receipt = await f.verify([ID("sess-1"), ID("sess-2")]);
      const gone = missing === "first" ? a.oldP : b.oldP;
      f.st.db.prepare("UPDATE erasures SET enqueued_at = enqueued_at + 1 WHERE key = ?").run(gone);
      const { fence, calls } = spy(f);
      const r = await f.execute(receipt, { fence });
      expect(r).toMatchObject({ ok: false, refused: "stale_prefix_receipt_missing", erased: 0 });
      expect(calls).toEqual([]);
      expect([f.s.count(a.oldP), f.s.count(b.oldP)]).toEqual([3, 3]);
    });

  test("the FIRST identity loses its survivor mid-run: the batch stops; the SECOND identity gets zero deleteVersion calls and an unchanged queue row", async () => {
    const f = await fixture();
    const a = f.dateMove(undefined, undefined, "sess-1", NOW - 60_000);
    const b = f.dateMove(undefined, undefined, "sess-2", NOW - 50_000);
    const receipt = await f.verify([ID("sess-1"), ID("sess-2")]);
    const bRow = JSON.stringify(f.row(b.oldP));
    const { fence, calls } = spy(f);
    let n = 0;
    f.s.onDelete = () => { if (++n === 1) f.s.mark(a.newCopy.chunks[0]!.key, NOW - 1); };
    const r = await f.execute(receipt, { fence });
    expect(r).toMatchObject({ ok: false, erased: 0, stoppedBy: "failed" });
    expect(r.fenced!.map((x) => [x.outcome, x.deleted])).toEqual([["partial", 1], ["refused_before_delete", 0]]);
    expect(calls.filter((k) => k.startsWith(b.oldP))).toEqual([]);
    expect(calls).toHaveLength(1);
    expect(JSON.stringify(f.row(b.oldP))).toBe(bRow);
    expect(f.s.count(b.oldP)).toBe(3);
    expect(() => stalePassStatus(r, false)).toThrow();
  });
});

describe("hard deadline", () => {
  test("short pages after delete 1 (1 entry per page) on a fake clock push past the deadline: stops before the next delete, partial, rest not_attempted", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    let n = 0, slow = false;
    f.s.rewrite = (_c, xml) => { if (slow) f.clock.now += 20 * 60_000; return xml; };
    f.s.onDelete = () => { if (++n === 1) { slow = true; f.s.pageSize = 1; } };
    const rep = (await f.execute(receipt)).fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_deadline", deleted: 1 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
    expect(f.s.deletes).toHaveLength(1);
  });

  test("a slow multipage NONCURRENT survivor history consumes the protected time before delete 1: refused, zero deletes", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const k = m.newCopy.chunks[0]!.key, body = m.newCopy.chunks[0]!.body;
    // Ten older (noncurrent) versions under the current pin.
    f.s.keys.set(k, [...f.s.keys.get(k)!, ...Array.from({ length: 10 }, (_, i) => ({ id: `v-old-${i}`, at: OLD - 1000 * (i + 1), body }))]);
    const receipt = await f.verify();
    expect(receipt.status).toBe("PASS");
    f.s.pageSize = 1;
    f.s.rewrite = (_c, xml) => { f.clock.now += 8 * 60_000; return xml; };
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_deadline", erased: 0 });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
    expect(f.s.deletes).toEqual([]);
    expect(f.s.count(m.oldP)).toBe(3);
  });

  test("a fence listing beyond its page budget aborts and refuses, zero deletes", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const k = m.newCopy.chunks[0]!.key, body = m.newCopy.chunks[0]!.body;
    f.s.keys.set(k, [...f.s.keys.get(k)!, ...Array.from({ length: 25 }, (_, i) => ({ id: `v-old-${i}`, at: OLD - 1000 * (i + 1), body }))]);
    const receipt = await f.verify();
    f.s.pageSize = 1;
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_listing_budget", erased: 0 });
    expect(f.s.deletes).toEqual([]);
  });

  test("issuance boundary on a fake clock: a delete whose timeout just fits is issued; one ms later it is NOT issued (stale_prefix_deadline, exact accounting)", async () => {
    // Protected until NOW + 1 h (attestation) - 2 x 30 s; one delete's worst case is 30 s.
    const cutoff = NOW + 60 * 60_000 - 60_000;
    for (const [at, issued] of [[cutoff - 30_000, true], [cutoff - 30_000 + 1, false]] as const) {
      const f = await fixture();
      const m = f.dateMove();
      const receipt = await f.verify();
      let armed = false;
      // The clock reaches `at` during the LAST listing of delete 2's fence check (the survivor listing).
      f.s.rewrite = (_c, xml) => { if (armed && xml.includes(`<Prefix>${m.newP}</Prefix>`)) { armed = false; f.clock.now = at; } return xml; };
      let n = 0;
      f.s.onDelete = () => { if (++n === 1) armed = true; };
      const r = await f.execute(receipt);
      const rep = r.fenced![0]!;
      if (issued) {
        expect(f.s.deletes.length).toBeGreaterThanOrEqual(2);
        expect(rep.versions[1]!.outcome).toBe("deleted");
      } else {
        expect(f.s.deletes).toHaveLength(1);
        expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_deadline", deleted: 1 });
        expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
      }
    }
  });
});

describe("issuance after signing (eraser request path)", () => {
  test("adapter: a slow signing step that crosses the caller's gate means ZERO fetch calls for that DELETE; fast signing passes", async () => {
    for (const slowMs of [2_100, 0]) {
      const s = new FakeS3Versions();
      const key = `${pfx("2026/11/01")}raw/x.ndjson.zst`;
      const id = s.put(key, OLD, undefined, new Uint8Array([1]));
      const clock = { now: NOW };
      const eraser = s.eraser({ signer: async (args) => { clock.now += slowMs; await signRequest(args); } });
      const checkedAt = clock.now;
      // The caller's gate: at most 2 s since its check; it returns the time left to an absolute deadline.
      const beforeFetch = () => { if (clock.now - checkedAt > 2_000) throw Object.assign(new Error("late"), { code: "stale_prefix_deadline" }); return 60_000; };
      const run = eraser.deleteVersion(key, id, { beforeFetch });
      if (slowMs) {
        await expect(run).rejects.toMatchObject({ code: "stale_prefix_deadline" });
        expect(s.deletes).toEqual([]);
        expect(s.count(key)).toBe(1);
      } else {
        await run;
        expect(s.deletes).toEqual([`${key}@${id}`]);
      }
    }
  });

  test("adapter: no time left to the absolute deadline after signing: fetch is never called", async () => {
    const s = new FakeS3Versions();
    const key = `${pfx("2026/11/01")}raw/x.ndjson.zst`;
    const id = s.put(key, OLD, undefined, new Uint8Array([1]));
    await expect(s.eraser().deleteVersion(key, id, { beforeFetch: () => 0 })).rejects.toMatchObject({ code: "stale_prefix_deadline" });
    expect(s.deletes).toEqual([]);
  });

  test("job: slow signing of delete 2 (2.1 s after its check) is refused after signing: that DELETE is never fetched, not_attempted, partial", async () => {
    for (const slow of [true, false]) {
      const f = await fixture();
      const m = f.dateMove();
      const receipt = await f.verify();
      let deletesSigned = 0;
      const fence = f.s.eraser({ signer: async (args) => { if (args.method === "DELETE" && ++deletesSigned === 2 && slow) f.clock.now += 2_100; await signRequest(args); } });
      const r = await f.execute(receipt, { fence });
      const rep = r.fenced![0]!;
      if (slow) {
        expect(deletesSigned).toBe(2);
        expect(f.s.deletes).toHaveLength(1); // the second DELETE was signed but never fetched
        expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_deadline", deleted: 1 });
        expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
        expect(f.s.count(m.oldP)).toBe(2);
      } else {
        expect(r).toMatchObject({ ok: true, erased: 1 });
        expect(f.s.deletes).toHaveLength(3);
      }
    }
  });

  test("job: slow abort-signal construction on DELETE 1 (2.101 s, after signing) is seen by the final gate: zero DELETE fetches, not_attempted, unchanged row; fast control erases", async () => {
    for (const slow of [true, false]) {
      const f = await fixture();
      const m = f.dateMove();
      const receipt = await f.verify();
      const real = AbortSignal.timeout;
      let armed = false, built = 0;
      // Signing a DELETE arms the stub; that DELETE's signal construction then takes 2.101 s on the job's clock.
      const fence = f.s.eraser({ signer: async (args) => { await signRequest(args); if (args.method === "DELETE") armed = true; } });
      AbortSignal.timeout = ((ms: number) => {
        if (armed) { armed = false; built++; if (slow) f.clock.now += 2_101; else f.clock.now += 100; }
        return real.call(AbortSignal, ms);
      }) as typeof AbortSignal.timeout;
      const before = f.row(m.oldP);
      let r: Awaited<ReturnType<typeof f.execute>>;
      try { r = await f.execute(receipt, { fence }); } finally { AbortSignal.timeout = real; }
      const rep = r.fenced![0]!;
      if (slow) {
        expect(built).toBe(1);
        expect(f.s.deletes).toEqual([]); // the signal was built, the gate then refused: never fetched
        expect(rep).toMatchObject({ outcome: "refused_before_delete", reason: "stale_prefix_deadline", deleted: 0 });
        expect(rep.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
        expect(f.row(m.oldP)).toEqual(before);
        expect(f.s.count(m.oldP)).toBe(3);
      } else {
        expect(built).toBe(3);
        expect(r).toMatchObject({ ok: true, erased: 1 });
        expect(f.s.deletes).toHaveLength(3);
      }
    }
  });

  test("adapter: the gate runs AFTER the abort timer is armed, and the armed full deadline must fit the time left (refused unsent, never shortened)", async () => {
    for (const [left, sent] of [[29_999, false], [30_000, true]] as const) {
      const s = new FakeS3Versions();
      const key = "t/x/a.json", id = s.put(key);
      const real = AbortSignal.timeout;
      const order: string[] = [];
      AbortSignal.timeout = ((ms: number) => { order.push(`signal:${ms}`); return real.call(AbortSignal, ms); }) as typeof AbortSignal.timeout;
      const dispatch: { phase?: string } = {};
      let err: unknown;
      try {
        await s.eraser({ requestDeadlineMs: 30_000 }).deleteVersion(key, id, { beforeFetch: () => (order.push("gate"), left), dispatch });
      } catch (e) { err = e; } finally { AbortSignal.timeout = real; }
      expect(order).toEqual(["signal:30000", "gate"]);
      if (sent) { expect(err).toBeUndefined(); expect(s.deletes).toHaveLength(1); expect(dispatch.phase).toBe("dispatched"); }
      else { expect(err).toMatchObject({ code: "stale_prefix_deadline", notSent: true }); expect(s.deletes).toEqual([]); expect(dispatch.phase).toBe("preparing"); }
    }
  });

  test("ambiguous DELETE: the diagnostic relist goes through the same gate; gated past the deadline it records unknown and issues no further request", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let listsAtDelete = -1;
    f.s.onDelete = () => { listsAtDelete = f.s.lists; f.clock.now = NOW + 2 * 3_600_000; return "transport"; };
    const r = await f.execute(receipt);
    const rep = r.fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_delete_ambiguous", deleted: 0 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["unknown", "not_attempted", "not_attempted"]);
    expect(f.s.lists).toBe(listsAtDelete); // the relist was refused before fetch; nothing after it
    expect(f.s.deletes).toHaveLength(1);
    expect(f.s.count(m.oldP)).toBe(2);
  });
});

describe("in-path gate judged at a fresh time (after the lease queries)", () => {
  /** Arms on signing a DELETE; the next lease read (inside the post-signing gate) runs `delay` first. */
  const delayedGate = (f: Awaited<ReturnType<typeof fixture>>, delay: () => void) => {
    let armed = false;
    const fence = f.s.eraser({ signer: async (args) => { await signRequest(args); if (args.method === "DELETE") armed = true; } });
    const real = f.st.leaseRemainingMs.bind(f.st);
    f.st.leaseRemainingMs = (...a: Parameters<typeof real>) => { if (armed) { armed = false; delay(); } return real(...a); };
    return fence;
  };

  test("a lease read inside the gate that advances the clock past the 2 s check age: ZERO fetch for that DELETE, not_attempted, the batch stops", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const fence = delayedGate(f, () => { f.clock.now += 2_101; });
    const r = await f.execute(receipt, { fence });
    expect(f.s.deletes).toEqual([]);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_deadline", erased: 0 });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
    expect(f.s.count(m.oldP)).toBe(3);
  });

  test("a lease read inside the gate that leaves a lease below request deadline + margin: ZERO fetch for that DELETE, not_attempted", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    // The survivor lease row is aged (as if its renewals stalled) so that 20 s remain: below 30 s + 5 s margin.
    const fence = delayedGate(f, () => void f.st.db.prepare("UPDATE leases SET at = ? WHERE prefix = ?").run(NOW - (2 * 3_600_000 - 20_000), m.newP));
    const r = await f.execute(receipt, { fence });
    expect(f.s.deletes).toEqual([]);
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_deadline" });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
  });

  test("control: the same path with a fast lease read issues normally", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const fence = delayedGate(f, () => {});
    expect(await f.execute(receipt, { fence })).toMatchObject({ ok: true, erased: 1 });
    expect(f.s.deletes).toHaveLength(3);
    expect(f.s.count(m.oldP)).toBe(0);
  });

  test("the eraser's own no-time-left refusal (timeout <= 0) before fetch maps to not_attempted, never ambiguous", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    const fence = f.s.eraser();
    const real = fence.deleteVersion;
    let n = 0;
    // The job's gate passes; the time left handed to the eraser is then 0 on delete 2.
    fence.deleteVersion = (k, v, o) => real(k, v, { ...o, beforeFetch: () => { const left = o!.beforeFetch!(); return ++n === 2 ? 0 : left; } });
    const rep = (await f.execute(receipt, { fence })).fenced![0]!;
    expect(f.s.deletes).toHaveLength(1);
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_deadline", deleted: 1 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "not_attempted", "not_attempted"]);
  });
});

describe("pre-dispatch failures are not sent (real adapter, mock fetch)", () => {
  test("the signer throws on the first DELETE: zero fetch calls, not_attempted, no relist, no backoff or attempt increment, zero deletes", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let listsAtThrow = -1;
    const fence = f.s.eraser({
      signer: async (args) => {
        if (args.method === "DELETE") { listsAtThrow = f.s.lists; throw Object.assign(new Error("signer broke"), { code: "signer_failed" }); }
        await signRequest(args);
      },
    });
    const before = f.row(m.oldP);
    const r = await f.execute(receipt, { fence });
    expect(f.s.deletes).toEqual([]);
    expect(f.s.lists).toBe(listsAtThrow); // no diagnostic relist, nothing after
    expect(r).toMatchObject({ ok: false, refused: "signer_failed", erased: 0, versionsDeleted: 0 });
    expect(r.fenced![0]).toMatchObject({ outcome: "refused_before_delete", reason: "signer_failed", deleted: 0 });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
    expect(f.row(m.oldP)).toEqual(before); // no backoff, no attempt
    expect(f.logs.some((l) => l.message === "erasure failed")).toBe(false);
    expect(f.s.count(m.oldP)).toBe(3);
  });

  test("building the abort signal fails before fetch (local preparation): zero fetch calls, not_attempted, no relist, no backoff", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const real = AbortSignal.timeout;
    let armed = false, listsAtThrow = -1;
    const fence = f.s.eraser({ signer: async (args) => { await signRequest(args); if (args.method === "DELETE") { armed = true; listsAtThrow = f.s.lists; } } });
    // Only the DELETE's signal construction fails; every list/GET before it builds its signal normally.
    AbortSignal.timeout = ((ms: number) => { if (armed) { armed = false; throw Object.assign(new RangeError("signal broke"), { code: "signal_failed" }); } return real.call(AbortSignal, ms); }) as typeof AbortSignal.timeout;
    const before = f.row(m.oldP);
    let r: Awaited<ReturnType<typeof f.execute>>;
    try { r = await f.execute(receipt, { fence }); } finally { AbortSignal.timeout = real; }
    expect(f.s.deletes).toEqual([]);
    expect(f.s.lists).toBe(listsAtThrow); // no diagnostic relist
    expect(r).toMatchObject({ ok: false, refused: "signal_failed", erased: 0, versionsDeleted: 0 });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
    expect(f.row(m.oldP)).toEqual(before); // no backoff, no attempt
    expect(f.s.count(m.oldP)).toBe(3);
  });

  test("building the DELETE URL fails (local preparation): zero fetch calls, not_attempted, no relist, unchanged row", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const realSet = URLSearchParams.prototype.set;
    let armed = false, listsAtThrow = -1;
    // Armed only inside the fence's deleteVersion (the inline re-verification GETs set versionId too).
    const fence = f.s.eraser();
    const real = fence.deleteVersion;
    fence.deleteVersion = (k, v, o) => { armed = true; return real(k, v, o).finally(() => { armed = false; }); };
    URLSearchParams.prototype.set = function (name: string, value: string) {
      if (armed && name === "versionId") { listsAtThrow = f.s.lists; throw Object.assign(new TypeError("url broke"), { code: "url_failed" }); }
      return realSet.call(this, name, value);
    };
    const before = f.row(m.oldP);
    let r: Awaited<ReturnType<typeof f.execute>>;
    try { r = await f.execute(receipt, { fence }); } finally { URLSearchParams.prototype.set = realSet; }
    expect(listsAtThrow).toBeGreaterThan(0);
    expect(f.s.deletes).toEqual([]);
    expect(f.s.lists).toBe(listsAtThrow); // no diagnostic relist
    expect(r).toMatchObject({ ok: false, refused: "url_failed", erased: 0, versionsDeleted: 0 });
    expect(r.fenced![0]!.versions.every((v) => v.outcome === "not_attempted")).toBe(true);
    expect(f.row(m.oldP)).toEqual(before);
    expect(f.s.count(m.oldP)).toBe(3);
  });

  test("a credential-shaped, notSent-tagged error AFTER dispatch stays ambiguous: relisted, no exit-75 stop, never not_attempted", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    let listsAtDelete = -1;
    f.s.onDelete = () => { listsAtDelete = f.s.lists; return "credential-after"; };
    const r = await f.execute(receipt); // returns: no credential throw (exit 75) for a dispatched request
    const rep = r.fenced![0]!;
    expect(f.s.deletes).toHaveLength(1);
    expect(f.s.lists).toBe(listsAtDelete + 1); // the diagnostic relist ran
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_delete_ambiguous" });
    expect(rep.versions[0]).toMatchObject({ outcome: "absent_observed", error_code: "short_lived_expired.eraser" });
  });

  test("a fence that never writes the dispatch record is not trusted: a notSent-tagged pre-fetch failure is still ambiguous", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    const fence = f.s.eraser({ signer: async (args) => { if (args.method === "DELETE") throw Object.assign(new Error("signer broke"), { code: "signer_failed" }); await signRequest(args); } });
    const real = fence.deleteVersion;
    fence.deleteVersion = (k, v, o) => real(k, v, { beforeFetch: o?.beforeFetch }); // drops the record
    const rep = (await f.execute(receipt, { fence })).fenced![0]!;
    expect(f.s.deletes).toEqual([]);
    expect(rep).toMatchObject({ reason: "stale_prefix_delete_ambiguous" });
    expect(rep.versions[0]).toMatchObject({ outcome: "present_after_error", error_code: "signer_failed" });
  });

  test("the signer throws a short_lived_* refusal: the credential stop (exit 75, item kept) and not_attempted", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    const fence = f.s.eraser({ signer: async (args) => { if (args.method === "DELETE") throw new ShortLivedCredentialError("short_lived_expired.eraser"); await signRequest(args); } });
    const before = f.row(m.oldP);
    let err: unknown;
    try { await f.execute(receipt, { fence }); } catch (e) { err = e; }
    expect((err as { code?: string }).code).toBe("short_lived_expired.eraser");
    expect(exitStatusForError(err)).toBe(75);
    expect(f.s.deletes).toEqual([]);
    expect(f.row(m.oldP)).toEqual(before);
    const fenceLog = f.logs.find((l) => l.message === "stale prefix fence")!;
    expect(fenceLog).toMatchObject({ outcome: "refused_before_delete", deleted: 0, not_attempted: 3, unknown: 0 });
    expect(f.leases()).toEqual([]);
  });

  test("control: a transport error AFTER fetch is dispatched stays ambiguous and is relisted", async () => {
    const f = await fixture();
    f.dateMove();
    const receipt = await f.verify();
    let listsAtDelete = -1;
    f.s.onDelete = () => { listsAtDelete = f.s.lists; return "transport"; };
    const rep = (await f.execute(receipt)).fenced![0]!;
    expect(f.s.deletes).toHaveLength(1);
    expect(f.s.lists).toBe(listsAtDelete + 1); // the diagnostic relist ran
    expect(rep).toMatchObject({ outcome: "partial", reason: "stale_prefix_delete_ambiguous" });
    expect(rep.versions[0]!.outcome).toBe("absent_observed");
  });
});
