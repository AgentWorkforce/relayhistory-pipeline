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
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob, STALE_RUN_LEASE, staleprefixIdentityHash, type StalePrefixPassOptions } from "../neon-s3-erasure/job.ts";
import { staleprefixOptions, stalePassStatus } from "../neon-s3-erasure/main.ts";
import {
  attestationProblem, chunkRecords, compareMultisets, evaluateLifecycle, parseReceipt, type ProtectionAttestation, type VerificationReceipt,
} from "../neon-s3-erasure/stale-prefix-verify.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";
import { attestation, BUCKET_SHA, buildCopy, line, seedBodies } from "./stale-prefix-fixture.ts";

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
  const job = () => new ErasureJob(authority(), st, s.eraser(), (f) => void logs.push(f as Logged), DEFAULT_ERASURE_CONFIG, () => NOW, async () => {});
  /** A date move: old copy at d=11/01 (queued stale), survivor at d=11/02 (exported). Chunks are ndjson line lists. */
  const dateMove = (old: string[][] = [[line("r1"), line("r2")], [line("r3")]], survivor: string[][] = old, session = "sess-1", at = NOW - 60_000) => {
    const oldP = pfx("2026/11/01", session), newP = pfx("2026/11/02", session);
    const oldCopy = seedBodies(s, oldP, OLD, old);
    const newCopy = seedBodies(s, newP, OLD + 1000, survivor);
    st.markExported(ORG, `g-${session}`, 1, [newCopy.manifestKey], at);
    st.enqueueErasure([{ key: oldP, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], at);
    return { oldP, newP, oldCopy, newCopy };
  };
  const verify = (ids: string[] = [ID()]) => job().staleprefixVerify({ onlyIdentities: ids, reader: s.reader(), bucketSha256: BUCKET_SHA });
  const execute = (receipt: VerificationReceipt, over: Partial<StalePrefixPassOptions> = {}) => {
    s.deletes = [];
    return job().staleprefixPass({ execute: true, onlyIdentities: receipt.allowlist, probe: s.eraser(), receipt, fence: s.eraser(), bucketSha256: BUCKET_SHA, attestation: attestation(NOW), ...over });
  };
  const row = (key: string) => st.db.prepare("SELECT * FROM erasures WHERE key = ?").get(key) as Record<string, unknown> | undefined;
  const leases = () => st.db.prepare("SELECT prefix, holder FROM leases").all() as { prefix: string; holder: string }[];
  return { path, st, s, logs, job, dateMove, verify, execute, row, leases };
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
    expect(r).toMatchObject({ ok: false, erased: 0, versionsDeleted: 0, stoppedBy: "refused" });
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

  test("a FAIL receipt is never erased (stale_prefix_receipt_failed); a PASS identity in the same pass still is; not ok", async () => {
    const f = await fixture();
    const bad = f.dateMove([[line("r1")], [line("r7")]], [[line("r1")]], "sess-1", NOW - 60_000);
    const good = f.dateMove(undefined, undefined, "sess-2", NOW - 50_000);
    const receipt = await f.verify([ID("sess-1"), ID("sess-2")]);
    expect(receipt.identities.map((i) => i.status)).toEqual(["FAIL", "PASS"]);
    const r = await f.execute(receipt);
    expect(r).toMatchObject({ ok: false, erased: 1 });
    expect(r.fenced!.map((x) => [x.outcome, x.reason ?? null])).toEqual([["refused_before_delete", "stale_prefix_receipt_failed"], ["completed", null]]);
    expect(f.s.count(bad.oldP)).toBe(3);
    expect(f.s.count(good.oldP)).toBe(0);
    expect(f.s.deletes.every((d) => d.startsWith(good.oldP))).toBe(true);
    expect(f.row(bad.oldP)).toMatchObject({ attempts: 0, error: null });
    expect(() => stalePassStatus(r, false)).toThrow();
  });

  test("an entry the receipt does not name (re-queued after verification) is refused stale_prefix_receipt_missing", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    f.st.db.prepare("UPDATE erasures SET enqueued_at = enqueued_at + 1 WHERE key = ?").run(m.oldP);
    const r = await f.execute(receipt);
    expect(r.fenced![0]).toMatchObject({ outcome: "refused_before_delete", reason: "stale_prefix_receipt_missing" });
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
    expect(f.logs.find((l) => l.message === "stale prefix fence")).toMatchObject({ outcome: "partial", deleted: 1, not_attempted: 2, failed: 0, error_code: "stale_prefix_survivor_drift" });
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

  test("a failed DELETE is partial with that version failed (never untouched)", async () => {
    const f = await fixture();
    const m = f.dateMove();
    const receipt = await f.verify();
    let n = 0;
    f.s.onDelete = () => (++n === 2 ? "lose" : undefined);
    const rep = (await f.execute(receipt)).fenced![0]!;
    expect(rep).toMatchObject({ outcome: "partial", reason: "s3_500", deleted: 1 });
    expect(rep.versions.map((v) => v.outcome)).toEqual(["deleted", "failed", "not_attempted"]);
    expect(f.row(m.oldP)).toMatchObject({ attempts: 1 });
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
    await expect(f.execute(receipt, { bucketSha256: sha("other") })).rejects.toMatchObject({ code: "stale_prefix_receipt_invalid" });
    expect(f.s.deletes).toEqual([]);
    const code = (args: string[]) => { try { staleprefixOptions(args, () => undefined); return null; } catch (e) { return (e as { code?: string }).code; } };
    const mode = ["--stale-prefixes-only", "--only-identity", ID()];
    expect(code([...mode, "--execute"])).toBe("stale_prefix_execute_requires_verified_receipt");
    expect(code([...mode, "--execute", "--verified-receipt", "r.json"])).toBe("stale_prefix_execute_requires_attestation");
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

  test("versioning not Enabled, and any attestation that is stale, incomplete, or leaves lifecycle change unprevented, refuses with zero deletes", async () => {
    const cases: [string, (f: Awaited<ReturnType<typeof fixture>>) => Partial<StalePrefixPassOptions>][] = [
      ["versioning_not_enabled", (f) => { f.s.versioning = { status: 200, body: "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>" }; return {}; }],
      ["attestation_stale", () => ({ attestation: attestation(NOW - 61 * 60_000) })],
      ["attestation_survivor_writers", () => ({ attestation: attestation(NOW, { survivor_writers: undefined } as unknown as Partial<ProtectionAttestation>) })],
      ["attestation_lifecycle", () => ({ attestation: attestation(NOW, { lifecycle: { current_version_expiration: true } } as unknown as Partial<ProtectionAttestation>) })],
      ["attestation_lifecycle_change_not_prevented", () => ({ attestation: attestation(NOW, { lifecycle_change: { principals: ["role/break-glass", "role/admin"], break_glass: ["role/break-glass"], prevented: true, preventive_control: "scp" } }) })],
      ["attestation_lifecycle_change_not_prevented", () => ({ attestation: attestation(NOW, { lifecycle_change: { principals: ["role/break-glass"], break_glass: ["role/break-glass"], prevented: false, preventive_control: "alarm-only" } } as unknown as Partial<ProtectionAttestation>) })],
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
