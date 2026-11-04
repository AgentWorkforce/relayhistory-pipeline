import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, readdirSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { readSummaryObject, summaryObjectKey, writeSummaryObject, prepareSummaryObject, putPreparedSummaryObject, prepareBoundSummaryValue, readBoundSummaryValue, type SummaryRecord } from "../summary-objects.ts";

const HEX_A = "a".repeat(64);
const HEX_B = "b".repeat(64);
const loc = { workspaceId: "ws1", machineId: "m1", harness: "claude", sessionId: "s1", startDate: "2026-10-29" };
const ids = { orgId: "org_1", workspaceId: "ws1" };
const record = (over: Partial<SummaryRecord> = {}): SummaryRecord => ({
  v: 1, kind: "short", revision: 3, status: "ready", summary: "SECRET-SUMMARY-TEXT", model: "extractive/v1",
  built_at: "2026-10-29T00:00:00.000Z", source_content_sha256: "c".repeat(64), trace_id: "01a188e1-ef86-7b16-938a-e91b6b78643b", ...over,
});
const allFiles = (dir: string): string[] =>
  readdirSync(dir).flatMap((f) => (statSync(join(dir, f)).isDirectory() ? allFiles(join(dir, f)) : [join(dir, f)]));

describe("summary objects (upstream envelope) with the HKDF keyring", () => {
  it("logical encrypted slices bind exact source, object namespace, key id and tenant", async () => {
    const root = mkdtempSync(join(tmpdir(), "bc-logical-"));
    const store = createFsStore({ root });
    const keyring = await masterKeyring(parseMasterKeys(`k1:${HEX_A}`));
    const key = "fixture/logical/source1/bob-slice/attempt.json.enc";
    const value = { protocol: "bob-slice-v1", source: HEX_A, text: "PRIVATE-MODEL-OUTPUT", tools: ["\ud800"] };
    const prepared = await prepareBoundSummaryValue({ keyring, ids, key, binding: HEX_A, value });
    expect((await store.head(key as never))).toBeUndefined();
    expect(new TextDecoder().decode(prepared.body)).not.toContain("PRIVATE-MODEL-OUTPUT");
    await putPreparedSummaryObject({ store, prepared });
    const read = { store, keyring, ids, key, binding: HEX_A, sha256: prepared.sha256, keyId: prepared.keyId };
    expect(await readBoundSummaryValue(read)).toEqual(value);
    expect(await readBoundSummaryValue({ ...read, binding: HEX_B })).toBeNull();
    expect(await readBoundSummaryValue({ ...read, keyId: "hkdf1:k2" })).toBeNull();
    expect(await readBoundSummaryValue({ ...read, ids: { ...ids, orgId: "foreign" } })).toBeNull();
    const moved = "fixture/logical/source2/bob-slice/attempt.json.enc";
    await store.put(moved as never, prepared.body);
    expect(await readBoundSummaryValue({ ...read, key: moved })).toBeNull();
    expect(await readSummaryObject({ ...read, kind: "short", revision: 1 })).toBeNull();
    for (const f of allFiles(root)) expect(readFileSync(f, "utf8")).not.toContain("PRIVATE-MODEL-OUTPUT");
  });

  it("immutable prepared writes reuse identical bytes and refuse changed bytes without overwriting", async () => {
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bc-immutable-")) });
    const keyring = await masterKeyring(parseMasterKeys(`k1:${HEX_A}`));
    const key = "fixture/publication/immutable/short.bin";
    const prepared = await prepareSummaryObject({ keyring, ids, key, record: record() });
    await putPreparedSummaryObject({ store, prepared });
    const version = (await store.head(key as never))?.version;
    await putPreparedSummaryObject({ store, prepared });
    expect((await store.head(key as never))?.version).toEqual(version);
    const another = await prepareSummaryObject({ keyring, ids, key, record: record({ summary: "different" }) });
    await expect(putPreparedSummaryObject({ store, prepared: another })).rejects.toMatchObject({ code: "summary_contended" });
    prepared.body[0] = prepared.body[0]! ^ 1;
    await expect(putPreparedSummaryObject({ store, prepared })).rejects.toMatchObject({ code: "summary_prepared_invalid", retryable: false });
    expect((await store.head(key as never))?.version).toEqual(version);
  });
  it("prepares exact encrypted registration bytes without storage and preserves envelope/AAD compatibility", async () => {
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bc-prepared-")) });
    const keyring = await masterKeyring(parseMasterKeys(`k1:${HEX_A}`));
    // Fixture key only: the managed publisher owns production qualified names.
    const key = "fixture/publication/attempt/short.json.enc";
    const prepared = await prepareSummaryObject({ keyring, ids, key, record: record() });
    expect(prepared.sha256).toBe(createHash("sha256").update(prepared.body).digest("hex"));
    expect(prepared.bytes).toBe(prepared.body.byteLength);
    expect(prepared.keyId).toBe("hkdf1:k1");
    expect(new TextDecoder().decode(prepared.body)).not.toContain("SECRET-SUMMARY-TEXT");
    await store.put(prepared.key as never, prepared.body, { ifNoneMatch: true, contentSha256: prepared.sha256 });
    expect((await readSummaryObject({ store, keyring, ids, key, sha256: prepared.sha256, kind: "short", revision: 3 }))?.summary).toBe("SECRET-SUMMARY-TEXT");
    const moved = "fixture/another-attempt/short.json.enc";
    await store.put(moved as never, prepared.body);
    expect(await readSummaryObject({ store, keyring, ids, key: moved, sha256: prepared.sha256, kind: "short", revision: 3 })).toBeNull();
    await expect(prepareSummaryObject({ keyring, ids, key, record: record({ summary: "x".repeat(1024 * 1024) }) }))
      .rejects.toMatchObject({ code: "summary_too_large", retryable: false });
  });

  it("bounds actual response bytes even when object metadata understates its size", async () => {
    let cancelled = false, keyReads = 0;
    const body = new ReadableStream<Uint8Array>({ start(c) { c.enqueue(new Uint8Array(2 * 1024 * 1024)); }, cancel() { cancelled = true; } });
    const store = { getObject: async () => ({ meta: { size: 1 }, body }) } as never;
    const keyring = { keyById: async () => { keyReads++; throw new Error("must not derive"); } } as never;
    expect(await readSummaryObject({ store, keyring, ids, key: "fixture/short.json.enc", sha256: HEX_A, kind: "short", revision: 3 })).toBeNull();
    expect(cancelled).toBe(true);
    expect(keyReads).toBe(0);
  });

  it("writes summary/<kind>.json.enc next to the raw, encrypted, and reads it back by pointer", async () => {
    const root = mkdtempSync(join(tmpdir(), "bc-"));
    const store = createFsStore({ root });
    const keyring = await masterKeyring(parseMasterKeys(`k1:${HEX_A}`));
    const key = String(summaryObjectKey(loc, "short"));
    expect(key).toBe("v1/ws=ws1/m=m1/h=claude/d=2026/10/29/s=s1/summary/short.json.enc");
    const w = await writeSummaryObject({ store, keyring, ids, key, record: record(), current: { revision: 3, manifestSha256: "f".repeat(64) } });
    expect(w.outcome).toBe("written");
    if (w.outcome !== "written") return;
    expect(w.keyId).toBe("hkdf1:k1");
    // No plaintext anywhere on disk (body or metadata sidecars).
    for (const f of allFiles(root)) expect(readFileSync(f, "utf8")).not.toContain("SECRET-SUMMARY-TEXT");
    const back = await readSummaryObject({ store, keyring, ids, key, sha256: w.sha256, kind: "short", revision: 3 });
    expect(back?.summary).toBe("SECRET-SUMMARY-TEXT");
    // Bound to kind, revision and workspace: any mismatch reads as not ready.
    expect(await readSummaryObject({ store, keyring, ids, key, sha256: w.sha256, kind: "long", revision: 3 })).toBeNull();
    expect(await readSummaryObject({ store, keyring, ids, key, sha256: w.sha256, kind: "short", revision: 4 })).toBeNull();
    expect(await readSummaryObject({ store, keyring, ids: { orgId: "org_1", workspaceId: "other" }, key, sha256: w.sha256, kind: "short", revision: 3 })).toBeNull();
    // A retry for the same manifest, revision and status reuses the stored bytes.
    const again = await writeSummaryObject({ store, keyring, ids, key, record: record({ built_at: "2026-10-30T00:00:00.000Z" }), current: { revision: 3, manifestSha256: "f".repeat(64) } });
    expect(again.outcome === "written" && again.sha256).toBe(w.sha256);
    // A record for another revision than the fence is refused.
    expect((await writeSummaryObject({ store, keyring, ids, key, record: record({ revision: 2 }), current: { revision: 3, manifestSha256: "f".repeat(64) } })).outcome).toBe("stale");
  });

  it("rotates: a new current master writes, the old one still reads, an unknown key id is refused", async () => {
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bc-")) });
    const old = await masterKeyring(parseMasterKeys(`k1:${HEX_A}`));
    const key = String(summaryObjectKey(loc, "long"));
    const w = await writeSummaryObject({ store, keyring: old, ids, key, record: record({ kind: "long" }), current: { revision: 3, manifestSha256: "e".repeat(64) } });
    if (w.outcome !== "written") throw new Error("not written");
    const rotated = await masterKeyring(parseMasterKeys(`k2:${HEX_B},k1:${HEX_A}`));
    expect((await rotated.currentKey(ids)).keyId).toBe("hkdf1:k2");
    expect((await readSummaryObject({ store, keyring: rotated, ids, key, sha256: w.sha256, kind: "long", revision: 3 }))?.summary).toBe("SECRET-SUMMARY-TEXT");
    // Without k1, the k1 object cannot be opened (crypto-shredded).
    const shredded = await masterKeyring(parseMasterKeys(`k2:${HEX_B}`));
    await expect(readSummaryObject({ store, keyring: shredded, ids, key, sha256: w.sha256, kind: "long", revision: 3 })).rejects.toThrow(/does not hold/);
    // After rotation a retry for the same manifest/revision/status re-encrypts under k2 (no reuse of k1).
    const rewritten = await writeSummaryObject({ store, keyring: rotated, ids, key, record: record({ kind: "long" }), current: { revision: 3, manifestSha256: "e".repeat(64) } });
    expect(rewritten.outcome === "written" && rewritten.keyId).toBe("hkdf1:k2");
    expect(rewritten.outcome === "written" && rewritten.sha256).not.toBe(w.sha256);
    const onlyK2 = await masterKeyring(parseMasterKeys(`k2:${HEX_B}`));
    if (rewritten.outcome !== "written") throw new Error("not rewritten");
    expect((await readSummaryObject({ store, keyring: onlyK2, ids, key, sha256: rewritten.sha256, kind: "long", revision: 3 }))?.summary).toBe("SECRET-SUMMARY-TEXT");
    // The old pointer no longer matches the stored object.
    expect(await readSummaryObject({ store, keyring: rotated, ids, key, sha256: w.sha256, kind: "long", revision: 3 })).toBeNull();
  });
});
