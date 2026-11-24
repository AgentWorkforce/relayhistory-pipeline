/**
 * The strict Azure Blob eraser (azure-blob-erasure/eraser.ts) against an in-memory fake container
 * (fake-azure-blob.ts): every version, snapshot and soft-deleted copy goes, exact identities only,
 * no redirect is followed, and erased is never reported while a recoverable copy remains.
 */
import { describe, expect, test } from "bun:test";
import { createAzureBlobStore } from "../../../packages/object-store/src/azure.ts";
import { AZURE_RESUME_MAX_PAGES, azureEraserLabel, createAzureBlobEraser } from "../azure-blob-erasure/eraser.ts";
import type { EraseCheckpoint, PagedEraser } from "../neon-s3-erasure/eraser.ts";
import { reconcileObligation, type Cas, type ObligationState, type ObligationStore, type WriteInvocation } from "../neon-s3-erasure/write-obligation.ts";
import { FAKE_ENDPOINT, FakeAzureBlob, type FakeAzureOptions } from "./fake-azure-blob.ts";

const DIR = "fxroot/fx-org/v1/normalized/session-analysis-v2/";
const KEY = `${DIR}fx-job.json`;
const authorize = async () => {};

function setup(opts: FakeAzureOptions & { pageSize?: number } = {}) {
  const fake = new FakeAzureBlob(opts);
  const eraser = (): PagedEraser => createAzureBlobEraser({
    account: fake.account, container: fake.container, endpoint: FAKE_ENDPOINT, accountKey: () => fake.accountKey, fetch: fake.fetch,
    ...(opts.pageSize ? { pageSize: opts.pageSize } : {}),
  });
  return { fake, eraser };
}

async function drain(e: PagedEraser, prefix: string, opts: { exactKey?: string; select?: (v: { versionId: string; key: string }) => boolean } = {}) {
  let checkpoint: EraseCheckpoint | null = null;
  for (let i = 0; i < 50; i++) {
    const r = await e.eraseStep(prefix, { scope: "fx", authorize, checkpoint, ...opts });
    if (r.status === "traversed") return;
    checkpoint = r.checkpoint;
  }
  throw new Error("no traversal");
}

const deletes = (fake: FakeAzureBlob) => fake.requests.filter((r) => r.method === "DELETE").map((r) => {
  const q = r.url.searchParams;
  const what = q.get("snapshot") !== null ? `snapshot` : q.get("versionid") !== null ? `version` : "base";
  return `${what}${q.get("deletetype") ? ":permanent" : ""}${r.headers.get("if-match") ? ":if-match" : ""}`;
});

describe("versioned account", () => {
  test("every version, the base blob and snapshots are erased; snapshots first, the base conditionally, then versions by id", async () => {
    const { fake, eraser } = setup();
    fake.put(KEY, "a"); fake.put(KEY, "b");
    fake.snapshot(KEY);
    fake.put(KEY, "c");
    await drain(eraser(), DIR);
    expect(fake.copies(KEY)).toEqual([]);
    expect(deletes(fake)).toEqual(["snapshot", "base:if-match", "version", "version", "version"]);
    expect(await eraser().verifyErased(DIR, { authorize })).toEqual({ status: "erased", listed: 0 });
    expect(fake.requests.every((r) => r.redirect === "manual")).toBe(true);
  });

  test("deleting the base blob only demotes the current version: its bytes stay, and verifyErased stays pending until its version delete", async () => {
    const { fake, eraser } = setup();
    const v = fake.put(KEY, "secret");
    let failed = false;
    fake.hook = (req) => {
      if (!failed && req.method === "DELETE" && req.url.searchParams.get("versionid") === v) { failed = true; return new Response(null, { status: 500 }); }
    };
    await expect(drain(eraser(), DIR)).rejects.toMatchObject({ code: "azure_500" });
    const left = fake.copies(KEY);
    expect(left).toHaveLength(1);
    expect(left[0]).toMatchObject({ id: v, current: false, deleted: false });
    expect(new TextDecoder().decode(left[0]!.bytes)).toBe("secret");
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "pending", reason: "remaining", remaining: 1 });
    await drain(eraser(), DIR);
    expect(fake.copies(KEY)).toEqual([]);
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "erased" });
  });

  test("a 412 on the conditional base delete (a successor became current) fails the step with nothing deleted", async () => {
    const { fake, eraser } = setup();
    fake.put(KEY, "old");
    let raced = false;
    fake.hook = (req) => {
      if (!raced && req.method === "DELETE") { raced = true; fake.put(KEY, "successor"); }
      return undefined;
    };
    await expect(drain(eraser(), DIR)).rejects.toMatchObject({ code: "azure_current_changed" });
    expect(fake.copies(KEY).map((r) => new TextDecoder().decode(r.bytes)).sort()).toEqual(["old", "successor"]);
    expect(fake.copies(KEY).find((r) => r.current)).toMatchObject({ deleted: false });
  });

  test("a 409 (snapshots present / conflict) is coded and never followed by a broader delete", async () => {
    const { fake, eraser } = setup();
    fake.put(KEY);
    fake.hook = (req) => (req.method === "DELETE" ? new Response(null, { status: 409 }) : undefined);
    await expect(drain(eraser(), DIR)).rejects.toMatchObject({ code: "azure_409" });
    expect(fake.requests.filter((r) => r.method === "DELETE")).toHaveLength(1);
    expect(fake.requests.some((r) => r.headers.has("x-ms-delete-snapshots"))).toBe(false);
  });

  test("404 on a delete is idempotent (already gone)", async () => {
    const { fake, eraser } = setup();
    fake.put(KEY); fake.put(KEY);
    fake.hook = (req) => (req.method === "DELETE" && req.url.searchParams.get("versionid") ? new Response(null, { status: 404 }) : undefined);
    await drain(eraser(), DIR);
  });
});

describe("owned versions survive (the sweeper's select, F1)", () => {
  test("a result put through the Azure store records its x-ms-version-id, and a sweep keeps exactly that version", async () => {
    const { fake, eraser } = setup();
    const stray = fake.put(KEY, "stray-earlier-attempt");
    const store = createAzureBlobStore({ account: fake.account, container: fake.container, endpoint: FAKE_ENDPOINT,
      auth: { kind: "sharedKey", accountKey: fake.accountKey }, fetch: fake.fetch, versionIds: true });
    const ack = await store.put(KEY as never, new TextEncoder().encode('{"published":true}'), { contentType: "application/json" });
    const owned = ack.version.version!;
    expect(owned).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(owned).not.toBe(stray);
    // The obligation the sweeper reconciles: one acked invocation, the owned version published.
    const row: ObligationState & { gone: boolean } = { key: KEY, identity: ["fx"], rev: 1, gone: false,
      invocations: [{ id: "inv-1", outcome: "acked", versionId: owned } as WriteInvocation], ownedVersions: [owned], ownersWithoutVersion: 0, checkpoint: null };
    const bump = (): Cas => ({ ok: true, rev: ++row.rev });
    const obligations: ObligationStore = {
      load: async () => (row.gone ? null : { ...row }),
      reserve: async (rev, ids) => (rev === row.rev && !ids.includes(owned) ? bump() : { ok: false }),
      saveCheckpoint: async (rev, cp) => (rev === row.rev ? ((row.checkpoint = cp), bump()) : { ok: false }),
      resolve: async (rev, ids) => (rev === row.rev ? ((row.invocations = row.invocations.filter((i) => !ids.includes(i.id))), bump()) : { ok: false }),
      reschedule: async (rev) => (rev === row.rev ? bump() : { ok: false }),
      retire: async (rev) => (rev === row.rev ? ((row.gone = true), { ok: true, rev: rev + 1 }) : { ok: false }),
    };
    expect(await reconcileObligation(eraser(), obligations)).toBe("retired");
    const left = fake.copies(KEY);
    expect(left.map((r) => r.id)).toEqual([owned]);
    expect(left[0]).toMatchObject({ current: true, deleted: false });
    expect(deletes(fake)).toEqual(["version"]);
  });
});

describe("soft delete", () => {
  test("a soft-deleted version keeps verifyErased pending; the next pass deletes it permanently (permanent delete allowed)", async () => {
    const { fake, eraser } = setup({ softDelete: true, permanentDelete: true });
    fake.put(KEY); fake.put(KEY);
    await drain(eraser(), DIR);
    expect(fake.copies(KEY).every((r) => r.deleted)).toBe(true);
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "pending", remaining: 2 });
    await drain(eraser(), DIR);
    expect(fake.copies(KEY)).toEqual([]);
    expect(deletes(fake).filter((d) => d.startsWith("version:permanent"))).toHaveLength(2);
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "erased" });
  });

  test("permanent delete refused: azure_soft_delete_retained, and never erased while the soft-deleted copy remains", async () => {
    const { fake, eraser } = setup({ softDelete: true, permanentDelete: false });
    fake.put(KEY);
    await drain(eraser(), DIR);
    await expect(drain(eraser(), DIR)).rejects.toMatchObject({ code: "azure_soft_delete_retained" });
    expect(fake.copies(KEY)).toHaveLength(1);
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "pending", remaining: 1 });
  });

  test("a 403 on the permanent delete is azure_permanent_delete_forbidden (permission), not retention", async () => {
    const { fake, eraser } = setup({ softDelete: true, permanentDelete: false, permanentDeleteRefusal: 403 });
    fake.put(KEY);
    await drain(eraser(), DIR);
    await expect(drain(eraser(), DIR)).rejects.toMatchObject({ code: "azure_permanent_delete_forbidden" });
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "pending", remaining: 1 });
  });

  test("a soft-deleted snapshot is permanently deleted by snapshot time", async () => {
    const { fake, eraser } = setup({ softDelete: true, permanentDelete: true });
    fake.put(KEY); fake.snapshot(KEY);
    await drain(eraser(), DIR);
    await drain(eraser(), DIR);
    expect(fake.copies(KEY)).toEqual([]);
    expect(deletes(fake)).toContain("snapshot:permanent");
  });

  test("versioned account, soft-deleted base WITH a version id: listed as deleted:<id> and deleted permanently", async () => {
    const { fake, eraser } = setup({ softDelete: true, permanentDelete: true });
    fake.put(KEY);
    fake.records[0]!.current = false; fake.records[0]!.deleted = true;
    const e = eraser();
    expect(await e.verifyErased(DIR, { authorize })).toMatchObject({ status: "pending", remaining: 1 });
    await drain(e, DIR);
    expect(fake.copies(KEY)).toEqual([]);
  });

  test("a soft-deleted base blob WITHOUT a version id cannot be erased: coded refusal, verifyErased pending (no silent erased)", async () => {
    const { fake, eraser } = setup({ softDelete: true, permanentDelete: true });
    fake.put(KEY);
    Object.assign(fake.records[0]!, { kind: "base", id: "", current: false, deleted: true });
    await expect(drain(eraser(), DIR)).rejects.toMatchObject({ code: "azure_soft_delete_retained" });
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "pending", remaining: 1 });
  });
});

describe("pagination and resume", () => {
  test("many keys and versions over small pages, within one instance", async () => {
    const { fake, eraser } = setup({ pageSize: 2 });
    for (let k = 0; k < 7; k++) for (let v = 0; v < 3; v++) fake.put(`${DIR}fx-${k}.json`);
    await drain(eraser(), DIR);
    expect(fake.records).toEqual([]);
  });

  test("a budget-cut step returns a checkpoint a FRESH instance resumes from (opaque markers are not carried)", async () => {
    const { fake, eraser } = setup({ pageSize: 2 });
    for (let k = 0; k < 6; k++) fake.put(`${DIR}fx-${k}.json`);
    const first = await eraser().eraseStep(DIR, { scope: "fx", authorize, budget: { pages: 1 } });
    expect(first.status).toBe("incomplete");
    const cp = (first as { checkpoint: EraseCheckpoint }).checkpoint;
    expect(cp.bucket).toBe(azureEraserLabel(fake.account, fake.container));
    let checkpoint: EraseCheckpoint | null = cp;
    for (;;) {
      const r = await eraser().eraseStep(DIR, { scope: "fx", authorize, checkpoint, budget: { pages: 1 } });
      if (r.status === "traversed") break;
      checkpoint = r.checkpoint;
    }
    expect(fake.records).toEqual([]);
  });

  test("an owned-keep selection with kept entries spanning pages still traverses", async () => {
    const { fake, eraser } = setup({ pageSize: 1 });
    const keep = [fake.put(KEY), fake.put(KEY)];
    fake.put(KEY);
    await drain(eraser(), DIR, { exactKey: KEY, select: (v) => !keep.includes(v.versionId) });
    expect(fake.copies(KEY).map((r) => r.id).sort()).toEqual([...keep].sort());
  });

  test("a resume point more than AZURE_RESUME_MAX_PAGES provider pages in is refused (bounded), not walked", async () => {
    const { fake, eraser } = setup({ pageSize: 1 });
    for (let k = 0; k < AZURE_RESUME_MAX_PAGES + 5; k++) fake.put(`${DIR}fx-${String(k).padStart(4, "0")}.json`);
    const checkpoint: EraseCheckpoint = { v: 1, bucket: azureEraserLabel(fake.account, fake.container), prefix: DIR, scope: "fx",
      afterKey: `${DIR}fx-${String(AZURE_RESUME_MAX_PAGES + 2).padStart(4, "0")}.json`, inKey: null };
    await expect(eraser().eraseStep(DIR, { scope: "fx", authorize, checkpoint })).rejects.toMatchObject({ code: "azure_resume_unbounded" });
    expect(fake.requests.some((r) => r.method === "DELETE")).toBe(false);
  });

  test("a checkpoint bound to another container is refused", async () => {
    const { fake, eraser } = setup();
    fake.put(KEY);
    const checkpoint: EraseCheckpoint = { v: 1, bucket: azureEraserLabel(fake.account, "fx-other"), prefix: DIR, scope: "fx", afterKey: null, inKey: null };
    await expect(eraser().eraseStep(DIR, { scope: "fx", authorize, checkpoint })).rejects.toMatchObject({ code: "erasure_checkpoint_scope" });
  });
});

describe("fresh traversals (Azure markers name the next item as of issue time)", () => {
  test("verifyErased on the same instance never reuses an earlier traversal's marker: an entry created since is found", async () => {
    const { fake, eraser } = setup({ pageSize: 2, markerMode: "next-item" });
    const [A, C, C2, D] = ["fx-a.json", "fx-c.json", "fx-c2.json", "fx-d.json"].map((k) => `${DIR}${k}`);
    fake.put(A!); fake.put(C!); fake.put(D!);
    const e = eraser();
    // Traversal 1 deletes nothing; its page ending at C gets a marker naming D (the next item then).
    expect(await e.eraseStep(DIR, { scope: "fx", authorize, select: () => false })).toMatchObject({ status: "traversed" });
    fake.put(C2!);   // sorts between C and D
    // Traversal 2 must see C2 (the earlier marker would jump from C straight to D and report erased).
    expect(await e.verifyErased(DIR, { authorize, select: (v) => v.key === C2 })).toMatchObject({ status: "pending", remaining: 1 });
    await drain(e, DIR, { select: (v) => v.key === C2 });
    expect(fake.copies(C2!)).toEqual([]);
    expect(await e.verifyErased(DIR, { authorize })).toMatchObject({ status: "pending", reason: "remaining" });
    expect(fake.records.map((r) => r.key).sort()).toEqual([A, C, D].sort());
  });

  test("two prefixes on one instance, interleaved, and a second verifyErased after more objects arrive: each sees everything", async () => {
    const { fake, eraser } = setup({ pageSize: 2, markerMode: "next-item" });
    const P1 = `${DIR}fx-p1/`, P2 = `${DIR}fx-p2/`;
    for (const k of ["a", "c", "e"]) { fake.put(`${P1}${k}`); fake.put(`${P2}${k}`); }
    const e = eraser();
    const none = () => false;
    expect(await e.eraseStep(P1, { scope: "fx", authorize, select: none })).toMatchObject({ status: "traversed" });
    expect(await e.eraseStep(P2, { scope: "fx", authorize, select: none })).toMatchObject({ status: "traversed" });
    for (const k of ["b", "d"]) { fake.put(`${P1}${k}`); fake.put(`${P2}${k}`); }
    for (const p of [P1, P2]) {
      const want = new Set([`${p}b`, `${p}d`]);
      const proof = await e.verifyErased(p, { authorize, select: (v) => want.has(v.key) });
      expect([p, proof.status]).toEqual([p, "pending"]);
      await drain(e, p, { select: (v) => want.has(v.key) });
      expect([p, fake.records.filter((r) => want.has(r.key))]).toEqual([p, []]);
      expect([p, (await e.verifyErased(p, { authorize, select: (v) => want.has(v.key) })).status]).toEqual([p, "erased"]);
    }
    expect(fake.records.map((r) => r.key).sort()).toEqual([`${P1}a`, `${P1}c`, `${P1}e`, `${P2}a`, `${P2}c`, `${P2}e`].sort());
  });

  test("one instance across a directory traversal and an exact-key traversal (the probe's pattern) erases everything", async () => {
    const { fake, eraser } = setup({ pageSize: 1, markerMode: "next-item" });
    for (const k of ["fx-1", "fx-2", "fx-3"]) fake.put(`${DIR}${k}/probe`);
    const e = eraser();
    await drain(e, DIR);
    fake.put(`${DIR}fx-4/probe`); fake.put(`${DIR}fx-4/probe`);
    await drain(e, `${DIR}fx-4/`, { exactKey: `${DIR}fx-4/probe` });
    expect(await e.verifyErased(DIR, { authorize })).toEqual({ status: "erased", listed: 0 });
    expect(fake.records).toEqual([]);
  });

  test("a full multi-page erase under next-item markers, resumed by fresh instances from checkpoints", async () => {
    const { fake, eraser } = setup({ pageSize: 2, markerMode: "next-item" });
    for (let k = 0; k < 5; k++) for (let v = 0; v < 3; v++) fake.put(`${DIR}fx-${k}.json`);
    let checkpoint: EraseCheckpoint | null = null;
    for (let i = 0; i < 100; i++) {
      const r = await eraser().eraseStep(DIR, { scope: "fx", authorize, checkpoint, budget: { pages: 1 } });
      if (r.status === "traversed") break;
      checkpoint = r.checkpoint;
    }
    expect(fake.records).toEqual([]);
    expect(await eraser().verifyErased(DIR, { authorize })).toMatchObject({ status: "erased" });
  });
});

describe("strict inventory", () => {
  const listing = (xml: string) => (req: { method: string; url: URL }) =>
    (req.method === "GET" ? new Response(xml, { status: 200 }) : undefined);
  const wrap = (blobs: string, next = "<NextMarker />") => `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="fx-container"><Blobs>${blobs}</Blobs>${next}</EnumerationResults>`;
  const blob = (name: string, extra = "") => `<Blob><Name>${name}</Name><VersionId>2026-11-23T10:00:00.0000001Z</VersionId>${extra}<Properties><Etag>0x1</Etag></Properties></Blob>`;
  const cases: [string, string, string][] = [
    ["unknown root", `<ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>`, "erasure_inventory"],
    ["truncated document", wrap(blob(`${DIR}a`)).slice(0, -10), "erasure_inventory"],
    ["missing Name", wrap(`<Blob><VersionId>x</VersionId><Properties/></Blob>`), "erasure_inventory"],
    ["missing Properties", wrap(`<Blob><Name>${DIR}a</Name></Blob>`), "erasure_inventory"],
    ["pagination unproven (no NextMarker)", wrap(blob(`${DIR}a`), ""), "erasure_inventory"],
    ["keys out of order", wrap(blob(`${DIR}b`) + blob(`${DIR}a`)), "erasure_inventory"],
    ["duplicate identity", wrap(blob(`${DIR}a`) + blob(`${DIR}a`)), "erasure_inventory"],
    ["foreign key", wrap(blob(`fxother/a`)), "erasure_foreign_key"],
    ["IsCurrentVersion not a boolean", wrap(blob(`${DIR}a`, "<IsCurrentVersion>yes</IsCurrentVersion>")), "erasure_inventory"],
    ["Encoded name", wrap(`<Blob><Name Encoded="true">${DIR}%01</Name><Properties/></Blob>`), "erasure_inventory"],
  ];
  for (const [name, xml, code] of cases)
    test(`refused: ${name}`, async () => {
      const { fake, eraser } = setup();
      fake.hook = listing(xml);
      await expect(eraser().eraseStep(DIR, { scope: "fx", authorize })).rejects.toMatchObject({ code });
      await expect(eraser().verifyErased(DIR, { authorize })).rejects.toMatchObject({ code });
      expect(fake.requests.some((r) => r.method === "DELETE")).toBe(false);
    });
});

describe("transport", () => {
  test("a 3xx on the listing or a delete is azure_redirect, and the redirect target never receives a request", async () => {
    for (const on of ["GET", "DELETE"]) {
      const { fake, eraser } = setup();
      fake.put(KEY);
      fake.hook = (req) => (req.method === on ? new Response(null, { status: 307, headers: { location: "https://fx-elsewhere.invalid/steal" } }) : undefined);
      await expect(drain(eraser(), DIR)).rejects.toMatchObject({ code: "azure_redirect" });
      expect(fake.requests.every((r) => r.url.hostname === "127.0.0.1")).toBe(true);
    }
  });

  test("a 403 (wrong key) is coded; the key, account and container never appear in an error", async () => {
    const { fake } = setup();
    fake.put(KEY);
    const wrongKey = Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64");
    const e = createAzureBlobEraser({ account: fake.account, container: fake.container, endpoint: FAKE_ENDPOINT, accountKey: () => wrongKey, fetch: fake.fetch });
    const error = await e.eraseStep(DIR, { scope: "fx", authorize }).catch((x) => x);
    expect(error).toMatchObject({ code: "azure_403" });
    const text = `${error.message} ${error.code} ${JSON.stringify(e)} ${String(error.stack ?? "")}`;
    for (const secret of [wrongKey, fake.accountKey, wrongKey.slice(10, 30)]) expect(text).not.toContain(secret);
    expect(error.message).not.toContain(fake.container);
  });

  test("a network failure is azure_unavailable", async () => {
    const e = createAzureBlobEraser({ account: "fxacct", container: "fx-container", endpoint: FAKE_ENDPOINT, accountKey: () => "AAAA",
      fetch: (async () => { throw new TypeError("connect refused"); }) as unknown as typeof fetch });
    await expect(e.verifyErased(DIR, { authorize })).rejects.toMatchObject({ code: "azure_unavailable" });
  });
});

describe("unversioned account (the runner refuses it; the eraser stays exact)", () => {
  test("the base blob is deleted conditionally and the listing proves zero", async () => {
    const { fake, eraser } = setup({ versioning: false });
    fake.put(KEY);
    await drain(eraser(), DIR);
    expect(fake.records).toEqual([]);
    expect(deletes(fake)).toEqual(["base:if-match"]);
  });
});
