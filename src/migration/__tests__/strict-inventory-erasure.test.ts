/**
 * Every S3 inventory the eraser acts on is strict (versions() defaults strict=true), and an
 * erase is acknowledged only after a post-delete strict inventory shows zero.
 */
import { expect, test } from "bun:test";
import { createS3Eraser } from "../neon-s3-erasure/eraser.ts";

const prefix = "96bulls/org/v1/ws=w/m=srv_x/d=2026/11/06/s=sess/";
const entry = (key: string, id: string) => `<Version><Key>${key}</Key><VersionId>${id}</VersionId><IsLatest>true</IsLatest></Version>`;
function fixture(body: (deleted: Set<string>) => string, honor = true) {
  const deleted = new Set<string>();
  const fetcher = (async (input: RequestInfo | URL, init?: RequestInit) => {
    if (init?.method === "DELETE") { if (honor) deleted.add(new URL(String(input)).searchParams.get("versionId")!); return new Response(null, { status: 204 }); }
    return new Response(body(deleted));
  }) as typeof fetch;
  return { deleted, eraser: createS3Eraser({ bucket: "b", region: "us-east-1", credentials: { accessKeyId: "a", secretAccessKey: "s" }, fetch: fetcher }) };
}
const doc = (entries: [string, string][], deleted: Set<string>) =>
  `<ListVersionsResult>${entries.filter(([, id]) => !deleted.has(id)).map(([k, id]) => entry(k, id)).join("")}<IsTruncated>false</IsTruncated></ListVersionsResult>`;

for (const [name, xml] of [
  ["truncated XML", `<ListVersionsResult>${entry(prefix + "a", "1")}<IsTruncated>false</IsTruncated><Version>`],
  ["no pagination proof", `<ListVersionsResult>${entry(prefix + "a", "1")}</ListVersionsResult>`],
  ["truncated without a cursor", `<ListVersionsResult>${entry(prefix + "a", "1")}<IsTruncated>true</IsTruncated></ListVersionsResult>`],
  ["missing VersionId", `<ListVersionsResult><Version><Key>${prefix}a</Key><IsLatest>true</IsLatest></Version><IsTruncated>false</IsTruncated></ListVersionsResult>`],
] as const) {
  test(`erasePrefix refuses ${name} with zero deletes`, async () => {
    const f = fixture(() => xml);
    await expect(f.eraser.erasePrefix(prefix)).rejects.toMatchObject({ code: "erasure_inventory" });
    expect(f.deleted.size).toBe(0);
  });
  test(`eraseObject refuses ${name} with zero deletes`, async () => {
    const f = fixture(() => xml);
    await expect(f.eraser.eraseObject(prefix + "a")).rejects.toMatchObject({ code: "erasure_inventory" });
    expect(f.deleted.size).toBe(0);
  });
}

test("a strict, complete inventory erases, and a post-delete inventory proves zero", async () => {
  const entries: [string, string][] = [[prefix + "a", "1"], [prefix + "manifest.json", "2"]];
  const f = fixture((d) => doc(entries, d));
  expect(await f.eraser.erasePrefix(prefix)).toBe(2);
  const g = fixture((d) => doc([[prefix + "a", "1"], [prefix + "ab", "9"]], d));
  expect(await g.eraser.eraseObject(prefix + "a")).toBe(1);
  expect([...g.deleted]).toEqual(["1"]);   // the sibling key under the same list prefix is untouched
});

test("versions that survive the deletes are never acknowledged as erased", async () => {
  const entries: [string, string][] = [[prefix + "a", "1"]];
  const f = fixture((d) => doc(entries, d), false);
  await expect(f.eraser.erasePrefix(prefix)).rejects.toMatchObject({ code: "erasure_inventory_changed" });
  const g = fixture((d) => doc(entries, d), false);
  await expect(g.eraser.eraseObject(prefix + "a")).rejects.toMatchObject({ code: "erasure_inventory_changed" });
});

test("the paged eraser refuses a key outside the requested prefix before any delete of that page", async () => {
  const f = fixture((d) => doc([[prefix + "a", "1"], ["96bulls/other/x", "2"]], d));
  await expect(f.eraser.eraseStep(prefix, { scope: "s", authorize: async () => {} })).rejects.toMatchObject({ code: "erasure_foreign_key" });
  await expect(f.eraser.verifyErased(prefix, { authorize: async () => {} })).rejects.toMatchObject({ code: "erasure_foreign_key" });
  expect(f.deleted.size).toBe(0);
});
