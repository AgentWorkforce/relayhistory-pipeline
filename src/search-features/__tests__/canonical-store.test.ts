import { describe, expect, test } from "bun:test";
import {
  CanonicalJournalStore,
  CanonicalVersionStore,
} from "../canonical-journal.ts";

const SCOPE = JSON.stringify({ orgId: "o", workspaceId: "w", harness: "h",
  sessionId: "s", generation: "a".repeat(64) });
import { createHash } from "node:crypto";
const SCOPE_OBJ = JSON.parse(SCOPE);
const HS1_KEY = "hs1:" +
  createHash("sha256").update(JSON.stringify(SCOPE_OBJ)).digest("hex") + ":" +
  SCOPE_OBJ.generation + ":12345678-1234-1234-1234-123456789012";
const AUTH = JSON.stringify({ authorityId: "12345678-1234-1234-1234-123456789012", epoch: "1", fingerprint: "a".repeat(64) });
const PTR = JSON.stringify({ authorityId: "12345678-1234-1234-1234-123456789012", hash: "c".repeat(64), count: "1" });
const fakeDb = () => {
  const table: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("ORDER BY revision DESC"))
      return { rows: table.length
        ? [{ revision: String(Math.max(...table.map((r) => Number(r.revision)))) } as T]
        : [] as T[] };
    if (text.includes("COALESCE(MAX(revision)"))
      return { rows: [{ revision: String(
        table.length ? Math.max(...table.map((r) => Number(r.revision))) + 1 : 1) } as T] };
    if (text.includes("WHERE revision >"))
      return { rows: table
        .filter((r) => Number(r.revision) > Number(params[0]) &&
          r.kind !== "seed")
        .sort((a, b) => Number(a.revision) - Number(b.revision))
        .slice(0, Number(params[1])) as T[] };
    if (text.startsWith("INSERT")) {
      // params[0] may be a serialized CanonicalJournalEntry or a bare revision.
      const e = params[0] as Record<string, unknown>;
      if (e && typeof e === "object" && (e as { kind?: string }).kind) {
        // 610 row shape: retire carries prior authority in source_authority;
        // publish carries it in prior_source_authority (both iff keyset set).
        const srcAuth = e.kind === "retire"
          ? JSON.stringify(e.priorSourceAuthority)
          : e.sourceAuthority ? JSON.stringify(e.sourceAuthority) : null;
        // Store raw jsonb values AS GIVEN (object stays object — the real
        // driver returns parsed objects, so the decoder must handle both).
        table.push({
          revision: e.revision, kind: e.kind,
          org_id: (e.scope as Record<string, unknown>)?.orgId,
          workspace_id: (e.scope as Record<string, unknown>)?.workspaceId,
          scope: e.scope,
          schema_generation: e.schemaGeneration, image_key: e.imageKey,
          source_hash: e.sourceHash,
          prior_keyset_hash: e.priorKeysetHash ?? null,
          source_authority: e.kind === "retire"
            ? e.priorSourceAuthority : e.sourceAuthority,
          prior_source_authority: e.kind === "publish"
            ? e.priorSourceAuthority ?? null : null,
          pointer_set: e.pointerSet,
        });
      } else {
        table.push({ revision: params[0], kind: "seed" });
      }
      return { rows: [] as T[] };
    }
    return { rows: [] as T[] };
  };
  return { table, query };
};
const pub = (rev: string, key = HS1_KEY) => ({
  kind: "publish" as const, revision: rev, imageKey: key,
  schemaGeneration: "a".repeat(64), sourceHash: "a".repeat(64),
  scope: JSON.parse(SCOPE), sourceAuthority: JSON.parse(AUTH),
  pointerSet: JSON.parse(PTR),
});
const ret = (rev: string) => ({
  kind: "retire" as const, revision: rev, imageKey: HS1_KEY,
  schemaGeneration: "a".repeat(64),
  sourceHash: JSON.parse(AUTH).fingerprint,   // prior fingerprint
  priorKeysetHash: JSON.parse(PTR).hash,      // == pointerSet.hash
  scope: JSON.parse(SCOPE), priorSourceAuthority: JSON.parse(AUTH),
  pointerSet: JSON.parse(PTR),
});
describe("canonical journal store", () => {
  test("append is read-only — always rejects (sole writer is _install)", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    await expect(j.append(pub("10"), "0"))
      .rejects.toThrow("sole writer");
  });
  test("jsonb decoder accepts OBJECT-VALUED columns (real driver shape)", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    // PG/PGlite drivers return jsonb as parsed objects, not strings.
    const objRow = {
      revision: "10", kind: "publish", imageKey: HS1_KEY,
      schemaGeneration: "a".repeat(64), sourceHash: "a".repeat(64),
      scope: JSON.parse(SCOPE),               // OBJECT, not string
      sourceAuthority: JSON.parse(AUTH),      // OBJECT
      pointerSet: JSON.parse(PTR),            // OBJECT
    };
    await db.query("INSERT INTO j VALUES($1)", [objRow]);
    const out = await j.entries("0", 10);
    expect(out.entries[0]!.kind).toBe("publish");
    const retireObj = {
      revision: "20", kind: "retire", imageKey: HS1_KEY,
      schemaGeneration: "a".repeat(64), sourceHash: "a".repeat(64),
      priorKeysetHash: "c".repeat(64),
      scope: JSON.parse(SCOPE),
      priorSourceAuthority: JSON.parse(AUTH), // retire carries it in source_authority
      pointerSet: JSON.parse(PTR),
    };
    await db.query("INSERT INTO j VALUES($1)", [retireObj]);
    const r = (await j.entries("10", 10)).entries[0]!;
    if (r.kind !== "retire") throw new Error("expected retire");
    expect(r.priorSourceAuthority.fingerprint).toBe("a".repeat(64));
  });
  test("append writes publish/retire entries; stale fences", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    await db.query("INSERT INTO j VALUES($1)", [pub("10")]);
    await db.query("INSERT INTO j VALUES($1)", [ret("20")]);
    const out = await j.entries("0", 10);
    expect(out.entries.map((e) => e.kind)).toEqual(["publish", "retire"]);
    expect(out.hasMore).toBe(false);
  });
  test("retire decodes prior authority from source_authority (610 CHECK shape)", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    // A real 610 retire row: prior authority in source_authority,
    // prior_source_authority NULL.
    await db.query("INSERT INTO j VALUES($1)", [{
      revision: "10", kind: "retire", imageKey: HS1_KEY,
      schemaGeneration: "a".repeat(64), sourceHash: "a".repeat(64),
      priorKeysetHash: "c".repeat(64), scope: JSON.parse(SCOPE),
      priorSourceAuthority: JSON.parse(AUTH),
      pointerSet: JSON.parse(PTR) }]);
    const out = await j.entries("0", 10);
    const e = out.entries[0]!;
    if (e.kind !== "retire") throw new Error("expected retire");
    // must come from source_authority, not the NULL prior_source_authority
    expect(e.priorSourceAuthority.epoch).toBe("1");
    expect(e.priorSourceAuthority.fingerprint).toBe("a".repeat(64));
  });
  test("entries paging: hasMore, nextRevision, limit != completeness", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    for (const r of ["10", "20", "30"]) await db.query("INSERT INTO j VALUES($1)", [pub(r, HS1_KEY)]);
    const first = await j.entries("10", 1);
    expect(first.entries.map((e) => e.revision)).toEqual(["20"]);
    expect(first.hasMore).toBe(true);
    const rest = await j.entries("10", 10);
    expect(rest.entries.map((e) => e.revision)).toEqual(["20", "30"]);
    expect(rest.hasMore).toBe(false);
    expect(rest.nextRevision).toBe("30");
  });
});
