import { describe, expect, test } from "bun:test";
import { CanonicalFeatureChanged } from "../canonical-feature-changed.ts";

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
    if (text.startsWith("INSERT")) {
      table.push({
        revision: params[0], kind: params[1], scope: params[2],
        schema_generation: params[3], image_key: params[4],
        source_hash: params[5], prior_keyset_hash: params[6] ?? null,
        source_authority: params[7], prior_source_authority: params[8],
        pointer_set: params[9],
      });
      return { rows: [] as T[] };
    }
    return { rows: [] as T[] };
  };
  return { table, query };
};
const base = {
  scope: { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s" },
  source: { epoch: "1", fingerprint: "a".repeat(64) },
  pointerSet: { hash: "p".repeat(64), count: 3 },
};
describe("canonical_feature_changed (620 invoker)", () => {
  test("priorSource null -> publish entry appended with managed revision", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    // seed the counter above the legacy clock first
    await db.query("INSERT INTO sessions.canonical_feature_journal(revision,kind,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES($1,'seed','','','','','','')", ["100"]);
    const rev = await fn.canonicalFeatureChanged({ ...base, priorSource: null });
    expect(rev).toBe("101");
    const row = db.table.find((r) => r.revision === "101")!;
    expect(row.kind).toBe("publish");
    expect(JSON.parse(row.source_authority as string)).toEqual(base.source);
    expect(row.prior_keyset_hash).toBe("");
  });
  test("priorSource present -> retire entry carries prior authority+keyset", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await db.query("INSERT INTO sessions.canonical_feature_journal(revision,kind,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES($1,'seed','','','','','','')", ["100"]);
    const prior = { epoch: "0", fingerprint: "b".repeat(64) };
    const rev = await fn.canonicalFeatureChanged({ ...base, priorSource: prior });
    const row = db.table.find((r) => r.revision === rev)!;
    expect(row.kind).toBe("retire");
    expect(row.prior_keyset_hash).toBe(prior.fingerprint);
    expect(JSON.parse(row.prior_source_authority as string)).toEqual(prior);
  });
  test("invalid input refuses before any write", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await expect(fn.canonicalFeatureChanged({
      scope: { orgId: "", workspaceId: "w", harness: "h", sessionId: "s" },
      source: base.source, priorSource: null, pointerSet: base.pointerSet,
    })).rejects.toThrow("invalid input");
    expect(db.table.length).toBe(0);
  });
});
