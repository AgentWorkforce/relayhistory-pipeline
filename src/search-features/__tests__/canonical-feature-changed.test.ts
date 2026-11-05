import { describe, expect, test } from "bun:test";
import { CanonicalFeatureChanged } from "../canonical-feature-changed.ts";

const fakeDb = () => {
  const table: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("ORDER BY revision DESC"))
      return { rows: table.length
        ? [{ revision: String(Math.max(...table.map((r) => Number(r.revision)))) } as T]
        : [] as T[] };
    if (text.includes("SELECT 1::text AS revision") && text.includes("kind = 'seed'"))
      return { rows: table.some((r) => r.kind === "seed")
        ? [{ revision: "1" } as T] : [] as T[] };
    if (text.includes("COALESCE(MAX(revision)")) {
      if (text.includes("kind = 'seed'")) {
        const seeds = table.filter((r) => r.kind === "seed");
        return { rows: seeds.length
          ? [{ revision: String(Math.max(...seeds.map((r) => Number(r.revision)))) } as T]
          : [] as T[] };
      }
      return { rows: [{ revision: String(
        table.length ? Math.max(...table.map((r) => Number(r.revision))) + 1 : 1) } as T] };
    }
    if (text.startsWith("INSERT")) {
      const isSeed = text.includes("'seed'");
      table.push({
        revision: params[0], kind: isSeed ? "seed" : params[1],
        org_id: params[2],
        workspace_id: params[3], scope: params[4],
        schema_generation: params[5], image_key: params[6],
        source_hash: params[7], prior_keyset_hash: params[8] ?? null,
        source_authority: params[9], prior_source_authority: params[10],
        pointer_set: params[11],
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
  pointerSet: { hash: "p".repeat(64), count: 1 },
};
describe("canonical_feature_changed (620 invoker)", () => {
  test("priorSource null -> publish entry appended with managed revision", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    // seed the counter above the legacy clock first
    await db.query("INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES($1,'seed','','','{}','','','{}','{}')", ["100"]);
    const rev = await fn.canonicalFeatureChanged({ ...base, priorSource: null });
    expect(rev).toBe("101");
    const row = db.table.find((r) => r.revision === "101")!;
    expect(row.kind).toBe("publish");
    expect(JSON.parse(row.source_authority as string)).toEqual(base.source);
    expect(row.prior_keyset_hash).toBe("");
  });
  test("replacement emits TWO ordered rows: retire(prior) then publish(new)", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await db.query("INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES($1,'seed','','','{}','','','{}','{}')", ["100"]);
    const prior = { epoch: "0", fingerprint: "b".repeat(64) };
    const priorPtr = { hash: "q".repeat(64), count: 1 };
    const rev = await fn.canonicalFeatureChanged({
      ...base, priorSource: prior, priorPointerSet: priorPtr });
    const rows = db.table.filter((r) => r.kind !== "seed")
      .sort((a, b) => Number(a.revision) - Number(b.revision));
    expect(rows.length).toBe(2);
    expect(rows[0]!.kind).toBe("retire");
    expect(rows[0]!.prior_keyset_hash).toBe(priorPtr.hash);   // prior SET hash
    expect(JSON.parse(rows[0]!.pointer_set as string)).toEqual(priorPtr);
    expect(rows[1]!.kind).toBe("publish");
    expect(rows[1]!.source_hash).toBe(base.source.fingerprint);
    expect(JSON.parse(rows[1]!.pointer_set as string)).toEqual(base.pointerSet); // NEW inventory
    expect(rev).toBe(rows[1]!.revision);
  });
  test("replacement without prior pointer set refuses — no partial write", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await db.query("INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES($1,'seed','','','{}','','','{}','{}')", ["100"]);
    await expect(fn.canonicalFeatureChanged({
      ...base,
      priorSource: { epoch: "0", fingerprint: "b".repeat(64) },
    })).rejects.toThrow("prior_incomplete");
    expect(db.table.filter((r) => r.kind !== "seed").length).toBe(0);
  });
  test("unseeded journal refuses next() — never allocates revision 1", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await expect(fn.canonicalFeatureChanged({ ...base, priorSource: null }))
      .rejects.toThrow("unseeded");
  });
  test("self-retire refused (prior fp == new fp)", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await db.query("INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES($1,'seed','','','{}','','','{}','{}')", ["100"]);
    await expect(fn.canonicalFeatureChanged({
      ...base,
      priorSource: { epoch: "0", fingerprint: base.source.fingerprint },
      priorPointerSet: { hash: "x".repeat(64), count: 1 },
    })).rejects.toThrow("self_retire");
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
