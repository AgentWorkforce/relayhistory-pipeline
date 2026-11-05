import { describe, expect, test } from "bun:test";
import { CanonicalFeatureChanged } from "../canonical-feature-changed.ts";

// The impl is a thin client of the owner-only SQL invoker — the fake emulates
// sessions.canonical_feature_changed: seed-check, allocate, append pair.
const fakeDb = () => {
  const table: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("sessions.canonical_feature_changed")) {
      const [scope, source, priorSource, pointerSet, priorPointerSet] =
        params.map((p) => (p ? JSON.parse(p as string) : null));
      if (!table.some((r) => r.kind === "seed"))
        throw new Error("unseeded");
      let rev = Math.max(...table.map((r) => Number(r.revision ?? 0)), 100);
      if (priorSource) {
        rev += 1;
        table.push({ revision: String(rev), kind: "retire",
          source_hash: priorSource.fingerprint,
          prior_keyset_hash: priorPointerSet.hash,
          prior_source_authority: JSON.stringify(priorSource),
          pointer_set: JSON.stringify(priorPointerSet),
          scope: JSON.stringify(scope), org_id: scope.orgId });
      }
      rev += 1;
      table.push({ revision: String(rev), kind: "publish",
        source_hash: source.fingerprint,
        prior_keyset_hash: priorSource ? priorPointerSet.hash : null,
        source_authority: JSON.stringify(source),
        prior_source_authority: priorSource ? JSON.stringify(priorSource) : null,
        pointer_set: JSON.stringify(pointerSet),
        scope: JSON.stringify(scope), org_id: scope.orgId });
      return { rows: [{ rev: String(rev) } as T] };
    }
    if (text.startsWith("INSERT")) {
      table.push({ revision: params[0], kind: "seed" });
      return { rows: [] as T[] };
    }
    return { rows: [] as T[] };
  };
  return { table, query };
};
const base = {
  scope: { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
           generation: "a".repeat(64) },
  source: { authorityId: "12345678-1234-1234-1234-123456789012",
            epoch: "1", fingerprint: "a".repeat(64) },
  pointerSet: { authorityId: "12345678-1234-1234-1234-123456789012",
                hash: "c".repeat(64), count: "1" },
};
const seed = (db: ReturnType<typeof fakeDb>) =>
  db.query("INSERT INTO x(revision,kind) VALUES($1,'seed')", ["100"]);
describe("canonical_feature_changed (620 invoker client)", () => {
  test("priorSource null -> one publish row, managed revision", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await seed(db);
    const rev = await fn.canonicalFeatureChanged({ ...base, priorSource: null });
    expect(rev).toBe("101");
    const row = db.table.find((r) => r.revision === "101")!;
    expect(row.kind).toBe("publish");
    expect(JSON.parse(row.source_authority as string)).toEqual(base.source);
  });
  test("replacement -> ordered retire(prior) then publish(new) pair", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await seed(db);
    const prior = { authorityId: "87654321-4321-4321-4321-210987654321",
                    epoch: "0", fingerprint: "b".repeat(64) };
    const priorPtr = { authorityId: "87654321-4321-4321-4321-210987654321",
                       hash: "d".repeat(64), count: "1" };
    const rev = await fn.canonicalFeatureChanged({
      ...base, priorSource: prior, priorPointerSet: priorPtr });
    const rows = db.table.filter((r) => r.kind !== "seed")
      .sort((a, b) => Number(a.revision) - Number(b.revision));
    expect(rows.length).toBe(2);
    expect(rows[0]!.kind).toBe("retire");
    expect(rows[0]!.prior_keyset_hash).toBe(priorPtr.hash);
    expect(JSON.parse(rows[0]!.pointer_set as string)).toEqual(priorPtr);
    expect(rows[1]!.kind).toBe("publish");
    expect(rows[1]!.source_hash).toBe(base.source.fingerprint);
    expect(rev).toBe(rows[1]!.revision);
  });
  test("unseeded journal refuses — SQL throws before any write", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await expect(fn.canonicalFeatureChanged({ ...base, priorSource: null }))
      .rejects.toThrow("unseeded");
  });
  test("prior params must be both-or-neither", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await seed(db);
    await expect(fn.canonicalFeatureChanged({
      ...base, priorSource: { authorityId: "u", epoch: "0", fingerprint: "b".repeat(64) },
    })).rejects.toThrow("prior_incomplete");
  });
  test("self-retire refused", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await seed(db);
    await expect(fn.canonicalFeatureChanged({
      ...base,
      priorSource: { authorityId: "87654321-4321-4321-4321-210987654321",
                     epoch: "0", fingerprint: base.source.fingerprint },
      priorPointerSet: { authorityId: "87654321-4321-4321-4321-210987654321",
                         hash: "d".repeat(64), count: "1" },
    })).rejects.toThrow("self_retire");
  });
  test("invalid input refuses before any write", async () => {
    const db = fakeDb();
    const fn = new CanonicalFeatureChanged({ query: db.query });
    await seed(db);
    await expect(fn.canonicalFeatureChanged({
      scope: { orgId: "", workspaceId: "w", harness: "h", sessionId: "s" },
      source: base.source, priorSource: null, pointerSet: base.pointerSet,
    })).rejects.toThrow("invalid input");
    expect(db.table.filter((r) => r.kind !== "seed").length).toBe(0);
  });
});
