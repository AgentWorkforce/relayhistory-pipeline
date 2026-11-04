import { expect, it } from "bun:test";
import { derivedPublication, withDerivedCurrent } from "../derived-publication.ts";
import type { ManifestV1, StageMessage } from "../vendor.ts";
const generation = "11111111-1111-1111-1111-111111111111", schema = "a".repeat(64);
const key = `96bulls/o/upload-generations/g=${generation}/v1/ws=w/m=srv_m/h=claude/d=2026/11/04/s=s/publications/22222222-2222-2222-2222-222222222222/manifest.json`;
const m = { revision: 2, session: { workspace_id: "w", harness: "claude", session_id: "s", machine_id: "srv_m" } } as ManifestV1;
const message = { ctx: { orgId: "o" } } as StageMessage;
const current = { managed: true, current: true, generation, revision: "2" };

it("managed keys cannot enter without compiled authority; contradictory current generation/revision never falls back", async () => {
  const noDb = { query: async () => { throw new Error("unexpected SQL"); } } as never;
  await expect(derivedPublication(noDb, message, m, key)).rejects.toMatchObject({ code: "derived_managed_authority_unavailable" });
  await expect(derivedPublication(noDb, message, m, key, "bad")).rejects.toMatchObject({ code: "derived_generation_invalid" });
  for (const bad of [{ ...current, generation: "33333333-3333-3333-3333-333333333333" }, { ...current, revision: "3" }, { managed: false, current: true, generation: null, revision: "2" }]) {
    const db = { query: async () => ({ rows: [{ result: bad }] }) } as never;
    await expect(derivedPublication(db, message, m, key, schema)).rejects.toMatchObject({ code: "derived_publication_invalid" });
  }
});

it("legacy events also probe the compiled generation and cannot write after a managed takeover", async () => {
  const calls: string[] = []; let writes = 0;
  const query = async (sql: string, params?: unknown[]) => {
    calls.push(sql);
    if (sql.includes("session_publication_")) {
      const evidence = JSON.parse(String(params?.[0]));
      expect(evidence.schemaGeneration).toBe(schema);
      expect(evidence.manifestKey).toBe("legacy-key");
      return { rows: [{ result: sql.includes("_probe")
        ? { managed: false, current: true, generation: null, revision: "2" }
        : { managed: true, current: false, generation, revision: "3" } }] };
    }
    if (sql.includes("session_index")) throw new Error("stale source must not reach the index lock");
    return { rows: [] };
  };
  const db = { query, pool: { connect: async () => ({ query, release() {} }) } } as never;
  const publication = await derivedPublication(db, message, m, "legacy-key", schema);
  const result = await withDerivedCurrent({ db, message, manifest: m, key: "legacy-key", publication }, async () => { writes++; });
  expect(result).toEqual({ current: false }); expect(writes).toBe(0);
  expect(calls.at(-1)).toBe("COMMIT");
});
