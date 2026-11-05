import { describe, expect, test } from "bun:test";
import { CanonicalPageAckStore } from "../canonical-ack-store.ts";

const fakeDb = () => {
  const ack: Record<string, unknown>[] = [];
  const state: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("canonical_feature_page_ack") && text.startsWith("INSERT")) {
      if (!ack.some((a) => a.cursor === params[2] && a.pool === params[1]))
        ack.push({ image_key: params[0], pool: params[1], cursor: params[2],
          page_sha256: params[3] });
      return { rows: [] as T[] };
    }
    if (text.includes("canonical_feature_page_ack"))
      return { rows: ack.filter((a) => a.image_key === params[0] &&
        a.pool === params[1]) as T[] };
    if (text.includes("canonical_feature_copy_state") && text.startsWith("SELECT"))
      return { rows: state.filter((s) => s.image_key === params[0])
        .map((s) => ({ finished: s.finished })) as T[] };
    if (text.includes("canonical_feature_copy_state") && text.includes("INSERT")) {
      const existing = state.find((s) => s.image_key === params[0]);
      if (!existing) {
        state.push({ image_key: params[0], finished: true,
          source_authority: params[4], profile_binding: params[5] });
        return { rows: [{ n: 1 } as T] };
      }
      if (existing.finished || existing.source_authority !== params[4] ||
          existing.profile_binding !== params[5])
        return { rows: [] as T[] };   // CAS conflict
      existing.finished = true;
      return { rows: [{ n: 1 } as T] };
    }
    return { rows: [] as T[] };
  };
  return { ack, state, query };
};
const entry = { kind: "publish" as const, revision: "10", imageKey: "i",
  schemaGeneration: "g", sourceHash: "a".repeat(64),
  scope: { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
           generation: "a".repeat(64) },
  sourceAuthority: { authorityId: "u", epoch: "1", fingerprint: "a".repeat(64) },
  pointerSet: { authorityId: "u", hash: "p".repeat(64), count: "1" } };
const AUTH = { authorityId: "u", epoch: "1", fingerprint: "a".repeat(64) };
describe("canonical page ack store", () => {
  test("ackPage idempotent + ackedCursors + resume", async () => {
    const db = fakeDb();
    const s = new CanonicalPageAckStore({ query: db.query });
    await s.ackPage({ entry, kind: "event", cursor: "p1", pageSha256: "h1" });
    await s.ackPage({ entry, kind: "event", cursor: "p1", pageSha256: "h1" }); // idempotent
    await s.ackPage({ entry, kind: "brief", cursor: "b1", pageSha256: "h2" });
    expect(await s.ackedCursors(entry, "event")).toEqual(["p1"]);
    const r = await s.resume(entry);
    expect(r.event).toBe("p1"); expect(r.brief).toBe("b1");
  });
  test("finish CAS: first succeeds, repeat refuses, changed binding refuses", async () => {
    const db = fakeDb();
    const s = new CanonicalPageAckStore({ query: db.query });
    await s.finish({ entry, terminalEvent: "te", terminalBrief: "tb",
      sourceAuthority: AUTH, profileBinding: "pb" });
    await expect(s.finish({ entry, terminalEvent: "te", terminalBrief: "tb",
      sourceAuthority: AUTH, profileBinding: "pb" }))
      .rejects.toThrow("already_finished");
  });
});
