import { describe, expect, test } from "bun:test";
import { copyCanonicalEntry } from "../canonical-cycle.ts";

const A = "a".repeat(64);
const entry = { kind: "publish" as const, revision: "10", imageKey: "i",
  schemaGeneration: A, sourceHash: A,
  scope: { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
    generation: A },
  sourceAuthority: { authorityId: "u", epoch: "1", fingerprint: A },
  pointerSet: { authorityId: "u", hash: A, count: "1" } };
const h = (k: unknown) =>
  require("node:crypto").createHash("sha256")
    .update(JSON.stringify(k)).digest("hex");
const page = { rows: [], cursor: null, source: "i",
  pointerSetHash: A, pageSha256: h([]) };
const images = { imageDescriptor: async () => ({ imageKey: "i",
    sourceHash: A, pointerSetHash: A, complete: true }),
  imagePages: async () => page,
  priorKeyPages: async () => page };
const ch = { database: "d", insert: async () => {} };
const acks = { begin: async () => "started" as const,
  resumeCursor: async () => ({ state: "empty" as const, cursor: null }),
  ackPage: async () => {}, finish: async () => {} };
describe("copy abort fences at every boundary", () => {
  test("abort during beforeWrite stops before CH", async () => {
    const c = new AbortController();
    let chTouched = 0, ackTouched = 0;
    const out = await copyCanonicalEntry(images, {
      ...acks, ackPage: async () => { ackTouched++; } },
      { ...ch, insert: async () => { chTouched++; } },
      { checkpoint: "0" }, entry, "t",
      { beforeWrite: async () => { c.abort(); return true; } },
      c.signal);
    expect(out.kind).toBe("refused");
    expect((out as { reason: string }).reason).toBe("aborted");
    expect(chTouched + ackTouched).toBe(0);
  });
  test("abort during CH stops before ackPage", async () => {
    const c = new AbortController();
    let ackTouched = 0;
    const out = await copyCanonicalEntry(images, {
      ...acks, ackPage: async () => { ackTouched++; } },
      { ...ch, insert: async () => { c.abort(); } },
      { checkpoint: "0" }, entry, "t",
      { beforeWrite: async () => true }, c.signal);
    expect((out as { reason?: string }).reason).toBe("aborted");
    expect(ackTouched).toBe(0);
  });
  test("abort during beforeFinish stops before finish", async () => {
    const c = new AbortController();
    let finTouched = 0;
    const out = await copyCanonicalEntry(images, {
      ...acks, finish: async () => { finTouched++; } },
      ch, { checkpoint: "0" }, entry, "t",
      { beforeWrite: async () => true,
        beforeFinish: async () => { c.abort(); return true; } },
      c.signal);
    expect((out as { reason?: string }).reason).toBe("aborted");
    expect(finTouched).toBe(0);
  });
});
