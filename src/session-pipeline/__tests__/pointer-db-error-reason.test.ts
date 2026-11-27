/**
 * Emitted JSON through the consumer path: a pointer failure with a pg-style 40001 logs "pointer index failed" with the
 * closed db_error_reason slug (an XX000: db_internal_reason), never the message text; other codes carry neither. Reason
 * extraction is best-effort: an unreadable message never rejects the batch. Synthetic ids only.
 */
import { expect, test } from "bun:test";
import { processBatch } from "../consumer.ts";

const KEY = "96bulls/org_synthetic/v1/ws=ws_synthetic/m=machine_a/h=claude/d=2026/11/06/s=session_synthetic/manifest.json";
const BODY = JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", s3: { object: { key: encodeURIComponent(KEY) } } }] });

/** The JSON lines written to stderr while `work` runs. */
async function stderrJson(work: () => Promise<void>): Promise<Record<string, unknown>[]> {
  const out: string[] = [];
  const original = process.stderr.write.bind(process.stderr);
  process.stderr.write = ((chunk: unknown) => { out.push(String(chunk)); return true; }) as typeof process.stderr.write;
  try { await work(); } finally { process.stderr.write = original; }
  return out.join("").split("\n").filter(Boolean).flatMap((l) => { try { return [JSON.parse(l) as Record<string, unknown>]; } catch { return []; } });
}

/** Pointer deps whose scope transaction raises `error` (as the pointer guard trigger would, under the lock). */
function pointerRaising(error: unknown) {
  return {
    db: { query: async () => ({ rows: [] }) }, reader: { provider: "s3" },
    orgs: { orgFor: async () => "org_synthetic" }, allowPrefix: () => true,
    lock: async () => { throw error; }, exists: async () => true, fanout: { send: async () => {} },
  } as never;
}

async function failedLines(error: unknown) {
  let result: unknown;
  const lines = await stderrJson(async () => { result = await processBatch([{ messageId: "m1", body: BODY }], { pointer: pointerRaising(error) }); });
  expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
  return lines;
}

test("emitted JSON: a 40001 pointer failure logs db_error_reason=pointer_mutation_busy and no message text", async () => {
  const lines = await failedLines(Object.assign(new Error("publication: pointer mutation busy"), { code: "40001" }));
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed).toHaveLength(1);
  expect(failed[0]).toMatchObject({ level: "error", stage: "session-indexed", error_code: "40001", db_error_reason: "pointer_mutation_busy" });
  expect(JSON.stringify(lines)).not.toContain("pointer mutation busy");
});

test("emitted JSON: an unknown 40001 message is other, and its text is never logged", async () => {
  const MARKER = "PRIVATE-MARKER-relation-secret_table";
  const lines = await failedLines({ code: "40001", message: `${MARKER} could not serialize` });
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed[0]).toMatchObject({ error_code: "40001", db_error_reason: "other" });
  expect(JSON.stringify(lines)).not.toContain(MARKER);
});

test("emitted JSON: a non-40001 pointer failure carries no db_error_reason", async () => {
  const lines = await failedLines({ code: "55P03", message: "publication: pointer mutation busy" });
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed[0]).toMatchObject({ error_code: "55P03" });
  expect("db_error_reason" in failed[0]! || "db_internal_reason" in failed[0]!).toBe(false);
  expect(JSON.stringify(lines)).not.toContain("pointer mutation busy");
});

/** A pg-style error whose `message` getter throws (a hostile or broken error object). */
function unreadable(code: string) {
  return { code, get message(): string { throw new Error("PRIVATE-MARKER-getter-text"); } };
}

test("processBatch: a 40001 with an unreadable message still fails only its item (partial batch), reason other, no raw text", async () => {
  // m2 is a non-manifest key under the same session: ignored, so it succeeds; m1's pointer failure is per-item.
  const other = KEY.replace("manifest.json", "chunk-0001.jsonl.zst");
  const body2 = JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", s3: { object: { key: encodeURIComponent(other) } } }] });
  let result: unknown;
  const lines = await stderrJson(async () => {
    result = await processBatch([{ messageId: "m1", body: BODY }, { messageId: "m2", body: body2 }], { pointer: pointerRaising(unreadable("40001")) });
  });
  expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed).toHaveLength(1);
  expect(failed[0]).toMatchObject({ level: "error", stage: "session-indexed", error_code: "40001", db_error_reason: "other" });
  expect(JSON.stringify(lines)).not.toContain("PRIVATE-MARKER");
});

test("processBatch: an XX000 with an unreadable message fails its item with db_internal_reason=other", async () => {
  const lines = await failedLines(unreadable("XX000"));
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed[0]).toMatchObject({ error_code: "XX000", db_internal_reason: "other" });
  expect("db_error_reason" in failed[0]!).toBe(false);
  expect(JSON.stringify(lines)).not.toContain("PRIVATE-MARKER");
});

test("emitted JSON: an XX000 logs its closed family, never the message text", async () => {
  const MARKER = "PRIVATE-MARKER-relation-secret_table";
  const lines = await failedLines(Object.assign(new Error(`cache lookup failed for relation ${MARKER}`), { code: "XX000" }));
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed).toHaveLength(1);
  expect(failed[0]).toMatchObject({ error_code: "XX000", db_internal_reason: "cache_lookup_failed" });
  expect("db_error_reason" in failed[0]!).toBe(false);
  expect(JSON.stringify(lines)).not.toContain(MARKER);
  expect(JSON.stringify(lines)).not.toContain("cache lookup failed");
});

/** An Error with no `code` whose name merely looks like a database code (errorFields' display fallback). */
function nameOnly(name: string, message: string) {
  return Object.assign(new Error(message), { name });
}

test("emitted JSON: a name-only 40001 (no code) keeps error_code=40001 for display but logs no db_error_reason", async () => {
  const lines = await failedLines(nameOnly("40001", "publication: pointer mutation busy"));
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed).toHaveLength(1);
  expect(failed[0]).toMatchObject({ level: "error", stage: "session-indexed", error_code: "40001" });
  expect("db_error_reason" in failed[0]!).toBe(false);
  expect("db_internal_reason" in failed[0]!).toBe(false);
  expect(JSON.stringify(lines)).not.toContain("pointer mutation busy");
});

test("emitted JSON: a name-only XX000 (no code) keeps error_code=XX000 for display but logs no db_internal_reason", async () => {
  const lines = await failedLines(nameOnly("XX000", "could not open relation with OID 1"));
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed).toHaveLength(1);
  expect(failed[0]).toMatchObject({ error_code: "XX000" });
  expect("db_error_reason" in failed[0]!).toBe(false);
  expect("db_internal_reason" in failed[0]!).toBe(false);
  expect(JSON.stringify(lines)).not.toContain("could not open relation");
});

test("emitted JSON: a non-string code with a 40001 name gets no reason (only a real string code counts)", async () => {
  const lines = await failedLines(Object.assign(new Error("publication: pointer mutation busy"), { name: "40001", code: 40001 }));
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed[0]).toMatchObject({ error_code: "40001" });
  expect("db_error_reason" in failed[0]!).toBe(false);
});

test("emitted JSON: a code read once — a getter that changes value cannot desync error_code and the reason", async () => {
  let reads = 0;
  const err = { get code(): string { reads += 1; return reads === 1 ? "55P03" : "40001"; }, message: "publication: pointer mutation busy" };
  const lines = await failedLines(err);
  const failed = lines.filter((l) => l.message === "pointer index failed");
  expect(failed[0]).toMatchObject({ error_code: "55P03" });
  expect("db_error_reason" in failed[0]!).toBe(false);
  expect(reads).toBe(1);
});
