import { expect, test } from "bun:test";
import { nextFullModelInput, prepareFullModelRow, FULL_MODEL_INPUT_BYTES } from "../full-model-input.ts";
import { createHash } from "node:crypto";
const signal = new AbortController().signal;
const binding = "a".repeat(64);
const row = (n: number) => ({ table: "convergence_events", row: { event_id: "event-" + n, accepted_seq: "9007199254740993",
  actor_role: n % 2 ? "user" : "assistant", content: "message " + n, tool_call: { arguments: [{ n }] }, record: { payload: { native_id: String(9000 + n) } } } });

test("frames every canonical row after the old ten-prompt sample, preserving arbitrary fields", async () => {
  const rows = Array.from({ length: 27 }, (_, n) => row(n)), fragments: any[] = [];
  let cursor: import("../full-model-input.ts").ModelInputCursor = { rowOrdinal: "0", utf16Offset: 0 };
  for (;;) {
    const part = await nextFullModelInput({ sourceBinding: binding, cursor, signal, readRow: async ordinal => rows[Number(ordinal)] ?? null });
    expect(Buffer.byteLength(part.input)).toBeLessThanOrEqual(FULL_MODEL_INPUT_BYTES);
    const envelope = JSON.parse(part.input); fragments.push(...envelope.fragments);
    cursor = part.next;
    if (part.inputComplete) break;
  }
  const rebuilt = new Map<string, string>();
  for (const f of fragments) rebuilt.set(f.rowOrdinal, (rebuilt.get(f.rowOrdinal) ?? "") + f.jsonFragment);
  expect([...rebuilt.values()]).toEqual(rows.map(r => JSON.stringify(r)));
  expect(rebuilt.get("26")).toContain("message 26");
  expect(rebuilt.get("26")).toContain("9007199254740993");
  expect(cursor).toEqual({ rowOrdinal: "27", utf16Offset: 0 });
});

test("segments a multi-megabyte message and arbitrary tool arrays losslessly across durable units", async () => {
  const big = row(0);
  big.row.content = "secret-shaped user data 😀\\u0000" .repeat(90_000) + "\ud800";
  big.row.tool_call.arguments = Array.from({ length: 10_000 }, (_, n) => ({ n }));
  const exact = prepareFullModelRow(big), parts: string[] = [], offsets: number[] = [];
  let cursor: import("../full-model-input.ts").ModelInputCursor = { rowOrdinal: "0", utf16Offset: 0 }, calls = 0;
  for (;;) {
    const out = await nextFullModelInput({ sourceBinding: binding, cursor, signal, readRow: async ordinal => ordinal === "0" ? big : null });
    calls++;
    expect(Buffer.byteLength(out.input)).toBeLessThanOrEqual(FULL_MODEL_INPUT_BYTES);
    for (const f of JSON.parse(out.input).fragments) { expect(f.rowSha256).toBe(exact.sha256); parts.push(f.jsonFragment); offsets.push(f.utf16Offset); }
    cursor = out.next; if (out.inputComplete) break;
  }
  expect(calls).toBeGreaterThan(20);
  expect(parts.join("")).toBe(exact.json);
  expect(createHash("sha256").update(parts.join("")).digest("hex")).toBe(exact.sha256);
  expect(new Set(offsets).size).toBe(offsets.length);
  expect(JSON.parse(parts.join("")).row.tool_call.arguments).toHaveLength(10_000);
});

test("missing partial row and canceled reads refuse instead of declaring complete", async () => {
  await expect(nextFullModelInput({ sourceBinding: binding, cursor: { rowOrdinal: "1", utf16Offset: 3, rowSha256: "a".repeat(64) }, signal, readRow: async () => null }))
    .rejects.toMatchObject({ code: "summary_source_missing_row" });
  const c = new AbortController(); c.abort();
  let reads = 0;
  await expect(nextFullModelInput({ sourceBinding: binding, cursor: { rowOrdinal: "0", utf16Offset: 0 }, signal: c.signal,
    readRow: async () => { reads++; return row(0); } })).rejects.toBeDefined();
  expect(reads).toBe(0);
});


test("partial continuation cannot splice a changed canonical row", async () => {
  const original = row(0); original.row.content = "x".repeat(100_000);
  const first = await nextFullModelInput({ sourceBinding: binding, cursor: { rowOrdinal: "0", utf16Offset: 0 }, signal,
    readRow: async ordinal => ordinal === "0" ? original : null });
  expect(first.next.utf16Offset).toBeGreaterThan(0);
  await expect(nextFullModelInput({ sourceBinding: binding, cursor: first.next, signal,
    readRow: async () => ({ ...original, row: { ...original.row, content: "changed" } }) }))
    .rejects.toMatchObject({ code: "summary_source_row_changed" });
});

test("uses official raw codec for nested large integers in model fragments and row SHA",async()=>{
 const {parseRelayhistoryJsonExact}=await import("../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts");
 const value={table:"conversation_turns",row:{record:{nested:[9007199254740993n,-9223372036854775808n]},content:"exact"}};
 const encoded=prepareFullModelRow(value as never);
 expect(encoded.json).toContain("9007199254740993");expect(encoded.json).not.toContain('"9007199254740993"');
 expect(encoded.sha256).toBe(createHash("sha256").update(encoded.json).digest("hex"));
 const part=await nextFullModelInput({sourceBinding:binding,cursor:{rowOrdinal:"0",utf16Offset:0},signal,readRow:async n=>n==="0"?value as never:null});
 const text=JSON.parse(part.input).fragments.map((x:any)=>x.jsonFragment).join("");
 expect(parseRelayhistoryJsonExact(text)).toEqual(value);expect(part.inputComplete).toBe(true);
});
