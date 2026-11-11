import { expect, test } from "bun:test";
import { ARTIFACT_KEYS, erasureReceipt, SessionEraseServiceError } from "../receipt.ts";
import { eraseRuntimeSettings } from "../flags.ts";
import { sessionEraseWorker, sqlSessionWriteGuard, type EraseLedger, type EraseWork } from "../runtime.ts";
const key = { orgId: "o", workspaceId: "w", source: "claude", sessionId: "s" };
const identity = { bucket: "b", keyRoot: "root", storeRef: "a".repeat(64) };
const work: EraseWork = { ...key, ...identity, sessionIds: ["s"] };
const receipt = () => ({ ...key, requestId: "r", status: "in_progress", requestedAt: new Date().toISOString(), completedAt: null,
 unreadable: true, unreadableAt: new Date().toISOString(), bytesErased: false, bytesErasedAt: null, uploadsFenced: true, legacyScope: "excluded",
 artifacts: Object.fromEntries(ARTIFACT_KEYS.map(k => [k, { count: 0, state: "pending" }])), failure: null });
test("wire receipt strips untrusted extras and refuses tenant mismatch, false completion, missing artifacts and unsafe counts", () => {
 const raw = { ...receipt(), transcript: "must not cross trust boundary", actor: { id: "not public" } };
 expect(erasureReceipt(raw, key)).not.toHaveProperty("transcript");
 expect(erasureReceipt(raw, key)).not.toHaveProperty("actor");
 for (const bad of [{ ...raw, orgId: "foreign" }, { ...raw, status: "completed", completedAt: new Date().toISOString() },
  { ...raw, artifacts: {} }, { ...raw, artifacts: { ...raw.artifacts, transcriptObjects: { count: Number.MAX_SAFE_INTEGER + 1, state: "done" } } }])
  expect(() => erasureReceipt(bad, key)).toThrow(SessionEraseServiceError);
});
test("activation is explicit and sync-only; typos and partial activation fail startup", () => {
 expect(eraseRuntimeSettings({}, { enabled: false })).toEqual({});
 for (const env of [{ SESSION_ERASE_BYTES_ENABLED: "1" }, { SESSION_ERASE_FENCE_ENFORCED: "true" }])
  expect(() => eraseRuntimeSettings(env, { enabled: false })).toThrow();
 expect(() => eraseRuntimeSettings({}, { enabled: true })).toThrow();
 expect(() => eraseRuntimeSettings({ SESSION_ERASE_FENCE_ENFORCED: "1" }, { enabled: true }, {})).toThrow(/synchronous/);
});
test("an ambiguous writer failure retains its durable obligation; fence denial never invokes object I/O", async () => {
 const calls: string[] = [];
 const guard = sqlSessionWriteGuard(async text => { calls.push(text); return { rows: [{ result: true }] }; });
 await expect(guard(key, [["claude", "s"]], async () => { throw Error("lost object-store response"); })).rejects.toThrow();
 expect(calls).toHaveLength(1);
 expect(calls[0]).toContain("write_begin");
 await guard(key, [["claude", "s"]], async () => 42);
 expect(calls).toHaveLength(3); expect(calls[2]).toContain("write_finish");
 const denied = sqlSessionWriteGuard(async () => ({ rows: [{ result: false }] }));
 let writes = 0;
 await expect(denied(key, [["claude", "s"]], async () => writes++)).rejects.toThrow();
 expect(writes).toBe(0);
});
function fixture(options: { authorize?: boolean; stages?: boolean; verified?: boolean; failSearch?: boolean; storeRef?: string } = {}) {
 const calls: string[] = [];
 const ledger = { next: async () => ({ ...work, storeRef: options.storeRef ?? work.storeRef }), authorize: async () => options.authorize !== false,
  release: async () => { calls.push("release"); }, purge: async () => { calls.push("purge"); },
  step: async (_w: unknown, _lease: string, artifact: string, state: string) => { calls.push(`${artifact}:${state}`); } } as unknown as EraseLedger;
 const worker = sessionEraseWorker({ ledger, identity,
  bytes: { stagesAbsent: async () => options.stages !== false, erase: async () => { calls.push("bytes"); return { verified: options.verified !== false, transcriptObjects: 1, pointerObjects: 1 } as any; } },
  search: { erase: async () => { calls.push("search"); if (options.failSearch) throw Error("secret provider body"); } },
  searchLock: async (_w, fn) => { calls.push("lock"); return fn(); } });
 return { worker, calls };
}
test("pending writer, mixed stages, partial byte inventory and wrong store cannot report completion", async () => {
 for (const options of [{ authorize: false }, { stages: false }, { verified: false }, { storeRef: "b".repeat(64) }]) {
  const f = fixture(options); await f.worker.scan();
  expect(f.calls).not.toContain("purge"); expect(f.calls).not.toContain("search");
  expect(f.calls).not.toContain("historyDerivedObjects:not_applicable"); expect(f.calls.at(-1)).toBe("release");
 }
});
test("search failure preserves pending metadata; verified success holds search lock across purge", async () => {
 const failed = fixture({ failSearch: true }); await failed.worker.scan();
 expect(failed.calls).not.toContain("purge"); expect(failed.calls).toContain("searchDocuments:failed_retryable");
 const success = fixture(); await success.worker.scan();
 expect(success.calls.slice(0, 5)).toEqual(["bytes", "transcriptObjects:done", "lock", "search", "purge"]);
 expect(success.calls.at(-2)).toBe("historyDerivedObjects:not_applicable");
});

test("write guard passes canonical and encoded physical identities to the same barrier",async()=>{
 let pairs:unknown;
 const guard=sqlSessionWriteGuard(async(text,values)=>{if(text.includes("write_begin"))pairs=JSON.parse(values[0] as string);return {rows:[{result:true}]};});
 await guard({orgId:"o",workspaceId:"w",userId:"not-an-rpc-field"} as any,[["claude","a:b"]],async()=>{});
 expect(pairs).toMatchObject({sessions:[["claude","a:b"]],physicalSessions:[["claude","a_b"]]});
 expect(pairs).not.toHaveProperty("userId");
});

test("a fenced physical namespace refuses permanently before any object work",async()=>{
 let writes=0;
 const guard=sqlSessionWriteGuard(async()=>({rows:[{result:false}]}));
 await expect(guard(key,[["claude","a:b"]],async()=>{writes++;})).rejects.toMatchObject({code:"operator_session_erased",status:409});
 expect(writes).toBe(0);
});
