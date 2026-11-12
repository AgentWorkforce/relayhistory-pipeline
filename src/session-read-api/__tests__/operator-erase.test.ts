import { expect, test } from "bun:test";
import { createReadApi } from "../app.ts";
import { memoryLiveness } from "./support.ts";
const path = "/v1/sessions/s/summary?workspace=w&source=claude&kind=short";
function fixture(mode: "before" | "during" | "outage" | "off") {
 let calls = 0, reads = 0, erased = mode === "before";
 const app = createReadApi({ verify: async () => ({ orgId: "o", workspaceId: "w", userId: "u", scopes: ["rth:read"] }),
  ch: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0, directoryWorkspaces: "*",
  ...(mode === "off" ? {} : { operatorErased: async () => { calls++; if (mode === "outage") throw Error("secret db response"); return erased; } }),
  briefSummary: { async read() { reads++; if (mode === "during") erased = true;
   return [{ org_id: "o", workspace_id: "w", harness: "claude", session_id: "s", description: "private summary", model: "model", generated_at: new Date() }]; } } });
 return { get: (auth = true) => app.request(path, { headers: auth ? { authorization: "Bearer fixture" } : {} }), stats: () => ({ calls, reads }) };
}
test("auth precedes fence lookup; a fenced session never reaches summary storage", async () => {
 const f = fixture("before"); expect((await f.get(false)).status).toBe(401); expect(f.stats()).toEqual({ calls: 0, reads: 0 });
 expect((await f.get()).status).toBe(404); expect(f.stats()).toEqual({ calls: 1, reads: 0 });
});
test("a fence during I/O replaces the successful body; outage fails closed without provider text", async () => {
 const f = fixture("during"), r = await f.get(); expect(r.status).toBe(404); expect(await r.text()).not.toContain("private summary"); expect(f.stats()).toEqual({ calls: 2, reads: 1 });
 const o = fixture("outage"), failure = await o.get(); expect(failure.status).toBe(503); expect(await failure.text()).not.toContain("secret db response"); expect(o.stats().reads).toBe(0);
});
test("default-off preserves existing summary behavior", async () => { const f = fixture("off"); expect((await f.get()).status).toBe(200); expect(f.stats()).toEqual({ calls: 0, reads: 1 }); });
