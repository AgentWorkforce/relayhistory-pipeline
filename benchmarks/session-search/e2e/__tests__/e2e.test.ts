/** Harness logic with no engine. Engine gates, with their negative controls, are in proof.ts. */
import { describe, expect, test } from "bun:test";
import { SCALES } from "../../corpus.ts";
import { recordText } from "../index-stage.ts";
import { checkShapeFile } from "../shape.ts";
import { ClickHouseHttp, SessionSearchStore } from "../store.ts";
import { NEEDLES, Oracle, oracleKey, transcriptBatches } from "../transcripts.ts";

describe("store chokepoint", () => {
  test("refuses a non-local engine before any socket", () => {
    expect(() => new ClickHouseHttp("https://example.clickhouse.cloud:8443", "u", "p")).toThrow("refusing non-local target");
  });
  test("refuses an unscoped search", async () => {
    const local = new ClickHouseHttp("http://127.0.0.1:1", "u", "p");
    const s = new SessionSearchStore(local, local);
    await expect(s.search({ orgId: "", workspaceIds: ["w"] }, { query: "x" })).rejects.toThrow("search_scope_required");
    await expect(s.search({ orgId: "o", workspaceIds: [] }, { query: "x" })).rejects.toThrow("search_scope_required");
    await expect(s.search({ orgId: "o", workspaceIds: ["w"] }, { query: "x".repeat(501) })).rejects.toThrow("invalid_query");
  });
});

describe("a failed response is never a page", () => {
  const row = JSON.stringify({ workspace_id: "w", harness: "h", session_id: "s", machine_id: "m", event_id: "e", at: "1", snippet: "x" });
  const against = async (status: number, body: string, run: (s: SessionSearchStore) => Promise<unknown>) => {
    const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response(body, { status }) });
    try {
      const http = new ClickHouseHttp(`http://127.0.0.1:${server.port}`, "u", "p");
      return await run(new SessionSearchStore(http, http)).then(() => "accepted", (e) => String(e));
    } finally { server.stop(true); }
  };
  const scope = { orgId: "o", workspaceIds: ["w"] };
  test("408 with partial rows is refused", async () => {
    expect(await against(408, `${row}\nCode: 159. DB::Exception: Timeout exceeded`, (s) => s.search(scope, { query: "abc" }))).toContain("clickhouse 408");
  });
  test("200 with an exception after rows is refused", async () => {
    expect(await against(200, `${row}\nCode: 159. DB::Exception: Timeout exceeded`, (s) => s.search(scope, { query: "abc" }))).not.toBe("accepted");
  });
  test("control: a complete 200 is accepted", async () => {
    expect(await against(200, `${row}\n`, (s) => s.search(scope, { query: "abc" }))).toBe("accepted");
  });
});

describe("recordText", () => {
  test("indexes values, not keys or envelope fields", () => {
    const r = recordText(JSON.stringify({ type: "user", uuid: "abc", timestamp: "2026-10-30T10:00:00.000Z", message: { role: "user", content: [{ text: "needle" }] } }));
    expect(r).toEqual({ text: "needle", kind: "user", ts: "2026-10-30T10:00:00.000Z" });
  });
  test("keeps a line that is not JSON", () => {
    expect(recordText("plain").text).toBe("plain");
  });
});

describe("shape file", () => {
  test("accepts aggregate counts", () => {
    expect(checkShapeFile({ events: 4_080_000, whaleOrgEvents: 1_300_000, orgs: 40, shareEventsOver4KB: 0.02 })).toEqual([]);
  });
  test("refuses anything that is not an allowed number", () => {
    expect(checkShapeFile({ topQueries: ["deploy"] })).toEqual(["unknown key topQueries"]);
    expect(checkShapeFile({ whaleOrgName: "acme" })).toEqual(["unknown key whaleOrgName"]);
    expect(checkShapeFile({ events: "4080000" })).toEqual(["events must be a non-negative number"]);
    expect(checkShapeFile({ orgs: 3 })).toEqual(["orgs under 10 could identify a tenant"]);
    expect(checkShapeFile([])).toEqual(["shape must be a flat object"]);
  });
});

describe("corpus and oracle", () => {
  const run = () => { const o = new Oracle(); const rows = [...transcriptBatches(SCALES.test!, o)].flat(); return { o, rows }; };
  test("is deterministic, carries no planted secret, and agrees with a scan", () => {
    const a = run(), b = run();
    expect(a.rows.length).toBe(b.rows.length);
    expect(a.rows.some((r) => r.text.includes("rk_live_"))).toBe(false);
    for (const n of NEEDLES.filter((x) => x.id !== "short")) {
      const q = n.query.toLowerCase();
      const scan = a.rows.filter((r) => r.orgId === "org-1" && r.text.toLowerCase().includes(q)).length;
      expect(`${n.id}:${a.o.counts.get(oracleKey("org-1", n.id)) ?? 0}`).toBe(`${n.id}:${scan}`);
    }
  });
  test("a beyond-prefix needle really sits past 4 KB", () => {
    const { rows } = run();
    const hit = rows.find((r) => r.text.includes("tailbeacon"));
    expect(hit && hit.text.indexOf("tailbeacon")).toBeGreaterThan(4096);
  });
});
