import { describe, expect, test } from "bun:test";
import { captureFrontierReceiptSource, frontierReceiptDomainSha256, FRONTIER_RECEIPT_READER_ROLE, RECEIPT_CAPTURE_IDENTITY_SQL, RECEIPT_CAPTURE_PAGE_SQL, RECEIPT_CAPTURE_PROTOCOL, type FrontierReceiptReadRequest } from "../frontier-receipt-source.ts";
import { frontierSha256 } from "../frontier-plan.ts";
import type { FrontierCaptureSql } from "../frontier-capture-source.ts";
const h = (x: string) => frontierSha256(x);
const tenant = { orgId: "fixture-org", workspaceId: "fixture-ws", userId: "fixture-user", originId: "fixture-origin" };
const input: FrontierReceiptReadRequest = { tenant, generation: h("gen"), profileId: "11111111-1111-4111-8111-111111111111", profileSha256: h("profile"), functionSha256: h("function"), functionOwner: "fixture_schema_owner",
  bounds: { pageLimit: 1, maxPages: 4, maxDecisions: 10, maxBytes: 20_000, maxDurationMs: 10_000 } };
const authority = { sourceIdentitySha256: h("source"), sourceGenerationSha256: h("generation"), sourceHead: "a".repeat(40), controlEpoch: h("control"), privacyBaselineSha256: h("privacy"), privacyVersion: "3" };
const winner = { tenant, recordId: "record", revision: "7", revisionId: "revision", source: "copilot", sessionId: "synthetic-session", seat: "12", bodyAccepted: true, candidateDigest: "a".repeat(64), firstDigest: "a".repeat(64) };
const loser = { ...winner, revisionId: "revision-losing", candidateDigest: "b".repeat(64), bodyAccepted: false };
function fixture() {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const records = [winner, loser];
  let index = 0, transaction = 0;
  const identity: Record<string, unknown> = { read_only: "on", isolation: "repeatable read", role: FRONTIER_RECEIPT_READER_ROLE, superuser: false, inherit: false, create_role: false,
    create_db: false, replication: false, bypass_rls: false, can_login: false, security_definer: true, volatility: "s", config: ["search_path=pg_catalog, pg_temp"], function_owner: input.functionOwner, definition_sha256: input.functionSha256 };
  const f = { calls, records, identity, mutate: (_: Record<string, unknown>, _page: number, _transaction: number) => {},
    db: { async query(text: string, params?: unknown[]) {
      calls.push({ text, params });
      if (text.startsWith("BEGIN")) { transaction++; index = 0; }
      if (text === RECEIPT_CAPTURE_IDENTITY_SQL) return { rows: [identity] };
      if (text !== RECEIPT_CAPTURE_PAGE_SQL) return { rows: [] };
      const request = JSON.parse(params![0] as string);
      const rows = records.slice(index, index + request.limit); index += rows.length;
      const last = rows.at(-1);
      const page = { protocol: RECEIPT_CAPTURE_PROTOCOL, profileId: input.profileId, profileSha256: input.profileSha256, tenant, generation: input.generation,
        authority, snapshot: `snapshot-${transaction}`, decisionCount: records.length, decisionsSha256: frontierReceiptDomainSha256(records), rows,
        next: last ? [last.recordId, last.revision, last.revisionId, last.candidateDigest] : null };
      f.mutate(page as unknown as Record<string, unknown>, index, transaction);
      return { rows: [{ capture: page }] };
    } } as FrontierCaptureSql };
  return f;
}
async function refused(f: ReturnType<typeof fixture>, code: string, request = input) {
  await expect(captureFrontierReceiptSource(f.db, request)).rejects.toMatchObject({ code });
  expect(f.calls.at(-1)?.text).toBe("ROLLBACK");
  expect(f.calls.every(c => /^(SELECT|BEGIN|SET LOCAL|COMMIT|ROLLBACK)/.test(c.text))).toBe(true);
}

describe("bounded receipt source capability client (synthetic SQL only)", () => {
  test("retains losing equal-revision candidate, reads terminal empty page and fresh closure", async () => {
    const f = fixture(); const capture = await captureFrontierReceiptSource(f.db, input);
    expect(capture.decisions).toEqual([winner, loser]);
    expect(capture.functionOwner).toBe(input.functionOwner);
    expect(capture.scan).toMatchObject({ pageItemCounts: [1, 1, 0], itemCount: 2, pagesRead: 3, complete: true, terminalCursor: null });
    expect(f.calls.filter(c => c.text === RECEIPT_CAPTURE_IDENTITY_SQL)).toHaveLength(2);
    expect(f.calls.filter(c => c.text === RECEIPT_CAPTURE_PAGE_SQL)).toHaveLength(4);
    expect(f.calls.at(-1)?.text).toBe("COMMIT");
    expect(f.calls.some(c => c.text.includes("ingest_receipt_seats_for_stage"))).toBe(false);
    const req = JSON.parse(f.calls.find(c => c.text === RECEIPT_CAPTURE_PAGE_SQL)!.params![0] as string);
    expect(req).toMatchObject({ profileId: input.profileId, profileSha256: input.profileSha256, tenant, after: null, limit: 1 });
  });
  test("zero decision domain still requires actual empty query and closure", async () => {
    const f = fixture(); f.records.splice(0);
    expect((await captureFrontierReceiptSource(f.db, input)).scan.pageItemCounts).toEqual([0]);
    expect(f.calls.filter(c => c.text === RECEIPT_CAPTURE_PAGE_SQL)).toHaveLength(2);
  });
  for (const [field, value] of Object.entries({ role: "postgres", superuser: true, inherit: true, create_role: true, create_db: true, replication: true, bypass_rls: true, can_login: true, read_only: "off", isolation: "read committed",
    definition_sha256: h("unreviewed"), volatility: "v", security_definer: false, config: ["search_path=public"], function_owner: "unqualified_owner" })) {
    test(`refuses capability drift ${field} before any receipt page`, async () => {
      const f = fixture(); f.identity[field] = value;
      await refused(f, "frontier_receipt_capability");
      expect(f.calls.some(c => c.text === RECEIPT_CAPTURE_PAGE_SQL)).toBe(false);
    });
  }
  for (const [field, value] of Object.entries({ tenant: { ...tenant, userId: "other" }, generation: h("other"), profileSha256: h("other"), unexpected: true })) {
    test(`refuses page scope/shape drift ${field}`, async () => {
      const f = fixture(); f.mutate = p => { p[field] = value; }; await refused(f, "frontier_receipt_page");
    });
  }
  test("refuses duplicate or regressing decision cursor", async () => {
    const f = fixture(); f.records[1] = winner; await refused(f, "frontier_receipt_order");
  });
  test("refuses terminal claim on a non-empty page", async () => {
    const f = fixture(); f.mutate = p => { p.next = null; }; await refused(f, "frontier_receipt_cursor");
  });
  test("refuses missing or surplus domain rows", async () => {
    for (const n of [1, 3]) {
      const f = fixture(); f.mutate = p => { p.decisionCount = n; };
      await refused(f, n === 1 ? "frontier_receipt_count" : "frontier_receipt_coverage");
    }
  });
  test("refuses domain digest mismatch", async () => {
    const f = fixture(); f.mutate = p => { p.decisionsSha256 = h("omitted-loser"); }; await refused(f, "frontier_receipt_coverage");
  });
  test("refuses snapshot drift within scan", async () => {
    const f = fixture(); f.mutate = (p, n) => { p.snapshot = `changed-${n}`; }; await refused(f, "frontier_receipt_changed");
  });
  test("refuses fresh closure privacy/authority drift", async () => {
    const f = fixture(); f.mutate = (p, _, tx) => { if (tx === 2) p.authority = { ...authority, privacyVersion: "4" }; }; await refused(f, "frontier_receipt_changed");
  });
  test("page bound cannot become EOF", async () => {
    await refused(fixture(), "frontier_receipt_page_bound", { ...input, bounds: { ...input.bounds, maxPages: 2 } });
  });
  test("bounds response bytes", async () => {
    await refused(fixture(), "frontier_receipt_byte_bound", { ...input, bounds: { ...input.bounds, maxBytes: 1024 } });
  });
  test("permission or absent-function error is sanitized, with rollback and no fallback", async () => {
    const f = fixture(); const query = f.db.query;
    f.db.query = async (text, params) => { if (text === RECEIPT_CAPTURE_PAGE_SQL) throw new Error("secret server text"); return query(text, params) as never; };
    await refused(f, "frontier_receipt_unavailable");
  });
  for (const failedBegin of [1, 2]) {
    test(`lost BEGIN response ${failedBegin} still attempts rollback without retry`, async () => {
      const f = fixture(); const original = f.db.query; let begins = 0;
      f.db.query = async (text, params) => {
        // The transaction takes effect before the transport loses its response.
        const result = await original(text, params);
        if (text.startsWith("BEGIN") && ++begins === failedBegin) throw new Error("lost BEGIN response");
        return result as never;
      };
      await refused(f, "frontier_receipt_unavailable");
      expect(begins).toBe(failedBegin);
      expect(f.calls.filter(c => c.text === "ROLLBACK")).toHaveLength(1);
    });
  }
  test("lost BEGIN plus failed rollback stays unavailable without retry or idle claim", async () => {
    const f = fixture(); const original = f.db.query;
    f.db.query = async (text, params) => {
      const result = await original(text, params);
      if (text.startsWith("BEGIN") || text === "ROLLBACK") throw new Error("broken transport private detail");
      return result as never;
    };
    await refused(f, "frontier_receipt_unavailable");
    expect(f.calls.map(c => c.text)).toEqual(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "ROLLBACK"]);
  });
  test("deadline stops before the next RPC", async () => {
    const f = fixture(); const original = f.db.query;
    f.db.query = async (text, params) => {
      const result = await original(text, params);
      if (text === RECEIPT_CAPTURE_IDENTITY_SQL) await Bun.sleep(25);
      return result as never;
    };
    await refused(f, "frontier_receipt_deadline", { ...input, bounds: { ...input.bounds, maxDurationMs: 10 } });
    expect(f.calls.some(c => c.text === RECEIPT_CAPTURE_PAGE_SQL)).toBe(false);
  });
  test("digest framing distinguishes null, empty and multibyte fields", () => {
    expect(frontierReceiptDomainSha256([])).toBe("359aa7b766744343011b7bdc9c8575dcf624d938d08dcc73677841f7e6b40819");
    expect(frontierReceiptDomainSha256([{ ...winner, sessionId: null }])).not.toBe(frontierReceiptDomainSha256([{ ...winner, sessionId: "" }]));
    expect(frontierReceiptDomainSha256([{ ...winner, recordId: "é:record" }])).toBe("b2383efe2438c67a7817f5963f732af02ff28e50743b2a3556f9bb18e784acb2");
  });
  test("request mutation after BEGIN cannot retarget capture", async () => {
    const f = fixture(); const request = structuredClone(input); const original = f.db.query;
    f.db.query = async (text, params) => {
      const result = await original(text, params);
      if (text.startsWith("BEGIN")) { request.tenant.orgId = "other-org"; request.generation = h("other"); request.functionOwner = "other_owner"; request.bounds.pageLimit = 1000; }
      return result as never;
    };
    const result = await captureFrontierReceiptSource(f.db, request);
    expect(result.tenant).toEqual(tenant); expect(result.generation).toBe(input.generation);
    expect(result.scan.pageLimit).toBe(1); expect(result.functionOwner).toBe(input.functionOwner);
  });
  for (const role of ["bulls96_session_ingest_seeder", "bulls96_session_ingest"]) {
    test(`refuses reused runtime role ${role}`, async () => {
      const f = fixture(); f.identity.role = role;
      await refused(f, "frontier_receipt_capability");
      expect(f.calls.some(c => c.text === RECEIPT_CAPTURE_PAGE_SQL)).toBe(false);
    });
  }
  test("requires independently qualified owner and at most 120 seconds", async () => {
    const f = fixture();
    for (const request of [
      { ...input, functionOwner: FRONTIER_RECEIPT_READER_ROLE },
      { ...input, functionOwner: "bulls96_session_ingest_seeder" },
      { ...input, functionOwner: "bulls96_session_ingest" },
      { ...input, functionOwner: "unqualified owner" },
      { ...input, bounds: { ...input.bounds, maxDurationMs: 120_001 } },
    ]) await expect(captureFrontierReceiptSource(f.db, request)).rejects.toMatchObject({ code: "frontier_receipt_input" });
    expect(f.calls).toHaveLength(0);
    expect((await captureFrontierReceiptSource(f.db, { ...input, bounds: { ...input.bounds, maxDurationMs: 120_000 } })).decisions).toHaveLength(2);
  });
  test("unsupported identifier encodings refuse before SQL", async () => {
    const f = fixture();
    for (const userId of ["\u{10000}", "\ue000", "\ud800", "bad id"]) {
      await expect(captureFrontierReceiptSource(f.db, { ...input, tenant: { ...tenant, userId } })).rejects.toMatchObject({ code: "frontier_receipt_input" });
    }
    expect(f.calls).toHaveLength(0);
  });
  test("invalid limits and unknown input fail before SQL", async () => {
    const f = fixture();
    await expect(captureFrontierReceiptSource(f.db, { ...input, bounds: { ...input.bounds, pageLimit: 0 } })).rejects.toMatchObject({ code: "frontier_receipt_input" });
    await expect(captureFrontierReceiptSource(f.db, { ...input, ownerUrl: "forbidden" } as never)).rejects.toMatchObject({ code: "frontier_receipt_input" });
    expect(f.calls).toHaveLength(0);
  });
});
