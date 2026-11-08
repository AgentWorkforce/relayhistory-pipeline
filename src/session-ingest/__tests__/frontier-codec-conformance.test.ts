import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { frontierCanonicalJson, frontierSha256, frontierExportRecordsSha256 } from "../frontier-plan.ts";
import { parseFrontierJson } from "../frontier-plan-input.ts";
import { frontierUtf8TupleCompare } from "../frontier-order.ts";
const root = new URL("../../../", import.meta.url);
const fixture = (name: string, sha: string) => {
  const bytes = readFileSync(new URL(`docs/contracts/${name}`, root));
  if (createHash("sha256").update(bytes).digest("hex") !== sha) throw new Error("frontier_vector_pin_mismatch");
  return JSON.parse(bytes.toString("utf8"));
};
const vectors = fixture("frontier-attester-contract-v1.vectors.json", "fd3f8908524b708b24e1511dc88795eb365304f7e3d74cb6267f4491a3231abe");
const design = fixture("../handoffs/frontier-attester-20261107/publication-frontier-attester-minimal-source-design-v3-20261107.json", "51d358b293790030dadcd5839358ac18109d570b2b6d05e97433b21efe5ec398");
const bigint = fixture("frontier-attester-bigint-v1.vectors.json", "867f6b3567ad976c1a01f4fa7940e6b0ce89d4ae7d5454d45a484d1486eb3e38");
// These are codec vectors, never real inventory, a verifier, or approval.
describe("pinned frontier codec conformance (not coverage verification)", () => {
  for (const v of [...vectors.positiveVectors, ...bigint.vectors]) {
    test(`published bytes and SHA256: ${v.id}`, () => {
      const parsed = parseFrontierJson(JSON.stringify(v.input));
      expect(Buffer.from(frontierCanonicalJson(parsed)).toString("hex")).toBe(v.canonicalUtf8Hex);
      expect(frontierSha256(parsed)).toBe(v.sha256);
    });
  }
  test("v3 audit example uses the existing protocol-bearing object hash", () => {
    const v = design.codec_and_independent_evidence_B2_A5_A6_A7_C4.vector;
    expect(Buffer.from(frontierCanonicalJson(v.input)).toString("hex")).toBe(v.utf8_hex);
    expect(frontierSha256(v.input)).toBe(v.sha256);
  });
  test("big integer inputs remain decimal strings, never implicit JS bigint/number coercion", () => {
    for (const v of bigint.vectors) {
      expect(typeof v.input.rowCount).toBe("string");
      expect(frontierCanonicalJson(v.input)).toContain(`"rowCount":"${v.input.rowCount}"`);
    }
    expect(() => frontierCanonicalJson(9007199254740993n)).toThrow("frontier_unsupported_value");
    expect(() => frontierCanonicalJson(Number("9007199254740993"))).toThrow("frontier_non_integer_number");
  });
  test("existing planner encoded-tuple order and new raw UTF8 field order stay distinct", () => {
    const order = vectors.orderingVector;
    expect([...order.rows].sort(frontierUtf8TupleCompare)).toEqual(order.newAuditRawUTF8_FieldTupleOrder);
    const tenant = { orgId: "fixture-org", workspaceId: "fixture-workspace", userId: "fixture-user", originId: "fixture-origin" };
    const rows = order.rows.map(([recordId]: string[]) => ({ tenant, recordId, revision: "1", source: "copilot", sessionId: "x", operation: "upsert" as const }));
    const expected = order.plannerEncodedTupleJS_UTF16_Order.map(([recordId]: string[]) => rows.find((r: {recordId: string}) => r.recordId === recordId));
    expect(frontierExportRecordsSha256(rows)).toBe(frontierSha256(expected));
    expect(frontierExportRecordsSha256(rows)).not.toBe(frontierSha256([...rows].reverse()));
  });
  test("published duplicate-key and unsafe bare-number inputs refuse before hash", () => {
    for (const id of ["duplicate_status_key", "unsafe_bare_integer"]) {
      const v = vectors.negativeKnownAnswers.find((v: {id: string}) => v.id === id);
      expect(() => parseFrontierJson(v.rawJson)).toThrow();
    }
    // Remaining typed audit-schema and S4 cases belong to the 050000 consumer,
    // not the generic JSON codec. No pass on those is inferred here.
  });
  test("raw tuple comparison never normalizes or repairs invalid UTF8 scalars", () => {
    expect(() => frontierUtf8TupleCompare(["\ud800"], ["x"])).toThrow("frontier_invalid_utf8_tuple");
    expect(() => frontierUtf8TupleCompare(["\udc00"], ["x"])).toThrow("frontier_invalid_utf8_tuple");
    expect(frontierUtf8TupleCompare(["a"], ["a", "b"])).toBeLessThan(0);
  });
});
