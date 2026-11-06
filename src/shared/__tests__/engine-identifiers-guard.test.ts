import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { BLOCKED, pendingMove, scanRepository, scanText } from "../../../scripts/check-engine-identifiers.ts";

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
// Synthetic stand-ins: the guard logic is proven with these, never with real values.
const synthetic = new Map([
  [sha("111122223333"), "aws-account"],
  [sha("abcdefghijklmnopqrst"), "supabase-ref"],
  [sha("i-0123456789abcdef0"), "ec2-instance"],
]);

describe("engine identifier guard", () => {
  test("finds identifiers in ARNs, usernames, hosts, bucket names and adjacent text", () => {
    const text = [
      "arn:aws:kms:us-east-1:111122223333:key/x",
      'user: "bulls96_session_ingest.abcdefghijklmnopqrst"',
      "db.abcdefghijklmnopqrst.supabase.co",
      "96bulls-transcripts-111122223333",
      "account111122223333 and 9111122223333",
      "projectabcdefghijklmnopqrstx",
      "instanceId:'i-0123456789abcdef0'",
      "clean line 11112222333 abcdefghijklmnopqrs",
    ].join("\n");
    const hits = scanText("x.ts", text, synthetic);
    expect(hits.map((h) => h.line)).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(new Set(hits.map((h) => h.kind))).toEqual(new Set(["aws-account", "supabase-ref", "ec2-instance"]));
  });
  test("stores only digests of the blocked production identifiers", () => {
    expect(BLOCKED.length).toBe(3);
    for (const b of BLOCKED) expect(b.sha256).toMatch(/^[0-9a-f]{64}$/);
  });
  test("pending-move entries match exactly or by directory prefix, nothing broader", () => {
    expect(pendingMove("scripts/ops/cutover-20261105/members.ts")).toBe(true);
    expect(pendingMove("src/hoopdb/owner-operator-apply.ts")).toBe(true);
    expect(pendingMove("src/hoopdb/owner-operator-apply.ts.bak")).toBe(false);
    expect(pendingMove("src/session-ingest/buffered-gc-job.ts")).toBe(false);
    expect(pendingMove("scripts/opsx/thing.ts")).toBe(false);
  });
  test("engine source (src/, packages/, scripts/) carries no production identifier", () => {
    const root = fileURLToPath(new URL("../../../", import.meta.url));
    const hits = scanRepository(root);
    expect(hits.map((h) => `${h.path}:${h.line}:${h.kind}`)).toEqual([]);
  });
});
