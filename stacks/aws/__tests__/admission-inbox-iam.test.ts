/**
 * The S3-first async admission keys (src/session-ingest/async-admission.ts) under the EXISTING
 * srv-writer grant, with no policy change. IAM resource ARNs match `*` across `/` (the same
 * semantics the production buffered stages `96bulls/<org>/buffered-batches/v1/ws=…/m=srv_…` rely
 * on, see buffered-gc-policy.ts). So `96bulls/*\/v1/ws=*\/m=srv_*` covers every inbox, outcome
 * and journal key. A key outside the grant would fail closed: the request path answers 503 and a
 * control refuses before its SQL.
 */
import { describe, expect, it } from "bun:test";
import { srvWriterObjects } from "../transcript-store.ts";
import { inboxKey, journalKey, outcomeKey, receiptDir } from "../../../src/session-ingest/async-admission.ts";

const bucketArn = "arn:aws:s3:::96bulls-aws-sessions-prod-transcripts-941433935742";
/** IAM resource-ARN wildcard semantics: `*` is any run of characters including `/`, `?` is one character. */
const iam = (pattern: string, value: string) =>
  new RegExp("^" + pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*").replaceAll("?", ".") + "$", "s").test(value);
const allowed = (key: string) => srvWriterObjects(bucketArn).some((p) => iam(p, `${bucketArn}/${key}`));

describe("async admission keys under the unchanged srv-writer grant", () => {
  const tenants = [
    { orgId: "org_2abc", workspaceId: "ws_1", userId: "user_1" },
    { orgId: "o", workspaceId: "", userId: "u" },
    { orgId: "org.with-dots_and-dash", workspaceId: "ws/with/slashes and spaces", userId: "user@example" },
  ];
  const receipt = "rhr_" + "a".repeat(64);
  it("allows the inbox, both outcomes and the journal for every tenant shape", () => {
    for (const t of tenants) {
      const keys = [
        inboxKey("96bulls", t, "origin-1", receipt, 1_800_000_000_000, "0123456789abcdef"),
        outcomeKey(receiptDir("96bulls", t, "origin-1", receipt), "admitted"),
        outcomeKey(receiptDir("96bulls", t, "origin-1", receipt), "refused"),
        journalKey("96bulls", t),
      ];
      for (const key of keys) expect({ key, allowed: allowed(key) }).toEqual({ key, allowed: true });
    }
  });
  it("the default srv-writer resources are exactly today's baseline (no inbox authority expansion used)", () => {
    expect(srvWriterObjects(bucketArn)).toEqual([
      `${bucketArn}/96bulls/*/v1/ws=*/m=srv_*`,
      `${bucketArn}/96bulls/*/live-sessions/*`,
      `${bucketArn}/96bulls/*/live-batches/*`,
    ]);
  });
  it("the matcher is not vacuous: a key without the ws=/m=srv_ shape is refused", () => {
    expect(allowed("96bulls/.admission-inbox/org/privacy-journal.json")).toBe(false);
    expect(allowed("other-root/.admission-inbox/org/v1/ws=w/m=srv_x/a")).toBe(false);
  });
});
