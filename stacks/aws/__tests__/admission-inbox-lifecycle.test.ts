/**
 * The default-off admission-inbox lifecycle rule (S3-first async ingest, docs/design/s3-first-async-ingest.md):
 * absent input = today's rules exactly; present = exactly one appended rule on the dedicated inbox prefix;
 * and the "admission inbox lifecycle" plan phase accepts only Alchemy's rendering of exactly that change.
 */
import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { formatDeclaredPropertyYaml } from "alchemy/Cli/PropertyDiff";
import { validateAwsInputs, type AwsInputs } from "../inputs.ts";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import { resolveTenantOrgs } from "../tenants.ts";
import { assertTenantSegment } from "../../../src/session-ingest/writer.ts";
import {
  ADMISSION_INBOX_LIFECYCLE_RULE,
  ADMISSION_INBOX_PREFIX,
  ERASURE_SLA_HOURS,
  assertTranscriptStorePlanContract,
  transcriptLifecycleRules,
} from "../transcript-store.ts";

const STAGE = "aws-sessions-prod";
const ACCOUNT = "941433935742";
const TODAY = [
  { ID: "expire-noncurrent-versions", Status: "Enabled", Filter: { Prefix: "" }, NoncurrentVersionExpiration: { NoncurrentDays: 1 } },
  { ID: "remove-expired-delete-markers", Status: "Enabled", Filter: { Prefix: "" }, Expiration: { ExpiredObjectDeleteMarker: true } },
  { ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 } },
];

describe("admission inbox lifecycle rule", () => {
  it("is absent by default: today's three rules, byte-identical", () => {
    expect(JSON.stringify(transcriptLifecycleRules())).toBe(JSON.stringify(TODAY));
    expect(JSON.stringify(transcriptLifecycleRules({}))).toBe(JSON.stringify(TODAY));
    expect(JSON.stringify(transcriptLifecycleRules({ admissionInbox: false }))).toBe(JSON.stringify(TODAY));
  });

  it("when enabled appends exactly one rule; the K-45 rules stay first and unchanged", () => {
    const rules = transcriptLifecycleRules({ admissionInbox: true });
    expect(JSON.stringify(rules.slice(0, 3))).toBe(JSON.stringify(TODAY));
    expect(rules).toHaveLength(4);
    expect(rules[3]).toEqual({ ID: "expire-admission-inbox", Status: "Enabled", Filter: { Prefix: "96bulls/.admission-inbox/" }, Expiration: { Days: 7 } });
    const noncurrent = rules.find((r) => "NoncurrentVersionExpiration" in r)! as { NoncurrentVersionExpiration: { NoncurrentDays: number } };
    expect((noncurrent.NoncurrentVersionExpiration.NoncurrentDays + 1) * 24).toBeLessThanOrEqual(ERASURE_SLA_HOURS);
  });

  it("can never match tenant data: the prefix is exactly the dedicated inbox namespace", () => {
    expect(ADMISSION_INBOX_PREFIX).toBe("96bulls/.admission-inbox/");
    expect(ADMISSION_INBOX_LIFECYCLE_RULE.Filter.Prefix).toBe(ADMISSION_INBOX_PREFIX);
    for (const tenantKey of ["96bulls/org-a/v1/ws=w/m=srv_x/h=claude/d=2026/11/08/s=s/manifest.json", "96bulls/org-a/buffered-batches/v1/ws=w/m=srv_x/b=1.staged.ndjson.zst", "96bulls/org-a/live-batches/v1/x"])
      expect(tenantKey.startsWith(ADMISSION_INBOX_PREFIX)).toBe(false);
    expect("96bulls/.admission-inbox/org-a/v1/ws=w/m=srv_x/r=ab/a=1-0.ndjson.zst".startsWith(ADMISSION_INBOX_PREFIX)).toBe(true);
    expect(["", "96bulls", "96bulls/"]).not.toContain(ADMISSION_INBOX_LIFECYCLE_RULE.Filter.Prefix);
  });

  it("cannot equal any 96bulls/<org>/ for an org id either the stack or the ingest accepts", () => {
    const segment = ADMISSION_INBOX_PREFIX.split("/")[1]!;
    expect(ADMISSION_INBOX_PREFIX.split("/")).toEqual(["96bulls", ".admission-inbox", ""]);
    expect(segment.startsWith(".")).toBe(true);
    // The stack's ORG_RE (tenants.ts) and the ingest's tenant segment check (writer.ts) both refuse it.
    expect(() => resolveTenantOrgs({ tenantOrgIds: [segment] })).toThrow(/not a valid org id/);
    expect(() => assertTenantSegment("96bulls", segment)).toThrow();
    // The undotted name IS a valid org id: the reason the root carries the dot.
    expect(() => resolveTenantOrgs({ tenantOrgIds: ["admission-inbox"] })).not.toThrow();
    expect(() => assertTenantSegment("96bulls", "admission-inbox")).not.toThrow();
  });
});

describe("transcript store inputs: admissionInboxLifecycle", () => {
  const example = JSON.parse(readFileSync(fileURLToPath(new URL("../examples/aws-sessions-prod.inputs.example.json", import.meta.url)), "utf8")) as AwsInputs;
  const filled = (patch: (i: AwsInputs & Record<string, unknown>) => void = () => {}) => {
    const i = structuredClone(example) as AwsInputs & Record<string, unknown>;
    delete i.$comment;
    i.transcriptStore!.bufferedGcRoleArn = `arn:aws:iam::${ACCOUNT}:role/test-buffered-gc`;
    i.approval = { ...i.approval!, decision: "approved", date: "2026-10-31", quote: "let's start the production s3 work now", capture: { provenanceSha256: "a".repeat(64), planSha256: "b".repeat(64), inputsSha256: "c".repeat(64), sourceCommit: "d".repeat(40) } };
    patch(i);
    return i;
  };
  it("is default-off and accepts only the literal true", () => {
    expect((filled().transcriptStore as Record<string, unknown>).admissionInboxLifecycle).toBeUndefined();
    expect(() => validateAwsInputs(STAGE, filled((i) => { (i.transcriptStore as Record<string, unknown>).admissionInboxLifecycle = true; }))).not.toThrow();
    for (const bad of [false, "true", 1, {}, null])
      expect(() => validateAwsInputs(STAGE, filled((i) => { (i.transcriptStore as Record<string, unknown>).admissionInboxLifecycle = bad; }))).toThrow(/admissionInboxLifecycle/);
  });
});

describe("plan phase: admission inbox lifecycle", () => {
  const resources = [
    "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
    "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
    "transcripts-key", "transcripts-key-alias",
  ];
  const h = (text: string) => createHash("sha256").update(text).digest("hex");
  const plan = (actions: Record<string, string>, extra: readonly string[] = []) => {
    const counts = new Map<string, number>();
    for (const action of Object.values(actions)) counts.set(action, (counts.get(action) ?? 0) + 1);
    const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: 0 changes";
    return `${summary}\n${[...resources, ...extra].map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  };
  const detailed = (compact: string) => compact.split("\n").map((line) => line.startsWith("[") ? `  ${line}` : line).join("\n");
  const provenance = (compact: string, full: string) => JSON.stringify({
    schema: 1, stage: STAGE,
    outputs: { plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h(full) } },
    summary: compact.split("\n")[0],
  });
  const check = (compact: string, full: string) => assertTranscriptStorePlanContract(compact, { detailedPlanText: full, provenanceText: provenance(compact, full) });
  /** The real Alchemy render of a transcripts bucket update, unchanged properties included. */
  const bucketDiff = (compact: string, oldProps: Record<string, unknown>, newProps: Record<string, unknown>) => {
    const base = { bucketName: "96bulls-aws-sessions-prod-transcripts-941433935742", versioning: "Enabled", objectOwnership: "BucketOwnerEnforced" };
    const document = formatDeclaredPropertyYaml({ ...base, ...oldProps }, { ...base, ...newProps }, "update");
    if (!document) throw new Error("fixture has no diff");
    return detailed(compact).replace("  [transcripts] update", `  [transcripts] update\n${document.lines.map((l) => `  ${l}`).join("\n")}`);
  };
  const enabled = transcriptLifecycleRules({ admissionInbox: true });

  it("accepts exactly the one-rule addition, with or without the session pipeline in the plan", () => {
    for (const extra of [[], SESSION_PIPELINE_RESOURCES] as const) {
      const compact = plan({ transcripts: "update" }, extra);
      const full = bucketDiff(compact, { lifecycleRules: TODAY }, { lifecycleRules: enabled });
      expect(check(compact, full)).toBe("admission inbox lifecycle");
    }
  });

  it("refuses any other lifecycle change: other days, other prefix, a removed or edited rule, a reversal", () => {
    const compact = plan({ transcripts: "update" });
    const variant = (patch: Record<string, unknown>) => [...TODAY, { ...ADMISSION_INBOX_LIFECYCLE_RULE, ...patch }];
    for (const [oldRules, newRules] of [
      [TODAY, variant({ Expiration: { Days: 1 } })],
      [TODAY, variant({ Filter: { Prefix: "96bulls/" } })],
      [TODAY, variant({ Filter: { Prefix: "" } })],
      [TODAY, variant({ ID: "something-else" })],
      [TODAY, [...TODAY.slice(1), ADMISSION_INBOX_LIFECYCLE_RULE]],
      [TODAY, [{ ...TODAY[0]!, NoncurrentVersionExpiration: { NoncurrentDays: 30 } }, ...TODAY.slice(1), ADMISSION_INBOX_LIFECYCLE_RULE]],
      [enabled, TODAY],
      [TODAY, [...enabled, { ...ADMISSION_INBOX_LIFECYCLE_RULE, ID: "second" }]],
    ] as const)
      expect(() => check(compact, bucketDiff(compact, { lifecycleRules: oldRules }, { lifecycleRules: newRules }))).toThrow(/admission inbox lifecycle/);
  });

  it("refuses a lifecycle addition that carries any other bucket change", () => {
    const compact = plan({ transcripts: "update" });
    for (const [oldExtra, newExtra] of [
      [{ versioning: "Enabled" }, { versioning: "Suspended" }],
      [{ policy: [{ Sid: "A", Effect: "Deny" }] }, { policy: [{ Sid: "A", Effect: "Allow" }] }],
      [{ tags: { a: "1" } }, { tags: { a: "2" } }],
    ] as const)
      expect(() => check(compact, bucketDiff(compact, { lifecycleRules: TODAY, ...oldExtra }, { lifecycleRules: enabled, ...newExtra }))).toThrow(/admission inbox lifecycle/);
  });

  it("is never combined with another resource change, and a transcripts update without a lifecycle diff is judged as before", () => {
    const both = plan({ transcripts: "update", "transcripts-key": "update" });
    expect(() => check(both, bucketDiff(both, { lifecycleRules: TODAY }, { lifecycleRules: enabled }))).toThrow(/action set does not match/);
    const lone = plan({ transcripts: "update" });
    expect(() => check(lone, detailed(lone))).toThrow(/action set does not match/);
  });
});
