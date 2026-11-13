/**
 * The transcript-store plan contract's upload inbox phases (HISTORY_UPLOAD_DIRECT_INFRA=on, decision 0004):
 * "upload inbox rollout" and "upload inbox update". Synthetic plans in the capture's line format.
 */
import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import { assertTranscriptStorePlanContract, TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_STAGE } from "../transcript-store.ts";
import { UPLOAD_INBOX_RESOURCES } from "../upload-inbox.ts";

const STAGE = TRANSCRIPT_STAGE;
const store = [
  "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
  "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
  "transcripts-key", "transcripts-key-alias",
];
const all = [...store, ...SESSION_PIPELINE_RESOURCES, ...UPLOAD_INBOX_RESOURCES];
const h = (text: string) => createHash("sha256").update(text).digest("hex");
const ROLE = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/96bulls-aws-upload-admission-aws-sessions-prod-synthetic00000`;
const OTHER = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/someone-else`;

/** Detailed blocks per updated resource (default: an empty block, as the compact-only tests need). */
function build(actions: Record<string, string>, blocks: Record<string, string[]> = {}, resources = all) {
  const counts = new Map<string, number>();
  for (const [r, a] of Object.entries(actions)) if (resources.includes(r) && a !== "noop") counts.set(a, (counts.get(a) ?? 0) + 1);
  const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: 0 changes";
  const compact = `${summary}\n${resources.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const detailed = `${summary}\n${resources.map((r) => [`[${r}] ${actions[r] ?? "noop"}`, ...(blocks[r] ?? [])].join("\n")).join("\n")}\n`;
  return { compact, detailed };
}
function check(actions: Record<string, string>, blocks: Record<string, string[]> = {}, snapshotRole: string | null = ROLE, resources = all) {
  const { compact, detailed } = build(actions, blocks, resources);
  return assertTranscriptStorePlanContract(compact, {
    detailedPlanText: detailed,
    provenanceText: JSON.stringify({
      schema: 1,
      stage: STAGE,
      outputs: { plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h(detailed) } },
      summary: compact.split("\n")[0],
    }),
    ...(snapshotRole === null ? {} : {
      stateSnapshotText: JSON.stringify({
        schema: 1,
        objects: [{ key: `96bulls/96bulls-aws/${STAGE}/upload-admission.json`, text: JSON.stringify({ status: "created", attr: { roleArn: snapshotRole } }) }],
      }),
    }),
  });
}

/** A policy block whose only change is the consumer role ARN -> (known after apply), as a principal and in a condition list. */
const cascade = (arn = ROLE, to = "(known after apply)") => [
  "  properties:",
  "    policy:",
  "  -     - Effect: Allow",
  "  -       Principal:",
  `  -         AWS: ${arn}`,
  "  -       Condition:",
  "  -         ArnNotEquals:",
  "  -           aws:PrincipalArn:",
  `  -             - ${arn}`,
  "  -             - arn:aws:iam::941433935742:user/96bulls/eraser",
  "  +     - Effect: Allow",
  "  +       Principal:",
  `  +         AWS: ${to}`,
  "  +       Condition:",
  "  +         ArnNotEquals:",
  "  +           aws:PrincipalArn:",
  `  +             - ${to}`,
  "  +             - arn:aws:iam::941433935742:user/96bulls/eraser",
];

describe("upload inbox rollout", () => {
  const creates = Object.fromEntries(UPLOAD_INBOX_RESOURCES.map((r) => [r, "create"]));
  const grants = { transcripts: "update", "transcripts-key": "update", "transcript-writer": "update", "transcript-reader": "update", "transcript-srv-writer": "update" };

  it("every upload resource created, with only the bucket, key and the three walled users updated", () => {
    expect(check({ ...creates, ...grants })).toBe("upload inbox rollout");
    // The rest after a partial apply.
    expect(check({ "upload-admission-mapping": "create", "upload-admission-errors-alarm": "create" })).toBe("upload inbox rollout");
  });

  it("an all-noop plan with the upload resources listed is post-apply", () => {
    expect(check({})).toBe("post-apply");
  });

  it("refuses any other store, pipeline or eraser change in the same plan", () => {
    for (const r of ["transcript-eraser", "trail", "transcripts-key-alias", "session-pipeline", "session-events", "access-logs"])
      expect(() => check({ ...creates, ...grants, [r]: "update" })).toThrow(/upload inbox rollout: .* may not update/);
  });

  it("a plan listing some upload resources but not all is refused", () => {
    expect(() => check({ ...creates }, {}, null, [...store, ...SESSION_PIPELINE_RESOURCES, "upload-admission"])).toThrow(/missing resource/);
  });
});

describe("upload inbox update", () => {
  it("a function update alone (code, flag, allowlist)", () => {
    expect(check({ "upload-admission": "update" })).toBe("upload inbox update");
    expect(check({ "upload-admission": "update", "upload-admission-mapping": "update" })).toBe("upload inbox update");
  });

  it("with the role-ARN cascade on the bucket, key, queue and secret", () => {
    const actions = { "upload-admission": "update", transcripts: "update", "transcripts-key": "update", "upload-admission-events": "update", "upload-admission-secret": "update" };
    const blocks = Object.fromEntries(["transcripts", "transcripts-key", "upload-admission-events", "upload-admission-secret"].map((r) => [r, cascade()]));
    expect(check(actions, blocks)).toBe("upload inbox update");
    // The deploy has no snapshot.
    expect(check(actions, blocks, null)).toBe("upload inbox update");
  });

  it("refuses a cascade that is not exactly the function's role becoming (known after apply)", () => {
    const actions = { "upload-admission": "update", transcripts: "update" };
    expect(() => check(actions, { transcripts: cascade(ROLE, '"*"') })).toThrow(/not only the role-ARN cascade/);
    expect(() => check(actions, { transcripts: cascade(OTHER) })).toThrow(/which is not the upload-admission function's role/);
    expect(() => check(actions, { transcripts: [] })).toThrow(/updates without the role-ARN cascade/);
    expect(() => check(actions, { transcripts: [...cascade(), "  +     - Sid: Extra"] })).toThrow(/adds or removes lines/);
  });

  it("refuses grant updates without a function update, and other changes alongside", () => {
    expect(() => check({ "upload-admission-events": "update" }, { "upload-admission-events": cascade() })).toThrow(/without an upload-admission function update/);
    expect(() => check({ "upload-admission": "update", "desktop-upload-role": "update" })).toThrow(/desktop-upload role changes only in a reviewed rollout/);
    expect(() => check({ "upload-admission": "update", "session-pipeline": "update" })).toThrow(/session-pipeline may not update/);
    expect(() => check({ "upload-admission": "update", "transcript-writer": "update" })).toThrow(/transcript-writer may not update/);
    expect(() => check({ "upload-admission": "update", "upload-admission-dlq": "create" })).toThrow(/upload-admission-dlq may not create/);
  });
});
