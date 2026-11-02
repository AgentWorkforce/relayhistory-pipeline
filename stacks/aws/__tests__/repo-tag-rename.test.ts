import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { REPO_TAG_RENAME, assertTranscriptStorePlanContract } from "../transcript-store.ts";

// The real detailed plan of aws-sessions-prod at main 489ec16 (bin/96bulls plan aws-sessions-prod --detailed), sanitized as
// the capture writes it (sanitizePlan), minus the three read API creates: the read API rolls out in its own, later
// capture. 3f8494c renamed the repository, so every tagged resource re-plans `96bulls:repo`; session-pipeline updates
// with no declared property changes, and its five grants show the known role-ARN cascade (four of them also re-tag).
const STAGE = "aws-sessions-prod";
const ACCOUNT = "941433935742";
const LIVE_ROLE = `arn:aws:iam::${ACCOUNT}:role/96bulls-aws-session-pipeline-aws-sessions-prod-nsvhn3beskmuvvon`;
const REAL = readFileSync(fileURLToPath(new URL("./fixtures/aws-sessions-prod.repo-tag-rename.detailed.txt", import.meta.url)), "utf8");
const OLD = `  -     96bulls:repo: ${REPO_TAG_RENAME.from}`;
const NEW = `  +     96bulls:repo: ${REPO_TAG_RENAME.to}`;

const h = (text: string) => createHash("sha256").update(text).digest("hex");
const isResourceLine = (l: string) => /^\[[a-z0-9-]+\] \S+$/.test(l);
/** Recount the summary from the resource lines, so a test changes only what it means to change. */
const recount = (full: string) => {
  const counts = new Map<string, number>();
  for (const l of full.split("\n")) if (isResourceLine(l) && !l.endsWith(" noop")) counts.set(l.split(" ")[1]!, (counts.get(l.split(" ")[1]!) ?? 0) + 1);
  const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: no changes";
  return full.replace(/^Plan: .*$/m, summary);
};
const snapshot = (fn: object = { status: "created", attr: { roleArn: LIVE_ROLE } }) => JSON.stringify({
  schema: 1,
  objects: [{ key: `96bulls/96bulls-aws/${STAGE}/session-pipeline.json`, text: JSON.stringify(fn) }],
});
/** `null`: the deploy's check, which has no state snapshot. */
const check = (detailed: string, stateSnapshot: string | null = snapshot()) => {
  const full = recount(detailed);
  const compact = `${full.split("\n").filter((l) => l.startsWith("Plan:") || isResourceLine(l)).join("\n")}\n`;
  return assertTranscriptStorePlanContract(compact, {
    detailedPlanText: full,
    provenanceText: JSON.stringify({
      schema: 1,
      stage: STAGE,
      outputs: { plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h(full) } },
      summary: compact.split("\n")[0],
    }),
    stateSnapshotText: stateSnapshot ?? undefined,
  });
};
/** Rewrite the n-th line (0-based) of a resource's block that matches `find`. */
const edit = (text: string, resource: string, find: RegExp | string, to: (line: string) => string | string[], nth = 0) => {
  const lines = text.split("\n");
  const start = lines.findIndex((l) => isResourceLine(l) && l.startsWith(`[${resource}] `));
  if (start < 0) throw new Error(`fixture has no ${resource}`);
  let seen = 0;
  for (let i = start + 1; i < lines.length && !isResourceLine(lines[i]!); i++) {
    const hit = typeof find === "string" ? lines[i] === find : find.test(lines[i]!);
    if (hit && seen++ === nth) {
      const out = to(lines[i]!);
      lines.splice(i, 1, ...(Array.isArray(out) ? out : [out]));
      return lines.join("\n");
    }
  }
  throw new Error(`fixture ${resource} has no match ${String(find)} #${nth}`);
};
const op = (text: string, resource: string, from: string, to: string) => {
  const out = text.replace(`[${resource}] ${from}`, `[${resource}] ${to}`);
  if (out === text) throw new Error(`fixture has no [${resource}] ${from}`);
  return out;
};
/** Replace a resource's whole block body. */
const body = (text: string, resource: string, lines: string[]) => {
  const all = text.split("\n");
  const start = all.indexOf(`[${resource}] update`);
  if (start < 0) throw new Error(`fixture has no ${resource} update`);
  let end = start + 1;
  while (end < all.length && !isResourceLine(all[end]!)) end++;
  all.splice(start + 1, end - start - 1, ...lines, "");
  return all.join("\n");
};

describe("repo tag rename: the real plan", () => {
  it("is exactly the rename, the function's no-op update and the role-ARN cascade", () => {
    const ops = REAL.split("\n").filter((l) => isResourceLine(l) && !l.endsWith(" noop"));
    expect(ops.length).toBe(20);
    expect(ops.every((l) => l.endsWith(" update"))).toBe(true);
    expect(REAL).toContain("Plan: 20 to update");
    expect(REAL).not.toContain("session-read-api");
    const lines = REAL.split("\n");
    expect(lines.filter((l) => l === OLD).length).toBe(18);
    expect(lines.filter((l) => l === NEW).length).toBe(18);
    expect(lines.filter((l) => /^  [+-] /.test(l) && /96bulls:repo:/.test(l)).length).toBe(36);
  });

  it("is accepted as the repo tag rename, with and without the state snapshot", () => {
    expect(check(REAL)).toBe("repo tag rename");
    // The deploy has no snapshot; it is bound to the captured state by the version digest.
    expect(check(REAL, null)).toBe("repo tag rename");
    // A partial apply's rest: fewer resources re-tagged (the others already noop).
    const rest = body(REAL, "trail", []).replace("[trail] update", "[trail] noop");
    expect(check(rest)).toBe("repo tag rename");
  });
});

describe("repo tag rename: refusals", () => {
  it("refuses a different new or old repo value", () => {
    const other = edit(REAL, "trail", NEW, () => "  +     96bulls:repo: AgentWorkforce/someone-else");
    expect(() => check(other)).toThrow(/repo tag rename: trail changes .*96BullSessions.* to .*someone-else.*, not exactly/);
    expect(() => check(other, null)).toThrow(/not exactly AgentWorkforce\/96BullSessions to AgentWorkforce\/sessions-pipeline/);
    const oldOther = edit(REAL, "access-logs", OLD, () => "  -     96bulls:repo: AgentWorkforce/elsewhere");
    expect(() => check(oldOther)).toThrow(/access-logs changes .*elsewhere.*, not exactly/);
    // Same value, different indentation (another position in the tags map).
    const indent = edit(REAL, "trail-key", NEW, () => "  +       96bulls:repo: AgentWorkforce/sessions-pipeline");
    expect(() => check(indent)).toThrow(/trail-key changes .* not exactly/);
  });

  it("refuses an added or removed tag line", () => {
    const added = edit(REAL, "trail-logs", NEW, (l) => [l, "  +     96bulls:repo2: x"]);
    expect(() => check(added)).toThrow(/trail-logs adds or removes lines \(1 - \/ 2 \+\)/);
    const removed = edit(REAL, "transcript-reader", NEW, () => []);
    expect(() => check(removed)).toThrow(/transcript-reader adds or removes lines \(1 - \/ 0 \+\)/);
    const onlyNew = edit(REAL, "transcript-writer", OLD, () => []);
    expect(() => check(onlyNew)).toThrow(/transcript-writer adds or removes lines \(0 - \/ 1 \+\)/);
    // A new tag key, not the repo tag.
    const newKey = edit(REAL, "transcript-eraser", "      96bulls:stack: aws-sessions-prod", (l) => [l, "  +     96bulls:owner: someone"]);
    expect(() => check(newKey)).toThrow(/transcript-eraser adds or removes lines \(0 - \/ 1 \+\)/);
    // The tag renamed twice in one resource.
    const doubled = edit(REAL, "access-logs", NEW, (l) => [l, OLD, NEW]);
    expect(() => check(doubled)).toThrow(/access-logs renames the repo tag 2 times/);
  });

  it("refuses any other tag key changing", () => {
    for (const [resource, key, current, value] of [
      ["trail", "96bulls:stack", "aws-sessions-prod", "aws-sessions-dev"],
      ["session-events-dlq", "96bulls:component", "session-pipeline", "other"],
      ["transcripts", "96bulls:stack", "aws-sessions-prod", "aws-proof"],
    ] as const) {
      const line = `      ${key}: ${current}`;
      const mutated = edit(REAL, resource, line, () => [`  -     ${key}: ${current}`, `  +     ${key}: ${value}`]);
      expect(() => check(mutated)).toThrow(new RegExp(`${resource} changes "${key}: ${current}" to "${key}: ${value}", not only the tag rename`));
    }
    // In the same run as the repo tag pair.
    const sameRun = edit(edit(REAL, "trail-key", OLD, (l) => [l, "  -     96bulls:stack: aws-sessions-prod"]), "trail-key", "      96bulls:stack: aws-sessions-prod", () => []);
    const sameRun2 = edit(sameRun, "trail-key", NEW, (l) => [l, "  +     96bulls:stack: aws-sessions-dev"]);
    expect(() => check(sameRun2)).toThrow(/trail-key changes "96bulls:stack: aws-sessions-prod" to "96bulls:stack: aws-sessions-dev"/);
  });

  it("refuses a non-tag property changing alongside the rename", () => {
    const versioning = edit(REAL, "access-logs", "    versioning: Enabled", () => ["  -   versioning: Enabled", "  +   versioning: Suspended"]);
    expect(() => check(versioning)).toThrow(/access-logs changes "versioning: Enabled" to "versioning: Suspended", not only the tag rename/);
    const threshold = edit(REAL, "session-events-age-alarm", "    Threshold: 900", () => ["  -   Threshold: 900", "  +   Threshold: 90000"]);
    expect(() => check(threshold)).toThrow(/session-events-age-alarm changes "Threshold: 900"/);
    // A grant's policy changing beyond the cascade.
    const action = edit(REAL, "session-pipeline-secret", "  +       - Action: secretsmanager:GetSecretValue", () => "  +       - Action: secretsmanager:*");
    expect(() => check(action)).toThrow(/session-pipeline-secret changes .*secretsmanager:\*.* not only the tag rename \(and the role-ARN cascade\)/);
    // The function itself with declared changes.
    const fn = body(REAL, "session-pipeline", ["  properties:", "  -   memorySize: 1024", "  +   memorySize: 4096"]);
    expect(() => check(fn)).toThrow(/session-pipeline may update only with no declared property changes/);
    // A resource that updates without the rename.
    const bare = body(REAL, "trail", ["  no declared property changes"]);
    expect(() => check(bare)).toThrow(/trail updates without the repo tag rename/);
    // A tag change hidden in a noop resource's block is not checked as a pair, so the plan is refused.
    const hidden = REAL.replace("[budget] noop\n", `[budget] noop\n${OLD}\n  +     96bulls:repo: AgentWorkforce/someone-else\n`);
    expect(() => check(hidden)).toThrow(/the plan changes 38 repo tag lines, but only 18 resources rename the tag/);
  });

  it("refuses the role-ARN cascade outside its exact shape", () => {
    // Another principal (the writer user) re-planned as unknown.
    const writer = edit(REAL, "transcripts-key", "  +           AWS: arn:aws:iam::941433935742:user/96bulls/96bulls-aws-sessions-prod-transcript-writer", () => "  +           AWS: (known after apply)");
    expect(() => check(writer)).toThrow(/transcripts-key replaces arn:aws:iam::941433935742:user\/96bulls\/96bulls-aws-sessions-prod-transcript-writer, which is not the session pipeline's role/);
    expect(() => check(writer, null)).toThrow(/must replace one role ARN in 941433935742/);
    expect(() => check(REAL, snapshot({ status: "created", attr: { roleArn: `arn:aws:iam::${ACCOUNT}:role/another-role` } }))).toThrow(/is not the session pipeline's role arn:aws:iam::941433935742:role\/another-role/);
    expect(() => check(REAL, snapshot({ status: "creating", attr: { roleArn: LIVE_ROLE } }))).toThrow(/session-pipeline is not created in state/);
    // The cascade in a resource that does not name the role.
    const notGrant = edit(REAL, "session-events-mapping", "    functionName: 96bulls-aws-sessions-prod-session-pipeline", () => [`  -   principal: ${LIVE_ROLE}`, "  +   principal: (known after apply)"]);
    expect(() => check(notGrant)).toThrow(/session-events-mapping changes "principal: .*" to "principal: \(known after apply\)", not only the tag rename\. Nothing/);
    // The cascade without the function updating.
    const noFn = REAL.replace("[session-pipeline] update\n  no declared property changes\n", "[session-pipeline] noop\n");
    expect(noFn).not.toBe(REAL);
    expect(() => check(noFn)).toThrow(/session-events changes "AWS: arn:aws:iam::941433935742:role\/96bulls-aws-session-pipeline-.*" to "AWS: \(known after apply\)", not only the tag rename \(and the role-ARN cascade\)/);
  });

  it("refuses a create (the read API) alongside the rename", () => {
    const readApi = ["session-read-api", "session-read-api-errors-alarm", "session-read-api-secret"]
      .map((r) => `[${r}] create\n  properties:\n    tags:\n      96bulls:repo: AgentWorkforce/sessions-pipeline\n`)
      .join("\n");
    const withReadApi = REAL.replace("[session-reconcile-continue] update", `${readApi}\n[session-reconcile-continue] update`);
    expect(() => check(withReadApi)).toThrow(/repo tag rename: only in-place updates may come with the rename, found session-read-api:create, session-read-api-errors-alarm:create, session-read-api-secret:create; apply the rename alone first/);
    expect(() => check(withReadApi, null)).toThrow(/session-read-api:create/);
    // Any other create, e.g. a store resource.
    expect(() => check(op(REAL, "budget", "noop", "create"))).toThrow(/found budget:create/);
  });

  it("refuses a delete or replacement", () => {
    for (const action of ["delete", "replace"]) {
      expect(() => check(op(REAL, "trail", "update", action))).toThrow(new RegExp(`trail has forbidden action ${action}`));
      expect(() => check(op(REAL, "budget", "noop", action))).toThrow(new RegExp(`budget has forbidden action ${action}`));
    }
  });
});
