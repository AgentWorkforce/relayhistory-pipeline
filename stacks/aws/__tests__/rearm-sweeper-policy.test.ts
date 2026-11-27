import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import {
  REARM_SWEEPER_BUCKET_SIDS, REARM_SWEEPER_KEY_SIDS, REARM_SWEEPER_SIDS, REARM_SWEEPER_STATE_DENY_SID, assertRearmSweeperRoleArn, rearmSweeperBucketStatements, rearmSweeperKeyStatement,
  rearmSweeperStateDenyStatement,
} from "../rearm-sweeper-policy.ts";
import { crossAccountWorkerBucketStatements } from "../cross-account-worker-policy.ts";
import {
  assertTranscriptStorePlanContract, rearmSweeperBucketTail, REARM_PASS_RECEIPT_PREFIX, REARM_PASS_RECEIPT_RULE_ID, transcriptBucketPolicy, transcriptKeyPolicy,
  transcriptLifecycleRules, transcriptNames, TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION, TRANSCRIPT_STAGE,
} from "../transcript-store.ts";

// Synthetic placeholders only (public repo): the bucket account and the sweeper's (other) account.
const accountId = "111122223333", sweeperAccount = "444455556666", region = "us-east-1";
const bucketName = "synthetic-transcripts-111122223333", bucketArn = `arn:aws:s3:::${bucketName}`;
const roleArn = `arn:aws:iam::${sweeperAccount}:role/synthetic-rearm-sweeper`;
const keyArgs = { accountId, region, bucketName, writerArn: "writer", srvWriterArn: "srv", readerArn: "reader", pipelineRoleArn: "pipeline" };

describe("rearm sweeper statements", () => {
  it("accepts only an exact named role and returns its account", () => {
    expect(assertRearmSweeperRoleArn(roleArn)).toBe(sweeperAccount);
    expect(assertRearmSweeperRoleArn(`arn:aws:iam::${accountId}:role/ops/rearm`)).toBe(accountId);
    for (const bad of [undefined, null, false, 7, "", "*", [roleArn], `arn:aws:iam::${sweeperAccount}:role/*`, `arn:aws:iam::${sweeperAccount}:user/rearm`,
      `arn:aws:iam::${sweeperAccount}:root`, `arn:aws:sts::${sweeperAccount}:assumed-role/rearm/s`, `arn:aws-cn:iam::${sweeperAccount}:role/rearm`,
      "arn:aws:iam::44445555666:role/rearm", `${roleArn} `, `${roleArn}\n`])
      expect(() => assertRearmSweeperRoleArn(bad)).toThrow(/rearmSweeperRoleArn must be an exact named IAM role/);
  });

  it("emits exactly the six live statements, in order", () => {
    const Principal = { AWS: roleArn }, t = `${bucketArn}/96bulls`;
    expect(rearmSweeperBucketStatements({ bucketArn, roleArn })).toEqual([
      { Sid: "RearmSweeperLists", Effect: "Allow", Principal, Action: ["s3:ListBucket", "s3:ListBucketVersions"], Resource: bucketArn },
      { Sid: "RearmSweeperReads", Effect: "Allow", Principal, Action: "s3:GetObject", Resource: [`${t}/.admission-inbox/*/ledger/rearm/*`, `${t}/.admission-inbox/*/ledger/owners/*`, `${t}/.admission-inbox/*/upload-fences.json`, `${t}/.ledger/rearm-sweeper/*`, `${t}/*/v1/ws=*/manifest.json`] },
      { Sid: "RearmSweeperWritesProgress", Effect: "Allow", Principal, Action: "s3:PutObject", Resource: [`${t}/.admission-inbox/*/ledger/rearm/*.done/*`, `${t}/.admission-inbox/*/ledger/rearm/*.corrupt/*`, `${t}/.ledger/rearm-sweeper/*`] },
      { Sid: "RearmSweeperDeletesCanonicalVersions", Effect: "Allow", Principal, Action: "s3:DeleteObjectVersion", Resource: [`${t}/*/v1/ws=*`, `${t}/*/live-sessions/v1/ws=*`] },
      { Sid: "RearmSweeperNeverDotPrefixes", Effect: "Deny", Principal, Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: `${t}/.*` },
    ]);
    expect(rearmSweeperKeyStatement({ region, bucketName, roleArn })).toEqual({
      Sid: "RearmSweeperReadsViaS3", Effect: "Allow", Principal, Action: ["kms:Decrypt", "kms:GenerateDataKey"], Resource: "*",
      Condition: { StringEquals: { "kms:ViaService": "s3.us-east-1.amazonaws.com", "kms:CallerAccount": sweeperAccount }, StringLike: { "kms:EncryptionContext:aws:s3:arn": `${bucketArn}/96bulls/*` } },
    });
    // Key order as in the live text.
    expect(rearmSweeperBucketStatements({ bucketArn, roleArn }).map((s) => Object.keys(s))).toEqual(Array(5).fill(["Sid", "Effect", "Principal", "Action", "Resource"]));
    expect(Object.keys(rearmSweeperKeyStatement({ region, bucketName, roleArn }))).toEqual(["Sid", "Effect", "Principal", "Action", "Resource", "Condition"]);
    expect(REARM_SWEEPER_SIDS).toEqual([...REARM_SWEEPER_BUCKET_SIDS, ...REARM_SWEEPER_KEY_SIDS]);
    expect(() => rearmSweeperBucketStatements({ bucketArn, roleArn: "*" })).toThrow();
    expect(() => rearmSweeperKeyStatement({ region, bucketName, roleArn: "" })).toThrow();
  });

  it("key policy: absent is unchanged; present inserts the one statement only", () => {
    const key = transcriptKeyPolicy(keyArgs);
    expect(transcriptKeyPolicy({ ...keyArgs, rearmSweeperRoleArn: undefined })).toEqual(key);
    const withRearm = transcriptKeyPolicy({ ...keyArgs, rearmSweeperRoleArn: roleArn });
    expect(withRearm.Statement.filter((s) => s.Sid !== "RearmSweeperReadsViaS3")).toEqual(key.Statement);
    expect(withRearm.Statement.filter((s) => s.Sid === "RearmSweeperReadsViaS3")).toEqual([rearmSweeperKeyStatement({ region, bucketName, roleArn })]);
    expect(() => transcriptKeyPolicy({ ...keyArgs, rearmSweeperRoleArn: `arn:aws:iam::${sweeperAccount}:user/x` })).toThrow();
  });
});

// ---- plan contract: the grants deploy alone, only for the input's role ----
const RESOURCES = [
  "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
  "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
  "transcripts-key", "transcripts-key-alias",
];
const h = (t: string) => createHash("sha256").update(t).digest("hex");
/** Alchemy-like YAML: sorted keys, `- ` list items, plain scalars unquoted (as in the captured fixtures). */
const scalar = (x: unknown) => typeof x === "string" && /^[A-Za-z][A-Za-z0-9:_./*=-]*$/.test(x) ? x : JSON.stringify(x);
function yaml(v: unknown, indent: number): string[] {
  const pad = " ".repeat(indent);
  if (Array.isArray(v)) return v.flatMap((item) => {
    if (item !== null && typeof item === "object") { const [first, ...rest] = yaml(item, indent + 2); return [`${pad}- ${first!.trimStart()}`, ...rest]; }
    return [`${pad}- ${scalar(item)}`];
  });
  return Object.keys(v as object).sort().flatMap((k) => {
    const x = (v as Record<string, unknown>)[k];
    return x !== null && typeof x === "object" ? [`${pad}${k}:`, ...yaml(x, indent + 2)] : [`${pad}${k}: ${scalar(x)}`];
  });
}
const mark = (m: "+" | "-", lines: string[]) => lines.map((l) => `  ${m} ${l.slice(2)}`);
// The plan contract is for the production stage, so its policies are built with the stage's own names.
const P = transcriptNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID);
const pKeyArn = `arn:aws:kms:${region}:${TRANSCRIPT_ACCOUNT_ID}:key/synthetic`, pEraser = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:user/synthetic-eraser`;
// The production stage always has the buffered GC, so the deny's principal value is a list.
const pGc = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-buffered-gc`;
// As the stage renders it: the deny exempts the sweeper (last), and the rearm statements come last in the policy, the
// D5 state deny after the five grants (stateDeny false: without it, the rollback shape).
const pBucket = (rearm?: string, worker?: string, gc: string | null = pGc, stateDeny = true) => [
  ...transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: P.bucketArn, keyArn: pKeyArn, eraserArn: pEraser, ...(gc ? { bufferedGcRoleArn: gc } : {}), ...(rearm ? { rearmSweeperRoleArn: rearm } : {}), ...(worker ? { crossAccountWorkerRoleArn: worker } : {}) }),
  ...(rearm ? rearmSweeperBucketStatements({ bucketArn: P.bucketArn, roleArn: rearm }) : []),
  ...(rearm && stateDeny ? [rearmSweeperStateDenyStatement({ bucketArn: P.bucketArn, roleArn: rearm })] : []),
];
/** The rearm statements as the stage adds them with the state deny on: the five grants, then the deny. */
const rearmAll = (role: string) => [...rearmSweeperBucketStatements({ bucketArn: P.bucketArn, roleArn: role }), rearmSweeperStateDenyStatement({ bucketArn: P.bucketArn, roleArn: role })];
/** The policy with OnlyTheEraserDeletes patched. */
const patchDeny = (policy: unknown[], patch: (s: any) => any) => policy.map((s: any) => s.Sid === "OnlyTheEraserDeletes" ? patch(s) : s);
const denyPrincipals = (policy: unknown[], list: unknown) => patchDeny(policy, (s) => ({ ...s, Condition: { ArnNotEquals: { "aws:PrincipalArn": list } } }));
const pKey = (rearm?: string, worker?: string) => transcriptKeyPolicy({ ...keyArgs, accountId: TRANSCRIPT_ACCOUNT_ID, region: TRANSCRIPT_REGION, bucketName: P.bucket, ...(rearm ? { rearmSweeperRoleArn: rearm } : {}), ...(worker ? { crossAccountWorkerRoleArn: worker } : {}) });
const bucketBlock = (live: unknown[], planned: unknown[], tagsLive = "a", tagsPlanned = "a") => [
  "  [transcripts] update", "  properties:", `    bucketName: ${P.bucket}`,
  ...mark("-", ["    policy:", ...yaml(live, 6)]), ...mark("+", ["    policy:", ...yaml(planned, 6)]),
  ...(tagsLive === tagsPlanned ? [`    tags: ${tagsLive}`] : [...mark("-", [`    tags: ${tagsLive}`]), ...mark("+", [`    tags: ${tagsPlanned}`])]),
];
const keyBlock = (live: { Statement: unknown[] }, planned: { Statement: unknown[] }) => [
  "  [transcripts-key] update", "  properties:", "    enableKeyRotation: true", "    policy:",
  ...mark("-", yaml(live, 6)), ...mark("+", yaml(planned, 6)),
];
function check(blocks: Record<string, string[]>, actions: Record<string, string>, uploadInputs?: Record<string, unknown>) {
  const counts = new Map<string, number>();
  for (const a of Object.values(actions)) counts.set(a, (counts.get(a) ?? 0) + 1);
  const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: 0 changes";
  const compact = `${summary}\n${RESOURCES.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const detailed = `${summary}\n${RESOURCES.map((r) => blocks[r] ? blocks[r]!.join("\n") : `  [${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const provenanceText = JSON.stringify({ schema: 1, stage: TRANSCRIPT_STAGE, summary,
    outputs: { plan: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.detailed.txt`, sha256: h(detailed) } } });
  return assertTranscriptStorePlanContract(compact, { detailedPlanText: detailed, provenanceText, ...(uploadInputs ? { uploadInputs } : {}) });
}

describe("rearm sweeper grants plan phase", () => {
  const liveBucket = pBucket(), plannedBucket = pBucket(roleArn);
  const liveKey = pKey(), plannedKey = pKey(roleArn);
  const both = { transcripts: "update", "transcripts-key": "update" };
  const set = { rearmSweeperRoleArn: roleArn };
  const grant = () => ({ transcripts: bucketBlock(liveBucket, plannedBucket), "transcripts-key": keyBlock(liveKey, plannedKey) });

  it("accepts exactly the six statements added, alone, for the input's role", () => {
    expect(check(grant(), both, set)).toBe("rearm sweeper grants");
    expect(check({ transcripts: bucketBlock(liveBucket, plannedBucket) }, { transcripts: "update" }, set)).toBe("rearm sweeper grants");
    expect(check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, { "transcripts-key": "update" }, set)).toBe("rearm sweeper grants");
    // No bridge / cross-account / backlog-ops in this key policy: the statement lands last.
    expect(plannedKey.Statement.at(-1)!.Sid).toBe("RearmSweeperReadsViaS3");
  });

  it("refuses the grant while the input is unset, or for another role", () => {
    expect(() => check(grant(), both)).toThrow(/rearmSweeperRoleArn is unset/);
    expect(() => check(grant(), both, {})).toThrow(/rearmSweeperRoleArn is unset/);
    expect(() => check(grant(), both, { rearmSweeperRoleArn: `arn:aws:iam::${sweeperAccount}:role/other` })).toThrow(/must name exactly transcriptStore.rearmSweeperRoleArn/);
    expect(() => check(grant(), both, { rearmSweeperRoleArn: "*" })).toThrow(/not an exact role ARN/);
  });

  it("refuses when mixed with any other change or phase", () => {
    expect(() => check({ transcripts: bucketBlock(liveBucket, plannedBucket) }, { transcripts: "update", "transcript-reader": "update" }, set)).toThrow(/must deploy alone/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, { "transcript-srv-writer": "create", "transcripts-key": "update" }, set)).toThrow(/must deploy alone/);
    // With the cross-account worker grants in the same plan: that phase runs first and refuses the extra lines
    // (the sweeper's deny line first, then the extra Sids).
    const worker = `arn:aws:iam::${sweeperAccount}:role/synthetic-sessions-worker`;
    expect(() => check({ transcripts: bucketBlock(liveBucket, pBucket(roleArn, worker)) }, { transcripts: "update" }, set)).toThrow(/cross-account worker grants: transcripts: existing statement OnlyTheEraserDeletes must stay byte-identical/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, denyPrincipals(pBucket(roleArn, worker), [pEraser, pGc])) }, { transcripts: "update" }, set)).toThrow(/cross-account worker grants: transcripts: must add/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, plannedBucket, "a", "b") }, { transcripts: "update" }, set)).toThrow(/only policy statements may change/);
  });

  it("refuses a non-rearm line added, an existing statement changed, or a partial grant", () => {
    const extra = [...plannedBucket, { Sid: "SomethingElse", Effect: "Allow", Principal: { AWS: roleArn }, Action: "s3:GetObject", Resource: `${P.bucketArn}/*` }];
    expect(() => check({ transcripts: bucketBlock(liveBucket, extra) }, { transcripts: "update" }, set)).toThrow(/must add \(or, to revoke, remove\) exactly/);
    const keyExtra = { ...plannedKey, Statement: [...plannedKey.Statement, { Sid: "AnotherGrant", Effect: "Allow", Principal: { AWS: roleArn }, Action: "kms:Decrypt", Resource: "*" }] };
    expect(() => check({ "transcripts-key": keyBlock(liveKey, keyExtra) }, { "transcripts-key": "update" }, set)).toThrow(/must add \(or, to revoke, remove\) exactly/);
    const crossWorker = [...plannedBucket, ...crossAccountWorkerBucketStatements({ bucketAccountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: P.bucketArn, roleArn })];
    expect(() => check({ transcripts: bucketBlock(liveBucket, crossWorker) }, { transcripts: "update" }, set)).toThrow(/cross-account worker grants: transcripts: existing statement OnlyTheEraserDeletes must stay byte-identical/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, denyPrincipals(crossWorker, [pEraser, pGc])) }, { transcripts: "update" }, set)).toThrow(/cross-account worker grants: transcripts: must add/);
    const tampered = plannedBucket.map((s: any) => s.Sid === "OnlyTheEraserDeletes" ? { ...s, Effect: "Allow" } : s);
    expect(() => check({ transcripts: bucketBlock(liveBucket, tampered) }, { transcripts: "update" }, set)).toThrow(/OnlyTheEraserDeletes must change by exactly one added line/);
    const otherTampered = plannedBucket.map((s: any) => s.Sid === "DenyTlsBelow12" ? { ...s, Effect: "Allow" } : s);
    expect(() => check({ transcripts: bucketBlock(liveBucket, otherTampered) }, { transcripts: "update" }, set)).toThrow(/DenyTlsBelow12 must stay byte-identical/);
    const partial = plannedBucket.filter((s) => s.Sid !== "RearmSweeperNeverDotPrefixes");
    expect(() => check({ transcripts: bucketBlock(liveBucket, partial) }, { transcripts: "update" }, set)).toThrow(/must add \(or, to revoke, remove\) exactly/);
  });

  it("refuses a rearm statement that is not exactly the reviewed one", () => {
    const sub = (sid: string, patch: (s: any) => any) => plannedBucket.map((s: any) => s.Sid === sid ? patch(s) : s);
    const subKey = (sid: string, patch: (s: any) => any) => ({ ...plannedKey, Statement: plannedKey.Statement.map((s: any) => s.Sid === sid ? patch(s) : s) });
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("RearmSweeperReads", (s) => ({ ...s, Principal: "*" }))) }, { transcripts: "update" }, set)).toThrow(/RearmSweeperReads must be exactly the reviewed statement/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("RearmSweeperDeletesCanonicalVersions", (s) => ({ ...s, Resource: `${P.bucketArn}/*` }))) }, { transcripts: "update" }, set)).toThrow(/RearmSweeperDeletesCanonicalVersions must be exactly the reviewed statement/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("RearmSweeperNeverDotPrefixes", (s) => ({ ...s, Effect: "Allow" }))) }, { transcripts: "update" }, set)).toThrow(/RearmSweeperNeverDotPrefixes must be exactly the reviewed statement/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("RearmSweeperReads", (s) => ({ ...s, Principal: { AWS: `arn:aws:iam::${sweeperAccount}:role/other` } }))) }, { transcripts: "update" }, set)).toThrow(/exactly one role/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, subKey("RearmSweeperReadsViaS3", ({ Condition, ...s }) => s)) }, { "transcripts-key": "update" }, set)).toThrow(/RearmSweeperReadsViaS3 must be exactly the reviewed statement/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, subKey("RearmSweeperReadsViaS3", (s) => ({ ...s, Action: "kms:*" }))) }, { "transcripts-key": "update" }, set)).toThrow(/RearmSweeperReadsViaS3 must be exactly the reviewed statement/);
  });

  it("the bucket's eraser-only deny gains exactly the sweeper, last, and nothing else", () => {
    // As rendered: [eraser, gc, sweeper], one more line than live.
    const deny = (p: unknown[]) => (p as any[]).find((s) => s.Sid === "OnlyTheEraserDeletes");
    expect(deny(liveBucket).Condition.ArnNotEquals["aws:PrincipalArn"]).toEqual([pEraser, pGc]);
    expect(deny(plannedBucket).Condition.ArnNotEquals["aws:PrincipalArn"]).toEqual([pEraser, pGc, roleArn]);
    const one = (planned: unknown[]) => check({ transcripts: bucketBlock(liveBucket, planned) }, { transcripts: "update" }, set);
    const gains = /OnlyTheEraserDeletes must change by exactly one added line, the rearm sweeper role as the last aws:PrincipalArn item/;
    // The statements added without the deny change.
    expect(() => one([...liveBucket, ...rearmAll(roleArn)])).toThrow(gains);
    // The role elsewhere in the list, twice, another role, or the right line plus any other change to the deny.
    expect(() => one(denyPrincipals(plannedBucket, [pEraser, roleArn, pGc]))).toThrow(gains);
    expect(() => one(denyPrincipals(plannedBucket, [roleArn, pEraser, pGc]))).toThrow(gains);
    expect(() => one(denyPrincipals(plannedBucket, [pEraser, pGc, roleArn, roleArn]))).toThrow(gains);
    const other = `arn:aws:iam::${sweeperAccount}:role/other`;
    expect(() => one(denyPrincipals(plannedBucket, [pEraser, pGc, other]))).toThrow(gains);
    expect(() => one(denyPrincipals(plannedBucket, [pEraser, roleArn]))).toThrow(gains);
    expect(() => one(patchDeny(plannedBucket, (s) => ({ ...s, Action: ["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObject"] })))).toThrow(gains);
    expect(() => one(patchDeny(plannedBucket, (s) => ({ ...s, Resource: [`${P.bucketArn}/96bulls/*`] })))).toThrow(gains);
    expect(() => one(patchDeny(plannedBucket, (s) => ({ ...s, Condition: { ArnNotEquals: { "aws:PrincipalArn": [pEraser, pGc, roleArn] }, Bool: { "aws:SecureTransport": "true" } } })))).toThrow(gains);
    expect(() => one(plannedBucket.filter((s: any) => s.Sid !== "OnlyTheEraserDeletes"))).toThrow(/existing statement OnlyTheEraserDeletes must stay byte-identical/);
    // A stage without the GC: the value turns from a string into a list, not one added line.
    expect(deny(pBucket(undefined, undefined, null)).Condition.ArnNotEquals["aws:PrincipalArn"]).toBe(pEraser);
    expect(() => check({ transcripts: bucketBlock(pBucket(undefined, undefined, null), pBucket(roleArn, undefined, null)) }, { transcripts: "update" }, set)).toThrow(gains);
  });

  it("a deny-only change (no rearm statement touched) is not this phase and the contract refuses it", () => {
    const denyOnly = denyPrincipals(liveBucket, [pEraser, pGc, roleArn]);
    for (const inputs of [set, undefined]) {
      let message = "";
      try { check({ transcripts: bucketBlock(liveBucket, denyOnly) }, { transcripts: "update" }, inputs); } catch (e) { message = String((e as Error).message); }
      expect(message).toMatch(/plan contract refused/);
      expect(message).not.toMatch(/rearm sweeper grants/);
    }
    // With the statements already in place on both sides: still refused.
    const statementsOnly = [...liveBucket, ...rearmAll(roleArn)];
    expect(() => check({ transcripts: bucketBlock(statementsOnly, plannedBucket) }, { transcripts: "update" }, set)).toThrow(/plan contract refused/);
  });

  it("accepts the revocation only while the input is unset", () => {
    const revoke = { transcripts: bucketBlock(plannedBucket, liveBucket), "transcripts-key": keyBlock(plannedKey, liveKey) };
    expect(check(revoke, both)).toBe("rearm sweeper grants");
    expect(check(revoke, both, {})).toBe("rearm sweeper grants");
    expect(() => check(revoke, both, set)).toThrow(/removes the rearm statements while transcriptStore.rearmSweeperRoleArn is set/);
    expect(() => check({ transcripts: bucketBlock(plannedBucket, liveBucket) }, { transcripts: "update", "transcript-reader": "update" })).toThrow(/must deploy alone/);
    // The deny must lose exactly the sweeper's line, no more and no less.
    const loses = /OnlyTheEraserDeletes must lose exactly one line, the rearm sweeper role as the last aws:PrincipalArn item/;
    const revokeTo = (planned: unknown[]) => () => check({ transcripts: bucketBlock(plannedBucket, planned) }, { transcripts: "update" });
    expect(revokeTo(denyPrincipals(liveBucket, [pEraser, pGc, roleArn]))).toThrow(loses);
    expect(revokeTo(denyPrincipals(liveBucket, [pEraser, roleArn]))).toThrow(loses);
    expect(revokeTo(denyPrincipals(liveBucket, pEraser))).toThrow(loses);
    expect(check({ transcripts: bucketBlock(plannedBucket, liveBucket) }, { transcripts: "update" })).toBe("rearm sweeper grants");
  });

  it("a policy re-rendered with the rearm statements on both sides is not this phase", () => {
    const extra = { ...plannedKey, Statement: [...plannedKey.Statement, { Sid: "SrvWriterSomething", Effect: "Allow", Principal: { AWS: "x" }, Action: "kms:Decrypt", Resource: "*" }] };
    expect(() => check({ "transcripts-key": keyBlock(plannedKey, extra) }, { "transcripts-key": "update" }, set)).not.toThrow(/rearm sweeper grants/);
    // Unchanged plans with the statements in place stay outside the phase.
    expect(check({}, {}, set)).toBe("post-apply");
  });
});

// ---- D5: only the sweeper role writes or deletes its own state; pass receipts expire ----
describe("D5 rearm sweeper state deny", () => {
  it("is exactly one Deny of PutObject and DeleteObject on the sweeper's state prefix, for everyone but the role", () => {
    expect(rearmSweeperStateDenyStatement({ bucketArn, roleArn })).toEqual({
      Sid: REARM_SWEEPER_STATE_DENY_SID, Effect: "Deny", Principal: "*", Action: ["s3:DeleteObject", "s3:PutObject"],
      Resource: `${bucketArn}/96bulls/.ledger/rearm-sweeper/*`, Condition: { ArnNotEquals: { "aws:PrincipalArn": roleArn } },
    });
    expect(Object.keys(rearmSweeperStateDenyStatement({ bucketArn, roleArn }))).toEqual(["Sid", "Effect", "Principal", "Action", "Resource", "Condition"]);
    // Its prefix is exactly the one the sweeper's grants read and write, and it covers the receipts' passes/.
    const writes = rearmSweeperBucketStatements({ bucketArn, roleArn }).find((x) => x.Sid === "RearmSweeperWritesProgress")!.Resource as string[];
    expect(writes).toContain(rearmSweeperStateDenyStatement({ bucketArn, roleArn }).Resource);
    expect(`${bucketArn}/${REARM_PASS_RECEIPT_PREFIX}x.json`.startsWith(rearmSweeperStateDenyStatement({ bucketArn, roleArn }).Resource.slice(0, -1))).toBe(true);
    for (const bad of ["*", "", `arn:aws:iam::${sweeperAccount}:user/x`]) expect(() => rearmSweeperStateDenyStatement({ bucketArn, roleArn: bad })).toThrow();
    expect(REARM_SWEEPER_STATE_DENY_SID.startsWith("RearmSweeper")).toBe(true); // guarded by every other phase like the grants
  });

  it("renders after the five grants while the role is set; false is the rollback; unset renders nothing", () => {
    const five = rearmSweeperBucketStatements({ bucketArn, roleArn });
    expect(rearmSweeperBucketTail(bucketArn, {})).toEqual([]);
    expect(rearmSweeperBucketTail(bucketArn, { rearmSweeperRoleArn: roleArn })).toEqual([...five, rearmSweeperStateDenyStatement({ bucketArn, roleArn })]);
    expect(rearmSweeperBucketTail(bucketArn, { rearmSweeperRoleArn: roleArn, rearmSweeperStateDeny: true })).toEqual([...five, rearmSweeperStateDenyStatement({ bucketArn, roleArn })]);
    expect(rearmSweeperBucketTail(bucketArn, { rearmSweeperRoleArn: roleArn, rearmSweeperStateDeny: false })).toEqual(five);
    // The retention input never affects the policy.
    expect(rearmSweeperBucketTail(bucketArn, { rearmSweeperRoleArn: roleArn, ...({ rearmPassReceiptRetentionDays: undefined } as object) })).toHaveLength(6);
  });
});

describe("D5 rearm sweeper writer deny plan phase", () => {
  const grantsOnly = pBucket(roleArn, undefined, pGc, false), withDeny = pBucket(roleArn);
  const set = { rearmSweeperRoleArn: roleArn };
  const one = (live: unknown[], planned: unknown[], inputs: Record<string, unknown> | undefined = set, actions: Record<string, string> = { transcripts: "update" }, blocks: Record<string, string[]> = {}) =>
    check({ transcripts: bucketBlock(live, planned), ...blocks }, actions, inputs);

  it("accepts adding exactly the deny, alone, behind the live grants, for the input's role", () => {
    expect(withDeny.at(-1)).toEqual(rearmSweeperStateDenyStatement({ bucketArn: P.bucketArn, roleArn }));
    expect(one(grantsOnly, withDeny)).toBe("rearm sweeper writer deny");
    expect(one(grantsOnly, withDeny, { ...set, rearmSweeperStateDeny: true })).toBe("rearm sweeper writer deny");
  });

  it("refuses it with any other change, input or statement", () => {
    expect(() => one(grantsOnly, withDeny, set, { transcripts: "update", "transcripts-key": "update" }, { "transcripts-key": keyBlock(pKey(roleArn), pKey(roleArn)) })).toThrow(/rearm sweeper writer deny: must deploy alone/);
    expect(() => one(grantsOnly, withDeny, set, { transcripts: "update", "transcript-reader": "update" })).toThrow(/must deploy alone/);
    expect(() => one(grantsOnly, withDeny, {})).toThrow(/rearm sweeper writer deny: transcriptStore.rearmSweeperRoleArn is unset/);
    expect(() => one(grantsOnly, withDeny, { rearmSweeperRoleArn: `arn:aws:iam::${sweeperAccount}:role/other` })).toThrow(/the live RearmSweeperLists must be exactly the reviewed grant/);
    // Each grant on its own: one re-pointed principal (a service, a list, another role, none) refuses, though the
    // other four still name the role. The same change on both sides keeps the grants "unchanged".
    const grant = (sid: string, patch: (x: any) => any) => (p: unknown[]) => p.map((x: any) => x.Sid === sid ? patch(x) : x);
    for (const sid of REARM_SWEEPER_BUCKET_SIDS) for (const patch of [
      (x: any) => ({ ...x, Principal: { Service: "s3.amazonaws.com" } }),
      (x: any) => ({ ...x, Principal: { AWS: [roleArn, pEraser] } }),
      (x: any) => ({ ...x, Principal: { AWS: `arn:aws:iam::${sweeperAccount}:role/other` } }),
      (x: any) => ({ ...x, Principal: "*" }),
      ({ Principal, ...x }: any) => x,
      (x: any) => ({ ...x, Resource: `${P.bucketArn}/*` }),
    ]) {
      const g = grant(sid, patch);
      expect(() => one(g(grantsOnly), g(withDeny))).toThrow(new RegExp(`the live ${sid} must be exactly the reviewed grant`));
      expect(() => one(g(withDeny), g(grantsOnly), { ...set, rearmSweeperStateDeny: false })).toThrow(new RegExp(`the live ${sid} must be exactly the reviewed grant`));
    }
    expect(() => one(grantsOnly, withDeny, { ...set, rearmSweeperStateDeny: false })).toThrow(/adds the state deny but transcriptStore.rearmSweeperStateDeny is false/);
    expect(() => one(grantsOnly, withDeny, { ...set, rearmSweeperStateDeny: "false" })).toThrow(/rearmSweeperStateDeny must be true or false/);
    const deny = (patch: (x: any) => any) => withDeny.map((x: any) => x.Sid === REARM_SWEEPER_STATE_DENY_SID ? patch(x) : x);
    for (const tampered of [
      deny((x) => ({ ...x, Action: ["s3:DeleteObject"] })),
      deny((x) => ({ ...x, Action: ["s3:*"] })),
      deny((x) => ({ ...x, Resource: `${P.bucketArn}/96bulls/*` })),
      deny((x) => ({ ...x, Effect: "Allow" })),
      deny((x) => ({ ...x, Condition: { ArnNotEquals: { "aws:PrincipalArn": [roleArn, pEraser] } } })),
      deny((x) => ({ ...x, Condition: { ArnNotEquals: { "aws:PrincipalArn": `arn:aws:iam::${sweeperAccount}:role/other` } } })),
    ]) expect(() => one(grantsOnly, tampered)).toThrow(/RearmSweeperOnlyItWritesItsState must be exactly the reviewed statement/);
    const other = withDeny.map((x: any) => x.Sid === "DenyTlsBelow12" ? { ...x, Effect: "Allow" } : x);
    expect(() => one(grantsOnly, other)).toThrow(/every other statement must stay byte-identical/);
    const grantChanged = withDeny.map((x: any) => x.Sid === "RearmSweeperReads" ? { ...x, Resource: `${P.bucketArn}/*` } : x);
    expect(() => one(grantsOnly, grantChanged)).toThrow(/plan contract refused/);
    expect(() => check({ transcripts: bucketBlock(grantsOnly, withDeny, "a", "b") }, { transcripts: "update" }, set)).toThrow(/only policy statements may change/);
  });

  it("rolls back (removes only the deny) only with rearmSweeperStateDeny false", () => {
    expect(one(withDeny, grantsOnly, { ...set, rearmSweeperStateDeny: false })).toBe("rearm sweeper writer deny");
    expect(() => one(withDeny, grantsOnly)).toThrow(/removes the state deny but transcriptStore.rearmSweeperStateDeny is not false/);
    expect(() => one(withDeny, grantsOnly, {})).toThrow(/rearmSweeperRoleArn is unset/);
    // A drifted live deny with this Sid is never removed under D5: a broadened resource, a changed principal
    // condition, a wider action set or another effect each refuse the rollback (and the full revocation).
    const off = { ...set, rearmSweeperStateDeny: false };
    const liveDeny = (patch: (x: any) => any) => withDeny.map((x: any) => x.Sid === REARM_SWEEPER_STATE_DENY_SID ? patch(x) : x);
    const drifts = [
      liveDeny((x) => ({ ...x, Resource: `${P.bucketArn}/96bulls/*` })),
      liveDeny((x) => ({ ...x, Resource: `${P.bucketArn}/*` })),
      liveDeny((x) => ({ ...x, Condition: { ArnNotEquals: { "aws:PrincipalArn": [roleArn, pEraser] } } })),
      liveDeny((x) => ({ ...x, Condition: { ArnNotEquals: { "aws:PrincipalArn": `arn:aws:iam::${sweeperAccount}:role/other` } } })),
      liveDeny((x) => ({ ...x, Action: ["s3:*"] })),
      liveDeny(({ Condition, ...x }) => x),
    ];
    for (const drifted of drifts) {
      expect(() => one(drifted, grantsOnly, off)).toThrow(/RearmSweeperOnlyItWritesItsState must be exactly the reviewed statement to be removed/);
      const both = { transcripts: "update", "transcripts-key": "update" };
      expect(() => check({ transcripts: bucketBlock(drifted, pBucket()), "transcripts-key": keyBlock(pKey(roleArn), pKey()) }, both)).toThrow(/RearmSweeperOnlyItWritesItsState must be exactly the reviewed statement to be removed/);
    }
    // The ordinary rollback and revocation stay accepted.
    expect(one(withDeny, grantsOnly, off)).toBe("rearm sweeper writer deny");
    expect(check({ transcripts: bucketBlock(withDeny, pBucket()), "transcripts-key": keyBlock(pKey(roleArn), pKey()) }, { transcripts: "update", "transcripts-key": "update" })).toBe("rearm sweeper grants");
  });

  it("a fresh grant carries the deny exactly when it is on; a revocation removes whichever set is live", () => {
    const both = { transcripts: "update", "transcripts-key": "update" };
    const fresh = (planned: unknown[]) => ({ transcripts: bucketBlock(pBucket(), planned), "transcripts-key": keyBlock(pKey(), pKey(roleArn)) });
    expect(check(fresh(withDeny), both, set)).toBe("rearm sweeper grants");
    expect(check(fresh(grantsOnly), both, { ...set, rearmSweeperStateDeny: false })).toBe("rearm sweeper grants");
    expect(() => check(fresh(grantsOnly), both, set)).toThrow(/must add RearmSweeperOnlyItWritesItsState with the grants/);
    expect(() => check(fresh(withDeny), both, { ...set, rearmSweeperStateDeny: false })).toThrow(/adds RearmSweeperOnlyItWritesItsState but transcriptStore.rearmSweeperStateDeny is false/);
    expect(() => check(fresh(withDeny), both)).toThrow(/rearmSweeperRoleArn is unset/);
    const revoke = (live: unknown[]) => ({ transcripts: bucketBlock(live, pBucket()), "transcripts-key": keyBlock(pKey(roleArn), pKey()) });
    expect(check(revoke(withDeny), both)).toBe("rearm sweeper grants");
    expect(check(revoke(grantsOnly), both)).toBe("rearm sweeper grants");
    expect(() => check(revoke(withDeny), both, set)).toThrow(/removes the rearm statements while transcriptStore.rearmSweeperRoleArn is set/);
    // Never the grants alone with the state deny left behind.
    const strand = { transcripts: bucketBlock(withDeny, [...pBucket(), rearmSweeperStateDenyStatement({ bucketArn: P.bucketArn, roleArn })]), "transcripts-key": keyBlock(pKey(roleArn), pKey()) };
    expect(() => check(strand, both)).toThrow(/must add \(or, to revoke, remove\) exactly/);
  });

  it("no other phase may carry it: with the cross-account worker grants it is refused there", () => {
    const worker = `arn:aws:iam::${sweeperAccount}:role/synthetic-sessions-worker`;
    expect(() => one(grantsOnly, [...withDeny, ...crossAccountWorkerBucketStatements({ bucketAccountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: P.bucketArn, roleArn: worker })])).toThrow(/plan contract refused/);
    expect(check({}, {}, set)).toBe("post-apply");
  });
});

describe("D5 pass-receipt lifecycle rule", () => {
  const base = [
    { ID: "expire-noncurrent-versions", Status: "Enabled", Filter: { Prefix: "" }, NoncurrentVersionExpiration: { NoncurrentDays: 1 } },
    { ID: "remove-expired-delete-markers", Status: "Enabled", Filter: { Prefix: "" }, Expiration: { ExpiredObjectDeleteMarker: true } },
    { ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 } },
  ];
  // Synthetic day counts for the tests only: the stage has no default and no chosen value.
  const D1 = 11, D2 = 13;

  it("omitted: the lifecycle rules are exactly as before; set: one current-expiry rule on passes/ only, last", () => {
    expect(transcriptLifecycleRules()).toEqual(base);
    expect(transcriptLifecycleRules({})).toEqual(base);
    expect(transcriptLifecycleRules({ rearmPassReceiptRetentionDays: D1 })).toEqual([...base,
      { ID: REARM_PASS_RECEIPT_RULE_ID, Status: "Enabled", Filter: { Prefix: "96bulls/.ledger/rearm-sweeper/passes/" }, Expiration: { Days: D1 } }]);
    // Never the cursors beside the receipts, never noncurrent-only, never a delete-marker rule.
    const rule = transcriptLifecycleRules({ rearmPassReceiptRetentionDays: D1 }).at(-1)!;
    expect(Object.keys(rule.Expiration!)).toEqual(["Days"]);
    expect("96bulls/.ledger/rearm-sweeper/cursor-0123.json".startsWith(rule.Filter.Prefix)).toBe(false);
    for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 31, "7", null, true] as unknown[])
      expect(() => transcriptLifecycleRules({ rearmPassReceiptRetentionDays: bad as number })).toThrow(/rearmPassReceiptRetentionDays must be a positive whole number of days/);
  });

  const lifeBlock = (liveDays: number | undefined, plannedDays: number | undefined, extra: string[] = []) => [
    "  [transcripts] update", "  properties:", `    bucketName: ${P.bucket}`,
    ...mark("-", ["    lifecycleRules:", ...yaml(transcriptLifecycleRules(liveDays === undefined ? {} : { rearmPassReceiptRetentionDays: liveDays }), 6)]),
    ...mark("+", ["    lifecycleRules:", ...yaml(transcriptLifecycleRules(plannedDays === undefined ? {} : { rearmPassReceiptRetentionDays: plannedDays }), 6)]),
    ...extra,
  ];
  const life = (block: string[], inputs: Record<string, unknown> = {}, actions: Record<string, string> = { transcripts: "update" }) => check({ transcripts: block }, actions, inputs);
  /** The phase, or the refusal's message (so a wrong outcome is an assertion, not a throw). */
  const outcome = (block: string[], inputs: Record<string, unknown> = {}) => { try { return life(block, inputs); } catch (e) { return String((e as Error).message); } };

  it("accepts adding, changing and (input unset) removing exactly the rule, alone", () => {
    expect(outcome(lifeBlock(undefined, D1), { rearmPassReceiptRetentionDays: D1 })).toBe("rearm receipt retention");
    expect(outcome(lifeBlock(D1, D2), { rearmPassReceiptRetentionDays: D2 })).toBe("rearm receipt retention");
    expect(outcome(lifeBlock(D1, undefined))).toBe("rearm receipt retention");
    // Independent of the sweeper role and the deny.
    expect(life(lifeBlock(undefined, D1), { rearmPassReceiptRetentionDays: D1, rearmSweeperRoleArn: roleArn })).toBe("rearm receipt retention");
    // The quoted prefix in the plan YAML is compared unquoted.
    expect(lifeBlock(undefined, D1).join("\n")).toContain('Prefix: "96bulls/.ledger/rearm-sweeper/passes/"');
  });

  it("refuses a mismatched, missing or invalid input", () => {
    expect(() => life(lifeBlock(undefined, D1), { rearmPassReceiptRetentionDays: D2 })).toThrow(/rearm receipt retention: the planned lifecycle rules must be exactly the stack's with the pass-receipt rule/);
    expect(() => life(lifeBlock(undefined, D1))).toThrow(/rearmPassReceiptRetentionDays is unset/);
    expect(() => life(lifeBlock(D1, undefined), { rearmPassReceiptRetentionDays: D1 })).toThrow(/with the pass-receipt rule/);
    for (const bad of [0, -3, 1.5, "11", null, true, Number.NaN])
      expect(() => life(lifeBlock(undefined, D1), { rearmPassReceiptRetentionDays: bad })).toThrow(/is not a positive whole number of days/);
  });

  it("refuses any other lifecycle change, any other property, or any other resource with it", () => {
    const tweak = (rules: unknown[]) => [
      "  [transcripts] update", "  properties:", `    bucketName: ${P.bucket}`,
      ...mark("-", ["    lifecycleRules:", ...yaml(transcriptLifecycleRules(), 6)]),
      ...mark("+", ["    lifecycleRules:", ...yaml(rules, 6)]),
    ];
    const withRule = transcriptLifecycleRules({ rearmPassReceiptRetentionDays: D1 });
    const inputs = { rearmPassReceiptRetentionDays: D1 };
    expect(() => life(tweak(withRule.map((r: any) => r.ID === "expire-noncurrent-versions" ? { ...r, NoncurrentVersionExpiration: { NoncurrentDays: 9 } } : r)), inputs)).toThrow(/planned lifecycle rules must be exactly/);
    expect(() => life(tweak(withRule.map((r: any) => r.ID === REARM_PASS_RECEIPT_RULE_ID ? { ...r, Filter: { Prefix: "96bulls/.ledger/" } } : r)), inputs)).toThrow(/planned lifecycle rules must be exactly/);
    expect(() => life(tweak(withRule.map((r: any) => r.ID === REARM_PASS_RECEIPT_RULE_ID ? { ...r, NoncurrentVersionExpiration: { NoncurrentDays: D1 } } : r)), inputs)).toThrow(/planned lifecycle rules must be exactly/);
    expect(() => life(tweak(withRule.filter((r: any) => r.ID !== "abort-incomplete-uploads")), inputs)).toThrow(/planned lifecycle rules must be exactly/);
    // A live side that is not the stack's.
    expect(() => life(["  [transcripts] update", "  properties:", ...mark("-", ["    lifecycleRules:", ...yaml(base.slice(1), 6)]), ...mark("+", ["    lifecycleRules:", ...yaml(withRule, 6)])], inputs)).toThrow(/live lifecycle rules must be exactly/);
    // Mixed with a policy change, or any other marked property.
    expect(() => life(lifeBlock(undefined, D1, [...mark("-", ["    tags: a"]), ...mark("+", ["    tags: b"])]), inputs)).toThrow(/only lifecycleRules may change/);
    const policyToo = [...lifeBlock(undefined, D1), ...mark("-", ["    policy:", ...yaml(pBucket(), 6)]), ...mark("+", ["    policy:", ...yaml(pBucket(roleArn), 6)])];
    expect(() => life(policyToo, { ...inputs, rearmSweeperRoleArn: roleArn })).toThrow(/only lifecycleRules may change/);
    expect(() => life(lifeBlock(undefined, D1), inputs, { transcripts: "update", "transcript-reader": "update" })).toThrow(/rearm receipt retention: must deploy alone/);
    expect(() => life(lifeBlock(D1, D1), inputs)).toThrow(/the lifecycle rules do not change/);
  });

  it("omitted retention leaves a plan without lifecycle changes outside this phase", () => {
    expect(check({}, {}, {})).toBe("post-apply");
    expect(check({}, {}, { rearmSweeperRoleArn: roleArn })).toBe("post-apply");
  });
});
