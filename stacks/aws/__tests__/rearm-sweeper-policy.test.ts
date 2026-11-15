import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import {
  REARM_SWEEPER_BUCKET_SIDS, REARM_SWEEPER_KEY_SIDS, REARM_SWEEPER_SIDS, assertRearmSweeperRoleArn, rearmSweeperBucketStatements, rearmSweeperKeyStatement,
} from "../rearm-sweeper-policy.ts";
import { crossAccountWorkerBucketStatements } from "../cross-account-worker-policy.ts";
import {
  assertTranscriptStorePlanContract, transcriptBucketPolicy, transcriptKeyPolicy, transcriptNames,
  TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION, TRANSCRIPT_STAGE,
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
// As the stage renders it: the deny exempts the sweeper (last), and the rearm statements come last in the policy.
const pBucket = (rearm?: string, worker?: string, gc: string | null = pGc) => [
  ...transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: P.bucketArn, keyArn: pKeyArn, eraserArn: pEraser, ...(gc ? { bufferedGcRoleArn: gc } : {}), ...(rearm ? { rearmSweeperRoleArn: rearm } : {}), ...(worker ? { crossAccountWorkerRoleArn: worker } : {}) }),
  ...(rearm ? rearmSweeperBucketStatements({ bucketArn: P.bucketArn, roleArn: rearm }) : []),
];
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
    expect(() => one([...liveBucket, ...rearmSweeperBucketStatements({ bucketArn: P.bucketArn, roleArn })])).toThrow(gains);
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
    const statementsOnly = [...liveBucket, ...rearmSweeperBucketStatements({ bucketArn: P.bucketArn, roleArn })];
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
