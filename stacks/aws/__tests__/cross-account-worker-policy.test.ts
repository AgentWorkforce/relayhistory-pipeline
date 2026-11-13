import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import {
  assertCrossAccountWorkerRoleArn, crossAccountWorkerBucketStatements, crossAccountWorkerKeyStatements,
} from "../cross-account-worker-policy.ts";
import {
  assertTranscriptStoreInputs, assertTranscriptStorePlanContract, transcriptBucketPolicy, transcriptKeyPolicy,
  TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION, TRANSCRIPT_STAGE,
} from "../transcript-store.ts";

// Synthetic placeholders only (public repo): the bucket account and the worker's (other) account.
const accountId = "111122223333", workerAccount = "444455556666", region = "us-east-1";
const bucketName = "synthetic-transcripts-111122223333", bucketArn = `arn:aws:s3:::${bucketName}`;
const roleArn = `arn:aws:iam::${workerAccount}:role/synthetic-sessions-worker`;
const keyArn = `arn:aws:kms:${region}:${accountId}:key/synthetic`, eraserArn = `arn:aws:iam::${accountId}:user/synthetic-eraser`;
const keyArgs = { accountId, region, bucketName, writerArn: "writer", srvWriterArn: "srv", readerArn: "reader", pipelineRoleArn: "pipeline" };
const gcRole = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-buffered-gc`;
const production = (extra: Record<string, unknown> = {}) => ({ accountId: TRANSCRIPT_ACCOUNT_ID, region: TRANSCRIPT_REGION, partition: "aws",
  transcriptStore: { bufferedGcRoleArn: gcRole, ...extra } });

describe("cross-account sessions worker grants", () => {
  it("accepts only an exact named role in another account; omission is allowed", () => {
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production())).not.toThrow();
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production({ crossAccountWorkerRoleArn: roleArn }))).not.toThrow();
    expect(assertCrossAccountWorkerRoleArn(`arn:aws:iam::${workerAccount}:role/path/worker`, accountId)).toBe(workerAccount);
    for (const bad of [null, false, "", "*", [roleArn], `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/same-account`,
      `arn:aws:iam::${workerAccount}:role/*`, `arn:aws:iam::${workerAccount}:user/worker`, `arn:aws:iam::${workerAccount}:root`,
      `arn:aws:sts::${workerAccount}:assumed-role/worker/s`, `arn:aws-cn:iam::${workerAccount}:role/worker`, `arn:aws:iam::12345:role/worker`,
      `${roleArn} `, `${roleArn}\n`]) {
      expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production({ crossAccountWorkerRoleArn: bad }))).toThrow(/crossAccountWorkerRoleArn/);
    }
  });

  it("emits exactly the reviewed statements (infra terraform/examples/aws-sessions-worker-cross-account)", () => {
    const Principal = { AWS: roleArn };
    expect(crossAccountWorkerBucketStatements({ bucketAccountId: accountId, bucketArn, roleArn })).toEqual([
      { Sid: "SessionsWorkerLists", Effect: "Allow", Principal, Action: "s3:ListBucket", Resource: bucketArn },
      { Sid: "SessionsWorkerReads", Effect: "Allow", Principal, Action: "s3:GetObject", Resource: `${bucketArn}/96bulls/*/v1/*` },
      { Sid: "SessionsWorkerWritesSummaries", Effect: "Allow", Principal, Action: "s3:PutObject", Resource: `${bucketArn}/96bulls/*/v1/*/summary/*` },
    ]);
    const via = { "kms:ViaService": "s3.us-east-1.amazonaws.com", "kms:CallerAccount": workerAccount };
    expect(crossAccountWorkerKeyStatements({ bucketAccountId: accountId, region, bucketName, roleArn })).toEqual([
      { Sid: "SessionsWorkerDecryptsViaS3", Effect: "Allow", Principal, Action: "kms:Decrypt", Resource: "*",
        Condition: { StringEquals: via, StringLike: { "kms:EncryptionContext:aws:s3:arn": `${bucketArn}/96bulls/*/v1/*` } } },
      { Sid: "SessionsWorkerEncryptsSummariesViaS3", Effect: "Allow", Principal, Action: "kms:GenerateDataKey", Resource: "*",
        Condition: { StringEquals: via, StringLike: { "kms:EncryptionContext:aws:s3:arn": `${bucketArn}/96bulls/*/v1/*/summary/*` } } },
    ]);
  });

  it("absent: both policies are unchanged; present: only the worker statements are appended", () => {
    const key = transcriptKeyPolicy(keyArgs);
    expect(transcriptKeyPolicy({ ...keyArgs, crossAccountWorkerRoleArn: undefined })).toEqual(key);
    const keyWith = transcriptKeyPolicy({ ...keyArgs, crossAccountWorkerRoleArn: roleArn });
    expect(keyWith.Statement.slice(0, -2)).toEqual(key.Statement);
    expect(keyWith.Statement.slice(-2).map((s) => s.Sid)).toEqual(["SessionsWorkerDecryptsViaS3", "SessionsWorkerEncryptsSummariesViaS3"]);
    const bucket = transcriptBucketPolicy({ accountId, bucketArn, keyArn, eraserArn });
    const bucketWith = transcriptBucketPolicy({ accountId, bucketArn, keyArn, eraserArn, crossAccountWorkerRoleArn: roleArn });
    expect(bucketWith.slice(0, -3)).toEqual(bucket);
    expect(bucketWith.slice(-3).map((s) => s.Sid)).toEqual(["SessionsWorkerLists", "SessionsWorkerReads", "SessionsWorkerWritesSummaries"]);
    expect(() => transcriptKeyPolicy({ ...keyArgs, crossAccountWorkerRoleArn: "" })).toThrow();
    // The bucket builder needs the bucket account to tell cross-account from same-account.
    expect(() => transcriptBucketPolicy({ bucketArn, keyArn, eraserArn, crossAccountWorkerRoleArn: roleArn })).toThrow();
  });

  it("never grants delete, old versions or writes outside summary/; the eraser-only deny still covers the worker", () => {
    const bucketWith = transcriptBucketPolicy({ accountId, bucketArn, keyArn, eraserArn, crossAccountWorkerRoleArn: roleArn }) as any[];
    const worker = bucketWith.filter((s) => s.Principal?.AWS === roleArn);
    for (const s of worker) {
      expect(s.Effect).toBe("Allow");
      expect([].concat(s.Action)).not.toContainEqual(expect.stringMatching(/Delete|Version|\*/));
    }
    expect(worker.find((s) => s.Action === "s3:PutObject").Resource).toEndWith("/summary/*");
    const deny = bucketWith.find((s) => s.Sid === "OnlyTheEraserDeletes");
    expect(deny.Principal).toBe("*");
    expect(JSON.stringify(deny.Condition)).not.toContain(roleArn);
  });
});

// ---- plan contract: the grants deploy alone, append-only ----
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
const bucketBlock = (live: unknown[], planned: unknown[], tagsLive = "a", tagsPlanned = "a") => [
  "  [transcripts] update", "  properties:", `    bucketName: ${bucketName}`,
  ...mark("-", ["    policy:", ...yaml(live, 6)]), ...mark("+", ["    policy:", ...yaml(planned, 6)]),
  ...(tagsLive === tagsPlanned ? [`    tags: ${tagsLive}`] : [...mark("-", [`    tags: ${tagsLive}`]), ...mark("+", [`    tags: ${tagsPlanned}`])]),
];
const keyBlock = (live: { Statement: unknown[] }, planned: { Statement: unknown[] }) => [
  "  [transcripts-key] update", "  properties:", "    enableKeyRotation: true", "    policy:",
  ...mark("-", yaml(live, 6)), ...mark("+", yaml(planned, 6)),
];
function check(blocks: Record<string, string[]>, actions: Record<string, string>) {
  const counts = Object.values(actions).length;
  const summary = counts ? `Plan: ${counts} to update` : "Plan: 0 changes";
  const compact = `${summary}\n${RESOURCES.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const detailed = `${summary}\n${RESOURCES.map((r) => blocks[r] ? blocks[r]!.join("\n") : `  [${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const provenanceText = JSON.stringify({ schema: 1, stage: TRANSCRIPT_STAGE, summary,
    outputs: { plan: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.detailed.txt`, sha256: h(detailed) } } });
  return assertTranscriptStorePlanContract(compact, { detailedPlanText: detailed, provenanceText });
}

describe("cross-account worker grants plan phase", () => {
  const liveBucket = transcriptBucketPolicy({ accountId, bucketArn, keyArn, eraserArn });
  const plannedBucket = transcriptBucketPolicy({ accountId, bucketArn, keyArn, eraserArn, crossAccountWorkerRoleArn: roleArn });
  const liveKey = transcriptKeyPolicy(keyArgs);
  const plannedKey = transcriptKeyPolicy({ ...keyArgs, crossAccountWorkerRoleArn: roleArn });
  const both = { transcripts: "update", "transcripts-key": "update" };

  it("accepts exactly the appended worker statements on the bucket and key policies", () => {
    expect(check({ transcripts: bucketBlock(liveBucket, plannedBucket), "transcripts-key": keyBlock(liveKey, plannedKey) }, both)).toBe("cross-account worker grants");
    expect(check({ transcripts: bucketBlock(liveBucket, plannedBucket) }, { transcripts: "update" })).toBe("cross-account worker grants");
    expect(check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, { "transcripts-key": "update" })).toBe("cross-account worker grants");
  });

  it("refuses a changed existing statement, a missing or extra Sid, and any other property change", () => {
    const tampered = plannedBucket.map((s) => s.Sid === "SseHeaderMustNameThisKey" ? { ...s, Effect: "Allow" } : s);
    expect(() => check({ transcripts: bucketBlock(liveBucket, tampered) }, { transcripts: "update" })).toThrow(/SseHeaderMustNameThisKey must stay byte-identical/);
    const dropped = plannedBucket.filter((s) => s.Sid !== "OnlyTheEraserDeletes");
    expect(() => check({ transcripts: bucketBlock(liveBucket, dropped) }, { transcripts: "update" })).toThrow(/OnlyTheEraserDeletes must stay byte-identical/);
    const missing = plannedBucket.filter((s) => s.Sid !== "SessionsWorkerWritesSummaries");
    expect(() => check({ transcripts: bucketBlock(liveBucket, missing) }, { transcripts: "update" })).toThrow(/must add \(or, to revoke, remove\) exactly/);
    const extra = [...plannedBucket, { ...plannedBucket.at(-1)!, Sid: "SomethingElse" }];
    expect(() => check({ transcripts: bucketBlock(liveBucket, extra) }, { transcripts: "update" })).toThrow(/must add \(or, to revoke, remove\) exactly/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, plannedBucket, "a", "b") }, { transcripts: "update" })).toThrow(/only policy statements may change/);
    const keyTampered = { ...plannedKey, Statement: plannedKey.Statement.map((s) => s.Sid === "ReaderDecryptsViaS3" ? { ...s, Action: ["kms:*"] } : s) };
    expect(() => check({ "transcripts-key": keyBlock(liveKey, keyTampered) }, { "transcripts-key": "update" })).toThrow(/ReaderDecryptsViaS3 must stay byte-identical/);
  });

  it("accepts the revocation (exactly the worker statements removed) and nothing half-way", () => {
    expect(check({ transcripts: bucketBlock(plannedBucket, liveBucket), "transcripts-key": keyBlock(plannedKey, liveKey) }, both)).toBe("cross-account worker grants");
    const partial = plannedBucket.filter((s) => s.Sid !== "SessionsWorkerLists");
    expect(() => check({ transcripts: bucketBlock(plannedBucket, partial) }, { transcripts: "update" })).toThrow(/must add \(or, to revoke, remove\) exactly/);
    const swap = [...liveBucket.filter((s) => s.Sid !== "DenyTlsBelow12"), ...plannedBucket.slice(-3)];
    expect(() => check({ transcripts: bucketBlock(liveBucket, swap) }, { transcripts: "update" })).toThrow(/DenyTlsBelow12 must stay byte-identical/);
  });

  it("is not this phase when anything else changes", () => {
    expect(() => check({ transcripts: bucketBlock(liveBucket, plannedBucket) }, { transcripts: "update", "transcript-reader": "update" })).toThrow(/plan contract refused/);
  });
});

describe("cross-account worker grants plan phase on Alchemy's real diff format", () => {
  // The captured #141 plan's live (-) bucket and key policies, replayed as an append of the worker statements.
  const real = readFileSync(new URL("./fixtures/aws-sessions-prod.pipeline-update.detailed.txt", import.meta.url), "utf8").split("\n");
  const block = (r: string) => {
    const start = real.findIndex((l) => l === `[${r}] update`);
    const end = real.findIndex((l, i) => i > start && /^\[[^\]]+\] \S+$/.test(l));
    return real.slice(start, end);
  };
  const appended = (r: string, extra: unknown[], at: number) => {
    const b = block(r);
    const live = b.filter((l) => l.startsWith("  - "));
    const extraLines = mark("+", yaml(extra, at)).map((l) => l);
    const planned = [...live.map((l) => `  + ${l.slice(4)}`), ...extraLines];
    return [`  [${r}] update`, ...b.slice(1).filter((l) => !/^ {2}[+-] /.test(l)), ...live, ...planned];
  };
  it("parses the captured policies and accepts only the append", () => {
    const realBucketArn = /bucketName: (\S+)/.exec(block("transcripts").join("\n"))![1]!;
    const ra = `arn:aws:iam::${workerAccount}:role/synthetic-sessions-worker`;
    const b = appended("transcripts", crossAccountWorkerBucketStatements({ bucketAccountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: `arn:aws:s3:::${realBucketArn}`, roleArn: ra }), 6);
    const k = appended("transcripts-key", crossAccountWorkerKeyStatements({ bucketAccountId: TRANSCRIPT_ACCOUNT_ID, region, bucketName: realBucketArn, roleArn: ra }), 8);
    expect(check({ transcripts: b, "transcripts-key": k }, { transcripts: "update", "transcripts-key": "update" })).toBe("cross-account worker grants");
  });
});

describe("cross-account worker grants: other key-only phases keep their own judgement", () => {
  it("a key policy re-rendered with the worker statements on both sides is not this phase", () => {
    const withWorker = transcriptKeyPolicy({ ...keyArgs, crossAccountWorkerRoleArn: roleArn });
    const extra = { ...withWorker, Statement: [...withWorker.Statement, { Sid: "SrvWriterSomething", Effect: "Allow", Principal: { AWS: "x" }, Action: "kms:Decrypt", Resource: "*" }] };
    expect(() => check({ "transcripts-key": keyBlock(withWorker, extra) }, { "transcripts-key": "update" })).not.toThrow(/cross-account worker grants/);
  });
});
