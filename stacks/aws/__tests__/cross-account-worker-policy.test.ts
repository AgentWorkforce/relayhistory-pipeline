import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  assertCrossAccountWorkerRoleArn, crossAccountWorkerBucketStatements, crossAccountWorkerKeyStatements,
} from "../cross-account-worker-policy.ts";
import {
  assertTranscriptStoreInputs, assertTranscriptStorePlanContract, transcriptBucketPolicy, transcriptKeyPolicy, transcriptNames,
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
// The plan contract is for the production stage, so its policies are built with the stage's own names.
const P = transcriptNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID);
const pKeyArn = `arn:aws:kms:${region}:${TRANSCRIPT_ACCOUNT_ID}:key/synthetic`, pEraser = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:user/synthetic-eraser`;
const pBucket = (worker?: string) => transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: P.bucketArn, keyArn: pKeyArn, eraserArn: pEraser, ...(worker ? { crossAccountWorkerRoleArn: worker } : {}) });
const pKey = (worker?: string) => transcriptKeyPolicy({ ...keyArgs, accountId: TRANSCRIPT_ACCOUNT_ID, bucketName: P.bucket, ...(worker ? { crossAccountWorkerRoleArn: worker } : {}) });
const bucketBlock = (live: unknown[], planned: unknown[], tagsLive = "a", tagsPlanned = "a") => [
  "  [transcripts] update", "  properties:", `    bucketName: ${P.bucket}`,
  ...mark("-", ["    policy:", ...yaml(live, 6)]), ...mark("+", ["    policy:", ...yaml(planned, 6)]),
  ...(tagsLive === tagsPlanned ? [`    tags: ${tagsLive}`] : [...mark("-", [`    tags: ${tagsLive}`]), ...mark("+", [`    tags: ${tagsPlanned}`])]),
];
const keyBlock = (live: { Statement: unknown[] }, planned: { Statement: unknown[] }) => [
  "  [transcripts-key] update", "  properties:", "    enableKeyRotation: true", "    policy:",
  ...mark("-", yaml(live, 6)), ...mark("+", yaml(planned, 6)),
];
function check(blocks: Record<string, string[]>, actions: Record<string, string>) {
  const counts = new Map<string, number>();
  for (const a of Object.values(actions)) counts.set(a, (counts.get(a) ?? 0) + 1);
  const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: 0 changes";
  const compact = `${summary}\n${RESOURCES.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const detailed = `${summary}\n${RESOURCES.map((r) => blocks[r] ? blocks[r]!.join("\n") : `  [${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const provenanceText = JSON.stringify({ schema: 1, stage: TRANSCRIPT_STAGE, summary,
    outputs: { plan: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.detailed.txt`, sha256: h(detailed) } } });
  return assertTranscriptStorePlanContract(compact, { detailedPlanText: detailed, provenanceText });
}

describe("cross-account worker grants plan phase", () => {
  const liveBucket = pBucket();
  const plannedBucket = pBucket(roleArn);
  const liveKey = pKey();
  const plannedKey = pKey(roleArn);
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

  it("refuses a worker statement that is not exactly the reviewed one", () => {
    const sub = (sid: string, patch: (s: any) => any) => plannedBucket.map((s: any) => s.Sid === sid ? patch(s) : s);
    const subKey = (sid: string, patch: (s: any) => any) => ({ ...plannedKey, Statement: plannedKey.Statement.map((s: any) => s.Sid === sid ? patch(s) : s) });
    // Widened principal: anyone, or a second role.
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("SessionsWorkerReads", (s) => ({ ...s, Principal: "*" }))) }, { transcripts: "update" })).toThrow(/SessionsWorkerReads must be exactly the reviewed statement/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("SessionsWorkerReads", (s) => ({ ...s, Principal: { AWS: `arn:aws:iam::${workerAccount}:role/other` } }))) }, { transcripts: "update" })).toThrow(/exactly one role/);
    // Widened actions and resources.
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("SessionsWorkerReads", (s) => ({ ...s, Action: "s3:*" }))) }, { transcripts: "update" })).toThrow(/SessionsWorkerReads must be exactly the reviewed statement/);
    expect(() => check({ transcripts: bucketBlock(liveBucket, sub("SessionsWorkerWritesSummaries", (s) => ({ ...s, Resource: `${P.bucketArn}/*` }))) }, { transcripts: "update" })).toThrow(/SessionsWorkerWritesSummaries must be exactly the reviewed statement/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, subKey("SessionsWorkerDecryptsViaS3", (s) => ({ ...s, Action: "kms:*" }))) }, { "transcripts-key": "update" })).toThrow(/SessionsWorkerDecryptsViaS3 must be exactly the reviewed statement/);
    // Dropped condition.
    expect(() => check({ "transcripts-key": keyBlock(liveKey, subKey("SessionsWorkerEncryptsSummariesViaS3", ({ Condition, ...s }) => s)) }, { "transcripts-key": "update" })).toThrow(/SessionsWorkerEncryptsSummariesViaS3 must be exactly the reviewed statement/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, subKey("SessionsWorkerDecryptsViaS3", (s) => ({ ...s, Condition: { StringEquals: { "kms:ViaService": "s3.us-east-1.amazonaws.com" } } }))) }, { "transcripts-key": "update" })).toThrow(/SessionsWorkerDecryptsViaS3 must be exactly the reviewed statement/);
    // A same-account role under the worker Sids.
    const same = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/inside`;
    expect(() => check({ transcripts: bucketBlock(liveBucket, [...liveBucket, ...crossAccountWorkerBucketStatements({ bucketAccountId: "999999999999", bucketArn: P.bucketArn, roleArn: same })]) }, { transcripts: "update" })).toThrow(/exact role in another account/);
  });

  it("refuses a plan whose bucket and key grants name different roles", () => {
    const other = `arn:aws:iam::${workerAccount}:role/other-worker`;
    expect(() => check({ transcripts: bucketBlock(liveBucket, pBucket(roleArn)), "transcripts-key": keyBlock(liveKey, pKey(other)) }, both)).toThrow(/must name the same role/);
  });

  it("the grant deploys alone: any other change in the same plan is refused, whatever phase it would match", () => {
    expect(() => check({ transcripts: bucketBlock(liveBucket, plannedBucket) }, { transcripts: "update", "transcript-reader": "update" })).toThrow(/must deploy alone/);
    // Ride-alongs on phases that accept a key or bucket update without inspecting it (P2-2).
    expect(() => check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, { "transcript-srv-writer": "create", "transcripts-key": "update" })).toThrow(/must deploy alone/);
    // A worker statement changed in place (role rotation) under another phase is refused too.
    const rotated = `arn:aws:iam::${workerAccount}:role/rotated`;
    expect(() => check({ "transcripts-key": keyBlock(plannedKey, pKey(rotated)) }, { "transcript-srv-writer": "create", "transcripts-key": "update" })).toThrow(/must deploy alone/);
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
    const withWorker = pKey(roleArn);
    const extra = { ...withWorker, Statement: [...withWorker.Statement, { Sid: "SrvWriterSomething", Effect: "Allow", Principal: { AWS: "x" }, Action: "kms:Decrypt", Resource: "*" }] };
    expect(() => check({ "transcripts-key": keyBlock(withWorker, extra) }, { "transcripts-key": "update" })).not.toThrow(/cross-account worker grants/);
  });
});

describe("cross-account worker grants: stage wiring", () => {
  it("the stage declaration puts the worker statements in BOTH full policy documents, and nothing without the input", () => {
    const modulePath = fileURLToPath(new URL("../transcript-store.ts", import.meta.url));
    const gc = `arn:aws:iam::${accountId}:role/synthetic-buffered-gc`;
    const script = `import { mock } from 'bun:test'; import * as E from 'effect/Effect';
      const seen=[]; const factory=(type)=>(name,props)=>{seen.push({type,name,props});return E.succeed({userName:props.userName,userArn:'arn:aws:iam::${accountId}:user/'+props.userName,keyArn:'${keyArn}',keyId:'synthetic',bucketName:props.bucketName,trailName:props.trailName})};
      mock.module('@alchemy.run/aws',()=>({IAM:{User:factory('user')},KMS:{Key:factory('key'),Alias:factory('alias')},S3:{Bucket:factory('bucket')},CloudTrail:{Trail:factory('trail')},Budgets:{Budget:factory('budget')}}));
      const {transcriptStoreStage}=await import(${JSON.stringify(modulePath)});
      const config={stage:'synthetic',accountId:'${accountId}',region:'${region}',budgetUsd:1,budgetEmail:'test@example.invalid',transcriptStore:{bufferedGcRoleArn:'${gc}',crossAccountWorkerRoleArn:'${roleArn}'},tags:()=>({})};
      await E.runPromise(transcriptStoreStage(config));const enabled=structuredClone(seen);seen.length=0;
      await E.runPromise(transcriptStoreStage({...config,transcriptStore:{bufferedGcRoleArn:'${gc}'}}));console.log(JSON.stringify({enabled,legacy:seen}));`;
    const result = spawnSync(process.execPath, ["--eval", script], { encoding: "utf8", env: { PATH: process.env.PATH }, timeout: 30_000 });
    expect(result.status).toBe(0);
    const { enabled, legacy } = JSON.parse(result.stdout) as { enabled: { name: string; props: any }[]; legacy: { name: string; props: any }[] };
    const name = `96bulls-synthetic-transcripts-${accountId}`;
    const worker = (s: any) => s.Sid.startsWith("SessionsWorker");
    expect(enabled.find((r) => r.name === "transcripts-key")!.props.policy.Statement.filter(worker)).toEqual(crossAccountWorkerKeyStatements({ bucketAccountId: accountId, region, bucketName: name, roleArn }));
    expect(enabled.find((r) => r.name === "transcripts")!.props.policy.filter(worker)).toEqual(crossAccountWorkerBucketStatements({ bucketAccountId: accountId, bucketArn: `arn:aws:s3:::${name}`, roleArn }));
    expect(legacy.find((r) => r.name === "transcripts-key")!.props.policy.Statement.some(worker)).toBe(false);
    expect(legacy.find((r) => r.name === "transcripts")!.props.policy.some(worker)).toBe(false);
    // Without the input, everything the stage declares is identical to the stage with it minus the worker statements.
    const strip = (rs: { name: string; props: any }[]) => JSON.stringify(rs.map((r) => r.name === "transcripts" ? { ...r, props: { ...r.props, policy: r.props.policy.filter((s: any) => !worker(s)) } }
      : r.name === "transcripts-key" ? { ...r, props: { ...r.props, policy: { ...r.props.policy, Statement: r.props.policy.Statement.filter((s: any) => !worker(s)) } } } : r));
    expect(strip(enabled)).toBe(strip(legacy));
  });
});
