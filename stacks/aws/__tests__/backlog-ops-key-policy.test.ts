import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { assertBacklogOpsRoles, backlogOpsKeyStatements, backlogOpsRoleArns } from "../backlog-ops-key-policy.ts";
import { crossAccountWorkerKeyStatements } from "../cross-account-worker-policy.ts";
import {
  assertTranscriptStoreInputs, assertTranscriptStorePlanContract, transcriptKeyPolicy, transcriptNames,
  TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION, TRANSCRIPT_STAGE,
} from "../transcript-store.ts";

// Synthetic placeholders only (public repo), except the production stage constants the contract is pinned to.
const accountId = "111122223333", region = "us-east-1", stage = "synthetic";
const bucketName = "synthetic-transcripts-111122223333";
const keyArgs = { accountId, region, bucketName, writerArn: "writer", srvWriterArn: "srv", readerArn: "reader", pipelineRoleArn: "pipeline" };
const roles = backlogOpsRoleArns(accountId, stage);
const prodRoles = backlogOpsRoleArns(TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_STAGE);
const gcRole = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-buffered-gc`;
const production = (extra: Record<string, unknown> = {}) => ({ accountId: TRANSCRIPT_ACCOUNT_ID, region: TRANSCRIPT_REGION, partition: "aws",
  transcriptStore: { bufferedGcRoleArn: gcRole, ...extra } });

describe("backlog ops key grants: input", () => {
  it("accepts only this account's exact stage roles; omission is allowed", () => {
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production())).not.toThrow();
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production({ backlogOpsRoles: prodRoles }))).not.toThrow();
    const other = "444455556666";
    for (const bad of [null, false, "", [], prodRoles.readRoleArn, {},
      { readRoleArn: prodRoles.readRoleArn }, { writeRoleArn: prodRoles.writeRoleArn },
      { ...prodRoles, extra: 1 },
      { readRoleArn: prodRoles.writeRoleArn, writeRoleArn: prodRoles.readRoleArn },
      backlogOpsRoleArns(other, TRANSCRIPT_STAGE),
      backlogOpsRoleArns(TRANSCRIPT_ACCOUNT_ID, "aws-sessions-dev"),
      { ...prodRoles, readRoleArn: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/*` },
      { ...prodRoles, writeRoleArn: `${prodRoles.writeRoleArn} ` },
      { ...prodRoles, writeRoleArn: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:user/96bulls-${TRANSCRIPT_STAGE}-backlog-ops-write` },
      { ...prodRoles, readRoleArn: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/path/96bulls-${TRANSCRIPT_STAGE}-backlog-ops-read` }]) {
      expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production({ backlogOpsRoles: bad }))).toThrow(/backlogOpsRoles/);
    }
    expect(assertBacklogOpsRoles(roles, accountId, stage)).toEqual(roles);
  });

  it("emits exactly the reviewed statements (the live hand-applied ones, 2026-11-13)", () => {
    const Condition = { StringEquals: { "kms:ViaService": "s3.us-east-1.amazonaws.com" } };
    expect(backlogOpsKeyStatements({ accountId, stage, region, roles })).toEqual([
      { Sid: "BacklogOpsReadDecryptsViaS3", Effect: "Allow", Principal: { AWS: `arn:aws:iam::${accountId}:role/96bulls-synthetic-backlog-ops-read` }, Action: "kms:Decrypt", Resource: "*", Condition },
      { Sid: "BacklogOpsWriteEncryptsViaS3", Effect: "Allow", Principal: { AWS: `arn:aws:iam::${accountId}:role/96bulls-synthetic-backlog-ops-write` }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition },
    ]);
  });

  it("absent: the key policy is unchanged; present: only the two statements are appended", () => {
    const key = transcriptKeyPolicy(keyArgs);
    expect(transcriptKeyPolicy({ ...keyArgs, backlogOps: undefined })).toEqual(key);
    const keyWith = transcriptKeyPolicy({ ...keyArgs, backlogOps: { stage, roles } });
    expect(keyWith.Statement.slice(0, -2)).toEqual(key.Statement);
    expect(keyWith.Statement.slice(-2).map((s) => s.Sid)).toEqual(["BacklogOpsReadDecryptsViaS3", "BacklogOpsWriteEncryptsViaS3"]);
    expect(() => transcriptKeyPolicy({ ...keyArgs, backlogOps: { stage, roles: { ...roles, readRoleArn: "*" } } })).toThrow(/backlogOpsRoles/);
  });

  it("never grants anything but Decrypt and GenerateDataKey, and only via S3", () => {
    for (const s of backlogOpsKeyStatements({ accountId, stage, region, roles })) {
      expect(s.Effect).toBe("Allow");
      for (const a of ([] as string[]).concat(s.Action)) expect(["kms:Decrypt", "kms:GenerateDataKey"]).toContain(a);
      expect(s.Condition.StringEquals["kms:ViaService"]).toBe("s3.us-east-1.amazonaws.com");
    }
  });
});

// ---- plan contract: the grants deploy alone, append-only (cross-account-worker-policy.test.ts precedent) ----
const RESOURCES = [
  "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
  "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
  "transcripts-key", "transcripts-key-alias",
];
const h = (t: string) => createHash("sha256").update(t).digest("hex");
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
const P = transcriptNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID);
const pKey = (o: { backlog?: boolean; worker?: string } = {}) => transcriptKeyPolicy({ ...keyArgs, accountId: TRANSCRIPT_ACCOUNT_ID, bucketName: P.bucket,
  ...(o.worker ? { crossAccountWorkerRoleArn: o.worker } : {}), ...(o.backlog ? { backlogOps: { stage: TRANSCRIPT_STAGE, roles: prodRoles } } : {}) });
const keyBlock = (live: { Statement: unknown[] }, planned: { Statement: unknown[] }, rotationLive = true, rotationPlanned = true) => [
  "  [transcripts-key] update", "  properties:",
  ...(rotationLive === rotationPlanned ? [`    enableKeyRotation: ${rotationLive}`] : [...mark("-", [`    enableKeyRotation: ${rotationLive}`]), ...mark("+", [`    enableKeyRotation: ${rotationPlanned}`])]),
  "    policy:", ...mark("-", yaml(live, 6)), ...mark("+", yaml(planned, 6)),
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
const keyOnly = { "transcripts-key": "update" };

describe("backlog ops key grants plan phase", () => {
  const liveKey = pKey();
  const plannedKey = pKey({ backlog: true });
  const sub = (sid: string, patch: (s: any) => any) => ({ ...plannedKey, Statement: plannedKey.Statement.map((s: any) => s.Sid === sid ? patch(s) : s) });

  it("accepts exactly the two appended statements (state without them, the input set: the first deploy)", () => {
    expect(check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, keyOnly)).toBe("backlog ops key grants");
  });

  it("accepts the revocation (exactly both removed) and nothing half-way", () => {
    expect(check({ "transcripts-key": keyBlock(plannedKey, liveKey) }, keyOnly)).toBe("backlog ops key grants");
    const one = { ...plannedKey, Statement: plannedKey.Statement.filter((s) => s.Sid !== "BacklogOpsWriteEncryptsViaS3") };
    expect(() => check({ "transcripts-key": keyBlock(liveKey, one) }, keyOnly)).toThrow(/must add \(or, to revoke, remove\) exactly/);
    expect(() => check({ "transcripts-key": keyBlock(plannedKey, one) }, keyOnly)).toThrow(/must add \(or, to revoke, remove\) exactly/);
  });

  it("refuses a changed existing statement, an extra Sid, and any other property change", () => {
    const tampered = sub("ReaderDecryptsViaS3", (s) => ({ ...s, Action: ["kms:*"] }));
    expect(() => check({ "transcripts-key": keyBlock(liveKey, tampered) }, keyOnly)).toThrow(/ReaderDecryptsViaS3 must stay byte-identical/);
    const extra = { ...plannedKey, Statement: [...plannedKey.Statement, { Sid: "SomethingElse", Effect: "Allow", Principal: { AWS: "x" }, Action: "kms:Decrypt", Resource: "*" }] };
    expect(() => check({ "transcripts-key": keyBlock(liveKey, extra) }, keyOnly)).toThrow(/must add \(or, to revoke, remove\) exactly/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, plannedKey, true, false) }, keyOnly)).toThrow(/only policy statements may change/);
  });

  it("refuses a backlog-ops statement that is not exactly the reviewed one", () => {
    // Widened principal: anyone, the account root, another role, another account.
    for (const Principal of ["*", { AWS: "*" }, { AWS: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:root` }, { AWS: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/other` },
      { AWS: `arn:aws:iam::444455556666:role/96bulls-${TRANSCRIPT_STAGE}-backlog-ops-read` }])
      expect(() => check({ "transcripts-key": keyBlock(liveKey, sub("BacklogOpsReadDecryptsViaS3", (s) => ({ ...s, Principal }))) }, keyOnly)).toThrow(/BacklogOpsReadDecryptsViaS3 must be exactly the reviewed statement/);
    // The read role must not get the write role's actions (swapped principals).
    const swapped = { ...plannedKey, Statement: plannedKey.Statement.map((s: any) => s.Sid === "BacklogOpsReadDecryptsViaS3" ? { ...s, Principal: { AWS: prodRoles.writeRoleArn } } : s.Sid === "BacklogOpsWriteEncryptsViaS3" ? { ...s, Principal: { AWS: prodRoles.readRoleArn } } : s) };
    expect(() => check({ "transcripts-key": keyBlock(liveKey, swapped) }, keyOnly)).toThrow(/must be exactly the reviewed statement/);
    // Widened actions.
    expect(() => check({ "transcripts-key": keyBlock(liveKey, sub("BacklogOpsReadDecryptsViaS3", (s) => ({ ...s, Action: ["kms:Decrypt", "kms:GenerateDataKey"] }))) }, keyOnly)).toThrow(/BacklogOpsReadDecryptsViaS3 must be exactly/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, sub("BacklogOpsWriteEncryptsViaS3", (s) => ({ ...s, Action: "kms:*" }))) }, keyOnly)).toThrow(/BacklogOpsWriteEncryptsViaS3 must be exactly/);
    // Dropped or widened condition.
    expect(() => check({ "transcripts-key": keyBlock(liveKey, sub("BacklogOpsWriteEncryptsViaS3", ({ Condition, ...s }) => s)) }, keyOnly)).toThrow(/BacklogOpsWriteEncryptsViaS3 must be exactly/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, sub("BacklogOpsReadDecryptsViaS3", (s) => ({ ...s, Condition: { StringLike: { "kms:ViaService": "*" } } }))) }, keyOnly)).toThrow(/BacklogOpsReadDecryptsViaS3 must be exactly/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, sub("BacklogOpsReadDecryptsViaS3", (s) => ({ ...s, Effect: "Deny" }))) }, keyOnly)).toThrow(/BacklogOpsReadDecryptsViaS3 must be exactly/);
  });

  it("the grant deploys alone: any other change in the same plan is refused, whatever phase it would match", () => {
    expect(() => check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, { ...keyOnly, "transcript-reader": "update" })).toThrow(/must deploy alone/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, { ...keyOnly, "transcript-srv-writer": "create" })).toThrow(/must deploy alone/);
    expect(() => check({ "transcripts-key": keyBlock(liveKey, plannedKey) }, { ...keyOnly, transcripts: "update" })).toThrow(/must deploy alone/);
    // Changed in place (a principal rewritten) under another phase is refused too.
    const moved = sub("BacklogOpsReadDecryptsViaS3", (s) => ({ ...s, Principal: { AWS: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/other` } }));
    expect(() => check({ "transcripts-key": keyBlock(plannedKey, moved) }, { ...keyOnly, "transcript-srv-writer": "create" })).toThrow(/must deploy alone/);
  });

  it("never together with the cross-account worker grants: each phase refuses the other's Sids", () => {
    const worker = "arn:aws:iam::444455556666:role/synthetic-sessions-worker";
    expect(() => check({ "transcripts-key": keyBlock(liveKey, pKey({ backlog: true, worker })) }, keyOnly)).toThrow(/must add \(or, to revoke, remove\) exactly/);
    // The worker grant alone over a key that already carries the backlog statements is still the worker phase.
    expect(check({ "transcripts-key": keyBlock(plannedKey, pKey({ backlog: true, worker })) }, keyOnly)).toBe("cross-account worker grants");
    // And the backlog grant alone over a key that already carries the worker statements is this phase.
    expect(check({ "transcripts-key": keyBlock(pKey({ worker }), pKey({ backlog: true, worker })) }, keyOnly)).toBe("backlog ops key grants");
    void crossAccountWorkerKeyStatements;
  });

  it("statements re-rendered unchanged on both sides are not this phase", () => {
    const extra = { ...plannedKey, Statement: [...plannedKey.Statement, { Sid: "SrvWriterSomething", Effect: "Allow", Principal: { AWS: "x" }, Action: "kms:Decrypt", Resource: "*" }] };
    expect(() => check({ "transcripts-key": keyBlock(plannedKey, extra) }, keyOnly)).not.toThrow(/backlog ops key grants/);
  });
});

describe("backlog ops key grants: stage wiring", () => {
  it("the stage declaration puts the statements in the full key policy, and nothing without the input", () => {
    const modulePath = fileURLToPath(new URL("../transcript-store.ts", import.meta.url));
    const gc = `arn:aws:iam::${accountId}:role/synthetic-buffered-gc`;
    const keyArn = `arn:aws:kms:${region}:${accountId}:key/synthetic`;
    const script = `import { mock } from 'bun:test'; import * as E from 'effect/Effect';
      const seen=[]; const factory=(type)=>(name,props)=>{seen.push({type,name,props});return E.succeed({userName:props.userName,userArn:'arn:aws:iam::${accountId}:user/'+props.userName,keyArn:'${keyArn}',keyId:'synthetic',bucketName:props.bucketName,trailName:props.trailName})};
      mock.module('@alchemy.run/aws',()=>({IAM:{User:factory('user')},KMS:{Key:factory('key'),Alias:factory('alias')},S3:{Bucket:factory('bucket')},CloudTrail:{Trail:factory('trail')},Budgets:{Budget:factory('budget')}}));
      const {transcriptStoreStage}=await import(${JSON.stringify(modulePath)});
      const config={stage:'${stage}',accountId:'${accountId}',region:'${region}',budgetUsd:1,budgetEmail:'test@example.invalid',transcriptStore:{bufferedGcRoleArn:'${gc}',backlogOpsRoles:${JSON.stringify(roles)}},tags:()=>({})};
      await E.runPromise(transcriptStoreStage(config));const enabled=structuredClone(seen);seen.length=0;
      await E.runPromise(transcriptStoreStage({...config,transcriptStore:{bufferedGcRoleArn:'${gc}'}}));console.log(JSON.stringify({enabled,legacy:seen}));`;
    const result = spawnSync(process.execPath, ["--eval", script], { encoding: "utf8", env: { PATH: process.env.PATH }, timeout: 30_000 });
    expect(result.status).toBe(0);
    const { enabled, legacy } = JSON.parse(result.stdout) as { enabled: { name: string; props: any }[]; legacy: { name: string; props: any }[] };
    const ours = (s: any) => s.Sid.startsWith("BacklogOps");
    expect(enabled.find((r) => r.name === "transcripts-key")!.props.policy.Statement.filter(ours)).toEqual(backlogOpsKeyStatements({ accountId, stage, region, roles }));
    expect(legacy.find((r) => r.name === "transcripts-key")!.props.policy.Statement.some(ours)).toBe(false);
    const strip = (rs: { name: string; props: any }[]) => JSON.stringify(rs.map((r) => r.name === "transcripts-key"
      ? { ...r, props: { ...r.props, policy: { ...r.props.policy, Statement: r.props.policy.Statement.filter((s: any) => !ours(s)) } } } : r));
    expect(strip(enabled)).toBe(strip(legacy));
  });
});
