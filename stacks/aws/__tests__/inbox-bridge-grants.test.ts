/**
 * The old-desktop inbox bridge's grants (relayhistory-cloud lib/inbox-bridge.ts): transcriptStore.inboxBridgeUserArn
 * adds one entry to the inbox wall and three via-S3 key statements, and the plan contract accepts that change only
 * alone, as "inbox bridge grants" (backlog-ops-key-policy.test.ts and cross-account-worker-policy.test.ts precedent).
 */
import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import {
  assertTranscriptStoreInputs, assertTranscriptStorePlanContract, transcriptBucketPolicy, transcriptKeyPolicy, transcriptNames,
  TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION, TRANSCRIPT_STAGE,
} from "../transcript-store.ts";
import { assertInboxBridgeUserArn, inboxBridgeKeyStatements, inboxBridgeUserArn, INBOX_BRIDGE_KEY_SIDS, uploadInboxBucketStatements, UPLOAD_INBOX_RESOURCES } from "../upload-inbox.ts";

// Synthetic placeholders only (public repo), except the production stage constants the contract is pinned to.
const accountId = "111122223333", region = "us-east-1", stage = "synthetic";
const bridge = inboxBridgeUserArn(accountId, stage);
const prodBridge = inboxBridgeUserArn(TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_STAGE);
const gcRole = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-buffered-gc`;
const uploadAdmission = { synthetic: true };
const production = (extra: Record<string, unknown> = {}) => ({ accountId: TRANSCRIPT_ACCOUNT_ID, region: TRANSCRIPT_REGION, partition: "aws",
  transcriptStore: { bufferedGcRoleArn: gcRole, ...extra } });

describe("inbox bridge grants: input", () => {
  it("accepts only this account's exact stage user, and only with the upload inbox", () => {
    expect(assertInboxBridgeUserArn(bridge, accountId, stage)).toBe(bridge);
    for (const bad of [null, "", "*", `arn:aws:iam::${accountId}:user/*`, `${bridge} `, inboxBridgeUserArn("444455556666", stage),
      inboxBridgeUserArn(accountId, "other"), `arn:aws:iam::${accountId}:role/96bulls-${stage}-inbox-bridge`,
      `arn:aws:iam::${accountId}:user/path/96bulls-${stage}-inbox-bridge`, `arn:aws:iam::${accountId}:root`])
      expect(() => assertInboxBridgeUserArn(bad, accountId, stage)).toThrow(/inboxBridgeUserArn/);
    // Without the upload inbox, the stage refuses it (the wall it joins would not exist).
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production({ inboxBridgeUserArn: prodBridge }))).toThrow(/needs the upload inbox/);
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, production({ inboxBridgeUserArn: inboxBridgeUserArn(TRANSCRIPT_ACCOUNT_ID, "other"), uploadAdmission }))).toThrow(/inboxBridgeUserArn/);
  });

  it("the key statements: encrypt inbox writes, decrypt journals and fence-authority markers, via S3 in this account only", () => {
    const st = inboxBridgeKeyStatements({ accountId, region, bucketName: "synthetic-bucket", inboxBridgeArn: bridge });
    expect(st.map((s) => s.Sid)).toEqual([...INBOX_BRIDGE_KEY_SIDS]);
    for (const s of st) {
      expect(s.Effect).toBe("Allow");
      expect(s.Principal).toEqual({ AWS: bridge });
      expect(s.Condition.StringEquals).toEqual({ "kms:ViaService": "s3.us-east-1.amazonaws.com", "kms:CallerAccount": accountId });
      for (const a of s.Action) expect(["kms:GenerateDataKey", "kms:Encrypt", "kms:Decrypt"]).toContain(a);
    }
    expect(st[0]!.Action).toEqual(["kms:GenerateDataKey", "kms:Encrypt"]);
    expect(st[0]!.Condition.StringLike["kms:EncryptionContext:aws:s3:arn"]).toBe("arn:aws:s3:::synthetic-bucket/96bulls/.upload-inbox/*");
    expect(st[1]!.Condition.StringLike["kms:EncryptionContext:aws:s3:arn"]).toMatch(/\/privacy-journal\.json$/);
    expect(st[2]!.Condition.StringLike["kms:EncryptionContext:aws:s3:arn"]).toBe("arn:aws:s3:::synthetic-bucket/96bulls/.ledger/fence-authority/*");
  });

  it("unset: the wall and the key are byte-for-byte as without it; set: the wall's last entry and the three key statements", () => {
    const base = { bucketArn: "arn:aws:s3:::b", eraserArn: "eraser", consumerRoleArn: "consumer", presignIssuerArn: "issuer" };
    const wall = uploadInboxBucketStatements(base);
    expect(uploadInboxBucketStatements({ ...base, inboxBridgeArn: undefined })).toEqual(wall);
    const withBridge = uploadInboxBucketStatements({ ...base, inboxBridgeArn: bridge });
    expect(withBridge[0]!.Condition.ArnNotEquals["aws:PrincipalArn"]).toEqual([...wall[0]!.Condition.ArnNotEquals["aws:PrincipalArn"], bridge]);
    // The bridge never deletes in the inbox.
    expect(withBridge[1]).toEqual(wall[1]);
  });
});

// ---- plan contract ----
const STORE = [
  "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
  "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
  "transcripts-key", "transcripts-key-alias",
];
const RESOURCES = [...STORE, ...SESSION_PIPELINE_RESOURCES, ...UPLOAD_INBOX_RESOURCES];
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
const pKeyArn = `arn:aws:kms:${TRANSCRIPT_REGION}:${TRANSCRIPT_ACCOUNT_ID}:key/synthetic`;
const pEraser = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:user/synthetic-eraser`;
const consumer = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-upload-admission`;
const uploadRole = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-desktop-upload`;
const inbox = (b?: string) => ({ uploadRoleArn: uploadRole, consumerRoleArn: consumer, ...(b ? { inboxBridgeArn: b } : {}) });
const pBucket = (b?: string) => transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: P.bucketArn, keyArn: pKeyArn, eraserArn: pEraser, uploadInbox: inbox(b) });
const keyArgs = { accountId: TRANSCRIPT_ACCOUNT_ID, region: TRANSCRIPT_REGION, bucketName: P.bucket, writerArn: "writer", srvWriterArn: "srv", readerArn: "reader", pipelineRoleArn: "pipeline" };
const pKey = (b?: string) => transcriptKeyPolicy({ ...keyArgs, uploadInbox: inbox(b) });
const bucketBlock = (live: unknown, planned: unknown) => [
  "  [transcripts] update", "  properties:", `    bucketName: ${P.bucket}`,
  ...mark("-", ["    policy:", ...yaml(live, 6)]), ...mark("+", ["    policy:", ...yaml(planned, 6)]), "    tags: a",
];
const keyBlock = (live: unknown, planned: unknown) => [
  "  [transcripts-key] update", "  properties:", "    enableKeyRotation: true", "    policy:", ...mark("-", yaml(live, 6)), ...mark("+", yaml(planned, 6)),
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
const both = { transcripts: "update", "transcripts-key": "update" };

describe("inbox bridge grants plan phase", () => {
  const lb = pBucket(), pb = pBucket(prodBridge), lk = pKey(), pk = pKey(prodBridge);
  const blocks = (b: [unknown, unknown], k: [unknown, unknown]) => ({ transcripts: bucketBlock(...b), "transcripts-key": keyBlock(...k) });
  const subKey = (sid: string, patch: (s: any) => any) => ({ ...pk, Statement: pk.Statement.map((s: any) => (s.Sid === sid ? patch(s) : s)) });
  const wallWith = (list: string[]) => pb.map((s: any) => (s.Sid === "UploadInboxOnlyItsRoles" ? { ...s, Condition: { ArnNotEquals: { "aws:PrincipalArn": list } } } : s));
  const wallList = (pb.find((s: any) => s.Sid === "UploadInboxOnlyItsRoles") as any).Condition.ArnNotEquals["aws:PrincipalArn"] as string[];

  it("accepts exactly the wall entry and the three key statements (the first deploy), and the revocation", () => {
    expect(check(blocks([lb, pb], [lk, pk]), both)).toBe("inbox bridge grants");
    expect(check(blocks([pb, lb], [pk, lk]), both)).toBe("inbox bridge grants");
  });

  it("the wall entry and the key statements change together, in the same direction", () => {
    expect(() => check({ transcripts: bucketBlock(lb, pb) }, { transcripts: "update" })).toThrow(/change together/);
    expect(() => check({ "transcripts-key": keyBlock(lk, pk) }, { "transcripts-key": "update" })).toThrow(/change together/);
    expect(() => check(blocks([lb, pb], [pk, lk]), both)).toThrow(/must add exactly/);
  });

  it("refuses another principal on the wall, a second entry, or any other wall change", () => {
    const other = inboxBridgeUserArn(TRANSCRIPT_ACCOUNT_ID, "aws-sessions-dev");
    expect(() => check(blocks([lb, wallWith([...wallList.slice(0, -1), other])], [lk, pk]), both)).toThrow(/must be exactly/);
    expect(() => check(blocks([lb, wallWith([...wallList, inboxBridgeUserArn("444455556666", TRANSCRIPT_STAGE)])], [lk, pk]), both)).toThrow(/exactly one bridge entry/);
    // The bridge added while another wall principal is dropped.
    expect(() => check(blocks([lb, wallWith(wallList.filter((a) => a !== uploadRole))], [lk, pk]), both)).toThrow(/may change only by the bridge entry/);
  });

  it("refuses any other bucket or key statement change, and any extra statement", () => {
    const tamperedBucket = pb.map((s: any) => (s.Sid === "DenyTlsBelow12" ? { ...s, Effect: "Allow" } : s));
    expect(() => check(blocks([lb, tamperedBucket], [lk, pk]), both)).toThrow(/DenyTlsBelow12 must stay byte-identical/);
    expect(() => check(blocks([lb, [...pb, { Sid: "Extra", Effect: "Allow", Principal: "*", Action: "s3:*", Resource: "*" }]], [lk, pk]), both)).toThrow(/no statement may be added or removed/);
    const tamperedKey = { ...pk, Statement: pk.Statement.map((s: any) => (s.Sid === "ReaderDecryptsViaS3" ? { ...s, Action: ["kms:*"] } : s)) };
    expect(() => check(blocks([lb, pb], [lk, tamperedKey]), both)).toThrow(/must stay byte-identical/);
  });

  it("refuses a bridge key statement that is not exactly the reviewed one", () => {
    for (const patch of [(s: any) => ({ ...s, Action: ["kms:*"] }), (s: any) => ({ ...s, Principal: "*" }), ({ Condition, ...s }: any) => s,
      (s: any) => ({ ...s, Condition: { ...s.Condition, StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${P.bucket}/*` } } })])
      expect(() => check(blocks([lb, pb], [lk, subKey("InboxBridgeEncryptsInboxViaS3", patch)]), both)).toThrow(/InboxBridgeEncryptsInboxViaS3 must be exactly/);
    const two = { ...pk, Statement: pk.Statement.filter((s: any) => s.Sid !== "InboxBridgeReadsFenceAuthorityViaS3") };
    expect(() => check(blocks([lb, pb], [lk, two]), both)).toThrow(/must add exactly/);
  });

  it("deploys alone: never inside an upload inbox update or rollout, or with any other change", () => {
    expect(() => check(blocks([lb, pb], [lk, pk]), { ...both, "upload-admission": "update" })).toThrow(/must deploy alone/);
    expect(() => check(blocks([lb, pb], [lk, pk]), { ...both, "transcript-reader": "update" })).toThrow(/must deploy alone/);
  });

  it("a wall and key re-rendered with the bridge already on them is not this phase (the role-ARN cascade)", () => {
    const cascade = (p: unknown) => JSON.parse(JSON.stringify(p).replaceAll(consumer, "(known after apply)"));
    const result = () => check(blocks([pb, cascade(pb)], [pk, cascade(pk)]), { ...both, "upload-admission": "update" });
    expect(result).not.toThrow(/inbox bridge grants/);
  });
});
