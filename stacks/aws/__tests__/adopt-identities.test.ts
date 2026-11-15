/**
 * transcriptStore.adoptIdentities (adopt-identities.ts): the hand-made IAM identities, rendered exactly with stage
 * values, declared only when set (unset: byte-identical), adopted in place and retained, judged live-vs-declared by
 * the pre-apply diff, and accepted by the plan contract only as its own "adopt identities" phase.
 */
import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  ADOPTED_IDENTITY_GROUPS, ADOPTED_IDENTITY_RESOURCES, BACKLOG_OPS_OIDC_SUB_PREFIX, adoptedIdentities, adoptedIdentityDifferences,
  assertAdoptIdentitiesInputs, inboxBridgeIdentityPolicy, type AdoptIdentitiesInputs, type LiveIdentity, type RenderedIdentity,
} from "../adopt-identities.ts";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import { assertTranscriptStoreInputs, assertTranscriptStorePlanContract, TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION, TRANSCRIPT_STAGE, transcriptNames } from "../transcript-store.ts";
import { uploadPresignIssuerPolicy } from "../upload-inbox.ts";
import { preApplyReport, type Run } from "../../../scripts/adopt-identities-preapply.ts";

// Synthetic placeholders only (public repo), except the production stage constants the contract is pinned to.
const accountId = "111122223333", region = "us-east-1", stage = "synthetic";
const bucketName = `96bulls-${stage}-transcripts-${accountId}`, bucketArn = `arn:aws:s3:::${bucketName}`;
const keyAlias = `alias/96bulls-${stage}-transcripts`, keyArn = `arn:aws:kms:${region}:${accountId}:key/00000000-0000-0000-0000-000000000000`;
const ctx = { stage, accountId, region, bucketName, keyAlias, keyArn };
const refuse = (why: string): never => { throw new Error(why); };

const readPolicy = { Version: "2012-10-17" as const, Statement: [
  { Sid: "List", Effect: "Allow", Action: "s3:ListBucket", Resource: bucketArn },
  { Sid: "Get", Effect: "Allow", Action: ["s3:GetObject"], Resource: `${bucketArn}/96bulls/*` },
  { Sid: "Key", Effect: "Allow", Action: "kms:Decrypt", Resource: keyArn, Condition: { StringEquals: { "kms:ViaService": `s3.${region}.amazonaws.com` } } },
] };
const writePolicy = { Version: "2012-10-17" as const, Statement: [...readPolicy.Statement,
  { Sid: "Put", Effect: "Allow", Action: "s3:PutObject", Resource: `${bucketArn}/96bulls/*`, Condition: { StringEquals: { "s3:x-amz-server-side-encryption": "aws:kms" } } },
  { Sid: "Encrypt", Effect: "Allow", Action: "kms:GenerateDataKey", Resource: keyArn, Condition: { StringEquals: { "kms:ViaService": `s3.${region}.amazonaws.com` } } },
  { Sid: "NoDelete", Effect: "Deny", Action: "s3:Delete*", Resource: "*" },
] };
const all: AdoptIdentitiesInputs = {
  githubOidcProvider: true,
  backlogOpsRoles: { read: { inlinePolicies: { "backlog-ops-read": readPolicy } }, write: { inlinePolicies: { "backlog-ops-write": writePolicy }, description: "synthetic description" } },
  uploadPresignIssuer: true,
  inboxBridge: true,
};

describe("adopt identities: input", () => {
  const check = (v: unknown) => assertAdoptIdentitiesInputs(v, { accountId, region, bucketName }, refuse);
  it("accepts each group alone and all together; omitted is none", () => {
    expect(check(undefined)).toBeUndefined();
    expect(check(all)).toEqual(all);
    for (const k of Object.keys(all) as (keyof AdoptIdentitiesInputs)[]) expect(check({ [k]: all[k] })).toEqual({ [k]: all[k] });
  });

  it("refuses an empty object, unknown groups and anything but true for the flags", () => {
    expect(() => check({})).toThrow(/at least one identity group/);
    expect(() => check({ deployer: true })).toThrow(/not a known identity group/);
    for (const k of ["githubOidcProvider", "uploadPresignIssuer", "inboxBridge"]) for (const bad of [false, "true", 1, {}])
      expect(() => check({ [k]: bad })).toThrow(/must be true/);
    expect(() => check({ backlogOpsRoles: { read: all.backlogOpsRoles!.read } })).toThrow(/exactly \{ read, write \}/);
    expect(() => check({ backlogOpsRoles: { ...all.backlogOpsRoles, extra: {} } })).toThrow(/exactly \{ read, write \}/);
  });

  it("refuses role policies that leave backlog-ops' needs or are not plain IAM documents", () => {
    const withRead = (statement: Record<string, unknown> | Record<string, unknown>[], extra: Record<string, unknown> = {}) =>
      check({ backlogOpsRoles: { write: all.backlogOpsRoles!.write, read: { inlinePolicies: { p: { Version: "2012-10-17", Statement: [statement].flat() } }, ...extra } } });
    const ok = { Effect: "Allow", Action: "s3:GetObject", Resource: `${bucketArn}/96bulls/*` };
    const via = { StringEquals: { "kms:ViaService": `s3.${region}.amazonaws.com` } };
    expect(() => withRead(ok)).not.toThrow();
    for (const [bad, why] of [
      [{ ...ok, Action: "s3:PutObject" }, /read role may be allowed only/],
      [{ ...ok, Action: "s3:DeleteObject" }, /read role may be allowed only/],
      [{ ...ok, Action: "s3:*" }, /read role may be allowed only/],
      [{ ...ok, Action: "*" }, /read role may be allowed only/],
      [{ ...ok, Resource: "*" }, /must name the transcripts bucket/],
      [{ ...ok, Resource: "arn:aws:s3:::other-bucket/*" }, /must name the transcripts bucket/],
      [{ ...ok, Resource: `${bucketArn}-suffix/*` }, /must name the transcripts bucket/],
      [{ Effect: "Allow", Action: "kms:Decrypt", Resource: `arn:aws:kms:${region}:444455556666:key/x`, Condition: via }, /a key in this account and region/],
      [{ Effect: "Allow", Action: "kms:Decrypt", Resource: `arn:aws:kms:eu-west-1:${accountId}:key/x`, Condition: via }, /a key in this account and region/],
      [{ Effect: "Allow", Action: "kms:Decrypt", Resource: keyArn }, /kms:ViaService/],
      [{ Effect: "Allow", Action: "kms:Decrypt", Resource: keyArn, Condition: { StringEquals: { "kms:ViaService": "s3.eu-west-1.amazonaws.com" } } }, /kms:ViaService/],
      [{ Effect: "Allow", Action: "kms:Decrypt", Resource: keyArn, Condition: { StringLike: { "kms:ViaService": "*" } } }, /kms:ViaService/],
      [{ Effect: "Allow", Action: ["s3:GetObject", "kms:Decrypt"], Resource: keyArn, Condition: via }, /must name the transcripts bucket/],
      [{ Effect: "Allow", NotAction: "s3:DeleteObject", Resource: bucketArn }, /no NotAction/],
      [{ ...ok, NotResource: bucketArn, Resource: undefined }, /no NotAction, no NotResource/],
      [{ ...ok, Principal: "*" }, /may not carry Principal/],
      [{ ...ok, Effect: "allow" }, /Effect must be Allow or Deny/],
      [{ ...ok, Action: [] }, /at least one action/],
    ] as const) expect(() => withRead(bad as never)).toThrow(why);
    // The writer may Put and GenerateDataKey; Deny statements only narrow.
    expect(() => check({ backlogOpsRoles: all.backlogOpsRoles })).not.toThrow();
    expect(() => withRead({ Effect: "Deny", NotAction: "s3:GetObject", NotResource: "*" })).not.toThrow();
    expect(() => check({ backlogOpsRoles: { ...all.backlogOpsRoles, read: { inlinePolicies: {} } } })).toThrow(/1-10 policy names/);
    expect(() => check({ backlogOpsRoles: { ...all.backlogOpsRoles, read: { inlinePolicies: { "bad name": readPolicy } } } })).toThrow(/invalid policy name/);
    expect(() => check({ backlogOpsRoles: { ...all.backlogOpsRoles, read: { inlinePolicies: { p: { ...readPolicy, Version: "2008-10-17" } } } } })).toThrow(/IAM policy document/);
    expect(() => withRead(ok, { description: "" })).toThrow(/description/);
    expect(() => withRead(ok, { managedPolicyArns: [] })).toThrow(/managedPolicyArns is not a known setting/);
  });

  it("the stage validates it, and refuses the stack's own presigned issuer beside the adopted one", () => {
    const prod = (store: Record<string, unknown>) => ({ accountId: TRANSCRIPT_ACCOUNT_ID, region: TRANSCRIPT_REGION, partition: "aws",
      transcriptStore: { bufferedGcRoleArn: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-buffered-gc`, ...store } });
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, prod({ adoptIdentities: { githubOidcProvider: true, uploadPresignIssuer: true, inboxBridge: true } }))).not.toThrow();
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, prod({ adoptIdentities: {} }))).toThrow(/at least one identity group/);
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, prod({ adoptIdentities: { uploadPresignIssuer: true }, uploadPresignIssuer: true }))).toThrow(/declare the same user/);
  });
});

describe("adopt identities: rendered with stage values", () => {
  const r = adoptedIdentities(all, ctx);
  const by = (resource: string) => r.find((x) => x.resource === resource)! as any;

  it("declares exactly the groups' resources, a managed policy before its user", () => {
    expect(r.map((x) => x.resource)).toEqual([...ADOPTED_IDENTITY_RESOURCES]);
    expect(adoptedIdentities({ inboxBridge: true }, ctx).map((x) => x.resource)).toEqual([...ADOPTED_IDENTITY_GROUPS.inboxBridge]);
  });

  it("the GitHub OIDC provider", () => {
    expect(by("adopted-github-oidc")).toEqual({ kind: "oidc-provider", resource: "adopted-github-oidc", arn: `arn:aws:iam::${accountId}:oidc-provider/token.actions.githubusercontent.com`,
      url: "https://token.actions.githubusercontent.com", host: "token.actions.githubusercontent.com", clientIDList: ["sts.amazonaws.com"] });
  });

  it("the backlog-ops roles: names, path, environment subjects, 6 h sessions, the pasted policies, no managed policies", () => {
    for (const [resource, mode, env] of [["adopted-backlog-ops-read", "read", "session-schema-production"], ["adopted-backlog-ops-write", "write", "session-backlog-execute"]] as const) {
      const role = by(resource);
      expect(role.roleName).toBe(`96bulls-${stage}-backlog-ops-${mode}`);
      expect(role.path).toBe("/");
      expect(role.arn).toBe(`arn:aws:iam::${accountId}:role/96bulls-${stage}-backlog-ops-${mode}`);
      expect(role.maxSessionDurationSeconds).toBe(21600);
      expect(role.managedPolicyArns).toEqual([]);
      expect(role.inlinePolicies).toEqual(all.backlogOpsRoles![mode].inlinePolicies);
      expect(role.trust).toEqual({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Federated: `arn:aws:iam::${accountId}:oidc-provider/token.actions.githubusercontent.com` },
        Action: ["sts:AssumeRoleWithWebIdentity"], Condition: { StringEquals: { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub": `${BACKLOG_OPS_OIDC_SUB_PREFIX}:environment:${env}` } } }] });
    }
    expect(by("adopted-backlog-ops-read").description).toBeUndefined();
    expect(by("adopted-backlog-ops-write").description).toBe("synthetic description");
    // The read role never trusts the execute environment's subject.
    expect(JSON.stringify(by("adopted-backlog-ops-read").trust)).not.toContain("session-backlog-execute");
  });

  it("the presigned issuer: user at /96bulls/, managed policy of the same name at /, the reviewed policy", () => {
    const name = `96bulls-${stage}-upload-presign-issuer`;
    expect(by("adopted-presign-issuer-policy")).toEqual({ kind: "managed-policy", resource: "adopted-presign-issuer-policy", policyName: name, path: "/",
      arn: `arn:aws:iam::${accountId}:policy/${name}`, document: uploadPresignIssuerPolicy({ bucketArn, accountId, region, keyAlias }) });
    expect(by("adopted-presign-issuer")).toEqual({ kind: "user", resource: "adopted-presign-issuer", userName: name, path: "/96bulls/",
      arn: `arn:aws:iam::${accountId}:user/96bulls/${name}`, purpose: "cloud-presigns-desktop-uploads", managedPolicyArns: [`arn:aws:iam::${accountId}:policy/${name}`] });
  });

  it("the inbox bridge: user at /, managed policy of the same name, exactly relayhistory-cloud's template rendered (key order too)", () => {
    const name = `96bulls-${stage}-inbox-bridge`;
    expect(by("adopted-inbox-bridge")).toMatchObject({ userName: name, path: "/", arn: `arn:aws:iam::${accountId}:user/${name}`, managedPolicyArns: [`arn:aws:iam::${accountId}:policy/${name}`] });
    const template = readFileSync(fileURLToPath(new URL("./fixtures/inbox-bridge-iam-policy.json", import.meta.url)), "utf8");
    const rendered = JSON.parse(template.replaceAll("<BUCKET>", bucketName).replaceAll("<KEY_ROOT>", "96bulls").replaceAll("<KMS_KEY_ARN>", keyArn).replaceAll("<REGION>", region));
    expect(template).not.toMatch(/\d{12}/);
    expect(JSON.stringify(by("adopted-inbox-bridge-policy").document)).toBe(JSON.stringify(rendered));
    expect(inboxBridgeIdentityPolicy({ bucketName, region, keyArn })).toEqual(rendered);
  });
});

describe("adopt identities: stage wiring", () => {
  const modulePath = fileURLToPath(new URL("../transcript-store.ts", import.meta.url));
  const gc = `arn:aws:iam::${accountId}:role/synthetic-buffered-gc`;
  const run = (adopt: unknown) => {
    const script = `import { mock } from 'bun:test'; import * as E from 'effect/Effect'; import * as O from 'effect/Option'; import * as AdoptPolicy from 'alchemy/AdoptPolicy'; import * as RemovalPolicy from 'alchemy/RemovalPolicy';
      const seen=[]; const factory=(type)=>(name,props)=>E.gen(function*(){const a=yield* E.serviceOption(AdoptPolicy.AdoptPolicy);const rp=yield* E.serviceOption(RemovalPolicy.RemovalPolicy);
        seen.push({type,name,props,adopt:O.getOrNull(a),removal:O.getOrNull(rp)});return {userName:props.userName,userArn:'arn:aws:iam::${accountId}:user/'+props.userName,keyArn:'${keyArn}',keyId:'synthetic',bucketName:props.bucketName,trailName:props.trailName}});
      mock.module('@alchemy.run/aws',()=>({IAM:{User:factory('user'),Role:factory('role'),Policy:factory('policy'),OpenIDConnectProvider:factory('oidc')},KMS:{Key:factory('key'),Alias:factory('alias')},S3:{Bucket:factory('bucket')},CloudTrail:{Trail:factory('trail')},Budgets:{Budget:factory('budget')}}));
      const {transcriptStoreStage}=await import(${JSON.stringify(modulePath)});
      const store={bufferedGcRoleArn:'${gc}'}; const adopt=${JSON.stringify(adopt ?? null)}; if(adopt) store.adoptIdentities=adopt;
      await E.runPromise(transcriptStoreStage({stage:'${stage}',accountId:'${accountId}',region:'${region}',budgetUsd:1,budgetEmail:'test@example.invalid',transcriptStore:store,tags:(c)=>({component:c})}));
      console.log(JSON.stringify(seen));`;
    const result = spawnSync(process.execPath, ["--eval", script], { encoding: "utf8", env: { PATH: process.env.PATH }, timeout: 30_000 });
    if (result.status !== 0) throw new Error(result.stderr);
    return JSON.parse(result.stdout) as { type: string; name: string; props: any; adopt: boolean | null; removal: string | null }[];
  };
  const enabled = run(all), legacy = run(undefined);
  const adopted = (rs: typeof enabled) => rs.filter((x) => ADOPTED_IDENTITY_RESOURCES.includes(x.name));

  it("unset: nothing adopted, and every other resource is byte-identical", () => {
    expect(adopted(legacy)).toEqual([]);
    expect(JSON.stringify(enabled.filter((x) => !ADOPTED_IDENTITY_RESOURCES.includes(x.name)))).toBe(JSON.stringify(legacy));
  });

  it("set: exact names, paths and policies; retained; adopt(true) only on the roles; never an access key", () => {
    const got = adopted(enabled);
    expect(got.map((x) => [x.type, x.name])).toEqual([["oidc", "adopted-github-oidc"], ["role", "adopted-backlog-ops-read"], ["role", "adopted-backlog-ops-write"],
      ["policy", "adopted-presign-issuer-policy"], ["user", "adopted-presign-issuer"], ["policy", "adopted-inbox-bridge-policy"], ["user", "adopted-inbox-bridge"]]);
    for (const x of got) {
      expect(x.removal).toBe("retain");
      expect(x.adopt).toBe(x.type === "role" ? true : null);
      expect(JSON.stringify(x.props)).not.toMatch(/AccessKey|accessKey|loginProfile|permissionsBoundary/);
    }
    const want = adoptedIdentities(all, ctx);
    const props = (n: string) => got.find((x) => x.name === n)!.props;
    expect(props("adopted-github-oidc")).toEqual({ url: "https://token.actions.githubusercontent.com", clientIDList: ["sts.amazonaws.com"], tags: { component: "iam" } });
    const read = want[1] as Extract<RenderedIdentity, { kind: "role" }>;
    expect(props("adopted-backlog-ops-read")).toEqual({ roleName: read.roleName, path: "/", assumeRolePolicyDocument: read.trust, inlinePolicies: read.inlinePolicies,
      managedPolicyArns: [], maxSessionDuration: "21600 seconds", tags: { component: "iam", "96bulls:purpose": "backlog-ops-read" } });
    expect(props("adopted-backlog-ops-write").description).toBe("synthetic description");
    expect(props("adopted-inbox-bridge-policy")).toEqual({ policyName: `96bulls-${stage}-inbox-bridge`, path: "/", policyDocument: inboxBridgeIdentityPolicy({ bucketName, region, keyArn }), tags: { component: "iam" } });
    expect(props("adopted-presign-issuer")).toEqual({ userName: `96bulls-${stage}-upload-presign-issuer`, path: "/96bulls/", managedPolicyArns: [`arn:aws:iam::${accountId}:policy/96bulls-${stage}-upload-presign-issuer`],
      tags: { component: "iam", "96bulls:purpose": "cloud-presigns-desktop-uploads" } });
    expect(props("adopted-inbox-bridge")).toEqual({ userName: `96bulls-${stage}-inbox-bridge`, path: "/", managedPolicyArns: [`arn:aws:iam::${accountId}:policy/96bulls-${stage}-inbox-bridge`],
      tags: { component: "iam", "96bulls:purpose": "relayhistory-cloud-bridges-old-desktop-uploads" } });
  });

  it("one group alone declares only that group", () => {
    expect(adopted(run({ inboxBridge: true })).map((x) => x.name)).toEqual([...ADOPTED_IDENTITY_GROUPS.inboxBridge]);
  });
});

// ---- the pre-apply diff ----
const liveOf = (r: RenderedIdentity): LiveIdentity => {
  const tags = { "96bulls:stack": stage };
  if (r.kind === "oidc-provider") return { exists: true, url: r.host, clientIDList: [...r.clientIDList], tags };
  if (r.kind === "managed-policy") return { exists: true, path: r.path, document: JSON.parse(JSON.stringify(r.document)), tags };
  if (r.kind === "user") return { exists: true, path: r.path, inlinePolicies: {}, managedPolicyArns: [...r.managedPolicyArns], tags };
  return { exists: true, path: r.path, trust: r.trust, inlinePolicies: r.inlinePolicies, managedPolicyArns: [], maxSessionDuration: r.maxSessionDurationSeconds, description: r.description, tags };
};

describe("adopt identities: the pre-apply diff", () => {
  const rendered = adoptedIdentities(all, ctx);
  const at = (resource: string) => rendered.find((x) => x.resource === resource)!;
  const diff = (resource: string, patch: (l: any) => any, o = {}) => adoptedIdentityDifferences(at(resource), patch({ ...liveOf(at(resource)) }), o);

  it("a live identity equal to the declaration is clean (one-item lists and key order do not matter for roles)", () => {
    for (const r of rendered) expect(adoptedIdentityDifferences(r, liveOf(r))).toEqual({ differences: [], notes: [] });
    const role = at("adopted-backlog-ops-read") as any;
    const reordered = { ...liveOf(role), trust: { Statement: [{ ...role.trust.Statement[0], Action: "sts:AssumeRoleWithWebIdentity" }], Version: "2012-10-17" } };
    expect(adoptedIdentityDifferences(role, reordered as LiveIdentity).differences).toEqual([]);
    expect(diff("adopted-github-oidc", (l) => ({ ...l, url: "https://token.actions.githubusercontent.com" })).differences).toEqual([]);
    const retagged = diff("adopted-presign-issuer", (l) => ({ ...l, tags: { "alchemy::id": "x", "96bulls:purpose": "other" } }));
    expect(retagged.differences).toEqual([]);
    expect(retagged.notes).toEqual([]);
  });

  it("refuses everything the apply would change", () => {
    const cases: [string, (l: any) => any, RegExp][] = [
      ["adopted-inbox-bridge", () => ({ exists: false }), /does not exist/],
      ["adopted-github-oidc", (l) => ({ ...l, clientIDList: ["sts.amazonaws.com", "other"] }), /clientIDList/],
      ["adopted-github-oidc", (l) => ({ ...l, url: "other.example" }), /^url$/],
      ["adopted-github-oidc", (l) => ({ ...l, tags: { owner: "x" } }), /tag owner would be removed/],
      ["adopted-github-oidc", (l) => ({ ...l, tags: { "96bulls:purpose": "x" } }), /tag 96bulls:purpose would be removed/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, tags: { "96bulls:owner": "x" } }), /tag 96bulls:owner would be removed/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, trust: { ...l.trust, Statement: [{ ...l.trust.Statement[0], Condition: { StringLike: { "token.actions.githubusercontent.com:sub": "repo:*" } } }] } }), /trust policy/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, inlinePolicies: { ...l.inlinePolicies, extra: readPolicy } }), /inline policy extra \(the apply would delete it\)/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, inlinePolicies: {} }), /inline policy backlog-ops-read \(the apply would add it\)/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, inlinePolicies: { "backlog-ops-read": writePolicy } }), /inline policy backlog-ops-read: document/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, managedPolicyArns: ["arn:aws:iam::aws:policy/ReadOnlyAccess"] }), /attached managed policies/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, maxSessionDuration: 3600 }), /maxSessionDuration/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, description: "live only" }), /description/],
      ["adopted-backlog-ops-write", (l) => ({ ...l, description: undefined }), /description/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, path: "/96bulls/" }), /path/],
      ["adopted-backlog-ops-read", (l) => ({ ...l, permissionsBoundary: "arn:aws:iam::aws:policy/x" }), /permissionsBoundary/],
      ["adopted-presign-issuer", (l) => ({ ...l, path: "/" }), /path/],
      ["adopted-presign-issuer", (l) => ({ ...l, inlinePolicies: { transcripts: readPolicy } }), /inline policy transcripts \(the apply would delete it\)/],
      ["adopted-presign-issuer", (l) => ({ ...l, managedPolicyArns: [] }), /attached managed policies/],
      ["adopted-inbox-bridge-policy", (l) => ({ ...l, path: "/96bulls/" }), /path/],
      ["adopted-inbox-bridge-policy", (l) => ({ ...l, document: { ...l.document, Statement: l.document.Statement.slice(1) } }), /^document$/],
    ];
    for (const [resource, patch, why] of cases) expect(diff(resource, patch).differences.join("\n")).toMatch(why);
  });

  it("a managed policy equivalent but not byte-identical is refused unless explicitly allowed (then a note)", () => {
    const reorder = (l: any) => ({ ...l, document: { Statement: l.document.Statement, Version: l.document.Version } });
    expect(diff("adopted-inbox-bridge-policy", reorder).differences).toEqual(["document: equivalent but not byte-identical (the apply would publish a new default version)"]);
    const allowed = diff("adopted-inbox-bridge-policy", reorder, { allowEquivalentPolicyVersion: true });
    expect(allowed.differences).toEqual([]);
    expect(allowed.notes.length).toBe(1);
  });
});

describe("adopt identities: the pre-apply script (read calls only)", () => {
  const prodNames = transcriptNames(TRANSCRIPT_STAGE, accountId);
  const prodKey = `arn:aws:kms:${region}:${accountId}:key/synthetic`;
  const rendered = adoptedIdentities(all, { stage: TRANSCRIPT_STAGE, accountId, region, bucketName: prodNames.bucket, keyAlias: prodNames.keyAlias, keyArn: prodKey });
  const roleInputs = { read: { inlinePolicies: { p: { Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: "s3:GetObject", Resource: `${prodNames.bucketArn}/*` }] } } },
    write: { inlinePolicies: { p: { Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: "s3:PutObject", Resource: `${prodNames.bucketArn}/*` }] } } } };
  const inputs = { accountId, region, transcriptStore: { adoptIdentities: { ...all, backlogOpsRoles: roleInputs } } };
  const fake = (o: { account?: string; missing?: string; tamper?: (args: string[], out: any) => any } = {}): Run & { calls: string[][] } => {
    const calls: string[][] = [];
    const want = adoptedIdentities(inputs.transcriptStore.adoptIdentities as AdoptIdentitiesInputs, { stage: TRANSCRIPT_STAGE, accountId, region, bucketName: prodNames.bucket, keyAlias: prodNames.keyAlias, keyArn: prodKey });
    const find = (pred: (r: any) => boolean) => want.find(pred) as any;
    const run = ((args: string[]) => {
      calls.push(args);
      const ok = (v: unknown) => ({ status: 0, stdout: JSON.stringify(o.tamper ? o.tamper(args, v) : v), stderr: "" });
      const [svc, verb] = args;
      const val = (flag: string) => args[args.indexOf(flag) + 1]!;
      if (o.missing && args.includes(o.missing)) return { status: 254, stdout: "", stderr: "An error occurred (NoSuchEntity) when calling the operation" };
      if (svc === "sts") return ok({ Account: o.account ?? accountId });
      if (svc === "kms") return ok({ KeyMetadata: { Arn: prodKey } });
      if (verb === "get-open-id-connect-provider") return ok({ Url: "token.actions.githubusercontent.com", ClientIDList: ["sts.amazonaws.com"] });
      if (verb?.endsWith("-tags")) return ok({ Tags: [] });
      if (verb === "get-policy") return ok({ Policy: { Path: "/", DefaultVersionId: "v1" } });
      if (verb === "get-policy-version") return ok({ PolicyVersion: { Document: find((r) => r.arn === val("--policy-arn")).document } });
      if (verb === "get-role") { const r = find((x) => x.roleName === val("--role-name")); return ok({ Role: { Path: "/", AssumeRolePolicyDocument: r.trust, MaxSessionDuration: 21600, Description: r.description } }); }
      if (verb === "list-role-policies") return ok({ PolicyNames: ["p"] });
      if (verb === "get-role-policy") return ok({ PolicyDocument: find((x) => x.roleName === val("--role-name")).inlinePolicies.p });
      if (verb === "list-attached-role-policies") return ok({ AttachedPolicies: [] });
      if (verb === "get-user") return ok({ User: { Path: find((x) => x.userName === val("--user-name")).path } });
      if (verb === "list-user-policies") return ok({ PolicyNames: [] });
      if (verb === "list-attached-user-policies") return ok({ AttachedPolicies: find((x) => x.userName === val("--user-name")).managedPolicyArns.map((PolicyArn: string) => ({ PolicyArn })) });
      throw new Error(`unexpected call ${args.join(" ")}`);
    }) as Run & { calls: string[][] };
    run.calls = calls;
    return run;
  };

  it("clean when live equals the declaration, using only get/list/describe calls", () => {
    expect(rendered.length).toBe(7);
    const run = fake();
    const report = preApplyReport(inputs, run);
    expect(report.adoptIdentitiesPreApply).toBe("clean");
    expect(report.resources.map((r) => r.resource)).toEqual([...ADOPTED_IDENTITY_RESOURCES]);
    for (const args of run.calls) expect(args[1]).toMatch(/^(get|list|describe)-/);
  });

  it("refused on a difference, a missing identity, or credentials for another account", () => {
    const tampered = preApplyReport(inputs, fake({ tamper: (args, v) => (args[1] === "list-attached-user-policies" ? { AttachedPolicies: [] } : v) }));
    expect(tampered.adoptIdentitiesPreApply).toBe("refused");
    expect(tampered.resources.find((r) => r.resource === "adopted-inbox-bridge")!.differences).toEqual(["attached managed policies (the apply would attach or detach)"]);
    const missing = preApplyReport(inputs, fake({ missing: `96bulls-${TRANSCRIPT_STAGE}-backlog-ops-write` }));
    expect(missing.resources.find((r) => r.resource === "adopted-backlog-ops-write")!.differences).toEqual(["role does not exist (the apply would create a new one)"]);
    expect(() => preApplyReport(inputs, fake({ account: "444455556666" }))).toThrow(/another account/);
    expect(() => preApplyReport({ accountId, region, transcriptStore: {} }, fake())).toThrow(/nothing to adopt/);
  });
});

// ---- plan contract ----
const STORE = [
  "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
  "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
  "transcripts-key", "transcripts-key-alias",
];
const h = (t: string) => createHash("sha256").update(t).digest("hex");
function contract(actions: Record<string, string>, o: { listed?: readonly string[]; blocks?: Record<string, string[]>; summary?: string } = {}) {
  const resources = [...STORE, ...SESSION_PIPELINE_RESOURCES, ...(o.listed ?? ADOPTED_IDENTITY_RESOURCES)];
  const counts = new Map<string, number>();
  for (const a of Object.values(actions)) if (a !== "noop") counts.set(a, (counts.get(a) ?? 0) + 1);
  const summary = o.summary ?? (counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: 0 changes");
  const compact = `${summary}\n${resources.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const detailed = `${summary}\n${resources.map((r) => {
    const a = actions[r] ?? "noop";
    if (o.blocks?.[r]) return o.blocks[r]!.join("\n");
    return a === "adopted" ? `\n[${r}] adopted\n  no declared property changes` : `[${r}] ${a}`;
  }).join("\n")}\n`;
  const provenanceText = JSON.stringify({ schema: 1, stage: TRANSCRIPT_STAGE, summary,
    outputs: { plan: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.detailed.txt`, sha256: h(detailed) } } });
  return assertTranscriptStorePlanContract(compact, { detailedPlanText: detailed, provenanceText });
}
const adoptAll = Object.fromEntries(ADOPTED_IDENTITY_RESOURCES.map((r) => [r, "adopted"]));

describe("adopt identities plan phase", () => {
  it("accepts every listed identity adopted, alone; and the rest after a partial apply", () => {
    expect(contract(adoptAll)).toBe("adopt identities");
    expect(contract({ "adopted-inbox-bridge": "adopted" })).toBe("adopt identities");
    expect(contract({ "adopted-inbox-bridge-policy": "adopted", "adopted-inbox-bridge": "adopted" }, { listed: ADOPTED_IDENTITY_GROUPS.inboxBridge })).toBe("adopt identities");
  });

  it("once adopted (all noop) the plan is judged as before", () => {
    expect(contract({})).toBe("post-apply");
    expect(contract({}, { listed: [] })).toBe("post-apply");
  });

  it("refuses any mix: another change beside an adoption", () => {
    for (const other of [{ "transcripts-key": "update" }, { transcripts: "update" }, { "transcript-reader": "update" }, { [SESSION_PIPELINE_RESOURCES[0]!]: "update" }])
      expect(() => contract({ ...adoptAll, ...other })).toThrow(/adopt identities must deploy alone/);
  });

  it("refuses `adopted` on any other resource, in every phase", () => {
    for (const r of ["transcripts", "transcripts-key", "transcript-writer", SESSION_PIPELINE_RESOURCES[0]!])
      for (const extra of [{}, adoptAll]) expect(() => contract({ ...extra, [r]: "adopted" })).toThrow(new RegExp(`${r} has forbidden action adopted`));
    // Without any adopted-identity resource listed at all.
    expect(() => contract({ "transcripts-key": "adopted" }, { listed: [] })).toThrow(/forbidden action adopted/);
  });

  it("an adopted-identity resource is never created, updated, replaced or deleted", () => {
    for (const a of ["create", "update"]) expect(() => contract({ "adopted-backlog-ops-read": a })).toThrow(/may only be adopted \(in its own phase\) or noop/);
    for (const a of ["replace", "delete", "adopt", "import"]) expect(() => contract({ "adopted-backlog-ops-read": a })).toThrow(/forbidden action/);
  });

  it("a repo tag rename may re-tag an adopted identity in place (judged by the rename phase), never adopt beside it", () => {
    const retag = ["[adopted-inbox-bridge] update", "  properties:", "  - " + "  96bulls:repo: AgentWorkforce/96BullSessions", "  + " + "  96bulls:repo: AgentWorkforce/sessions-pipeline"];
    expect(() => contract({ "adopted-inbox-bridge": "update" }, { blocks: { "adopted-inbox-bridge": retag } })).not.toThrow(/may only be adopted/);
    expect(() => contract({ "adopted-inbox-bridge": "update" })).toThrow(/may only be adopted \(in its own phase\) or noop, not update/);
    expect(() => contract({ "adopted-inbox-bridge": "update", "adopted-inbox-bridge-policy": "adopted" }, { blocks: { "adopted-inbox-bridge": retag } })).toThrow(/adopt identities must deploy alone/);
  });

  it("a group is planned whole, and the adopted block shows no declared property changes", () => {
    expect(() => contract({ "adopted-inbox-bridge": "adopted" }, { listed: ["adopted-inbox-bridge"] })).toThrow(/missing resource adopted-inbox-bridge-policy/);
    expect(() => contract(adoptAll, { blocks: { "adopted-inbox-bridge": ["", "[adopted-inbox-bridge] adopted", "  properties:", "    + path: /other/"] } })).toThrow(/no declared property changes/);
    expect(() => contract(adoptAll, { blocks: { "adopted-inbox-bridge": ["[adopted-inbox-bridge] adopted"] } })).toThrow(/no declared property changes/);
  });

  it("the summary must count the adoptions", () => {
    expect(() => contract(adoptAll, { summary: "Plan: 6 to adopted" })).toThrow(/summary says 6 adopted but resource lines contain 7/);
    expect(() => contract(adoptAll, { summary: "Plan: 0 changes" })).toThrow(/summary says no changes/);
  });
});
