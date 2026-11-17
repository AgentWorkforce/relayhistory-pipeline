/**
 * The IAM identities created by hand outside the aws-sessions-prod stack, brought into it by ADOPTION (Khaliq,
 * 2026-11-15: "it should always be in code"). Each group is opt-in under `transcriptStore.adoptIdentities`; unset,
 * nothing here is declared and every other resource renders byte-identical.
 *
 *   githubOidcProvider   the GitHub Actions OIDC provider (token.actions.githubusercontent.com, audience
 *                        sts.amazonaws.com) the backlog-ops roles trust. Thumbprints stay AWS-managed.
 *   backlogOpsRoles      96bulls-<stage>-backlog-ops-read / -write (path /), trusted only for this repository's
 *                        session-schema-production (read) and session-backlog-execute (write) environment
 *                        subjects, 6 h sessions. The repository records no identity policy for them, so the inline
 *                        policies come from the private stage inputs, pasted from the live roles, and are refused
 *                        unless they stay inside backlog-ops' documented needs (assertAdoptedRolePolicies).
 *                        Optional `ledgerBucket` { bucket, keyRoot } (the relocated upload ledger) also admits
 *                        exactly `backlogOpsLedgerStatements` on that bucket; unset, nothing changes.
 *   uploadPresignIssuer  the user 96bulls-<stage>-upload-presign-issuer (path /96bulls/) and its managed policy of
 *                        the same name (path /): upload-inbox.ts uploadPresignIssuerPolicy. Identity only: the wall
 *                        entry and key statements that name it are a separate input.
 *   inboxBridge          the user 96bulls-<stage>-inbox-bridge (path /) and its managed policy of the same name
 *                        (path /): exactly relayhistory-cloud docs/ops/inbox-bridge-iam-policy.json, rendered.
 *
 * Adoption is in place: Alchemy reads the live identity, keeps it (same ARN, unique id, access keys and trust), and
 * reconciles it to the declaration. The reconcile CONVERGES: it detaches managed policies and deletes inline
 * policies that are not declared, rewrites a trust or a policy that differs, and drops tags it does not set. So the
 * declaration must equal the live identity. The live diff (adopt-identities-live.ts) proves that read-only inside
 * the plan capture, whose provenance records its report hash, and again before the deploy, which refuses unless the
 * hash is the same (docs/aws-stage.md "Adopt identities"). No access key is declared: keys stay in 1Password.
 *
 * Every adopted resource is retained (RemovalPolicy.retain), so its deletion never deletes the identity; unsetting a
 * group plans as a deletion, which the contract refuses, so un-adopting needs its own reviewed phase. The roles carry no Alchemy ownership tags, so only they need adopt(true); users, managed
 * policies and the OIDC provider have no ownership check in Alchemy and are adopted silently.
 *
 * The plan contract (transcript-store.ts) accepts the `adopted` action only here, as its own "adopt identities"
 * phase, for exactly these resources, with every other resource noop.
 */
import * as AWS from "@alchemy.run/aws";
// Subpath imports: the "alchemy" index cuts a large stdout short (the stage-rendering tests read it from a child).
import * as AdoptPolicy from "alchemy/AdoptPolicy";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Effect from "effect/Effect";
import { BACKLOG_OPS_DRY_ENVIRONMENT, BACKLOG_OPS_EXECUTE_ENVIRONMENT, BACKLOG_OPS_SESSION_SECONDS } from "../../src/session-ingest/backlog-ops.ts";
import { TENANT_KEY_BASE } from "./tenants.ts";
import { presignIssuerUserName, uploadPresignIssuerPolicy } from "./upload-inbox.ts";

export const GITHUB_OIDC_HOST = "token.actions.githubusercontent.com";
export const GITHUB_OIDC_URL = `https://${GITHUB_OIDC_HOST}`;
export const GITHUB_OIDC_AUDIENCE = "sts.amazonaws.com";
/**
 * This repository's immutable-id OIDC subject prefix (docs/backlog-ops-workflow.md "IAM"): the owner and repository
 * ids are fixed, the name segment follows the repository's current name.
 */
export const BACKLOG_OPS_OIDC_SUB_PREFIX = "repo:AgentWorkforce@252050005/relayhistory-pipeline@1437714333";

/** The Alchemy ids, per input group. A group's resources are declared (and must be planned) together. */
export const ADOPTED_IDENTITY_GROUPS = {
  githubOidcProvider: ["adopted-github-oidc"],
  backlogOpsRoles: ["adopted-backlog-ops-read", "adopted-backlog-ops-write"],
  uploadPresignIssuer: ["adopted-presign-issuer-policy", "adopted-presign-issuer"],
  inboxBridge: ["adopted-inbox-bridge-policy", "adopted-inbox-bridge"],
} as const;
export type AdoptedIdentityGroup = keyof typeof ADOPTED_IDENTITY_GROUPS;
export const ADOPTED_IDENTITY_RESOURCES: readonly string[] = Object.values(ADOPTED_IDENTITY_GROUPS).flat();

export interface PolicyDoc {
  Version: "2012-10-17";
  Statement: Record<string, unknown>[];
  Id?: string;
}

/** One backlog-ops role's identity side, exactly as live: its inline policies and (if it has one) its description. */
export interface AdoptedRolePolicies {
  inlinePolicies: Record<string, PolicyDoc>;
  description?: string;
}

/**
 * The relocated upload ledger's bucket (docs/ledger-relocation.md, docs/backlog-ops-workflow.md "IAM" item 5), which
 * the backlog-ops roles may also reach once it is set: its plain bucket name (it may be in another account) and the
 * key root the tools use (`96bulls`). Unset: the roles may touch only the transcripts bucket, exactly as before.
 */
export interface BacklogOpsLedgerBucket { bucket: string; keyRoot: string }

export interface AdoptIdentitiesInputs {
  githubOidcProvider?: true;
  backlogOpsRoles?: { read: AdoptedRolePolicies; write: AdoptedRolePolicies; ledgerBucket?: BacklogOpsLedgerBucket };
  uploadPresignIssuer?: true;
  inboxBridge?: true;
}

const plain = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v) && Object.getPrototypeOf(v) === Object.prototype;
const list = (v: unknown): unknown[] => (Array.isArray(v) ? v : [v]);

const ROLE_ACTIONS = {
  read: ["s3:ListBucket", "s3:GetObject", "kms:Decrypt"],
  write: ["s3:ListBucket", "s3:GetObject", "kms:Decrypt", "s3:PutObject", "kms:GenerateDataKey"],
} as const;
const POLICY_NAME = /^[A-Za-z0-9+=,.@_-]{1,128}$/;
const STATEMENT_KEYS = new Set(["Sid", "Effect", "Action", "NotAction", "Resource", "NotResource", "Condition"]);

/**
 * The validation context: the stage's bucket, the transcripts key's ARN once known (the pre-apply diff), and the
 * ledger bucket when `backlogOpsRoles.ledgerBucket` is set (assertAdoptIdentitiesInputs fills it in).
 */
export interface RolePolicyContext { accountId: string; region: string; bucketName: string; keyArn?: string; ledger?: BacklogOpsLedgerBucket }

const S3_BUCKET_NAME = /^(?!.*\.\.)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
/** One literal path segment: no wildcard, no slash, never `.` or `..`. */
const LEDGER_KEY_ROOT = /^(?!\.{1,2}$)[A-Za-z0-9._-]{1,64}$/;

/** The ledger bucket's ARNs the roles may name (and nothing else in it). */
export function backlogOpsLedgerResources(l: BacklogOpsLedgerBucket) {
  const bucketArn = `arn:aws:s3:::${l.bucket}`;
  return {
    bucketArn,
    /** Batch records, revision shards, ownership stubs, re-arm markers (ledger-store.ts isLedgerKey). */
    objects: `${bucketArn}/${l.keyRoot}/.admission-inbox/*/ledger/*`,
    /** The ledger-home marker (ledger-store.ts ledgerHomeKey). */
    home: `${bucketArn}/${l.keyRoot}/.ledger/fence-authority/.ledger-home.json`,
  };
}

/**
 * The identity-side statements a backlog-ops role needs on the ledger bucket, exactly (what the validator accepts,
 * and what an operator adds to the live role before pasting it back). Read: s3:GetObject on the ledger keys and the
 * marker, and s3:ListBucket on the ledger bucket with NO condition: S3 answers a GET or HEAD of a missing key with 404
 * only when the caller may s3:ListBucket for that very request, and a GET carries no s3:prefix, so a prefix-conditioned
 * List never applies there and a missing shard, stub or marker would read as 403 (access_denied, every real run
 * failing). Write: also s3:PutObject on the ledger keys, with NO s3:x-amz-server-side-encryption condition: the
 * tools send no encryption header there (the bucket's SSE-S3 default applies). No delete, no KMS. Cross-account, the
 * ledger bucket's own policy must grant the same to the role (kept with that bucket's infrastructure, not here).
 */
export function backlogOpsLedgerStatements(mode: "read" | "write", l: BacklogOpsLedgerBucket): Record<string, unknown>[] {
  const r = backlogOpsLedgerResources(l);
  return [
    { Sid: "LedgerRead", Effect: "Allow", Action: "s3:GetObject", Resource: [r.objects, r.home] },
    { Sid: "LedgerList", Effect: "Allow", Action: "s3:ListBucket", Resource: r.bucketArn, },
    ...(mode === "write" ? [{ Sid: "LedgerWrite", Effect: "Allow", Action: "s3:PutObject", Resource: r.objects }] : []),
  ];
}

/** `backlogOpsRoles.ledgerBucket`: { bucket: a plain bucket name other than the transcripts bucket, keyRoot }. */
export function assertBacklogOpsLedgerBucket(value: unknown, transcriptsBucket: string, refuse: (why: string) => never): BacklogOpsLedgerBucket {
  const at = "transcriptStore.adoptIdentities.backlogOpsRoles.ledgerBucket";
  if (!plain(value) || Object.keys(value).length !== 2 || !("bucket" in value) || !("keyRoot" in value)) return refuse(`${at} must be exactly { bucket, keyRoot }`);
  if (typeof value.bucket !== "string" || !S3_BUCKET_NAME.test(value.bucket)) refuse(`${at}.bucket must be a plain S3 bucket name`);
  if (value.bucket === transcriptsBucket) refuse(`${at}.bucket must not be the transcripts bucket`);
  if (typeof value.keyRoot !== "string" || !LEDGER_KEY_ROOT.test(value.keyRoot)) refuse(`${at}.keyRoot must be one literal key segment (the tools' key root)`);
  return value as unknown as BacklogOpsLedgerBucket;
}

/** Any s3:x-amz-server-side-encryption* key under any condition operator. */
const hasSseCondition = (condition: unknown) =>
  plain(condition) && Object.values(condition).some((block) => plain(block) && Object.keys(block).some((k) => k.toLowerCase().startsWith("s3:x-amz-server-side-encryption")));
/**
 * A backlog-ops role's pasted inline policies, held to what docs/backlog-ops-workflow.md ("IAM") says the roles may
 * do. Every Allow statement names its actions and resources (no NotAction, no NotResource, no Principal), and each
 * action only on its documented resource:
 *   s3:ListBucket                 the transcripts bucket itself;
 *   s3:GetObject, s3:PutObject    exactly `<bucket>/96bulls/*` (the one documented wildcard);
 *   s3:PutObject                  (writer only) with StringEquals s3:x-amz-server-side-encryption = aws:kms and
 *                                 s3:x-amz-server-side-encryption-aws-kms-key-id = the transcripts key;
 *   kms:Decrypt, kms:GenerateDataKey (the latter writer only)  the transcripts key, with StringEquals
 *                                 kms:ViaService = s3.<region>.amazonaws.com.
 * With `c.ledger` (backlogOpsRoles.ledgerBucket) also, and only, `backlogOpsLedgerStatements`' grants on the ledger
 * bucket, each in a statement of its own bucket:
 *   s3:GetObject                  `<ledger>/<root>/.admission-inbox/*\/ledger/*` and the ledger-home marker;
 *   s3:ListBucket                 the ledger bucket itself, with NO condition (a missing key's GET is a 404 only
 *                                 with an unconditional List: a GET carries no s3:prefix, so a prefix-conditioned
 *                                 List would make it a 403);
 *   s3:PutObject                  (writer only) `<ledger>/<root>/.admission-inbox/*\/ledger/*`, with no
 *                                 s3:x-amz-server-side-encryption condition (the tools send no encryption header).
 * No KMS action ever names the ledger bucket.
 * "The transcripts key" is its exact ARN when `keyArn` is given (the pre-apply diff and the capture, which resolve
 * the key's alias live), and otherwise a key ARN in this account and region (the stack's input check, before the
 * key's ARN is known). Deny statements may say anything: they only narrow.
 */
export function assertAdoptedRolePolicies(value: unknown, mode: "read" | "write", c: RolePolicyContext, refuse: (why: string) => never): AdoptedRolePolicies {
  const at = `transcriptStore.adoptIdentities.backlogOpsRoles.${mode}`;
  if (!plain(value)) return refuse(`${at} must be { inlinePolicies, description? }`);
  for (const k of Object.keys(value)) if (k !== "inlinePolicies" && k !== "description") refuse(`${at}.${k} is not a known setting`);
  if (value.description !== undefined && (typeof value.description !== "string" || value.description.length === 0 || value.description.length > 1000 || /[\u0000-\u001f]/.test(value.description)))
    refuse(`${at}.description must be the live role's description (1-1000 printable characters), or omitted when it has none`);
  const policies = value.inlinePolicies;
  if (!plain(policies) || Object.keys(policies).length === 0 || Object.keys(policies).length > 10) return refuse(`${at}.inlinePolicies must map 1-10 policy names to the live documents`);
  const bucketArn = `arn:aws:s3:::${c.bucketName}`;
  const objects = `${bucketArn}/${TENANT_KEY_BASE}/*`;
  const keyPattern = new RegExp(`^arn:aws:kms:${c.region}:${c.accountId}:key/[A-Za-z0-9-]{1,64}$`);
  const isKey = (r: unknown) => typeof r === "string" && (c.keyArn !== undefined ? r === c.keyArn : keyPattern.test(r));
  const allowed: readonly string[] = ROLE_ACTIONS[mode];
  const ledger = c.ledger !== undefined ? backlogOpsLedgerResources(c.ledger) : undefined;
  for (const [name, doc] of Object.entries(policies)) {
    const p = `${at}.inlinePolicies[${JSON.stringify(name)}]`;
    if (!POLICY_NAME.test(name)) refuse(`${at}.inlinePolicies has an invalid policy name`);
    if (!plain(doc) || doc.Version !== "2012-10-17" || !Array.isArray(doc.Statement) || doc.Statement.length === 0) refuse(`${p} must be an IAM policy document (Version 2012-10-17, a Statement list)`);
    const d = doc as Record<string, unknown>;
    for (const k of Object.keys(d)) if (k !== "Version" && k !== "Statement" && k !== "Id") refuse(`${p}.${k} is not allowed`);
    for (const s of d.Statement as unknown[]) {
      if (!plain(s)) return refuse(`${p} has a statement that is not an object`);
      for (const k of Object.keys(s)) if (!STATEMENT_KEYS.has(k)) refuse(`${p}: a statement may not carry ${k}`);
      if (s.Effect === "Deny") continue;
      if (s.Effect !== "Allow") refuse(`${p}: Effect must be Allow or Deny`);
      if (s.NotAction !== undefined || s.NotResource !== undefined || s.Action === undefined || s.Resource === undefined)
        refuse(`${p}: an Allow statement names its Action and Resource (no NotAction, no NotResource)`);
      const actions = list(s.Action), resources = list(s.Resource);
      if (actions.length === 0 || resources.length === 0) refuse(`${p}: an Allow statement names at least one action and resource`);
      for (const a of actions) if (typeof a !== "string" || !allowed.includes(a)) refuse(`${p}: the ${mode} role may be allowed only ${allowed.join(", ")}`);
      const equals = plain(s.Condition) && plain(s.Condition.StringEquals) ? (s.Condition.StringEquals as Record<string, unknown>) : {};
      for (const a of actions as string[]) {
        // A statement naming the ledger bucket is judged by the ledger's rules alone (never mixed with the transcripts').
        const onLedger = ledger !== undefined && resources.some((r) => typeof r === "string" && (r === ledger.bucketArn || r.startsWith(`${ledger.bucketArn}/`)));
        if (ledger && onLedger && a === "s3:ListBucket") {
          if (!resources.every((r) => r === ledger.bucketArn)) refuse(`${p}: s3:ListBucket on the ledger bucket only on the bucket itself`);
          if (s.Condition !== undefined)
            refuse(`${p}: s3:ListBucket on the ledger bucket takes no Condition: S3 answers a GET of a missing key with 404 only when the caller may List for that request, and a GET carries no s3:prefix, so any prefix condition makes every missing shard, stub or marker a 403 (access_denied)`);
        } else if (ledger && onLedger && a === "s3:GetObject") {
          if (!resources.every((r) => r === ledger.objects || r === ledger.home)) refuse(`${p}: s3:GetObject on the ledger bucket only on exactly ${ledger.objects} and ${ledger.home}`);
        } else if (ledger && onLedger && a === "s3:PutObject") {
          if (!resources.every((r) => r === ledger.objects)) refuse(`${p}: s3:PutObject on the ledger bucket only on exactly ${ledger.objects}`);
          if (hasSseCondition(s.Condition)) refuse(`${p}: s3:PutObject on the ledger bucket takes no s3:x-amz-server-side-encryption condition (the tools send no encryption header)`);
        } else if (a === "s3:ListBucket") {
          if (!resources.every((r) => r === bucketArn)) refuse(`${p}: s3:ListBucket only on the transcripts bucket itself`);
        } else if (a === "s3:GetObject" || a === "s3:PutObject") {
          if (!resources.every((r) => r === objects)) refuse(`${p}: ${a} only on exactly ${objects}`);
          if (a === "s3:PutObject" && (equals["s3:x-amz-server-side-encryption"] !== "aws:kms" || !isKey(equals["s3:x-amz-server-side-encryption-aws-kms-key-id"])))
            refuse(`${p}: s3:PutObject needs StringEquals s3:x-amz-server-side-encryption aws:kms and s3:x-amz-server-side-encryption-aws-kms-key-id the transcripts key`);
        } else {
          if (!resources.every(isKey)) refuse(`${p}: ${a} only on the transcripts key`);
          if (equals["kms:ViaService"] !== `s3.${c.region}.amazonaws.com`) refuse(`${p}: ${a} needs StringEquals kms:ViaService s3.${c.region}.amazonaws.com`);
        }
      }
    }
  }
  return value as unknown as AdoptedRolePolicies;
}

/** `transcriptStore.adoptIdentities`: at least one group (omit the whole input for none), each exactly as documented. */
export function assertAdoptIdentitiesInputs(value: unknown, c: RolePolicyContext, refuse: (why: string) => never): AdoptIdentitiesInputs | undefined {
  if (value === undefined) return undefined;
  if (!plain(value) || Object.keys(value).length === 0) return refuse("transcriptStore.adoptIdentities must name at least one identity group (omit it for none)");
  for (const k of Object.keys(value)) if (!(k in ADOPTED_IDENTITY_GROUPS)) refuse(`transcriptStore.adoptIdentities.${k} is not a known identity group`);
  for (const k of ["githubOidcProvider", "uploadPresignIssuer", "inboxBridge"] as const)
    if (value[k] !== undefined && value[k] !== true) refuse(`transcriptStore.adoptIdentities.${k} must be true (omit for off)`);
  if (value.backlogOpsRoles !== undefined) {
    const roles = value.backlogOpsRoles;
    if (!plain(roles) || !("read" in roles) || !("write" in roles) || Object.keys(roles).some((k) => k !== "read" && k !== "write" && k !== "ledgerBucket"))
      refuse("transcriptStore.adoptIdentities.backlogOpsRoles must be exactly { read, write } (and optionally ledgerBucket)");
    const r = roles as Record<string, unknown>;
    const ledger = r.ledgerBucket === undefined ? undefined : assertBacklogOpsLedgerBucket(r.ledgerBucket, c.bucketName, refuse);
    const rc: RolePolicyContext = ledger ? { ...c, ledger } : c;
    assertAdoptedRolePolicies(r.read, "read", rc, refuse);
    assertAdoptedRolePolicies(r.write, "write", rc, refuse);
  }
  return value as AdoptIdentitiesInputs;
}

// ---- the rendered identities ----

export interface RenderedOidcProvider { kind: "oidc-provider"; resource: string; arn: string; url: string; host: string; clientIDList: [string] }
export interface RenderedRole {
  kind: "role"; resource: string; roleName: string; path: "/"; arn: string; purpose: string;
  trust: PolicyDoc; inlinePolicies: Record<string, PolicyDoc>; managedPolicyArns: []; maxSessionDurationSeconds: number; description?: string;
}
export interface RenderedPolicy { kind: "managed-policy"; resource: string; policyName: string; path: "/"; arn: string; document: PolicyDoc }
export interface RenderedUser { kind: "user"; resource: string; userName: string; path: string; arn: string; purpose: string; managedPolicyArns: [string] }
export type RenderedIdentity = RenderedOidcProvider | RenderedRole | RenderedPolicy | RenderedUser;

export interface AdoptContext {
  stage: string;
  accountId: string;
  region: string;
  bucketName: string;
  keyAlias: string;
  /** The transcripts key's ARN (the inbox bridge policy names it). */
  keyArn: string;
}

export const githubOidcProviderArn = (accountId: string) => `arn:aws:iam::${accountId}:oidc-provider/${GITHUB_OIDC_HOST}`;
export const backlogOpsRoleName = (stage: string, mode: "read" | "write") => `96bulls-${stage}-backlog-ops-${mode}`;
export const inboxBridgeUserName = (stage: string) => `96bulls-${stage}-inbox-bridge`;
const managedPolicyArn = (accountId: string, name: string) => `arn:aws:iam::${accountId}:policy/${name}`;

/** Trusted only for this repository's jobs in one environment (the githubImagePublishTrust shape, hardening.ts). */
export function backlogOpsTrustPolicy(accountId: string, environment: string): PolicyDoc {
  return {
    Version: "2012-10-17",
    Statement: [
      {
        Effect: "Allow",
        Principal: { Federated: githubOidcProviderArn(accountId) },
        Action: ["sts:AssumeRoleWithWebIdentity"],
        Condition: {
          StringEquals: {
            [`${GITHUB_OIDC_HOST}:aud`]: GITHUB_OIDC_AUDIENCE,
            [`${GITHUB_OIDC_HOST}:sub`]: `${BACKLOG_OPS_OIDC_SUB_PREFIX}:environment:${environment}`,
          },
        },
      },
    ],
  };
}

/**
 * relayhistory-cloud docs/ops/inbox-bridge-iam-policy.json with <BUCKET>, <KEY_ROOT>, <KMS_KEY_ARN> and <REGION>
 * filled in. Keys in the template's order: IAM keeps the submitted document, and Alchemy compares managed policy
 * documents as JSON text, so the order is part of "exactly".
 */
export function inboxBridgeIdentityPolicy(i: { bucketName: string; region: string; keyArn: string }): PolicyDoc {
  const b = `arn:aws:s3:::${i.bucketName}`, root = TENANT_KEY_BASE;
  const inbox = `${b}/${root}/.upload-inbox/*`;
  const fence = `${b}/${root}/.ledger/fence-authority/*`;
  const journals = `${b}/${root}/.admission-inbox/*/privacy-journal.json`;
  return {
    Version: "2012-10-17",
    Statement: [
      {
        Sid: "InboxWriteCreateOnlySseKms",
        Effect: "Allow",
        Action: "s3:PutObject",
        Resource: inbox,
        Condition: {
          StringEquals: {
            "s3:if-none-match": "*",
            "s3:x-amz-server-side-encryption": "aws:kms",
            "s3:x-amz-server-side-encryption-aws-kms-key-id": i.keyArn,
          },
        },
      },
      { Sid: "ReadGateAndJournals", Effect: "Allow", Action: "s3:GetObject", Resource: [fence, journals] },
      {
        Sid: "ProveAbsenceOfOneKey",
        Effect: "Allow",
        Action: "s3:ListBucket",
        Resource: b,
        Condition: {
          StringLike: { "s3:prefix": [`${root}/.ledger/fence-authority/*.json`, `${root}/.admission-inbox/*/privacy-journal.json`] },
          NumericLessThanEquals: { "s3:max-keys": "1" },
        },
      },
      {
        Sid: "EncryptInboxWritesViaS3",
        Effect: "Allow",
        Action: "kms:GenerateDataKey",
        Resource: i.keyArn,
        Condition: { StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com` }, StringLike: { "kms:EncryptionContext:aws:s3:arn": inbox } },
      },
      {
        Sid: "DecryptGateAndJournalsViaS3",
        Effect: "Allow",
        Action: "kms:Decrypt",
        Resource: i.keyArn,
        Condition: { StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com` }, StringLike: { "kms:EncryptionContext:aws:s3:arn": [fence, journals] } },
      },
      { Sid: "NothingElseInS3", Effect: "Deny", Action: "s3:*", NotResource: [inbox, fence, journals, b] },
      { Sid: "NoOtherS3Actions", Effect: "Deny", NotAction: ["s3:PutObject", "s3:GetObject", "s3:ListBucket"], Resource: "arn:aws:s3:::*" },
      { Sid: "OnlyS3AndKms", Effect: "Deny", NotAction: ["s3:*", "kms:*"], Resource: "*" },
    ],
  };
}

/** Every identity the given groups adopt, in declaration order (a managed policy before the user it is attached to). */
export function adoptedIdentities(adopt: AdoptIdentitiesInputs, c: AdoptContext): RenderedIdentity[] {
  const g = ADOPTED_IDENTITY_GROUPS;
  const out: RenderedIdentity[] = [];
  if (adopt.githubOidcProvider)
    out.push({ kind: "oidc-provider", resource: g.githubOidcProvider[0], arn: githubOidcProviderArn(c.accountId), url: GITHUB_OIDC_URL, host: GITHUB_OIDC_HOST, clientIDList: [GITHUB_OIDC_AUDIENCE] });
  if (adopt.backlogOpsRoles) {
    for (const [i, mode] of (["read", "write"] as const).entries()) {
      const p = adopt.backlogOpsRoles[mode];
      const roleName = backlogOpsRoleName(c.stage, mode);
      out.push({
        kind: "role", resource: g.backlogOpsRoles[i]!, roleName, path: "/", arn: `arn:aws:iam::${c.accountId}:role/${roleName}`, purpose: `backlog-ops-${mode}`,
        trust: backlogOpsTrustPolicy(c.accountId, mode === "read" ? BACKLOG_OPS_DRY_ENVIRONMENT : BACKLOG_OPS_EXECUTE_ENVIRONMENT),
        inlinePolicies: p.inlinePolicies, managedPolicyArns: [], maxSessionDurationSeconds: BACKLOG_OPS_SESSION_SECONDS,
        ...(p.description !== undefined ? { description: p.description } : {}),
      });
    }
  }
  const userWithPolicy = (group: "uploadPresignIssuer" | "inboxBridge", name: string, userPath: string, purpose: string, document: PolicyDoc) => {
    const policyArn = managedPolicyArn(c.accountId, name);
    out.push({ kind: "managed-policy", resource: g[group][0], policyName: name, path: "/", arn: policyArn, document });
    out.push({ kind: "user", resource: g[group][1], userName: name, path: userPath, arn: `arn:aws:iam::${c.accountId}:user${userPath}${name}`, purpose, managedPolicyArns: [policyArn] });
  };
  if (adopt.uploadPresignIssuer)
    userWithPolicy("uploadPresignIssuer", presignIssuerUserName(c.stage), "/96bulls/", "cloud-presigns-desktop-uploads",
      uploadPresignIssuerPolicy({ bucketArn: `arn:aws:s3:::${c.bucketName}`, accountId: c.accountId, region: c.region, keyAlias: c.keyAlias }) as unknown as PolicyDoc);
  if (adopt.inboxBridge)
    userWithPolicy("inboxBridge", inboxBridgeUserName(c.stage), "/", "relayhistory-cloud-bridges-old-desktop-uploads", inboxBridgeIdentityPolicy({ bucketName: c.bucketName, region: c.region, keyArn: c.keyArn }));
  return out;
}

/**
 * Declares the rendered identities. Each is retained on removal; the roles (Unowned to Alchemy: no ownership tags)
 * are taken over with adopt(true). No access keys, login profiles or permissions boundaries are declared.
 */
export const declareAdoptedIdentities = (identities: readonly RenderedIdentity[], tags: (component: string) => Record<string, string>) =>
  Effect.gen(function* () {
    const retain = RemovalPolicy.retain();
    for (const r of identities) {
      if (r.kind === "oidc-provider")
        yield* AWS.IAM.OpenIDConnectProvider(r.resource, { url: r.url, clientIDList: r.clientIDList, tags: tags("iam") }).pipe(retain);
      else if (r.kind === "role")
        yield* AWS.IAM.Role(r.resource, {
          roleName: r.roleName,
          path: r.path,
          assumeRolePolicyDocument: r.trust as never,
          inlinePolicies: r.inlinePolicies as never,
          managedPolicyArns: r.managedPolicyArns,
          maxSessionDuration: `${r.maxSessionDurationSeconds} seconds`,
          ...(r.description !== undefined ? { description: r.description } : {}),
          tags: { ...tags("iam"), "96bulls:purpose": r.purpose },
        } as never).pipe(retain, AdoptPolicy.adopt(true));
      else if (r.kind === "managed-policy")
        yield* AWS.IAM.Policy(r.resource, { policyName: r.policyName, path: r.path, policyDocument: r.document as never, tags: tags("iam") }).pipe(retain);
      else
        yield* AWS.IAM.User(r.resource, { userName: r.userName, path: r.path, managedPolicyArns: r.managedPolicyArns, tags: { ...tags("iam"), "96bulls:purpose": r.purpose } }).pipe(retain);
    }
  });

// ---- the live diff (adopt-identities-live.ts fetches; this judges) ----

/**
 * The tag keys each declaration sets: the stack's tags (alchemy.aws.run.ts tags()), `96bulls:purpose` on the users and
 * roles, and Alchemy's `alchemy::*` ownership tags, which its OIDC provider reconcile does not set.
 */
export function stackTagKeys(kind: RenderedIdentity["kind"]): ReadonlySet<string> {
  return new Set<string>([
    "96bulls:stack", "96bulls:component", "96bulls:repo",
    ...(kind === "role" || kind === "user" ? ["96bulls:purpose"] : []),
    ...(kind === "oidc-provider" ? [] : ["alchemy::stack", "alchemy::stage", "alchemy::id"]),
  ]);
}

/** What the operator's read-only IAM calls return for one identity; `exists: false` for NoSuchEntity. */
export type LiveIdentity =
  | { exists: false }
  | {
      exists: true;
      path?: string;
      url?: string;
      clientIDList?: string[];
      trust?: unknown;
      inlinePolicies?: Record<string, unknown>;
      managedPolicyArns?: string[];
      permissionsBoundary?: string;
      maxSessionDuration?: number;
      description?: string;
      document?: unknown;
      tags: Record<string, string>;
    };

/** Alchemy's own comparison for trusts and inline policies (key order and one-item lists do not matter). */
export const normalizedPolicy = (doc: unknown) => AWS.IAM.normalizePolicyDocument(doc as object);

/**
 * Every way the apply would change the live identity, as short field paths (never document contents). Empty means
 * adopting it writes nothing but the stack's own tags. A managed policy document must be byte-identical as JSON (the
 * apply compares it that way and otherwise publishes a new default version, pruning the oldest at five).
 */
export function adoptedIdentityDifferences(r: RenderedIdentity, live: LiveIdentity): string[] {
  const differences: string[] = [];
  if (!live.exists) return [`${r.kind} does not exist (the apply would create a new one)`];
  // The reconcile drops every live tag key the stack does not set (a key it sets gets the stack's value).
  const setKeys = stackTagKeys(r.kind);
  for (const k of Object.keys(live.tags).sort()) if (!setKeys.has(k)) differences.push(`tag ${k} would be removed`);
  const same = (a: unknown, b: unknown) => normalizedPolicy(a) === normalizedPolicy(b);
  const sameSet = (a: readonly string[] = [], b: readonly string[] = []) => JSON.stringify([...a].sort()) === JSON.stringify([...b].sort());
  if (r.kind === "oidc-provider") {
    if (live.url !== r.host && live.url !== r.url) differences.push("url");
    if (!sameSet(live.clientIDList, r.clientIDList)) differences.push("clientIDList (the apply would add or remove client ids)");
    return differences;
  }
  if (r.kind === "managed-policy") {
    if ((live.path ?? "/") !== r.path) differences.push("path (the apply would create a new policy)");
    if (JSON.stringify(live.document ?? null) !== JSON.stringify(r.document))
      differences.push(same(live.document, r.document) ? "document: equivalent but not byte-identical (the apply would publish a new default version)" : "document");
    return differences;
  }
  if ((live.path ?? "/") !== r.path) differences.push("path (the plan would replace it)");
  if (live.permissionsBoundary !== undefined) differences.push("permissionsBoundary (the apply would delete it)");
  if (!sameSet(live.managedPolicyArns, r.managedPolicyArns)) differences.push("attached managed policies (the apply would attach or detach)");
  const liveInline = live.inlinePolicies ?? {};
  const wantInline: Record<string, unknown> = r.kind === "role" ? r.inlinePolicies : {};
  for (const name of Object.keys(liveInline).sort()) if (!(name in wantInline)) differences.push(`inline policy ${name} (the apply would delete it)`);
  for (const name of Object.keys(wantInline).sort()) {
    if (!(name in liveInline)) differences.push(`inline policy ${name} (the apply would add it)`);
    else if (!same(liveInline[name], wantInline[name])) differences.push(`inline policy ${name}: document`);
  }
  if (r.kind === "role") {
    if (live.trust === undefined || !same(live.trust, r.trust)) differences.push("trust policy");
    if (live.maxSessionDuration !== r.maxSessionDurationSeconds) differences.push("maxSessionDuration");
    if ((live.description ?? undefined) !== r.description) differences.push("description");
  }
  return differences;
}
