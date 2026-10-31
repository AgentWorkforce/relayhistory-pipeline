/**
 * The AWS stage's inputs (`.96bulls/<stage>/aws-inputs.json`, written by hand
 * and never committed) and their validation. Pure, so a stage's inputs can be
 * checked without providers or credentials; the stack calls `validateAwsInputs`
 * before it declares anything. `stacks/aws/examples/aws-proof.inputs.example.json`
 * is the fresh, fully hardened proof stage, kept valid by a test.
 */
import { assertServicesImage, clusterEndpointAccess, hoopdbBackend, type HoopDbBackend } from "./hardening.ts";
import { assertRelaycastInputs, assertSessionDirectoryPreconditions, ingressExposure, type IngestExposureInput, type RelaycastInputs } from "./proof-addons.ts";

export interface AwsInputs {
  /** The target account id, repeated on the command line to deploy. */
  accountId: string;
  region: string;
  /** Khaliq's words, verbatim, with the date: the record that the stage was cleared. */
  clearedBy: string;
  budgetUsd: number;
  budgetEmail: string;
  /** Where HoopDb lives (RDS/Supabase URL in Secrets Manager); not created here. */
  hoopdbSecretArn?: string;
  /**
   * Account-wide resources another stage in this account already owns: the
   * `96bulls-services` ECR repository, the GitHub OIDC provider and the image
   * publisher role. Set for a second stage in the same account (e.g. `aws-proof`
   * beside `aws-sessions-dev`), which then uses that repository.
   */
  accountShared?: { ownerStage: string };
  /** Khaliq, 2026-10-28: EKS is the first cloud compute target. Omit to declare the baseline only. */
  eks?: {
    chartVersion: string; runtimeSecretName: string; tenantOrgId: string; relayauthJwksUrl: string; relayhistoryOrigin: string; relayauthIssuer?: string;
    relayauth?: { chartVersion: string; secretName: string };
    /** The digest publish-image-ecr pushed to this account's repository. Bare `sha256:<64 hex>`; tags are refused. */
    imageDigest: string;
    /** The full 40-hex commit that image was built from. Required: the stage refuses an image without source provenance. */
    imageSourceRevision: string;
    /** EKS API: private endpoint always; public only for these operator CIDRs (omit or [] = private only; never 0.0.0.0/0). */
    apiAccess?: { publicAccessCidrs?: string[] };
    /** HoopDb: "external" (default; HOOPDB_URL from runtimeSecretName, i.e. Supabase) or "cnpg" (in-cluster, proof stages only). */
    hoopdb?: { backend?: HoopDbBackend };
    /** Relaycast core chart; chartVersion and imageDigest are inputs so 0.2.0 slots in without a code change. */
    relaycast?: RelaycastInputs;
    /** Search-path NetworkPolicies for the #73 PostgREST profile (follow-up wires the Deployment). Needs cnpg + relayauth. */
    sessionDirectory?: { enabled: boolean };
    /** Ingest hostname on Cloudflare DNS (needs CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID at deploy). scheme defaults to internet-facing; WAF always attached. */
    ingress?: { hostname: string; cloudflareZoneId: string; zoneDomain: string; cloudflareTokenSecret: string; certValidation?: { name: string; value: string } } & IngestExposureInput;
  };
  /** Khaliq, 2026-10-28: dev HoopDb is a Supabase project in the org the 96Bulls team owns. */
  supabase?: { organizationId: string; projectName: string; region: string; plan?: "free" | "pro"; dbPassRef: string };
}

/** Everything the EKS stage would refuse, checked up front. Returns the resolved choices. */
export function validateAwsInputs(stage: string, inputs: AwsInputs) {
  if (!/^\d{12}$/.test(inputs.accountId ?? "")) throw new Error("96bulls: accountId must be a 12-digit AWS account id. Nothing was created.");
  if (inputs.accountShared) {
    const owner = inputs.accountShared.ownerStage;
    if (!/^aws-[a-z0-9-]+$/.test(owner ?? "") || owner === stage)
      throw new Error(`96bulls: accountShared.ownerStage must name another aws-* stage in this account, got ${JSON.stringify(owner)}. Nothing was created.`);
  }
  const eks = inputs.eks;
  if (!eks) return { endpointAccess: undefined, hoopdb: undefined };
  if (eks.relayauth && eks.relayauthIssuer)
    throw new Error("96bulls: eks.relayauth (self-hosted) and eks.relayauthIssuer are both set; the self-hosted issuer is the in-cluster RelayAuth. Remove relayauthIssuer. Nothing was created.");
  assertServicesImage(eks);
  const endpointAccess = clusterEndpointAccess(eks.apiAccess?.publicAccessCidrs);
  const hoopdb = hoopdbBackend(stage, eks.hoopdb?.backend);
  if (eks.relaycast) assertRelaycastInputs(eks.relaycast);
  if (eks.sessionDirectory?.enabled) assertSessionDirectoryPreconditions({ hoopdb, relayauthTls: Boolean(eks.relayauth) });
  if (eks.ingress) ingressExposure({ ...(eks.ingress.scheme ? { scheme: eks.ingress.scheme } : {}), ...(eks.ingress.rateLimitPer5Min !== undefined ? { rateLimitPer5Min: eks.ingress.rateLimitPer5Min } : {}) });
  return { endpointAccess, hoopdb };
}

/** This account's services repository, when another stage owns it. */
export const sharedServicesRepository = (accountId: string, region: string) => `${accountId}.dkr.ecr.${region}.amazonaws.com/96bulls-services`;
