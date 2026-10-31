/**
 * The target fence. The guard (guard.ts) proves *who* we are and *which
 * cluster* we talk to; the fence proves *what* each step touches.
 *
 * Every allowed value is HARDCODED here for the aws-proof stage. Nothing in the
 * fence is derived from inputs.json: config may only restate these values
 * (proofConfigIssues refuses any difference), and every step's argv is checked
 * against the constants in every mode, dry-run included (stepFenceIssue).
 * Changing a target means a reviewed code change to this file.
 *
 * Production-shaped names are refused everywhere, even inside the proof account.
 */
import { LOCAL_READONLY_CONTEXT, PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_STAGE } from "./guard.ts";

export const PROOF_REGION = "us-west-2";
export const PROOF_CONTEXT = `arn:aws:eks:${PROOF_REGION}:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
/** The proof stage's public ingest host and RelayHistory origin. Never sessions-dev or a production host. */
export const PROOF_HOST = "proof-sessions.agentrelay.com";
export const PROOF_ORIGIN = `https://${PROOF_HOST}`;
/** stacks/aws/alchemy.aws.run.ts `name()`: 96bulls-<stage>-<base>[-<account>]. */
export const PROOF_NAME_PREFIX = `96bulls-${PROOF_STAGE}-`;
export const PROOF_LOCKER_BUCKET = `${PROOF_NAME_PREFIX}locker-${PROOF_ACCOUNT_ID}`;
export const PROOF_BACKUP_BUCKET = `${PROOF_NAME_PREFIX}hoopdb-backups-${PROOF_ACCOUNT_ID}`;
export const PROOF_RELEASE = "96bulls";
export const PROOF_CHART_REPO = "https://AgentWorkforce.github.io/helm-charts";
export const PROOF_CHART_HOST = "agentworkforce.github.io";

/** Namespaces and their services, fixed by stacks/aws/eks.ts and relayauth.ts. */
export const PROOF_NAMESPACE = "96bulls";
export const PROOF_RELAYAUTH = { namespace: "relayauth", service: "relayauth" } as const;
export const PROOF_RELAYCAST = { namespace: "relaycast", service: "relaycast" } as const;
export const PROOF_CNPG_NAMESPACE = "cnpg-system";
export const PROOF_INGEST_SERVICE = "96bulls-alleyoop";
export const PROOF_CORE_DEPLOYMENTS = ["96bulls-alleyoop-ingest", "96bulls-worker-hoopdb-indexer", "96bulls-worker-referee", "96bulls-worker-bob-costas"] as const;
export const PROOF_HOOPDB = { cluster: "hoopdb", database: "hoopdb" } as const;
export const PROOF_RESTORE_CLUSTER = "hoopdb-restore-proof";
/** Namespaces that hold objects the proof creates (and teardown may delete). */
export const PROOF_WRITABLE_NAMESPACES = [PROOF_NAMESPACE, PROOF_RELAYCAST.namespace] as const;

/**
 * The transcript backend. Empty until the synthetic ClickHouse Cloud service is
 * provisioned and its exact host is added here in a reviewed commit; until then
 * every configured transcript backend is refused and the proof stays INCOMPLETE.
 */
export const PROOF_TRANSCRIPT_HOSTS: readonly string[] = [];
export const PROOF_TRANSCRIPT_TABLE = { database: "proof", table: "transcripts" } as const;

/** Production- or shared-shaped tokens. Matched as whole segments, case-insensitive. */
export const FORBIDDEN_NAME = /(^|[^a-z0-9])(prod|production|sessions-prod|sessions-dev|staging|hosted)([^a-z0-9]|$)/i;
/** Any URL carrying userinfo (`scheme://user[:pass]@host`). */
export const URL_USERINFO = /\b[a-z][a-z0-9+.-]*:\/\/[^\s/?#@]+@/i;

export interface Fence {
  context: string;
  region?: string;
  namespaces: ReadonlySet<string>;
  /** Namespaces a delete may name. Deletes without an explicit one of these are refused. */
  deleteNamespaces: ReadonlySet<string>;
  buckets: ReadonlySet<string>;
  deployments: ReadonlySet<string>;
  /** Services reachable by port-forward or in-cluster DNS. */
  services: ReadonlySet<string>;
  /** CNPG Cluster names (`clusters.postgresql.cnpg.io <name>`). */
  pgClusters: ReadonlySet<string>;
  /** psql `-d` databases. */
  databases: ReadonlySet<string>;
  releases: ReadonlySet<string>;
  hosts: ReadonlySet<string>;
  /** Account every ARN must carry; undefined for the kind fence (no ARN is allowed at all). */
  accountId?: string;
}

export const PROOF_FENCE: Fence = Object.freeze({
  context: PROOF_CONTEXT,
  region: PROOF_REGION,
  namespaces: new Set([PROOF_NAMESPACE, PROOF_RELAYAUTH.namespace, PROOF_RELAYCAST.namespace, PROOF_CNPG_NAMESPACE]),
  deleteNamespaces: new Set(PROOF_WRITABLE_NAMESPACES),
  buckets: new Set([PROOF_LOCKER_BUCKET, PROOF_BACKUP_BUCKET]),
  deployments: new Set([...PROOF_CORE_DEPLOYMENTS, PROOF_RELAYAUTH.service, PROOF_RELAYCAST.service]),
  services: new Set([PROOF_INGEST_SERVICE, PROOF_RELAYAUTH.service, PROOF_RELAYCAST.service]),
  pgClusters: new Set([PROOF_HOOPDB.cluster, PROOF_RESTORE_CLUSTER]),
  databases: new Set([PROOF_HOOPDB.database]),
  releases: new Set([PROOF_RELEASE]),
  hosts: new Set(["127.0.0.1", PROOF_HOST, PROOF_CHART_HOST, ...PROOF_TRANSCRIPT_HOSTS]),
  accountId: PROOF_ACCOUNT_ID,
});

/** The local kind cluster: reads only (the guard refuses every mutation), and no AWS at all. */
export const KIND_FENCE: Fence = Object.freeze({
  context: LOCAL_READONLY_CONTEXT,
  namespaces: new Set(["96bulls-e2e", "relayauth-e2e", "relaycast-proof", "cnpg-system"]),
  deleteNamespaces: new Set<string>(),
  buckets: new Set<string>(),
  deployments: new Set(["bulls-alleyoop-ingest", "bulls-worker-hoopdb-indexer", "bulls-worker-referee", "bulls-worker-bob-costas", "relayauth", "relaycast"]),
  services: new Set(["bulls-alleyoop", "relayauth", "relaycast"]),
  pgClusters: new Set(["hoopdb"]),
  databases: new Set(["hoopdb"]),
  releases: new Set(["bulls"]),
  hosts: new Set(["127.0.0.1", PROOF_CHART_HOST]),
});

export const fenceFor = (target: "proof" | "kind-readonly"): Fence => (target === "kind-readonly" ? KIND_FENCE : PROOF_FENCE);

interface FenceSource {
  stage: string;
  kubeContext: string;
  region: string;
  namespace: string;
  release: string;
  chartRepo: string;
  coreDeployments: string[];
  hoopdb: { cluster: string; database: string };
  locker: { bucket: string };
  backupBucket?: string;
  relayauth: { namespace: string; service: string };
  relaycast: { namespace: string; service: string };
  ingest: { service: string };
  ingress: { hostname: string };
  historyOrigin: string;
  transcriptSearch: { backend: string; endpoint?: string; database?: string; table?: string };
}

/** Every string anywhere in a value, with its path. */
function strings(v: unknown, path = ""): Array<[string, string]> {
  if (typeof v === "string") return [[path, v]];
  if (Array.isArray(v)) return v.flatMap((x, i) => strings(x, `${path}[${i}]`));
  if (v && typeof v === "object") return Object.entries(v).flatMap(([k, x]) => strings(x, path ? `${path}.${k}` : k));
  return [];
}

const hostOf = (u: string | undefined) => {
  try {
    return u ? new URL(u).hostname.toLowerCase() : undefined;
  } catch {
    return undefined;
  }
};

/**
 * Config-level fence. For the proof stage every target field must equal its
 * hardcoded constant; config cannot introduce a target. Empty array = ok.
 */
export function proofConfigIssues(c: FenceSource & Record<string, unknown>): string[] {
  const issues: string[] = [];
  for (const [path, v] of strings(c)) {
    if (FORBIDDEN_NAME.test(v)) issues.push(`${path} is production- or shared-shaped`);
    if (URL_USERINFO.test(v)) issues.push(`${path} carries URL credentials (userinfo); credentials come from the environment only`);
  }
  if (c.stage !== PROOF_STAGE) return issues;
  const want = (path: string, got: unknown, exp: unknown) => {
    if (JSON.stringify(got) !== JSON.stringify(exp)) issues.push(`${path} must be ${JSON.stringify(exp)} (got ${JSON.stringify(got)})`);
  };
  want("region", c.region, PROOF_REGION);
  want("kubeContext", c.kubeContext, PROOF_CONTEXT);
  want("namespace", c.namespace, PROOF_NAMESPACE);
  want("release", c.release, PROOF_RELEASE);
  want("chartRepo", c.chartRepo, PROOF_CHART_REPO);
  want("coreDeployments", c.coreDeployments, PROOF_CORE_DEPLOYMENTS);
  want("hoopdb", { cluster: c.hoopdb.cluster, database: c.hoopdb.database }, PROOF_HOOPDB);
  want("locker.bucket", c.locker.bucket, PROOF_LOCKER_BUCKET);
  if (c.backupBucket !== undefined) want("backupBucket", c.backupBucket, PROOF_BACKUP_BUCKET);
  want("relayauth", { namespace: c.relayauth.namespace, service: c.relayauth.service }, PROOF_RELAYAUTH);
  want("relaycast", { namespace: c.relaycast.namespace, service: c.relaycast.service }, PROOF_RELAYCAST);
  want("ingest.service", c.ingest.service, PROOF_INGEST_SERVICE);
  want("ingress.hostname", c.ingress.hostname, PROOF_HOST);
  want("historyOrigin", c.historyOrigin, PROOF_ORIGIN);
  const t = c.transcriptSearch;
  if (t.backend !== "none") {
    const h = hostOf(t.endpoint);
    if (!h || !PROOF_TRANSCRIPT_HOSTS.includes(h))
      issues.push(`transcriptSearch.endpoint host ${h ?? "?"} is not an allowlisted proof transcript host (fence.ts PROOF_TRANSCRIPT_HOSTS: ${PROOF_TRANSCRIPT_HOSTS.join(", ") || "none yet"})`);
    want("transcriptSearch database/table", { database: t.database, table: t.table }, PROOF_TRANSCRIPT_TABLE);
  }
  return issues;
}

const valueAfter = (argv: readonly string[], flags: readonly string[]) => {
  const out: string[] = [];
  argv.forEach((a, i) => {
    for (const f of flags) {
      if (a === f && argv[i + 1] !== undefined) out.push(argv[i + 1]!);
      else if (a.startsWith(`${f}=`)) out.push(a.slice(f.length + 1));
    }
  });
  return out;
};

const placeholder = (v: string) => v.startsWith("<");
const ok = (set: ReadonlySet<string>, v: string) => set.has(v) || placeholder(v);
const HELM_RELEASE_SUBCOMMANDS = new Set(["upgrade", "rollback", "uninstall", "history", "status", "get"]);

/**
 * Runtime fence on one step: the first reason its argv names a target outside
 * the hardcoded fence, or undefined. Placeholders (`<...>`) appear only in
 * dry-run plans.
 */
export function stepFenceIssue(argv: readonly string[], f: Fence): string | undefined {
  for (const a of argv) {
    if (FORBIDDEN_NAME.test(a)) return "production_shaped_target";
    if (URL_USERINFO.test(a)) return "url_credentials";
    if (a === "-A" || a === "--all-namespaces" || a.startsWith("--all-namespaces=")) return "all_namespaces_refused";
  }
  for (const v of valueAfter(argv, ["--context", "--kube-context"])) if (v !== f.context) return "context_outside_fence";
  for (const v of valueAfter(argv, ["--region"])) if (v !== f.region) return "region_outside_fence";
  const namespaces = valueAfter(argv, ["-n", "--namespace"]);
  for (const v of namespaces) if (!f.namespaces.has(v)) return "namespace_outside_fence";
  for (const v of valueAfter(argv, ["--bucket"])) if (!ok(f.buckets, v)) return "bucket_outside_fence";
  // Region inside shell strings (sh -c "aws ... --region X") as well as as a flag.
  for (const a of argv) for (const m of a.matchAll(/--region[=\s]+'?([a-z0-9-]+)/g)) if (m[1] !== f.region) return "region_outside_fence";
  if (argv[0] === "aws" && !valueAfter(argv, ["--region"]).length) return "region_missing";
  if (argv[0] === "kubectl") {
    let verbAt = -1;
    for (let i = 1; i < argv.length; i++) {
      const a = argv[i]!;
      if (a.startsWith("-")) {
        if (["--context", "-n", "--namespace"].includes(a)) i++;
        continue;
      }
      verbAt = i;
      break;
    }
    const verb = argv[verbAt];
    // A delete must name exactly one explicit, writable namespace; the default namespace is never implied.
    if (verb === "delete") {
      const isNamespaceDelete = argv[verbAt + 1] === "namespace";
      if (isNamespaceDelete) {
        if (!f.deleteNamespaces.has(argv[verbAt + 2] ?? "")) return "delete_namespace_outside_fence";
      } else if (namespaces.length !== 1 || !f.deleteNamespaces.has(namespaces[0]!)) return "delete_needs_allowlisted_namespace";
    }
    if (verb === "apply" && namespaces.length !== 1) return "apply_needs_namespace";
    if (verb === "exec") {
      for (const d of valueAfter(argv, ["-d"])) if (!f.databases.has(d)) return "database_outside_fence";
    }
    for (let i = 0; i < argv.length - 1; i++)
      if (/^clusters\.postgresql\.cnpg\.io$/.test(argv[i]!) && !argv[i + 1]!.startsWith("-") && !ok(f.pgClusters, argv[i + 1]!)) return "pg_cluster_outside_fence";
  }
  if (argv[0] === "helm") {
    const i = argv.findIndex((a) => HELM_RELEASE_SUBCOMMANDS.has(a));
    if (i >= 0 && !ok(f.releases, argv[i + 1] ?? "")) return "release_outside_fence";
  }
  for (const a of argv) {
    for (const m of a.matchAll(/s3:\/\/([^/\s'"]+)/g)) if (!ok(f.buckets, m[1]!)) return "bucket_outside_fence";
    for (const m of a.matchAll(/(?:^|[\s"'])deployment\/([A-Za-z0-9._-]+)/g)) if (!f.deployments.has(m[1]!)) return "deployment_outside_fence";
    for (const m of a.matchAll(/(?:^|[\s"'])svc\/([A-Za-z0-9._-]+)/g)) if (!f.services.has(m[1]!)) return "service_outside_fence";
    for (const m of a.matchAll(/(?:^|[\s"'])(?:clusters\.postgresql\.cnpg\.io|backups\.postgresql\.cnpg\.io)\/([A-Za-z0-9._-]+)/g))
      if (m[0].includes("clusters.") && !f.pgClusters.has(m[1]!)) return "pg_cluster_outside_fence";
    for (const m of a.matchAll(/\bhttps?:\/\/([^/:\s'"?#]+)/g)) {
      const host = m[1]!.toLowerCase();
      // In-cluster service DNS is allowed only for a fenced service in a fenced namespace: <svc>.<ns>.svc
      const svc = /^([a-z0-9-]+)\.([a-z0-9-]+)\.svc(\.cluster\.local)?$/.exec(host);
      if (svc ? !(f.namespaces.has(svc[2]!) && f.services.has(svc[1]!)) : !f.hosts.has(host) && !placeholder(host)) return "host_outside_fence";
    }
    for (const m of a.matchAll(/arn:aws:[a-z0-9-]+:([a-z0-9-]*):(\d{12})?:/g)) {
      if (m[2] && m[2] !== f.accountId) return "arn_outside_fence";
      if (m[1] && m[1] !== f.region) return "arn_region_outside_fence";
    }
  }
  return undefined;
}
