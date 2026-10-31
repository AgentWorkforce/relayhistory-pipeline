/**
 * Typed-operation admission. Before anything spawns, every step's argv is
 * parsed into one operation and must match an explicit allowlist written here
 * for the hardcoded fence. There is no free-form escape:
 *
 *   - only kubectl, helm and aws are ever spawned; shells, env wrappers and
 *     every other program are refused, so nothing can hide inside `sh -c`;
 *   - kubectl must carry exactly `--context <fence context>` first, helm
 *     `--kube-context <fence context>` (except `helm show`), aws `--region`;
 *   - mutation is decided by the parser, not by the step's own label, and a
 *     mutating operation is admitted only as an exact (verb, kind, namespace,
 *     name) entry below; a step that mutates but says it reads is refused;
 *   - the relayauth namespace is read-only except `rollout restart
 *     deployment/relayauth`; the kind fence admits reads only.
 *
 * In-process actions (`HTTP`, `PROBE`) are never spawned; their hosts are fenced.
 */
import { FORBIDDEN_NAME, PROOF_CHART_REPO, PROOF_CORE_DEPLOYMENTS, PROOF_INGEST_SERVICE, PROOF_LOCKER_BUCKET, PROOF_RELEASE, URL_USERINFO, type Fence } from "./fence.ts";
import { PROOF_ACCOUNT_ID, PROOF_REGION, PROOF_STAGE } from "./guard.ts";

export type Admission = { ok: true; mutating: boolean; op: string } | { ok: false; code: string };

const refuse = (code: string): Admission => ({ ok: false, code });
const read = (op: string): Admission => ({ ok: true, mutating: false, op });
const write = (op: string): Admission => ({ ok: true, mutating: true, op });

const PLACEHOLDER = /^<[^<>]+>$/;

/* ------------------------------------------------------------- per-namespace allowlists */

interface NsRules {
  deployments: readonly string[];
  services: readonly string[];
  /** Pods that may be exec'd into or deleted (by exact name or pattern). */
  pods: RegExp;
}

const PROOF_NS: Record<string, NsRules> = {
  "96bulls": { deployments: PROOF_CORE_DEPLOYMENTS, services: [PROOF_INGEST_SERVICE], pods: /^hoopdb(-restore-proof)?-\d+$/ },
  relayauth: { deployments: ["relayauth"], services: ["relayauth"], pods: /^$/ },
  relaycast: { deployments: ["relaycast"], services: ["relaycast"], pods: /^proof-relaycast-aws-proof$/ },
  "cnpg-system": { deployments: [], services: [], pods: /^$/ },
};

const KIND_NS: Record<string, NsRules> = {
  "96bulls-e2e": { deployments: [], services: [], pods: /^$/ },
  "relayauth-e2e": { deployments: [], services: [], pods: /^$/ },
  "relaycast-proof": { deployments: [], services: [], pods: /^$/ },
  "cnpg-system": { deployments: [], services: [], pods: /^$/ },
};

const PSQL = ["psql", "-d", "hoopdb", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-f", "-"];
const PROBE_POD = "proof-relaycast-aws-proof";
const PROOF_LABEL = "96bulls.proof=true";
const CNPG_CLUSTER = "clusters.postgresql.cnpg.io";
const CNPG_BACKUP = "backups.postgresql.cnpg.io";
const BACKUP_NAME = /^proof-[a-z0-9-]{1,40}$/;
const DIGEST_IMAGE = /^[a-z0-9.-]+(:\d+)?\/[a-z0-9._/-]+@sha256:[0-9a-f]{64}$/;

/** Flags that change the target or credentials of a tool. Never admitted. */
const DANGEROUS_FLAGS = /^(-A|--all-namespaces|--kubeconfig|--server|-s|--token|--as|--as-group|--as-uid|--user|--cluster|--namespace|--dbname|--profile|--endpoint-url|--no-verify-ssl|--insecure-skip-tls-verify|--kube-apiserver|--kube-token|--kube-as-user|--kubeconfig)(=|$)/;

/* ------------------------------------------------------------- kubectl */

/** Split kubectl args (after `--context X`) into -n, label, positionals, the `--` tail, and other flags. */
function parseKubectl(rest: readonly string[]) {
  const ns: string[] = [];
  const labels: string[] = [];
  const pos: string[] = [];
  const flags: string[] = [];
  let tail: string[] | undefined;
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i]!;
    if (a === "--") {
      tail = rest.slice(i + 1);
      break;
    }
    if (a === "-n") ns.push(rest[++i] ?? "");
    else if (a === "-l") labels.push(rest[++i] ?? "");
    else if (a === "-o" || a === "-c" || a === "-f" || a === "--overrides") flags.push(`${a}=${rest[++i] ?? ""}`);
    else if (a.startsWith("-")) flags.push(a);
    else pos.push(a);
  }
  return { ns, labels, pos, flags, tail };
}

function admitKubectl(argv: readonly string[], f: Fence, ph: boolean, stdin: string | undefined): Admission {
  if (argv[1] !== "--context" || argv[2] !== f.context) return refuse(argv[1] === "--context" ? "context_outside_fence" : "context_missing");
  const { ns: nss, labels, pos, flags, tail } = parseKubectl(argv.slice(3));
  if (argv.slice(3).includes("--context")) return refuse("context_repeated");
  const rules = f.target === "proof" ? PROOF_NS : KIND_NS;
  if (nss.length > 1) return refuse("namespace_repeated");
  const ns = nss[0];
  if (ns !== undefined && !rules[ns]) return refuse("namespace_outside_fence");
  const r = ns ? rules[ns]! : undefined;
  const name = (v: string | undefined, okv: (x: string) => boolean) => v !== undefined && (okv(v) || (ph && PLACEHOLDER.test(v)));
  const [verb, ...args] = pos;
  const label = labels.length === 0 ? undefined : labels.length === 1 ? labels[0] : "multiple";
  if (label !== undefined && label !== PROOF_LABEL) return refuse("label_outside_fence");

  // --- the kind fence: reads of pods only.
  if (f.target === "kind-readonly") {
    if (verb === "get" && args.length === 1 && args[0] === "pods" && r && !tail) return read("kubectl get pods");
    return refuse("kubectl_operation_not_allowlisted");
  }

  // --- reads
  if (verb === "get" && !tail) {
    if (!ns && args[0] === "namespace" && args[1] === "96bulls" && args.length === 2) return read("get namespace");
    if (!r) return refuse("namespace_required");
    if (args.length === 1 && (args[0] === "pods" || args[0] === "ingress")) return read(`get ${args[0]}`);
    if (args[0] === CNPG_CLUSTER && args.length === 2 && name(args[1], (x) => x === "hoopdb" || x === "hoopdb-restore-proof") && ns === "96bulls") return read("get cnpg cluster");
    if (args.length === 1 && args[0]!.startsWith(`${CNPG_BACKUP}/`) && name(args[0]!.slice(CNPG_BACKUP.length + 1), (b) => BACKUP_NAME.test(b)) && ns === "96bulls") return read("get backup");
    if (args.length === 1 && args[0] === `all,pvc,${CNPG_BACKUP},${CNPG_CLUSTER}` && label === PROOF_LABEL && (ns === "96bulls" || ns === "relaycast")) return read("get proof objects");
    return refuse("kubectl_operation_not_allowlisted");
  }
  // The one namespace-scoped mutation without -n: deleting the proof release's own namespace.
  if (verb === "delete" && !ns && !tail && args.length === 2 && args[0] === "namespace" && args[1] === "96bulls") return write("delete namespace 96bulls");
  if (!r) return refuse("namespace_required");
  if (verb === "logs" && args.length === 1 && name(args[0]!.replace(/^deployment\//, ""), (d) => r.deployments.includes(d)) && args[0]!.startsWith("deployment/") && !tail) return read("logs");
  if (verb === "wait" && args.length === 1 && !tail) {
    const t = args[0]!;
    if (ns === "relaycast" && t === `pod/${PROBE_POD}`) return read("wait probe pod");
    if (ns === "96bulls" && (t === `${CNPG_CLUSTER}/hoopdb` || t === `${CNPG_CLUSTER}/hoopdb-restore-proof`)) return read("wait cnpg cluster");
    if (ns === "96bulls" && t.startsWith(`${CNPG_BACKUP}/`) && BACKUP_NAME.test(t.slice(CNPG_BACKUP.length + 1))) return read("wait backup");
    return refuse("kubectl_operation_not_allowlisted");
  }
  if (verb === "rollout" && args.length === 2 && args[1]!.startsWith("deployment/") && !tail) {
    const d = args[1]!.slice("deployment/".length);
    if (!r.deployments.includes(d)) return refuse("deployment_outside_fence");
    if (args[0] === "status") return read("rollout status");
    if (args[0] === "restart") return write(`rollout restart ${ns}/${d}`);
    return refuse("kubectl_operation_not_allowlisted");
  }
  if (verb === "port-forward" && args.length === 2 && args[0]!.startsWith("svc/") && /^\d+:\d+$/.test(args[1]!) && !tail)
    return r.services.includes(args[0]!.slice(4)) ? read("port-forward") : refuse("service_outside_fence");
  if (verb === "exec" && args.length === 1 && tail) {
    if (!name(args[0], (p) => r.pods.test(p))) return refuse("pod_outside_fence");
    if (ns === "96bulls" && tail.join(" ") === PSQL.join(" ") && flags.includes("-c=postgres")) return read("psql");
    if (ns === "relaycast" && args[0] === PROBE_POD && tail.length === 4 && tail.slice(0, 3).join(" ") === "bun run -" && (tail[3] === "before" || tail[3] === "after"))
      return write(`relaycast probe ${tail[3]}`);
    return refuse("exec_command_not_allowlisted");
  }
  // --- mutations: exact entries only. relayauth never reaches here except rollout restart above.
  if (ns === "relayauth") return refuse("relayauth_read_only");
  if (verb === "run" && ns === "relaycast" && args.length === 1 && args[0] === PROBE_POD) {
    const image = flags.find((x) => x.startsWith("--image="))?.slice("--image=".length) ?? "";
    if (!(DIGEST_IMAGE.test(image) || (ph && PLACEHOLDER.test(image)))) return refuse("probe_image_not_digest_pinned");
    if (tail?.join(" ") !== "sleep 7200") return refuse("probe_command_not_allowlisted");
    for (const e of flags.filter((x) => x.startsWith("--env="))) if (e !== "--env=RC_URL=http://relaycast.relaycast.svc:8787") return refuse("probe_env_not_allowlisted");
    const ov = flags.find((x) => x.startsWith("--overrides="));
    if (ov) {
      const o = JSON.parse(ov.slice("--overrides=".length)) as { spec?: { containers?: Array<{ image?: string }> } };
      if ((o.spec?.containers ?? []).some((c) => c.image !== image)) return refuse("probe_override_image_mismatch");
    }
    return write("run probe pod");
  }
  if (verb === "delete") {
    if (tail) return refuse("delete_not_allowlisted");
    if (args.length === 2 && args[0] === "pod" && name(args[1], (p) => r.pods.test(p))) return write(`delete pod ${ns}`);
    if (ns === "96bulls" && args.length === 2 && args[0] === CNPG_CLUSTER && args[1] === "hoopdb-restore-proof") return write("delete restore cluster");
    if (ns === "96bulls" && args.length === 2 && args[0] === CNPG_BACKUP && name(args[1], (b) => BACKUP_NAME.test(b))) return write("delete backup");
    if ((ns === "96bulls" || ns === "relaycast") && args.length === 1 && args[0] === `${CNPG_BACKUP},${CNPG_CLUSTER},pods` && label === PROOF_LABEL) return write("delete proof objects");
    return refuse("delete_not_allowlisted");
  }
  if (verb === "apply" && args.length === 0 && flags.includes("-f=-") && ns === "96bulls") {
    let m: { apiVersion?: string; kind?: string; metadata?: { name?: string; namespace?: string; labels?: Record<string, string> }; spec?: Record<string, unknown> };
    try {
      m = JSON.parse(stdin ?? "");
    } catch {
      return refuse("apply_manifest_unreadable");
    }
    if (m.apiVersion !== "postgresql.cnpg.io/v1" || m.metadata?.namespace !== "96bulls" || m.metadata?.labels?.["96bulls.proof"] !== "true") return refuse("apply_manifest_outside_fence");
    if (m.kind === "Backup" && BACKUP_NAME.test(m.metadata?.name ?? "") && (m.spec?.cluster as { name?: string } | undefined)?.name === "hoopdb") return write("apply backup");
    if (m.kind === "Cluster" && m.metadata?.name === "hoopdb-restore-proof" && (m.spec?.bootstrap as { recovery?: unknown } | undefined)?.recovery) return write("apply restore cluster");
    return refuse("apply_manifest_outside_fence");
  }
  return refuse("kubectl_operation_not_allowlisted");
}

/* ------------------------------------------------------------- helm */

function admitHelm(argv: readonly string[], f: Fence, ph: boolean): Admission {
  if (argv[1] === "show") {
    if (argv[2] === "values" && ["96bulls", "relayauth", "relaycast"].includes(argv[3] ?? "") && argv[4] === "--repo" && argv[5] === PROOF_CHART_REPO && argv[6] === "--version" && /^\d+\.\d+\.\d+$/.test(argv[7] ?? "") && argv.length === 8)
      return read("helm show values");
    return refuse("helm_operation_not_allowlisted");
  }
  if (argv[1] !== "--kube-context" || argv[2] !== f.context) return refuse(argv[1] === "--kube-context" ? "context_outside_fence" : "context_missing");
  if (argv[3] !== "-n") return refuse("namespace_required");
  const ns = argv[4]!;
  const [sub, ...rest] = argv.slice(5);
  const nsOk = f.target === "proof" ? Object.keys(PROOF_NS).includes(ns) : Object.keys(KIND_NS).includes(ns);
  if (!nsOk) return refuse("namespace_outside_fence");
  if (sub === "list" && rest.join(" ") === "-o json") return read("helm list");
  if (f.target !== "proof" || ns !== "96bulls") return refuse("helm_operation_not_allowlisted");
  if (rest[0] !== PROOF_RELEASE) return refuse("release_outside_fence");
  const tail = rest.slice(1).join(" ");
  if ((sub === "history" || sub === "status") && tail === "-o json") return read(`helm ${sub}`);
  if (sub === "upgrade" && rest[1] === "96bulls" && rest[2] === "--repo" && rest[3] === PROOF_CHART_REPO && rest[4] === "--version" && (/^\d+\.\d+\.\d+$/.test(rest[5] ?? "") || (ph && PLACEHOLDER.test(rest[5] ?? ""))) && rest.slice(6).join(" ") === "--reuse-values --wait --timeout 10m")
    return write("helm upgrade");
  if (sub === "rollback" && (/^\d+$/.test(rest[1] ?? "") || (ph && PLACEHOLDER.test(rest[1] ?? ""))) && rest.slice(2).join(" ") === "--wait --timeout 10m") return write("helm rollback");
  if (sub === "uninstall" && tail === "--wait") return write("helm uninstall");
  return refuse("helm_operation_not_allowlisted");
}

/* ------------------------------------------------------------- aws */

const ARN = /^arn:aws:([a-z0-9-]+):([a-z0-9-]*):(\d{12})?:(.+)$/;

function arnOk(v: string, ph: boolean): boolean {
  if (ph && PLACEHOLDER.test(v)) return true;
  const m = ARN.exec(v);
  return !!m && m[2] === PROOF_REGION && m[3] === PROOF_ACCOUNT_ID;
}

function admitAws(argv: readonly string[], f: Fence, ph: boolean): Admission {
  if (f.target !== "proof") return refuse("aws_outside_fence");
  const region = argv.flatMap((a, i) => (a === "--region" ? [argv[i + 1]] : a.startsWith("--region=") ? [a.slice(9)] : []));
  if (region.length !== 1) return refuse(region.length ? "region_repeated" : "region_missing");
  if (region[0] !== PROOF_REGION) return refuse("region_outside_fence");
  const opt = (flag: string) => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const [svc, cmd] = [argv[1], argv[2]];
  const bucket = opt("--bucket");
  const buckets = f.buckets;
  if (svc === "s3api") {
    if (!bucket || !buckets.has(bucket)) return refuse("bucket_outside_fence");
    if (cmd === "list-objects-v2" || cmd === "head-object" || cmd === "get-bucket-versioning") return read(`s3api ${cmd}`);
    if (cmd === "delete-object" && bucket === PROOF_LOCKER_BUCKET && /^(v1\/ws=[^/]+\/m=proof-[a-z0-9]+-m\d+\/h=claude\/d=\d{4}\/\d{2}\/\d{2}\/s=[0-9a-f-]{36}\/summary\/short\.json\.enc)$/.test(opt("--key") ?? ""))
      return write("delete summary object");
    return refuse("aws_operation_not_allowlisted");
  }
  if (svc === "s3" && cmd === "cp" && argv[argv.length - 1] === "-") {
    const src = argv.find((a) => a.startsWith("s3://")) ?? "";
    const m = /^s3:\/\/([^/]+)\/(telemetry\/traces\/.+)$/.exec(src);
    return m && m[1] === PROOF_LOCKER_BUCKET ? read("s3 cp trace batch") : refuse("bucket_outside_fence");
  }
  if (svc === "elbv2" && (cmd === "describe-load-balancers" || cmd === "describe-ssl-policies")) return read(`elbv2 ${cmd}`);
  if (svc === "elbv2" && cmd === "describe-listeners") return arnOk(opt("--load-balancer-arn") ?? "", ph) ? read("elbv2 describe-listeners") : refuse("arn_outside_fence");
  if (svc === "wafv2" && cmd === "get-web-acl-for-resource") return arnOk(opt("--resource-arn") ?? "", ph) ? read("wafv2 get-web-acl-for-resource") : refuse("arn_outside_fence");
  if (svc === "kms" && cmd === "describe-key") return arnOk(opt("--key-id") ?? "", ph) ? read("kms describe-key") : refuse("arn_outside_fence");
  if (svc === "eks" && cmd === "list-clusters") return read("eks list-clusters");
  if (svc === "resourcegroupstaggingapi" && cmd === "get-resources" && opt("--tag-filters") === `Key=96bulls:stack,Values=${PROOF_STAGE}`) return read("tagged resources");
  return refuse("aws_operation_not_allowlisted");
}

/* ------------------------------------------------------------- in-process */

function admitInProcess(argv: readonly string[], f: Fence, mutating: boolean | undefined): Admission {
  // f.hosts is the hardcoded fence's list (PROOF_FENCE: 127.0.0.1, the proof host, the chart host, PROOF_TRANSCRIPT_HOSTS).
  for (const a of argv)
    for (const m of a.matchAll(/\bhttps?:\/\/([^/:\s'"?#]+)/g)) if (!f.hosts.has(m[1]!.toLowerCase())) return refuse("host_outside_fence");
  if (f.target !== "proof") return refuse("in_process_outside_fence");
  return mutating ? write(`${argv[0]} action`) : read(`${argv[0]} action`);
}

/* ------------------------------------------------------------- entry */

/**
 * Admit one step. `labelledMutating` is the step's own claim; a step that the
 * parser finds mutating but is labelled read is refused (`mutation_mislabeled`).
 * `placeholders` allows `<...>` names, and only dry-run passes it.
 */
export function admitStep(step: { argv: readonly string[]; stdin?: string; mutating?: boolean }, f: Fence, opts: { placeholders?: boolean } = {}): Admission {
  const argv = step.argv;
  const ph = !!opts.placeholders;
  for (const a of argv) {
    if (FORBIDDEN_NAME.test(a)) return refuse("production_shaped_target");
    if (URL_USERINFO.test(a)) return refuse("url_credentials");
    if (DANGEROUS_FLAGS.test(a)) return refuse(a === "-A" || a.startsWith("--all-namespaces") ? "all_namespaces_refused" : "target_override_flag");
  }
  let a: Admission;
  switch (argv[0]) {
    case "kubectl":
      a = admitKubectl(argv, f, ph, step.stdin);
      break;
    case "helm":
      a = admitHelm(argv, f, ph);
      break;
    case "aws":
      a = admitAws(argv, f, ph);
      break;
    case "HTTP":
    case "PROBE":
      a = admitInProcess(argv, f, step.mutating);
      break;
    default:
      // sh, bash, env, xargs, ... : no wrapper can hide a command.
      return refuse("program_not_allowlisted");
  }
  if (a.ok && a.mutating && step.mutating === false) return refuse("mutation_mislabeled");
  if (a.ok && a.mutating && f.target !== "proof") return refuse("kind_is_read_only");
  return a;
}

/** Compatibility helper for tests: the refusal code, or undefined when admitted. */
export function stepFenceIssue(argv: readonly string[], f: Fence, opts: { placeholders?: boolean; stdin?: string } = {}): string | undefined {
  const a = admitStep({ argv, stdin: opts.stdin }, f, opts);
  return a.ok ? undefined : a.code;
}
