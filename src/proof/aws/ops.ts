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
import { FORBIDDEN_NAME, PROOF_RELAYAUTH_TLS, PROOF_SEARCH_CLIENT_POD, PROOF_KEY_PREFIX_BASE, PROOF_TENANT_ORGS, PROOF_CHART_REPO, PROOF_CORE_DEPLOYMENTS, PROOF_INGEST_SERVICE, PROOF_LOCKER_BUCKET, PROOF_POSTGREST, PROOF_PROBE_IMAGES, PROOF_RELEASE, URL_USERINFO, type Fence } from "./fence.ts";
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_REGION, PROOF_STAGE } from "./guard.ts";

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
  "96bulls": { deployments: PROOF_CORE_DEPLOYMENTS, services: [PROOF_INGEST_SERVICE, PROOF_POSTGREST.service], pods: /^(hoopdb(-restore-proof)?-\d+|proof-search-client)$/ },
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

/* ------------------------------------------------------------- flag normalization */

/**
 * Every tool's argv is normalized before any check: `--x v`, `--x=v`, `-n v`,
 * `-nv` and `-n=v` all become one canonical flag, aliases (`--namespace`,
 * `--selector`, ...) map to it, and then:
 *   - a flag that can retarget the tool (server, cluster, user, token,
 *     impersonation, kubeconfig, CA/TLS overrides, profiles, endpoints) is
 *     refused outright, in any spelling;
 *   - any canonical flag appearing twice is refused (no last-wins);
 *   - any flag not in the tool's allowlist is refused.
 */
interface FlagSpec {
  /** canonical name -> takes a value */
  known: Record<string, "value" | "bool" | "either">;
  aliases: Record<string, string>;
  /** single-letter flags that take a value (`-nX`, `-n=X`, `-n X`) */
  shortValued: ReadonlySet<string>;
  shortBool: ReadonlySet<string>;
  retarget: ReadonlySet<string>;
}

const KUBECTL_RETARGET = ["kubeconfig", "server", "s", "cluster", "user", "token", "as", "as-group", "as-uid", "certificate-authority", "client-certificate", "client-key", "insecure-skip-tls-verify", "tls-server-name", "username", "password", "all-namespaces", "A", "request-timeout", "cache-dir", "profile"];

const KUBECTL: FlagSpec = {
  known: {
    context: "value", n: "value", l: "value", o: "value", c: "value", f: "value", overrides: "value",
    image: "value", restart: "value", labels: "value", env: "value", for: "value", timeout: "value", since: "value",
    wait: "either", "all-containers": "bool", prefix: "bool", "ignore-not-found": "bool", command: "bool", i: "bool",
  },
  aliases: { namespace: "n", selector: "l", output: "o", container: "c", filename: "f", stdin: "i" },
  shortValued: new Set(["n", "l", "o", "c", "f"]),
  shortBool: new Set(["i"]),
  retarget: new Set(KUBECTL_RETARGET),
};

const HELM: FlagSpec = {
  known: { "kube-context": "value", n: "value", o: "value", repo: "value", version: "value", timeout: "value", "reuse-values": "bool", wait: "bool" },
  aliases: { namespace: "n", output: "o" },
  shortValued: new Set(["n", "o"]),
  shortBool: new Set(),
  retarget: new Set(["kubeconfig", "kube-apiserver", "kube-token", "kube-as-user", "kube-as-group", "kube-ca-file", "kube-insecure-skip-tls-verify", "kube-tls-server-name", "registry-config", "repository-config", "repository-cache", "username", "password", "ca-file", "cert-file", "key-file", "insecure-skip-tls-verify", "pass-credentials", "post-renderer"]),
};

const AWS: FlagSpec = {
  known: { region: "value", bucket: "value", key: "value", prefix: "value", query: "value", output: "value", "load-balancer-arn": "value", names: "value", "resource-arn": "value", "key-id": "value", "tag-filters": "value", name: "value" },
  aliases: {},
  shortValued: new Set(),
  shortBool: new Set(),
  retarget: new Set(["profile", "endpoint-url", "no-verify-ssl", "ca-bundle", "no-sign-request", "cli-input-json", "cli-input-yaml", "cli-binary-format", "debug"]),
};

export interface Tokens {
  flags: Map<string, string | true>;
  pos: string[];
  tail?: string[];
}

export function tokenize(args: readonly string[], spec: FlagSpec): Tokens | { error: string } {
  const flags = new Map<string, string | true>();
  const pos: string[] = [];
  let tail: string[] | undefined;
  const set = (raw: string, value: string | true): string | undefined => {
    const name = spec.aliases[raw] ?? raw;
    if (spec.retarget.has(raw) || spec.retarget.has(name)) return raw === "A" || name === "all-namespaces" ? "all_namespaces_refused" : "retarget_flag_refused";
    const kind = spec.known[name];
    if (!kind) return "flag_not_allowlisted";
    if (flags.has(name)) return "flag_repeated";
    if (kind === "bool" && value !== true) return "flag_value_not_allowed";
    if (kind === "value" && value === true) return "flag_value_missing";
    flags.set(name, value);
    return undefined;
  };
  for (let i = 0; i < args.length; i++) {
    const a = args[i]!;
    if (a === "--") {
      tail = args.slice(i + 1);
      break;
    }
    let err: string | undefined;
    if (a.startsWith("--")) {
      const eq = a.indexOf("=");
      const raw = eq >= 0 ? a.slice(2, eq) : a.slice(2);
      if (eq >= 0) err = set(raw, a.slice(eq + 1));
      else {
        const name = spec.aliases[raw] ?? raw;
        const kind = spec.known[name];
        if (kind === "value" && !spec.retarget.has(raw)) {
          if (args[i + 1] === undefined) return { error: "flag_value_missing" };
          err = set(raw, args[++i]!);
        } else err = set(raw, true);
      }
    } else if (a.startsWith("-") && a.length > 1) {
      const letter = a[1]!;
      const rest = a.slice(2);
      if (spec.shortValued.has(letter)) {
        const v = rest.startsWith("=") ? rest.slice(1) : rest;
        if (v) err = set(letter, v);
        else {
          if (args[i + 1] === undefined) return { error: "flag_value_missing" };
          err = set(letter, args[++i]!);
        }
      } else if (spec.shortBool.has(letter) && rest === "") err = set(letter, true);
      else err = spec.retarget.has(letter) ? (letter === "A" ? "all_namespaces_refused" : "retarget_flag_refused") : "flag_not_allowlisted";
    } else pos.push(a);
    if (err) return { error: err };
  }
  return { flags, pos, ...(tail ? { tail } : {}) };
}

/* ------------------------------------------------------------- probe pods */

export const RELAYCAST_PROBE_URL = "http://relaycast.relaycast.svc:8787";

/**
 * The exact pod specs the harness may create. `kubectl run` is admitted only
 * when its --labels and --overrides equal these byte for byte, so no volume,
 * privilege or extra container can be smuggled in through the override.
 */
export function probePod(kind: "relaycast" | "search", image: string): { name: string; namespace: string; labels: string; overrides: string } {
  if (kind === "relaycast")
    return {
      name: PROBE_POD,
      namespace: "relaycast",
      labels: PROOF_LABEL,
      overrides: JSON.stringify({ spec: { volumes: [{ name: "probe", emptyDir: { medium: "Memory" } }], containers: [{ name: PROBE_POD, image, command: ["sleep", "7200"], env: [{ name: "RC_URL", value: RELAYCAST_PROBE_URL }], volumeMounts: [{ name: "probe", mountPath: "/probe" }] }] } }),
    };
  return {
    name: PROOF_SEARCH_CLIENT_POD,
    namespace: PROOF_POSTGREST.namespace,
    labels: [PROOF_LABEL, PROOF_RELAYAUTH_TLS.clientLabel.join("="), PROOF_POSTGREST.clientLabel.join("=")].join(","),
    overrides: JSON.stringify({ spec: { volumes: [{ name: "ca", configMap: { name: PROOF_RELAYAUTH_TLS.caConfigMap } }], containers: [{ name: PROOF_SEARCH_CLIENT_POD, image, command: ["sleep", "7200"], volumeMounts: [{ name: "ca", mountPath: "/ca", readOnly: true }] }] } }),
  };
}

/* ------------------------------------------------------------- kubectl */

function admitKubectl(argv: readonly string[], f: Fence, ph: boolean, stdin: string | undefined): Admission {
  const t = tokenize(argv.slice(1), KUBECTL);
  if ("error" in t) return refuse(t.error);
  const fl = t.flags;
  const ctx = fl.get("context");
  if (ctx === undefined) return refuse("context_missing");
  if (ctx !== f.context) return refuse("context_outside_fence");
  const rules = f.target === "proof" ? PROOF_NS : KIND_NS;
  const ns = fl.get("n") as string | undefined;
  if (ns !== undefined && !rules[ns]) return refuse("namespace_outside_fence");
  const r = ns ? rules[ns]! : undefined;
  const name = (v: string | undefined, okv: (x: string) => boolean) => v !== undefined && (okv(v) || (ph && PLACEHOLDER.test(v)));
  const [verb, ...args] = t.pos;
  const tail = t.tail;
  const label = fl.get("l") as string | undefined;
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
    if (args.length === 2 && args[0] === "service" && r.services.includes(args[1]!)) return read("get service");
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
    if (ns === PROOF_POSTGREST.namespace && t === `pod/${PROOF_SEARCH_CLIENT_POD}`) return read("wait search client");
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
    if (ns === "96bulls" && tail.join(" ") === PSQL.join(" ") && fl.get("c") === "postgres") return read("psql");
    if (ns === "relaycast" && args[0] === PROBE_POD && tail.length === 4 && tail.slice(0, 3).join(" ") === "bun run -" && (tail[3] === "before" || tail[3] === "after"))
      return write(`relaycast probe ${tail[3]}`);
    // The in-cluster search client: exchange at RelayAuth TLS, then the PostgREST RPC. Reads only.
    if (ns === PROOF_POSTGREST.namespace && args[0] === PROOF_SEARCH_CLIENT_POD && tail.join(" ") === "bun run -") return read("in-cluster search");
    return refuse("exec_command_not_allowlisted");
  }
  // --- mutations: exact entries only. relayauth never reaches here except rollout restart above.
  if (ns === "relayauth") return refuse("relayauth_read_only");
  if (verb === "run" && args.length === 1 && ((ns === "relaycast" && args[0] === PROBE_POD) || (ns === PROOF_POSTGREST.namespace && args[0] === PROOF_SEARCH_CLIENT_POD))) {
    const image = (fl.get("image") as string | undefined) ?? "";
    // Not any digest-pinned image: only the allowlisted probe images.
    if (!(PROOF_PROBE_IMAGES.includes(image) || (ph && PLACEHOLDER.test(image)))) return refuse(DIGEST_IMAGE.test(image) ? "probe_image_not_allowlisted" : "probe_image_not_digest_pinned");
    if (tail?.join(" ") !== "sleep 7200") return refuse("probe_command_not_allowlisted");
    const want = probePod(args[0] === PROBE_POD ? "relaycast" : "search", image);
    if (fl.get("labels") !== want.labels) return refuse("probe_labels_not_allowlisted");
    if (fl.get("overrides") !== want.overrides) return refuse("probe_overrides_not_allowlisted");
    if (fl.get("restart") !== "Never") return refuse("probe_restart_not_allowlisted");
    const env = fl.get("env");
    if (env !== undefined && !(args[0] === PROBE_POD && env === `RC_URL=${RELAYCAST_PROBE_URL}`)) return refuse("probe_env_not_allowlisted");
    return write(`run ${args[0]}`);
  }
  if (verb === "delete") {
    if (tail) return refuse("delete_not_allowlisted");
    if (args.length === 2 && args[0] === "pod" && name(args[1], (p) => r.pods.test(p))) return write(`delete pod ${ns}`);
    if (ns === "96bulls" && args.length === 2 && args[0] === CNPG_CLUSTER && args[1] === "hoopdb-restore-proof") return write("delete restore cluster");
    if (ns === "96bulls" && args.length === 2 && args[0] === CNPG_BACKUP && name(args[1], (b) => BACKUP_NAME.test(b))) return write("delete backup");
    if ((ns === "96bulls" || ns === "relaycast") && args.length === 1 && args[0] === `${CNPG_BACKUP},${CNPG_CLUSTER},pods` && label === PROOF_LABEL) return write("delete proof objects");
    return refuse("delete_not_allowlisted");
  }
  if (verb === "apply" && args.length === 0 && fl.get("f") === "-" && ns === "96bulls") {
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
  const t = tokenize(argv.slice(1), HELM);
  if ("error" in t) return refuse(t.error);
  if (t.tail) return refuse("helm_operation_not_allowlisted");
  const fl = t.flags;
  const only = (...keys: string[]) => [...fl.keys()].every((k) => keys.includes(k)) && keys.every((k) => fl.has(k));
  const ver = (v: unknown) => typeof v === "string" && (/^\d+\.\d+\.\d+$/.test(v) || (ph && PLACEHOLDER.test(v)));
  const [sub, ...rest] = t.pos;
  if (sub === "show") {
    if (rest[0] === "values" && rest.length === 2 && ["96bulls", "relayauth", "relaycast"].includes(rest[1]!) && only("repo", "version") && fl.get("repo") === PROOF_CHART_REPO && ver(fl.get("version")))
      return read("helm show values");
    return refuse("helm_operation_not_allowlisted");
  }
  const ctx = fl.get("kube-context");
  if (ctx === undefined) return refuse("context_missing");
  if (ctx !== f.context) return refuse("context_outside_fence");
  const ns = fl.get("n");
  if (typeof ns !== "string") return refuse("namespace_required");
  const nsOk = f.target === "proof" ? Object.keys(PROOF_NS).includes(ns) : Object.keys(KIND_NS).includes(ns);
  if (!nsOk) return refuse("namespace_outside_fence");
  if (sub === "list" && rest.length === 0 && only("kube-context", "n", "o") && fl.get("o") === "json") return read("helm list");
  if (f.target !== "proof" || ns !== "96bulls") return refuse("helm_operation_not_allowlisted");
  if (rest[0] !== PROOF_RELEASE) return refuse("release_outside_fence");
  if ((sub === "history" || sub === "status") && rest.length === 1 && only("kube-context", "n", "o") && fl.get("o") === "json") return read(`helm ${sub}`);
  if (sub === "upgrade" && rest.length === 2 && rest[1] === "96bulls" && only("kube-context", "n", "repo", "version", "reuse-values", "wait", "timeout") && fl.get("repo") === PROOF_CHART_REPO && ver(fl.get("version")) && fl.get("timeout") === "10m")
    return write("helm upgrade");
  if (sub === "rollback" && rest.length === 2 && (/^\d+$/.test(rest[1]!) || (ph && PLACEHOLDER.test(rest[1]!))) && only("kube-context", "n", "wait", "timeout") && fl.get("timeout") === "10m") return write("helm rollback");
  if (sub === "uninstall" && rest.length === 1 && only("kube-context", "n", "wait")) return write("helm uninstall");
  return refuse("helm_operation_not_allowlisted");
}

/* ------------------------------------------------------------- aws */

/** A planted session's short summary object, under a proof tenant's `96bulls/<org>/` prefix. */
const SUMMARY_KEY = new RegExp(`^${PROOF_KEY_PREFIX_BASE}/(${PROOF_TENANT_ORGS.join("|")})/v1/ws=[^/]+/m=proof-[a-z0-9]+-m\\d+/h=claude/d=\\d{4}/\\d{2}/\\d{2}/s=[0-9a-f-]{36}/summary/short\\.json\\.enc$`);

const ARN = /^arn:aws:([a-z0-9-]+):([a-z0-9-]*):(\d{12})?:(.+)$/;

function arnOk(v: string, ph: boolean): boolean {
  if (ph && PLACEHOLDER.test(v)) return true;
  const m = ARN.exec(v);
  return !!m && m[2] === PROOF_REGION && m[3] === PROOF_ACCOUNT_ID;
}

function admitAws(argv: readonly string[], f: Fence, ph: boolean): Admission {
  if (f.target !== "proof") return refuse("aws_outside_fence");
  const t = tokenize(argv.slice(1), AWS);
  if ("error" in t) return refuse(t.error);
  if (t.tail) return refuse("aws_operation_not_allowlisted");
  const fl = t.flags;
  const region = fl.get("region");
  if (region === undefined) return refuse("region_missing");
  if (region !== PROOF_REGION) return refuse("region_outside_fence");
  const opt = (flag: string) => fl.get(flag) as string | undefined;
  const [svc, cmd, ...args] = t.pos;
  const bucket = opt("bucket");
  const buckets = f.buckets;
  if (svc === "s3api") {
    if (!bucket || !buckets.has(bucket)) return refuse("bucket_outside_fence");
    if (cmd === "list-objects-v2" || cmd === "head-object" || cmd === "get-bucket-versioning" || cmd === "head-bucket") return read(`s3api ${cmd}`);
    if (cmd === "delete-object" && bucket === PROOF_LOCKER_BUCKET && SUMMARY_KEY.test(opt("key") ?? ""))
      return write("delete summary object");
    return refuse("aws_operation_not_allowlisted");
  }
  if (svc === "s3" && cmd === "cp" && args.length === 2 && args[1] === "-") {
    const src = args[0]!;
    const m = /^s3:\/\/([^/]+)\/(telemetry\/traces\/.+)$/.exec(src);
    return m && m[1] === PROOF_LOCKER_BUCKET ? read("s3 cp trace batch") : refuse("bucket_outside_fence");
  }
  if (svc === "elbv2" && (cmd === "describe-load-balancers" || cmd === "describe-ssl-policies")) return read(`elbv2 ${cmd}`);
  if (svc === "elbv2" && cmd === "describe-listeners") return arnOk(opt("load-balancer-arn") ?? "", ph) ? read("elbv2 describe-listeners") : refuse("arn_outside_fence");
  if (svc === "wafv2" && cmd === "get-web-acl-for-resource") return arnOk(opt("resource-arn") ?? "", ph) ? read("wafv2 get-web-acl-for-resource") : refuse("arn_outside_fence");
  if (svc === "kms" && cmd === "describe-key") return arnOk(opt("key-id") ?? "", ph) ? read("kms describe-key") : refuse("arn_outside_fence");
  if (svc === "eks" && cmd === "list-clusters") return read("eks list-clusters");
  if (svc === "eks" && cmd === "describe-cluster" && opt("name") === PROOF_CLUSTER) return read("eks describe-cluster");
  if (svc === "resourcegroupstaggingapi" && cmd === "get-resources" && opt("tag-filters") === `Key=96bulls:stack,Values=${PROOF_STAGE}`) return read("tagged resources");
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
