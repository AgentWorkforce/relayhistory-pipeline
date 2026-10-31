/**
 * Checks 5-8 as exact command plans, plus the pure arithmetic and residue
 * checks that decide them: restart persistence (rollout restarts + CNPG
 * failover + a Relaycast roundtrip from inside the cluster), upgrade/rollback,
 * backup/restore with RPO/RTO, and teardown.
 *
 * Every mutating step here needs --confirm-live, the account/cluster guard, and
 * Khaliq's direct OK recorded before the run (docs/aws-e2e-proof.md).
 */
import type { ProofConfig } from "./config.ts";
import type { Step } from "./exec.ts";
import { PROOF_STAGE } from "./guard.ts";
import { kubectl } from "./probes.ts";

const mut = (id: string, argv: string[], touches: string[], timeoutMs?: number): Step => ({ id, argv, mutating: true, touches, approval: "khaliq", ...(timeoutMs ? { timeoutMs } : {}) });
const read = (id: string, argv: string[], touches: string[], timeoutMs?: number): Step => ({ id, argv, mutating: false, touches, approval: "operator", ...(timeoutMs ? { timeoutMs } : {}) });

/* ------------------------------------------------------------- restart */

export function rolloutRestartSteps(c: ProofConfig): Step[] {
  return c.coreDeployments.flatMap((d) => [
    mut(`restart.${d}`, kubectl(c, "-n", c.namespace, "rollout", "restart", `deployment/${d}`), [`k8s:${c.namespace}/deployment/${d} (pods replaced)`]),
    read(`restart.${d}.status`, kubectl(c, "-n", c.namespace, "rollout", "status", `deployment/${d}`, "--timeout=300s"), [`k8s:${c.namespace}/deployment/${d}`], 320_000),
  ]);
}

export function cnpgInstancesStep(c: ProofConfig): Step {
  return read("failover.instances", kubectl(c, "-n", c.namespace, "get", "clusters.postgresql.cnpg.io", c.hoopdb.cluster, "-o", "jsonpath={.spec.instances}"), [`k8s:${c.namespace}/cnpg-cluster/${c.hoopdb.cluster}`]);
}

/**
 * CNPG fails over when the primary pod is deleted; the operator promotes the
 * most advanced replica. Needs instances >= 2 (the chart default is 1, so the
 * proof install must set hoopdb.cluster.instances).
 */
export function failoverSteps(c: ProofConfig, primary: string): Step[] {
  return [
    mut("failover.delete-primary", kubectl(c, "-n", c.namespace, "delete", "pod", primary || "<hoopdb primary>", "--wait=false"), [`k8s:${c.namespace}/pod/${primary || "<primary>"} (deleted; CNPG promotes a replica)`]),
    read("failover.wait-ready", kubectl(c, "-n", c.namespace, "wait", "--for=condition=Ready", `clusters.postgresql.cnpg.io/${c.hoopdb.cluster}`, "--timeout=600s"), [`k8s:${c.namespace}/cnpg-cluster/${c.hoopdb.cluster}`], 620_000),
  ];
}

export function failoverVerdict(before: string, after: string, instances: number): { ok: boolean; blocked: boolean; reason: string } {
  if (!(instances >= 2)) return { ok: false, blocked: true, reason: `CNPG cluster has ${instances || 0} instance(s); failover needs >= 2` };
  if (!before || !after) return { ok: false, blocked: false, reason: "primary unknown before or after" };
  if (before === after) return { ok: false, blocked: false, reason: "primary did not change" };
  return { ok: true, blocked: false, reason: `primary moved ${before} -> ${after}` };
}

/* ------------------------------------------------------------- relaycast */

export const RELAYCAST_PROBE_POD = `proof-relaycast-${PROOF_STAGE}`;

/**
 * Assertions each phase must print as PASS. Before the restart: a fresh
 * workspace, agent, message send and receive. After it: the same workspace key
 * and agent token still work, the pre-restart message is still there, and a new
 * message round-trips. Missing, failed or extra-FAIL lines all fail the phase.
 */
export const RELAYCAST_ASSERTIONS = {
  before: ["health", "workspace", "agent", "message.send", "message.receive"],
  after: ["health", "workspace", "agent", "message.send", "message.receive", "message.persisted"],
} as const;

/**
 * Runs inside the probe pod with bun. Keeps the workspace key in the pod's own
 * in-memory emptyDir between phases; prints one PASS/FAIL line per assertion,
 * never a key or a body.
 */
export const RELAYCAST_PROBE_SOURCE = String.raw`
const base = process.env.RC_URL, phase = process.argv.at(-1), f = "/probe/state.json";
const fs = await import("node:fs");
const j = async (r) => { const b = await r.json().catch(() => ({})); return { s: r.status, d: b.data ?? b }; };
const auth = (k) => ({ authorization: "Bearer " + k, "content-type": "application/json" });
const out = (ok, what) => { console.log((ok ? "PASS " : "FAIL ") + phase + " " + what); if (!ok) process.exitCode = 1; };
const guard = async (what, fn) => { try { out(!!(await fn()), what); } catch { out(false, what); } };
const read = async (token) => { const r = await fetch(base + "/v1/channels/general/messages?limit=100", { headers: auth(token) }); return r.status === 200 ? await r.text() : ""; };
await guard("health", async () => (await fetch(base + "/health")).status === 200);
let st = fs.existsSync(f) ? JSON.parse(fs.readFileSync(f, "utf8")) : null;
if (phase === "before") {
  st = {};
  await guard("workspace", async () => { const ws = await j(await fetch(base + "/v1/workspaces", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name: "aws-proof-" + Date.now(), expires_in_seconds: 7200 }) })); st.key = ws.d.api_key; return ws.s < 300 && !!st.key; });
  await guard("agent", async () => { const ag = await j(await fetch(base + "/v1/agents", { method: "POST", headers: auth(st.key), body: JSON.stringify({ name: "proof-agent", type: "agent" }) })); st.token = ag.d.token; return ag.s < 300 && !!st.token; });
  fs.writeFileSync(f, JSON.stringify(st), { mode: 0o600 });
} else {
  await guard("workspace", async () => !!st?.key && (await fetch(base + "/v1/agents", { headers: auth(st.key) })).status === 200);
  await guard("agent", async () => !!st?.token && (await fetch(base + "/v1/agents", { headers: auth(st.key) }).then((r) => r.text())).includes("proof-agent"));
  await guard("message.persisted", async () => !!st?.before && (await read(st.token)).includes(st.before));
}
const marker = "proof-" + phase + "-" + crypto.randomUUID();
await guard("message.send", async () => !!st?.token && (await fetch(base + "/v1/channels/general/messages", { method: "POST", headers: auth(st.token), body: JSON.stringify({ text: marker }) })).status < 300);
await guard("message.receive", async () => !!st?.token && (await read(st.token)).includes(marker));
if (phase === "before" && st) { st.before = marker; fs.writeFileSync(f, JSON.stringify(st), { mode: 0o600 }); }
`;

export function relaycastProbeSteps(c: ProofConfig, phase: "start" | "before" | "after" | "delete"): Step[] {
  const ns = c.relaycast.namespace;
  const url = `http://${c.relaycast.service}.${ns}.svc:${c.relaycast.port}`;
  if (phase === "start")
    return [
      mut("relaycast.probe-pod", kubectl(c, "-n", ns, "run", RELAYCAST_PROBE_POD, `--image=${c.probeImage ?? "<digest-pinned probe image>"}`, "--restart=Never", "--labels=96bulls.proof=true", `--env=RC_URL=${url}`, "--overrides", JSON.stringify({ spec: { volumes: [{ name: "probe", emptyDir: { medium: "Memory" } }], containers: [{ name: RELAYCAST_PROBE_POD, image: c.probeImage ?? "<digest-pinned probe image>", command: ["sleep", "7200"], env: [{ name: "RC_URL", value: url }], volumeMounts: [{ name: "probe", mountPath: "/probe" }] }] } }), "--command", "--", "sleep", "7200"), [`k8s:${ns}/pod/${RELAYCAST_PROBE_POD} (created)`]),
      read("relaycast.probe-ready", kubectl(c, "-n", ns, "wait", "--for=condition=Ready", `pod/${RELAYCAST_PROBE_POD}`, "--timeout=180s"), [`k8s:${ns}/pod/${RELAYCAST_PROBE_POD}`], 200_000),
    ];
  if (phase === "delete") return [mut("relaycast.probe-delete", kubectl(c, "-n", ns, "delete", "pod", RELAYCAST_PROBE_POD, "--ignore-not-found"), [`k8s:${ns}/pod/${RELAYCAST_PROBE_POD} (deleted)`])];
  // Both phases write: "before" creates an ephemeral workspace, agent and message; "after" sends one more message.
  const argv = kubectl(c, "-n", ns, "exec", "-i", RELAYCAST_PROBE_POD, "--", "bun", "run", "-", phase);
  return [{ ...mut(`relaycast.roundtrip.${phase}`, argv, [`relaycast:${ns} (ephemeral workspace, agent, messages)`]), stdin: RELAYCAST_PROBE_SOURCE }];
}

/**
 * All-or-nothing: every required assertion for the phase must appear exactly as
 * a PASS line, and no FAIL line may appear. A missing assertion (the probe died
 * early, or the step never ran) is a failure, never a partial pass.
 */
export function relaycastVerdict(stdout: string, phase: "before" | "after", stepRan = true): { ok: boolean; failures: string[] } {
  if (!stepRan) return { ok: false, failures: [`${phase}: probe step did not run`] };
  const lines = stdout.split("\n").map((l) => l.trim());
  const failures: string[] = [];
  for (const l of lines) if (l.startsWith("FAIL ")) failures.push(l.slice(5));
  for (const a of RELAYCAST_ASSERTIONS[phase]) if (!lines.includes(`PASS ${phase} ${a}`) && !failures.includes(`${phase} ${a}`)) failures.push(`${phase} ${a} missing`);
  return { ok: failures.length === 0, failures };
}

export const relaycastRestartSteps = (c: ProofConfig): Step[] => [
  mut("restart.relaycast", kubectl(c, "-n", c.relaycast.namespace, "rollout", "restart", `deployment/${c.relaycast.service}`), [`k8s:${c.relaycast.namespace}/deployment/${c.relaycast.service} (pod replaced; PVC kept)`]),
  read("restart.relaycast.status", kubectl(c, "-n", c.relaycast.namespace, "rollout", "status", `deployment/${c.relaycast.service}`, "--timeout=300s"), [`k8s:${c.relaycast.namespace}/deployment/${c.relaycast.service}`], 320_000),
];

/* ------------------------------------------------------------- upgrade / rollback */

export function helmHistoryStep(c: ProofConfig): Step {
  return read("helm.history", ["helm", "--kube-context", c.kubeContext, "-n", c.namespace, "history", c.release, "-o", "json"], [`helm:${c.namespace}/${c.release}`]);
}

/**
 * `--reuse-values` keeps the proof install's inputs; only the chart version
 * changes. --atomic is avoided on purpose: a failed upgrade must stay visible.
 */
export function upgradeRollbackSteps(c: ProofConfig, priorRevision: string): Step[] {
  const helm = (...a: string[]) => ["helm", "--kube-context", c.kubeContext, "-n", c.namespace, ...a];
  const cand = c.candidateChartVersion ?? "<candidate chart version>";
  return [
    mut("upgrade.candidate", helm("upgrade", c.release, "96bulls", "--repo", c.chartRepo, "--version", cand, "--reuse-values", "--wait", "--timeout", "10m"), [`helm:${c.namespace}/${c.release} -> ${cand}`], 660_000),
    read("upgrade.status", helm("status", c.release, "-o", "json"), [`helm:${c.namespace}/${c.release}`]),
    mut("rollback.prior", helm("rollback", c.release, priorRevision || "<prior revision>", "--wait", "--timeout", "10m"), [`helm:${c.namespace}/${c.release} -> revision ${priorRevision || "<prior>"}`], 660_000),
    read("rollback.status", helm("status", c.release, "-o", "json"), [`helm:${c.namespace}/${c.release}`]),
  ];
}

/* ------------------------------------------------------------- backup / restore */

export const RESTORE_CLUSTER = "hoopdb-restore-proof";

/**
 * On-demand CNPG Backup to the barman object store configured on the cluster,
 * then a recovery into a separate Cluster (never in place). The planted
 * session count in the restored cluster is compared with the source.
 */
export function backupRestoreSteps(c: ProofConfig, backupName: string): Step[] {
  const backup = { apiVersion: "postgresql.cnpg.io/v1", kind: "Backup", metadata: { name: backupName, namespace: c.namespace, labels: { "96bulls.proof": "true" } }, spec: { cluster: { name: c.hoopdb.cluster }, method: "barmanObjectStore" } };
  const restore = {
    apiVersion: "postgresql.cnpg.io/v1",
    kind: "Cluster",
    metadata: { name: RESTORE_CLUSTER, namespace: c.namespace, labels: { "96bulls.proof": "true" } },
    spec: {
      instances: 1,
      storage: { size: "10Gi" },
      bootstrap: { recovery: { backup: { name: backupName } } },
    },
  };
  return [
    read("backup.s3-versioning", ["aws", "s3api", "get-bucket-versioning", "--region", c.region, "--bucket", c.locker.bucket, "--output", "json"], [`s3:${c.locker.bucket} (versioning config)`]),
    read("backup.cluster-config", kubectl(c, "-n", c.namespace, "get", "clusters.postgresql.cnpg.io", c.hoopdb.cluster, "-o", "jsonpath={.spec.backup.barmanObjectStore.destinationPath}"), [`k8s:${c.namespace}/cnpg-cluster/${c.hoopdb.cluster}`]),
    { ...mut("backup.create", kubectl(c, "apply", "-f", "-"), [`k8s:${c.namespace}/backup/${backupName} (created)`, `s3:${c.backupBucket ?? "<backup bucket>"} (base backup + WAL written)`]), stdin: JSON.stringify(backup) },
    read("backup.wait", kubectl(c, "-n", c.namespace, "wait", "--for=jsonpath={.status.phase}=completed", `backups.postgresql.cnpg.io/${backupName}`, "--timeout=1200s"), [`k8s:${c.namespace}/backup/${backupName}`], 1_220_000),
    read("backup.status", kubectl(c, "-n", c.namespace, "get", `backups.postgresql.cnpg.io/${backupName}`, "-o", "json"), [`k8s:${c.namespace}/backup/${backupName}`]),
    { ...mut("restore.create", kubectl(c, "apply", "-f", "-"), [`k8s:${c.namespace}/cnpg-cluster/${RESTORE_CLUSTER} (created, 10Gi PVC)`]), stdin: JSON.stringify(restore) },
    read("restore.wait", kubectl(c, "-n", c.namespace, "wait", "--for=condition=Ready", `clusters.postgresql.cnpg.io/${RESTORE_CLUSTER}`, "--timeout=1800s"), [`k8s:${c.namespace}/cnpg-cluster/${RESTORE_CLUSTER}`], 1_820_000),
  ];
}

export function restoreCleanupSteps(c: ProofConfig, backupName: string): Step[] {
  return [
    mut("restore.delete", kubectl(c, "-n", c.namespace, "delete", "clusters.postgresql.cnpg.io", RESTORE_CLUSTER, "--ignore-not-found", "--wait=true"), [`k8s:${c.namespace}/cnpg-cluster/${RESTORE_CLUSTER} (deleted with its PVC)`]),
    mut("backup.delete", kubectl(c, "-n", c.namespace, "delete", "backups.postgresql.cnpg.io", backupName, "--ignore-not-found"), [`k8s:${c.namespace}/backup/${backupName} (CR deleted; objects stay under the bucket's retention)`]),
  ];
}

/**
 * RPO and RTO from observed timestamps (ms).
 *
 * RPO is the data-loss window: zero when every planted write committed before
 * the backup stopped is present in the restore; otherwise the span from the
 * last write recovered before the first lost one (or the first lost write, if
 * none was recovered) to the newest write that should have been recovered.
 * RTO is restore requested -> restored cluster Ready.
 */
export function rpoRto(o: {
  plantedWritesMs: readonly number[];
  presentInRestoreMs: readonly number[];
  backupStoppedMs: number;
  restoreRequestedMs: number;
  restoreReadyMs: number;
}): { rpoMs: number; rtoMs: number; lostWrites: number } {
  if (o.restoreReadyMs < o.restoreRequestedMs) throw new RangeError("restore ready before it was requested");
  const due = o.plantedWritesMs.filter((t) => t <= o.backupStoppedMs);
  const present = new Set(o.presentInRestoreMs);
  const lost = due.filter((t) => !present.has(t));
  let rpoMs = 0;
  if (lost.length) {
    const firstLost = Math.min(...lost);
    const recoveredBefore = o.presentInRestoreMs.filter((t) => t < firstLost);
    const from = recoveredBefore.length ? Math.max(...recoveredBefore) : firstLost;
    rpoMs = Math.max(...due) - from;
  }
  return { rpoMs, rtoMs: o.restoreReadyMs - o.restoreRequestedMs, lostWrites: lost.length };
}

/**
 * The backup verdict. Empty samples prove nothing: no planted write in the
 * source before the backup, or nothing readable in the restore, is a failure,
 * never an RPO of 0. Cleanup (restore cluster + Backup CR) must succeed too.
 */
export function evaluateBackup(o: {
  plantedWritesMs: readonly number[];
  /** undefined when the restored cluster could not be queried. */
  presentInRestoreMs: readonly number[] | undefined;
  backupStoppedMs: number;
  restoreRequestedMs: number;
  restoreReadyMs: number;
  cleanup: ReadonlyArray<{ id: string; ok: boolean }>;
}): { failures: string[]; metrics: { rpoMs: number | null; rtoMs: number | null; lostWrites: number | null; plantedBeforeBackup: number; presentInRestore: number } } {
  const failures: string[] = [];
  const due = o.plantedWritesMs.filter((t) => t <= o.backupStoppedMs);
  if (o.plantedWritesMs.length === 0) failures.push("no planted writes found in the source cluster; nothing to measure RPO against");
  else if (due.length === 0) failures.push("no planted write was committed before the backup stopped; RPO is unmeasured");
  if (o.presentInRestoreMs === undefined) failures.push("could not query the restored cluster");
  else if (o.presentInRestoreMs.length === 0) failures.push("the restored cluster holds none of the planted sessions");
  for (const c of o.cleanup) if (!c.ok) failures.push(`cleanup step ${c.id} failed; restore resources may remain`);
  if (o.cleanup.length === 0) failures.push("cleanup did not run");
  let m: ReturnType<typeof rpoRto> | undefined;
  if (due.length && o.presentInRestoreMs?.length) {
    m = rpoRto({ plantedWritesMs: o.plantedWritesMs, presentInRestoreMs: o.presentInRestoreMs, backupStoppedMs: o.backupStoppedMs, restoreRequestedMs: o.restoreRequestedMs, restoreReadyMs: o.restoreReadyMs });
    if (m.lostWrites) failures.push(`${m.lostWrites} planted write(s) committed before the backup are missing from the restore (RPO ${m.rpoMs} ms)`);
  }
  return { failures, metrics: { rpoMs: m?.rpoMs ?? null, rtoMs: m?.rtoMs ?? null, lostWrites: m?.lostWrites ?? null, plantedBeforeBackup: due.length, presentInRestore: o.presentInRestoreMs?.length ?? 0 } };
}

export function versioningEnabled(getBucketVersioningJson: string): boolean {
  try {
    return (JSON.parse(getBucketVersioningJson) as { Status?: string }).Status === "Enabled";
  } catch {
    return false;
  }
}

/* ------------------------------------------------------------- teardown */

/** The stack tag every Alchemy AWS resource carries (stacks/aws/alchemy.aws.run.ts `tags()`). */
export const PROOF_TAG = { key: "96bulls:stack", value: PROOF_STAGE } as const;

/**
 * Teardown removes only what the proof created inside the proof cluster and
 * the proof-tagged AWS resources. The cluster itself and its account baseline
 * are torn down by `96bulls down aws-proof` (Alchemy), which is a separate,
 * Khaliq-approved step and is printed, not run, here.
 */
export function teardownSteps(c: ProofConfig): Step[] {
  return [
    ...relaycastProbeSteps(c, "delete"),
    mut("teardown.proof-objects", kubectl(c, "delete", "backups.postgresql.cnpg.io,clusters.postgresql.cnpg.io,pods", "-A", "-l", "96bulls.proof=true", "--ignore-not-found", "--wait=true"), ["k8s:* objects labelled 96bulls.proof=true (deleted)"], 600_000),
    mut("teardown.helm-uninstall", ["helm", "--kube-context", c.kubeContext, "-n", c.namespace, "uninstall", c.release, "--wait"], [`helm:${c.namespace}/${c.release} (uninstalled)`], 600_000),
    mut("teardown.namespace", kubectl(c, "delete", "namespace", c.namespace, "--ignore-not-found", "--wait=true", "--timeout=600s"), [`k8s:namespace/${c.namespace} (deleted, PVCs included)`], 620_000),
  ];
}

export function teardownVerifySteps(c: ProofConfig): Step[] {
  return [
    read("teardown.verify.cluster", ["aws", "eks", "list-clusters", "--region", c.region, "--query", "clusters", "--output", "json"], [`aws:eks clusters in ${c.region} (list)`]),
    read("teardown.verify.k8s", kubectl(c, "get", "all,pvc,backups.postgresql.cnpg.io,clusters.postgresql.cnpg.io", "-A", "-l", "96bulls.proof=true", "-o", "json"), ["k8s:* (list, labelled)"]),
    read("teardown.verify.namespace", kubectl(c, "get", "namespace", c.namespace, "--ignore-not-found", "-o", "name"), [`k8s:namespace/${c.namespace}`]),
    read("teardown.verify.aws-tagged", ["aws", "resourcegroupstaggingapi", "get-resources", "--region", c.region, "--tag-filters", `Key=${PROOF_TAG.key},Values=${PROOF_TAG.value}`, "--output", "json"], [`aws:${c.region} resources tagged ${PROOF_TAG.key}=${PROOF_TAG.value} (list)`]),
  ];
}

/** Printed only: destroying the stage itself is Khaliq's call and runs through Alchemy. */
export const STAGE_DESTROY_COMMAND = `bin/96bulls down ${PROOF_STAGE}   # Alchemy destroy; requires Khaliq's OK; not run by this harness`;

/**
 * What is left. While the cluster exists, in-cluster leftovers are residue and
 * tagged AWS resources are the stage itself (pending `96bulls down`). Once the
 * cluster is gone, every tagged resource is residue.
 */
export function teardownResidue(o: { clusterGone: boolean; k8sJson: string; namespaceOut: string; taggedJson: string }): { inCluster: string[]; tagged: string[] } {
  const residue: string[] = [];
  const tagged: string[] = [];
  if (!o.clusterGone) try {
    const items = (JSON.parse(o.k8sJson || "{}") as { items?: Array<{ kind: string; metadata: { namespace?: string; name: string } }> }).items ?? [];
    for (const i of items) residue.push(`k8s ${i.kind} ${i.metadata.namespace ?? ""}/${i.metadata.name}`);
  } catch {
    residue.push("k8s listing unreadable");
  }
  if (!o.clusterGone && o.namespaceOut.trim()) residue.push(`k8s ${o.namespaceOut.trim()} still exists`);
  try {
    const list = (JSON.parse(o.taggedJson) as { ResourceTagMappingList?: Array<{ ResourceARN: string }> }).ResourceTagMappingList ?? [];
    for (const r of list) tagged.push(`aws ${r.ResourceARN}`);
  } catch {
    tagged.push("aws tagged-resource listing unreadable");
  }
  return { inCluster: residue, tagged };
}

/** Gone only when the listing itself succeeded and does not name the cluster; a failed call proves nothing. */
export function clusterGone(listOk: boolean, listJson: string, name: string): boolean | undefined {
  if (!listOk) return undefined;
  try {
    return !(JSON.parse(listJson) as string[]).includes(name);
  } catch {
    return undefined;
  }
}
