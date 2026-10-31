import { describe, expect, test } from "bun:test";
import { defaultConfig } from "../config.ts";
import { collectImages, evaluateFreshInstall, expectedFromValues, parseChartRef, repositoryOf, type PodList } from "../fresh-install.ts";
import {
  burstPlan, evaluateBurst, evaluateListeners, evaluateRoutes, evaluateTls, evaluateWafConfig, hostIsProofAlb, ingestRoutes, routeRefused, runBurst, type WebAcl,
} from "../ingress.ts";
import {
  backupRestoreSteps, clusterGone, evaluateBackup, failoverVerdict, RELAYCAST_ASSERTIONS, relaycastProbeSteps, relaycastVerdict, rolloutRestartSteps, rpoRto, teardownResidue, teardownSteps, upgradeRollbackSteps,
} from "../lifecycle.ts";

const D = (c: string) => `sha256:${c.repeat(64)}`;

describe("fresh install", () => {
  const pod = (image: string, imageID: string, phase = "Running"): PodList["items"][number] => ({
    metadata: { name: "p", namespace: "96bulls" },
    spec: { containers: [{ name: "c", image }] },
    status: { phase, containerStatuses: [{ name: "c", image, imageID }] },
  });
  const release = [{ name: "96bulls", namespace: "96bulls", revision: "1", chart: "96bulls-0.3.0", app_version: "0.1.0", status: "deployed" }];
  test("repository normalization matches spec, status and values forms", () => {
    expect(repositoryOf(`ghcr.io/a/b:1.0@${D("a")}`)).toBe("ghcr.io/a/b");
    expect(repositoryOf("otel/collector:0.1")).toBe("docker.io/otel/collector");
    expect(repositoryOf("alpine")).toBe("docker.io/library/alpine");
    expect(repositoryOf("localhost:5000/x:tag")).toBe("localhost:5000/x");
  });
  test("pinned, running and released digests all agree: pass", () => {
    const v = evaluateFreshInstall({ items: [pod(`ghcr.io/a/b@${D("a")}`, `ghcr.io/a/b@${D("a")}`)] }, release, { "ghcr.io/a/b": D("a") });
    expect(v).toMatchObject({ ok: true, blocked: false, failures: [] });
    expect(v.charts[0]!.chart).toBe("96bulls-0.3.0");
  });
  test("tag-only images, running/spec drift and non-released digests fail", () => {
    const v = evaluateFreshInstall({ items: [
      pod("ghcr.io/a/b:latest", `ghcr.io/a/b@${D("a")}`),
      pod(`ghcr.io/a/c@${D("b")}`, `ghcr.io/a/c@${D("c")}`),
      pod(`ghcr.io/a/d@${D("d")}`, `ghcr.io/a/d@${D("d")}`),
      pod(`ghcr.io/a/e@${D("e")}`, `ghcr.io/a/e@${D("e")}`),
    ] }, release, { "ghcr.io/a/c": D("b"), "ghcr.io/a/d": D("0") });
    expect(v.ok).toBe(false);
    expect(v.failures.join("\n")).toMatch(/not digest-pinned/);
    expect(v.failures.join("\n")).toMatch(/running sha256:c+ differs from spec/);
    expect(v.failures.join("\n")).toMatch(/is not the released\/input digest/);
    expect(v.failures.join("\n")).toMatch(/no released\/input digest recorded for ghcr.io\/a\/e/);
  });
  test("completed job pods are not running images", () => {
    expect(collectImages({ items: [pod("x:1", "", "Succeeded")] })).toEqual([]);
  });
  test("no expectations at all is blocked, not a pass", () => {
    expect(evaluateFreshInstall({ items: [pod(`ghcr.io/a/b@${D("a")}`, `ghcr.io/a/b@${D("a")}`)] }, release, {})).toMatchObject({ ok: false, blocked: true });
  });
  test("released values are read in all three chart shapes", () => {
    const yaml = [
      `imageName: ghcr.io/cloudnative-pg/postgresql:17@${D("1")}`,
      "image:", "  repository: cgr.dev/chainguard/minio", `  tag: latest@${D("2")}`,
      "image:", "  repository: ghcr.io/agentworkforce/relayauth", "  # comment", `  digest: ${D("3")}`,
    ].join("\n");
    expect(expectedFromValues(yaml)).toEqual({
      "ghcr.io/cloudnative-pg/postgresql": D("1"),
      "cgr.dev/chainguard/minio": D("2"),
      "ghcr.io/agentworkforce/relayauth": D("3"),
    });
    expect(parseChartRef("relayauth-0.1.0")).toEqual({ name: "relayauth", version: "0.1.0" });
    expect(parseChartRef("96bulls-worker-0.4.0-rc.1")).toEqual({ name: "96bulls-worker", version: "0.4.0-rc.1" });
  });
});

describe("public ingress", () => {
  const routes = ingestRoutes({ otherTraceId: "01a18f4a-e978-7d04-b07c-4aa8dc28dc65", otherManifestKey: "v1/ws=default/m=x/h=claude/d=2026/10/30/s=y/manifest.json", other: { machineId: "x", harness: "claude", sessionId: "y", startDate: "2026-10-30" } });
  test("write probes name the other tenant's object so they reach the ownership check", () => {
    const chunk = new URL(`https://h${routes[0]!.path}`).searchParams;
    expect(Object.fromEntries(chunk)).toMatchObject({ date: "2026-10-30", machine: "x", harness: "claude", session: "y" });
    expect(routes[0]!.body).toBe("");
    expect(JSON.parse(routes[1]!.body!).key).toContain("s=y/manifest.json");
  });
  test("refusal rules: 401/403 everywhere; 404 only for by-id with a wrong-tenant token or internal routes", () => {
    const byId = routes.find((r) => r.byId)!;
    const write = routes.find((r) => r.path.startsWith("/v1/pipeline/manifests") && r.method === "POST")!;
    const internal = routes.find((r) => r.internal)!;
    expect(routeRefused({ route: write, credential: "none", status: 401 })).toBe(true);
    expect(routeRefused({ route: write, credential: "none", status: 404 })).toBe(false);
    expect(routeRefused({ route: write, credential: "wrong-tenant", status: 400 })).toBe(false);
    expect(routeRefused({ route: byId, credential: "wrong-tenant", status: 404 })).toBe(true);
    expect(routeRefused({ route: byId, credential: "none", status: 404 })).toBe(false);
    expect(routeRefused({ route: internal, credential: "none", status: 404 })).toBe(true);
    expect(routeRefused({ route: write, credential: "none", status: 200 })).toBe(false);
  });
  test("healthz must be 200 and every probe refused", () => {
    expect(evaluateRoutes(routes.map((route) => ({ route, credential: "none", status: 401 })), 200)).toEqual([]);
    const f = evaluateRoutes([{ route: routes[0]!, credential: "none", status: 202 }], 503);
    expect(f).toHaveLength(2);
  });
  test("listeners: HTTPS 443 with ACM only; any HTTP listener or open :80 fails", () => {
    const https = { Port: 443, Protocol: "HTTPS", SslPolicy: "ELBSecurityPolicy-TLS13-1-2-2021-06", Certificates: [{ CertificateArn: "arn:aws:acm:us-west-2:500268966557:certificate/x" }] };
    expect(evaluateListeners([https], "closed")).toEqual([]);
    expect(evaluateListeners([https, { Port: 80, Protocol: "HTTP", DefaultActions: [{ Type: "redirect" }] }], "closed")[0]).toMatch(/plaintext HTTP listener on :80 \(redirect only\)/);
    expect(evaluateListeners([https], "open")).toEqual(["TCP :80 on the public host accepted a connection"]);
    expect(evaluateListeners([{ ...https, Certificates: [{ CertificateArn: "arn:aws:iam::1:server-certificate/x" }] }], "closed")[0]).toMatch(/ACM/);
  });
  test("TLS: policy floor, legacy handshake, SAN and expiry", () => {
    const now = Date.parse("2026-10-30T00:00:00Z");
    const good = { legacyAccepted: false, modern: { ok: true, protocol: "TLSv1.3", validTo: "Jan  1 00:00:00 2027 GMT", subjectAltName: "DNS:sessions-dev.agentrelay.com" } };
    const policy = { Name: "ELBSecurityPolicy-TLS13-1-2-2021-06", SslProtocols: ["TLSv1.2", "TLSv1.3"] };
    expect(evaluateTls(policy, good, "sessions-dev.agentrelay.com", now)).toEqual([]);
    expect(evaluateTls({ Name: "ELBSecurityPolicy-2016-08", SslProtocols: ["TLSv1", "TLSv1.1", "TLSv1.2"] }, good, "sessions-dev.agentrelay.com", now)[0]).toMatch(/allows TLSv1, TLSv1.1/);
    expect(evaluateTls(policy, { ...good, legacyAccepted: true }, "sessions-dev.agentrelay.com", now)).toEqual(["a TLS 1.0/1.1 handshake succeeded"]);
    expect(evaluateTls(policy, { ...good, modern: { ...good.modern, subjectAltName: "DNS:*.agentrelay.com" } }, "sessions-dev.agentrelay.com", now)).toEqual([]);
    expect(evaluateTls(policy, { ...good, modern: { ...good.modern, subjectAltName: "DNS:*.agentrelay.com" } }, "a.b.agentrelay.com", now)[0]).toMatch(/no SAN/);
    expect(evaluateTls(policy, { ...good, modern: { ...good.modern, validTo: "Jan  1 00:00:00 2020 GMT" } }, "sessions-dev.agentrelay.com", now)).toEqual(["certificate expired"]);
  });
  const acl: WebAcl = { Name: "proof-acl", ARN: "arn", Rules: [{ Name: "rate", Statement: { RateBasedStatement: { Limit: 100, EvaluationWindowSec: 60 } }, Action: { Block: {} } }] };
  test("WAF: an associated ACL with a blocking rate rule", () => {
    expect(evaluateWafConfig(acl)).toEqual([]);
    expect(evaluateWafConfig(undefined)).toEqual(["no WAFv2 web ACL is associated with the ALB"]);
    expect(evaluateWafConfig({ ...acl, Rules: [{ ...acl.Rules![0]!, Action: { Count: {} } }] })[0]).toMatch(/only count/);
  });
  test("the burst is bounded: above the rule limit, at most 5000 requests and 300 s", () => {
    expect(burstPlan(acl)).toEqual({ maxRequests: 170, concurrency: 10, deadlineMs: 180_000, path: "/healthz" });
    expect(burstPlan(acl, { maxRequests: 50 })).toEqual({ refused: "maxRequests 50 cannot exceed rule limit 100" });
    expect(burstPlan(acl, { maxRequests: 9000 })).toMatchObject({ refused: expect.stringMatching(/hard cap/) });
    expect(burstPlan(acl, { concurrency: 500, deadlineMs: 10_000_000 })).toMatchObject({ concurrency: 25, deadlineMs: 300_000 });
    expect(burstPlan(undefined)).toEqual({ refused: "no blocking rate-based rule to exercise" });
  });
  test("the burst stops at the first WAF block and never exceeds its cap", async () => {
    let n = 0;
    const plan = { maxRequests: 40, concurrency: 5, deadlineMs: 10_000, path: "/healthz" };
    const r = await runBurst("https://h", plan, async () => ({ status: ++n > 12 ? 403 : 200 }));
    expect(r.blocked).toBeGreaterThan(0);
    expect(r.sent).toBe(15);
    expect(r.firstBlockAfter).toBe(13);
    expect(evaluateBurst(r, plan)).toEqual([]);
    const never = await runBurst("https://h", plan, async () => ({ status: 200 }));
    expect(never.sent).toBe(40);
    expect(evaluateBurst(never, plan)[0]).toMatch(/did not block/);
  });
  test("active probes only when the host resolves to the proof ALB", () => {
    expect(hostIsProofAlb(["1.1.1.1", "2.2.2.2"], ["2.2.2.2", "1.1.1.1"])).toBe(true);
    expect(hostIsProofAlb(["104.16.0.1"], ["1.1.1.1"])).toBe(false);
    expect(hostIsProofAlb([], ["1.1.1.1"])).toBe(false);
  });
});

describe("lifecycle plans and verdicts", () => {
  const c = { ...defaultConfig(), candidateChartVersion: "0.3.1", probeImage: `ghcr.io/x@${D("a")}` };
  test("every mutating lifecycle step needs Khaliq's approval; reads never do", () => {
    const all = [
      ...rolloutRestartSteps(c), ...relaycastProbeSteps(c, "start"), ...relaycastProbeSteps(c, "before"), ...relaycastProbeSteps(c, "after"),
      ...upgradeRollbackSteps(c, "3"), ...backupRestoreSteps(c, "proof-x"), ...teardownSteps(c),
    ];
    for (const s of all) expect(s.approval).toBe(s.mutating ? "khaliq" : "operator");
    expect(all.filter((s) => s.mutating).length).toBeGreaterThan(10);
    for (const s of all) if (s.argv[0] === "kubectl") expect(s.argv.slice(0, 3)).toEqual(["kubectl", "--context", c.kubeContext]);
    for (const s of all) if (s.argv[0] === "helm") expect(s.argv.slice(0, 3)).toEqual(["helm", "--kube-context", c.kubeContext]);
  });
  test("restore goes into a separate cluster, never in place", () => {
    const create = backupRestoreSteps(c, "proof-x").find((s) => s.id === "restore.create")!;
    const spec = JSON.parse(create.stdin!) as { metadata: { name: string }; spec: { bootstrap: { recovery: { backup: { name: string } } } } };
    expect(spec.metadata.name).not.toBe(c.hoopdb.cluster);
    expect(spec.spec.bootstrap.recovery.backup.name).toBe("proof-x");
  });
  test("failover needs >= 2 instances and a new primary", () => {
    expect(failoverVerdict("hoopdb-1", "", 1)).toMatchObject({ ok: false, blocked: true });
    expect(failoverVerdict("hoopdb-1", "hoopdb-1", 2)).toMatchObject({ ok: false, blocked: false });
    expect(failoverVerdict("hoopdb-1", "hoopdb-2", 2)).toMatchObject({ ok: true });
  });
  test("RPO is 0 when every pre-backup write is restored; the loss window otherwise; RTO is request to ready", () => {
    expect(rpoRto({ plantedWritesMs: [100, 200, 300, 900], presentInRestoreMs: [100, 200, 300], backupStoppedMs: 500, restoreRequestedMs: 1000, restoreReadyMs: 61_000 })).toEqual({ rpoMs: 0, rtoMs: 60_000, lostWrites: 0 });
    expect(rpoRto({ plantedWritesMs: [100, 200, 300, 400], presentInRestoreMs: [100, 200], backupStoppedMs: 500, restoreRequestedMs: 0, restoreReadyMs: 5 })).toEqual({ rpoMs: 200, rtoMs: 5, lostWrites: 2 });
    expect(() => rpoRto({ plantedWritesMs: [], presentInRestoreMs: [], backupStoppedMs: 0, restoreRequestedMs: 10, restoreReadyMs: 5 })).toThrow();
  });
  test("relaycast: every assertion must PASS in each phase; partial, failed, missing or unrun is FAIL", () => {
    const pass = (phase: "before" | "after") => RELAYCAST_ASSERTIONS[phase].map((x) => `PASS ${phase} ${x}`).join("\n");
    expect(relaycastVerdict(pass("before"), "before")).toEqual({ ok: true, failures: [] });
    expect(relaycastVerdict(pass("after"), "after")).toEqual({ ok: true, failures: [] });
    // The reviewed defect: health alone used to pass.
    expect(relaycastVerdict("PASS before health\n", "before")).toEqual({ ok: false, failures: ["before workspace missing", "before agent missing", "before message.send missing", "before message.receive missing"] });
    expect(relaycastVerdict(pass("after").replace("PASS after message.persisted", "FAIL after message.persisted"), "after")).toEqual({ ok: false, failures: ["after message.persisted"] });
    // "before" lines do not satisfy "after".
    expect(relaycastVerdict(pass("before"), "after").ok).toBe(false);
    expect(relaycastVerdict(pass("before"), "before", false)).toEqual({ ok: false, failures: ["before: probe step did not run"] });
    expect(relaycastVerdict("", "after").failures).toHaveLength(RELAYCAST_ASSERTIONS.after.length);
  });
  test("backup: empty source or restore samples and failed cleanup are FAIL, never RPO 0", () => {
    const base = { backupStoppedMs: 500, restoreRequestedMs: 1000, restoreReadyMs: 2000, cleanup: [{ id: "restore.delete", ok: true }, { id: "backup.delete", ok: true }] };
    expect(evaluateBackup({ ...base, plantedWritesMs: [100, 200], presentInRestoreMs: [100, 200] })).toEqual({ failures: [], metrics: { rpoMs: 0, rtoMs: 1000, lostWrites: 0, plantedBeforeBackup: 2, presentInRestore: 2 } });
    const empty = evaluateBackup({ ...base, plantedWritesMs: [], presentInRestoreMs: [] });
    expect(empty.metrics.rpoMs).toBeNull();
    expect(empty.failures).toEqual(["no planted writes found in the source cluster; nothing to measure RPO against", "the restored cluster holds none of the planted sessions"]);
    expect(evaluateBackup({ ...base, plantedWritesMs: [900], presentInRestoreMs: [900] }).failures[0]).toMatch(/before the backup stopped/);
    expect(evaluateBackup({ ...base, plantedWritesMs: [100], presentInRestoreMs: undefined }).failures).toEqual(["could not query the restored cluster"]);
    expect(evaluateBackup({ ...base, plantedWritesMs: [100], presentInRestoreMs: [100], cleanup: [{ id: "restore.delete", ok: false }] }).failures).toEqual(["cleanup step restore.delete failed; restore resources may remain"]);
    expect(evaluateBackup({ ...base, plantedWritesMs: [100], presentInRestoreMs: [100], cleanup: [] }).failures).toEqual(["cleanup did not run"]);
  });
  test("teardown residue: in-cluster leftovers while the cluster exists; every tagged resource once it is gone", () => {
    const k8s = JSON.stringify({ items: [{ kind: "Pod", metadata: { namespace: "relaycast", name: "proof-relaycast-aws-proof" } }] });
    const tagged = JSON.stringify({ ResourceTagMappingList: [{ ResourceARN: "arn:aws:s3:::96bulls-aws-proof-locker-500268966557" }] });
    expect(teardownResidue({ clusterGone: false, k8sJson: k8s, namespaceOut: "namespace/96bulls", taggedJson: tagged })).toEqual({
      inCluster: ["k8s Pod relaycast/proof-relaycast-aws-proof", "k8s namespace/96bulls still exists"],
      tagged: ["aws arn:aws:s3:::96bulls-aws-proof-locker-500268966557"],
    });
    expect(teardownResidue({ clusterGone: true, k8sJson: "{}", namespaceOut: "", taggedJson: JSON.stringify({ ResourceTagMappingList: [] }) })).toEqual({ inCluster: [], tagged: [] });
  });
  test("a failed cluster listing never counts as 'cluster gone'", () => {
    expect(clusterGone(false, "", "96bulls-aws-proof")).toBeUndefined();
    expect(clusterGone(true, '["96bulls-aws-proof"]', "96bulls-aws-proof")).toBe(false);
    expect(clusterGone(true, "[]", "96bulls-aws-proof")).toBe(true);
  });
});
