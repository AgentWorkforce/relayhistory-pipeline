import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import {
  MANAGED_TITLE_PREFIX,
  alleyoopWriteCondition,
  ensureConditionalBucketBindings,
  reconcileConditionalBindings,
  workerWriteCondition,
  type BucketPolicy,
  type ConditionalBinding,
} from "../bucket-iam.ts";

const BUCKET = "awf-conformance-locker-agentrelay-96bulls-soc2-2610";
const WORKER = "serviceAccount:awf-conformance-worker@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com";
const ALLEYOOP = "serviceAccount:awf-conformance-alleyoop@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com";
const desired: ConditionalBinding[] = [
  { role: "roles/storage.objectUser", member: WORKER, condition: workerWriteCondition(BUCKET) },
  { role: "roles/storage.objectUser", member: ALLEYOOP, condition: alleyoopWriteCondition(BUCKET) },
];
const viewer = { role: "roles/storage.objectViewer", members: [WORKER, ALLEYOOP] };

describe("locker write conditions", () => {
  it("scope each component to its own object names under v1/", () => {
    const objects = `projects/_/buckets/${BUCKET}/objects/v1/`;
    expect(alleyoopWriteCondition(BUCKET).expression).toBe(
      `resource.name.startsWith("${objects}") && (resource.name.endsWith("/manifest.json") || resource.name.endsWith(".zst"))`,
    );
    expect(workerWriteCondition(BUCKET).expression).toBe(
      `resource.name.startsWith("${objects}") && (resource.name.endsWith("/summary/short.json.enc") || resource.name.endsWith("/summary/long.json.enc"))`,
    );
    expect(alleyoopWriteCondition(BUCKET).title.startsWith(MANAGED_TITLE_PREFIX)).toBe(true);
    expect(workerWriteCondition(BUCKET).title).not.toBe(alleyoopWriteCondition(BUCKET).title);
  });

  it("refuse a bucket name that could break out of the CEL string", () => {
    for (const bad of ['b") || true || ("', "UPPER", "a", "x/y"])
      expect(() => alleyoopWriteCondition(bad)).toThrow("malformed");
  });

  it("match the object keys the components really write", () => {
    // The exact key builders' shapes (packages/object-store/src/layout.ts, summary-objects.ts).
    const session = "v1/ws=default/m=box/h=claude/d=2026/11/01/s=0190";
    const writes = {
      alleyoop: [`${session}/manifest.json`, `${session}/parked/box/manifest.json`, `${session}/raw/0000000000-0-abcdef12.jsonl.zst`],
      worker: [`${session}/summary/short.json.enc`, `${session}/summary/long.json.enc`],
    };
    const suffixes = {
      alleyoop: ["/manifest.json", ".zst"],
      worker: ["/summary/short.json.enc", "/summary/long.json.enc"],
    };
    for (const [who, keys] of Object.entries(writes) as Array<[keyof typeof writes, string[]]>)
      for (const key of keys) {
        expect(suffixes[who].some((s) => key.endsWith(s))).toBe(true);
        const other = who === "worker" ? "alleyoop" : "worker";
        expect(suffixes[other].some((s) => key.endsWith(s))).toBe(false);
      }
  });
});

describe("reconcileConditionalBindings", () => {
  it("adds the managed bindings at policy version 3 and keeps unmanaged ones", () => {
    const owner = { role: "roles/storage.legacyBucketOwner", members: ["projectOwner:p"] };
    const { policy, changed } = reconcileConditionalBindings({ version: 1, etag: "e1", bindings: [owner, viewer] }, desired);
    expect(changed).toBe(true);
    expect(policy.version).toBe(3);
    expect(policy.etag).toBe("e1");
    expect(policy.bindings).toEqual([
      owner,
      viewer,
      { role: "roles/storage.objectUser", members: [WORKER], condition: workerWriteCondition(BUCKET) },
      { role: "roles/storage.objectUser", members: [ALLEYOOP], condition: alleyoopWriteCondition(BUCKET) },
    ]);
  });

  it("is a no-op once converged", () => {
    const first = reconcileConditionalBindings({ version: 3, bindings: [viewer] }, desired).policy;
    expect(reconcileConditionalBindings(first, desired).changed).toBe(false);
  });

  it("drops a stale managed expression and a member no longer desired, never an unmanaged binding", () => {
    const stale = {
      role: "roles/storage.objectUser",
      members: [ALLEYOOP, "serviceAccount:old-runtime-acct@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com"],
      condition: { title: `${MANAGED_TITLE_PREFIX}alleyoop`, expression: "true" },
    };
    const foreign = {
      role: "roles/storage.objectUser",
      members: [WORKER],
      condition: { title: "someone-elses", expression: "true" },
    };
    const { policy } = reconcileConditionalBindings({ version: 3, bindings: [stale, foreign] }, desired);
    expect(policy.bindings).toContainEqual(foreign);
    expect(JSON.stringify(policy.bindings)).not.toContain("old-runtime-acct");
    expect(policy.bindings!.filter((b) => b.condition?.expression === "true")).toEqual([foreign]);
  });

  it("strips a forbidden member from every binding, unconditional or not, and refuses a forbidden desired member", () => {
    const RUNTIME = "serviceAccount:awf-conformance-runtime@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com";
    const legacy = { role: "roles/storage.objectUser", members: [RUNTIME] };
    const shared = { role: "roles/storage.objectViewer", members: [RUNTIME, WORKER] };
    const conditioned = { role: "roles/storage.objectAdmin", members: [RUNTIME], condition: { title: "x", expression: "true" } };
    const { policy, changed } = reconcileConditionalBindings(
      { version: 3, bindings: [legacy, shared, conditioned] },
      desired,
      [RUNTIME],
    );
    expect(changed).toBe(true);
    expect(JSON.stringify(policy.bindings)).not.toContain("awf-conformance-runtime");
    expect(policy.bindings).toContainEqual({ role: "roles/storage.objectViewer", members: [WORKER] });
    expect(() => reconcileConditionalBindings({}, desired, [WORKER])).toThrow("also desired");
    expect(() => reconcileConditionalBindings({}, desired, ["allUsers"])).toThrow("malformed");
  });

  it("refuses anything outside the managed namespace, a non-service-account member, or a non-storage role", () => {
    const base = desired[0]!;
    expect(() => reconcileConditionalBindings({}, [{ ...base, condition: { ...base.condition, title: "x" } }])).toThrow("managed namespace");
    expect(() => reconcileConditionalBindings({}, [{ ...base, member: "allUsers" }])).toThrow("service account");
    expect(() => reconcileConditionalBindings({}, [{ ...base, role: "roles/owner" }])).toThrow("Cloud Storage role");
  });
});

describe("ensureConditionalBucketBindings", () => {
  const response = (status: number, body: unknown = {}) =>
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

  it("reads v3, writes under the read etag, and retries a concurrent write", async () => {
    const calls: Array<{ url: string; method: string; body?: BucketPolicy }> = [];
    let reads = 0;
    const transport = async (url: string, init: RequestInit) => {
      calls.push({ url, method: init.method!, body: init.body ? JSON.parse(String(init.body)) : undefined });
      expect((init.headers as Record<string, string>).authorization).toBe("Bearer tok");
      if (init.method === "GET") return response(200, { version: 1, etag: `e${++reads}`, bindings: [viewer] });
      return response(reads === 1 ? 412 : 200);
    };
    expect(await ensureConditionalBucketBindings(BUCKET, desired, { bearer: "tok" }, transport)).toEqual({ changed: true });
    expect(calls.map((c) => c.method)).toEqual(["GET", "PUT", "GET", "PUT"]);
    expect(calls[0]!.url).toBe(`https://storage.googleapis.com/storage/v1/b/${BUCKET}/iam?optionsRequestedPolicyVersion=3`);
    expect(calls[3]!.body!.etag).toBe("e2");
    expect(calls[3]!.body!.version).toBe(3);
  });

  it("does not write when already converged, and fails closed on a read or write error", async () => {
    const converged = reconcileConditionalBindings({ version: 3, etag: "e", bindings: [viewer] }, desired).policy;
    let writes = 0;
    expect(
      await ensureConditionalBucketBindings(BUCKET, desired, { bearer: "t" }, async (_u, init) =>
        init.method === "GET" ? response(200, converged) : (writes++, response(200)),
      ),
    ).toEqual({ changed: false });
    expect(writes).toBe(0);
    await expect(ensureConditionalBucketBindings(BUCKET, desired, { bearer: "t" }, async () => response(403))).rejects.toThrow("read failed with HTTP 403");
    await expect(
      ensureConditionalBucketBindings(BUCKET, desired, { bearer: "t" }, async (_u, init) =>
        init.method === "GET" ? response(200, {}) : response(400),
      ),
    ).rejects.toThrow("write failed with HTTP 400");
  });
});

describe("GCP stack identities", () => {
  const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");

  it("no longer grants the shared runtime identity the bucket or Pub/Sub", () => {
    expect(stack).not.toContain('"runtime-locker"');
    expect(stack).not.toMatch(/IAM\.Member\(`runtime-\$\{id\}`/);
    expect(stack).not.toContain("roles/storage.objectUser\"),\n      member: runtimeMember");
    expect(stack).toContain('GCP.IAM.ServiceAccount("worker-identity"');
    expect(stack).toContain('GCP.IAM.ServiceAccount("alleyoop-identity"');
    expect(stack).toContain('EnsureLockerWriteScopes("locker-write-scopes"');
    expect(stack).toContain("forbidden: [`serviceAccount:${runtimeEmail}`]");
  });
});
