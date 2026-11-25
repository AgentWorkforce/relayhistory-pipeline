/**
 * The runner's data-plane versioning proof for LOCKER_PROVIDER=azure (blob-versioning.ts) against the in-memory
 * fake container: a version id is required, the probe is erased through the strict eraser and verified, soft delete
 * without permanent delete refuses, a 3xx is never followed, and transient failures throw by code.
 */
import { describe, expect, test } from "bun:test";
import { PROBE_LEFTOVER_AGE_MS, probeBlobVersioning, runnerProbePrefix } from "../blob-versioning.ts";
import { FAKE_ENDPOINT, FakeAzureBlob, type FakeAzureOptions } from "../../migration/__tests__/fake-azure-blob.ts";

const PREFIX = runnerProbePrefix("fxroot");

function probe(opts: FakeAzureOptions = {}) {
  const fake = new FakeAzureBlob(opts);
  const run = () => probeBlobVersioning({ account: fake.account, container: fake.container, endpoint: FAKE_ENDPOINT,
    accountKey: () => fake.accountKey, prefix: PREFIX, fetch: fake.fetch, id: () => "fx-probe-id" });
  return { fake, run };
}

describe("blob versioning probe", () => {
  test("the reserved prefix is a dot directory under the key root", () => {
    expect(PREFIX).toBe("fxroot/.runner-probe/session-analysis/");
    expect(runnerProbePrefix("/fxroot/")).toBe(PREFIX);
  });

  test("versioning on: Enabled, and the probe leaves no copy (put, listed, erased, verified; every request redirect manual)", async () => {
    const { fake, run } = probe();
    expect(await run()).toBe("Enabled");
    expect(fake.records).toEqual([]);
    expect(fake.requests.some((r) => r.method === "PUT" && r.headers.get("if-none-match") === "*")).toBe(true);
    expect(fake.requests.every((r) => r.redirect === "manual")).toBe(true);
  });

  test("no x-ms-version-id: not_versioned (and the probe blob is deleted, conditionally)", async () => {
    const { fake, run } = probe({ versioning: false });
    expect(await run()).toBe("not_versioned");
    expect(fake.records).toEqual([]);
    const del = fake.requests.find((r) => r.method === "DELETE");
    expect(del?.headers.get("if-match")).toBeTruthy();
  });

  test("a version id outside Azure's timestamp shape: unverified", async () => {
    const { run } = probe({ versionHeader: () => "not-a-version" });
    expect(await run()).toBe("unverified");
  });

  test("a version id the inventory never lists (a store that answers the header but keeps no versions): unverified", async () => {
    const { fake, run } = probe({ versioning: false, versionHeader: () => "2026-11-23T10:00:00.1234567Z" });
    expect(await run()).toBe("unverified");
    void fake;
  });

  test("soft delete with permanent delete allowed: Enabled, nothing retained", async () => {
    const { fake, run } = probe({ softDelete: true, permanentDelete: true });
    expect(await run()).toBe("Enabled");
    expect(fake.records).toEqual([]);
  });

  test("soft delete without permanent delete: soft_delete_retained (erasure could never complete)", async () => {
    const { run } = probe({ softDelete: true, permanentDelete: false });
    expect(await run()).toBe("soft_delete_retained");
  });

  test("stale leftovers of an interrupted probe are erased first; a fresh probe in flight (another runner) is never touched", async () => {
    const { fake, run } = probe();
    fake.put(`${PREFIX}fx-earlier/probe`); fake.put(`${PREFIX}fx-earlier/probe`);
    for (const r of fake.records) r.modified = Date.now() - PROBE_LEFTOVER_AGE_MS - 60_000;
    fake.put(`${PREFIX}fx-other-runner/probe`);
    expect(await run()).toBe("Enabled");
    expect(fake.records.map((r) => r.key)).toEqual([`${PREFIX}fx-other-runner/probe`]);
  });

  test("two runners probing at once: both pass and neither deletes the other's probe version", async () => {
    const fake = new FakeAzureBlob();
    let n = 0;
    const deletedOther: string[] = [];
    fake.hook = (req) => {
      if (req.method === "DELETE") {
        const mine = req.url.pathname.includes("/fx-probe-a/") ? "a" : req.url.pathname.includes("/fx-probe-b/") ? "b" : "?";
        deletedOther.push(mine);
      }
      return undefined;
    };
    const one = (id: string) => probeBlobVersioning({ account: fake.account, container: fake.container, endpoint: FAKE_ENDPOINT,
      accountKey: () => fake.accountKey, prefix: PREFIX, fetch: fake.fetch, id: () => { n++; return id; } });
    expect(await Promise.all([one("fx-probe-a"), one("fx-probe-b")])).toEqual(["Enabled", "Enabled"]);
    expect(n).toBe(2);
    expect(fake.records).toEqual([]);
    // Each probe's deletes stay inside its own directory (a's and b's counts are its own put's base + version).
    expect(deletedOther.filter((d) => d === "a")).toHaveLength(2);
    expect(deletedOther.filter((d) => d === "b")).toHaveLength(2);
    expect(deletedOther.filter((d) => d === "?")).toHaveLength(0);
  });

  test("a 404 (container missing) on the put or the listing: container_missing", async () => {
    for (const on of ["PUT", "GET"]) {
      const { fake, run } = probe();
      fake.hook = (req) => (req.method === on ? new Response("<Error><Code>ContainerNotFound</Code></Error>", { status: 404 }) : undefined);
      expect([on, await run()]).toEqual([on, "container_missing"]);
    }
  });

  test("soft delete with permanent delete forbidden (403): permanent_delete_forbidden, distinct from retention", async () => {
    const { run } = probe({ softDelete: true, permanentDelete: false, permanentDeleteRefusal: 403 });
    expect(await run()).toBe("permanent_delete_forbidden");
  });

  test("a 3xx on the put or the listing: redirect, and the Location is never requested", async () => {
    for (const on of ["PUT", "GET"]) {
      const { fake, run } = probe();
      fake.hook = (req) => (req.method === on ? new Response(null, { status: 302, headers: { location: "https://fx-elsewhere.invalid/" } }) : undefined);
      expect(await run()).toBe("redirect");
      expect(fake.requests.every((r) => r.url.hostname === "127.0.0.1")).toBe(true);
    }
  });

  test("a 403 (wrong key): auth_refused", async () => {
    const { fake } = probe();
    const verdict = await probeBlobVersioning({ account: fake.account, container: fake.container, endpoint: FAKE_ENDPOINT,
      accountKey: () => Buffer.from(new Uint8Array(64)).toString("base64"), prefix: PREFIX, fetch: fake.fetch });
    expect(verdict).toBe("auth_refused");
  });

  test("a 409 on the create-only put: unverified", async () => {
    const { fake, run } = probe();
    fake.hook = (req) => (req.method === "PUT" ? new Response(null, { status: 409 }) : undefined);
    expect(await run()).toBe("unverified");
  });

  test("a 5xx or a network failure throws blob_versioning_unavailable (transient)", async () => {
    const { fake, run } = probe();
    fake.hook = (req) => (req.method === "PUT" ? new Response(null, { status: 503 }) : undefined);
    await expect(run()).rejects.toMatchObject({ code: "blob_versioning_unavailable" });
    const down = probeBlobVersioning({ account: "fxacct", container: "fx-container", endpoint: FAKE_ENDPOINT, accountKey: () => "AAAA", prefix: PREFIX,
      fetch: (async () => { throw new TypeError("refused"); }) as unknown as typeof fetch });
    await expect(down).rejects.toMatchObject({ code: "blob_versioning_unavailable" });
  });

  test("a prefix outside the reserved probe directory is refused before any request", async () => {
    const { fake } = probe();
    const verdict = await probeBlobVersioning({ account: fake.account, container: fake.container, endpoint: FAKE_ENDPOINT,
      accountKey: () => fake.accountKey, prefix: "fxroot/fx-org/", fetch: fake.fetch });
    expect(verdict).toBe("unverified");
    expect(fake.requests).toEqual([]);
  });
});
