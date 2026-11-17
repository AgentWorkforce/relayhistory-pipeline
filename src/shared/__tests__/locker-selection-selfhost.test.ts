import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { vendorBindings } from "../env.ts";
import { buildLockerStore } from "../locker.ts";
import { assertLockerSelection, lockerProvider } from "../locker-selection.ts";

const ACCOUNT_KEY = Buffer.from("synthetic-azure-account-key").toString("base64");

function withRoot<T>(work: (root: string) => T): T {
  const root = mkdtempSync(join(tmpdir(), "locker-fs-"));
  try { return work(root); } finally { rmSync(root, { recursive: true, force: true }); }
}

describe("LOCKER_PROVIDER fs and azure (self-host, only when named)", () => {
  test("unset, blank and s3 stay S3; gcs stays GCS", () => {
    for (const p of [undefined, "", " ", "s3"]) expect(lockerProvider(p === undefined ? {} : { LOCKER_PROVIDER: p })).toBe("s3");
    expect(lockerProvider({ LOCKER_PROVIDER: "gcs" })).toBe("gcs");
    const s3 = buildLockerStore({ LOCKER_BUCKET: "locker", LOCKER_ACCESS_KEY_ID: "a", LOCKER_SECRET_ACCESS_KEY: "b" });
    expect([s3.provider, s3.store.backend]).toEqual(["s3", "s3"]);
    const gcs = buildLockerStore({ LOCKER_PROVIDER: "gcs", LOCKER_BUCKET: "locker", LOCKER_ENDPOINT: "http://127.0.0.1:4443" });
    expect([gcs.provider, gcs.store.backend]).toEqual(["gcs", "gcs"]);
    // Existing refusals keep their messages.
    expect(() => assertLockerSelection({ LOCKER_PROVIDER: "gcs" })).toThrow("96bulls: LOCKER_PROVIDER=gcs requires LOCKER_BUCKET");
    expect(() => buildLockerStore({ LOCKER_BUCKET: "locker" })).toThrow("no Locker credentials");
  });

  test("an unknown provider is refused without echoing the value", () => {
    let message = "";
    try { lockerProvider({ LOCKER_PROVIDER: "minio-secret" }); } catch (e) { message = (e as Error).message; }
    expect(message).toBe("96bulls: LOCKER_PROVIDER must be s3 or gcs (or fs or azure on self-host)");
  });

  const fsEnv = (over: Record<string, string | undefined>) => ({
    LOCKER_PROVIDER: "fs", LOCKER_BUCKET: "local", RELAYHISTORY_SELFHOST_INGEST: "true", ...over,
  });

  test("fs needs an absolute LOCKER_FS_ROOT", () => {
    expect(() => buildLockerStore(fsEnv({}))).toThrow("LOCKER_PROVIDER=fs requires LOCKER_FS_ROOT");
    expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: "  " }))).toThrow("requires LOCKER_FS_ROOT");
    expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: "data/locker" }))).toThrow("LOCKER_FS_ROOT must be an absolute path");
    expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: "./locker" }))).toThrow("must be an absolute path");
  });

  test("LOCKER_FS_ROOT must be normalized, free of .. and not the filesystem root; the message names only the variable", () => {
    for (const root of ["/", "/srv/../etc", "/srv/locker/..", "/srv//locker", "/srv/./locker", "/srv/locker/."]) {
      let message = "";
      try { assertLockerSelection(fsEnv({ LOCKER_FS_ROOT: root })); } catch (e) { message = (e as Error).message; }
      expect([root, message]).toEqual([root, "96bulls: LOCKER_FS_ROOT must be an absolute path (normalized, not /)"]);
    }
    for (const root of ["/srv/locker", "/srv/locker/", "/var/lib/relayhistory"]) expect(() => assertLockerSelection(fsEnv({ LOCKER_FS_ROOT: root }))).not.toThrow();
  });

  test("fs is self-host only and refused on Lambda", () => {
    withRoot((root) => {
      expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: root, RELAYHISTORY_SELFHOST_INGEST: undefined }))).toThrow("LOCKER_PROVIDER=fs is self-host only");
      expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: root, RELAYHISTORY_SELFHOST_INGEST: "1" }))).toThrow("self-host only");
      expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: root, AWS_LAMBDA_FUNCTION_NAME: "fn" }))).toThrow("refused on a hosted Lambda");
      expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: root, LOCKER_ACCESS_KEY_ID: "a" }))).toThrow("takes no S3 keys");
      expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: root, LOCKER_ENDPOINT: "http://127.0.0.1:9000" }))).toThrow("takes no LOCKER_ENDPOINT");
      expect(() => buildLockerStore(fsEnv({ LOCKER_FS_ROOT: root, LOCKER_BUCKET: undefined }))).toThrow("LOCKER_PROVIDER=fs requires LOCKER_BUCKET");
    });
  });

  test("fs with an absolute root and a self-host flag is selected and round-trips", async () => {
    await withRoot(async (root) => {
      for (const flag of ["RELAYHISTORY_SELFHOST_INGEST", "RELAYHISTORY_SELFHOST_READ_API", "RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER"]) {
        const raw = buildLockerStore({ LOCKER_PROVIDER: "fs", LOCKER_BUCKET: "local", LOCKER_FS_ROOT: root, [flag]: "true" });
        expect([raw.provider, raw.bucket, raw.store.backend]).toEqual(["fs", "local", "fs"]);
      }
      const { store } = buildLockerStore(fsEnv({ LOCKER_FS_ROOT: root }));
      const { objectKey, readAll } = await import("../vendor.ts");
      const key = objectKey("96bulls/probe.json");
      await store.put(key, new TextEncoder().encode("{}"), { ifNoneMatch: true, contentType: "application/json" });
      expect(new TextDecoder().decode(await readAll((await store.getObject(key)).body))).toBe("{}");
    });
  });

  const azureEnv = (over: Record<string, string | undefined>) => ({
    LOCKER_PROVIDER: "azure", LOCKER_BUCKET: "sessions", LOCKER_AZURE_ACCOUNT: "devstoreaccount1", LOCKER_AZURE_ACCOUNT_KEY: ACCOUNT_KEY, ...over,
  });

  test("azure needs its account, a base64 account key and the container", () => {
    const raw = buildLockerStore(azureEnv({ LOCKER_ENDPOINT: "http://127.0.0.1:10000/devstoreaccount1" }));
    expect([raw.provider, raw.bucket, raw.store.backend]).toEqual(["azure", "sessions", "azure"]);
    expect(buildLockerStore(azureEnv({})).provider).toBe("azure");
    expect(() => buildLockerStore(azureEnv({ LOCKER_AZURE_ACCOUNT: undefined }))).toThrow("requires LOCKER_AZURE_ACCOUNT");
    expect(() => buildLockerStore(azureEnv({ LOCKER_AZURE_ACCOUNT: "Bad.Account" }))).toThrow("requires LOCKER_AZURE_ACCOUNT");
    expect(() => buildLockerStore(azureEnv({ LOCKER_BUCKET: undefined }))).toThrow("LOCKER_PROVIDER=azure requires LOCKER_BUCKET");
    expect(() => buildLockerStore(azureEnv({ LOCKER_ACCESS_KEY_ID: "a", LOCKER_SECRET_ACCESS_KEY: "b" }))).toThrow("takes no S3 keys");
    expect(() => buildLockerStore(azureEnv({ LOCKER_ENDPOINT: "ftp://127.0.0.1" }))).toThrow("azure LOCKER_ENDPOINT must be an https URL");
    let message = "";
    try { buildLockerStore(azureEnv({ LOCKER_AZURE_ACCOUNT_KEY: "not base64 secret!" })); } catch (e) { message = (e as Error).message; }
    expect(message).toContain("requires LOCKER_AZURE_ACCOUNT_KEY");
    expect(message).not.toContain("secret!");
    expect(() => buildLockerStore(azureEnv({ LOCKER_AZURE_ACCOUNT_KEY: undefined }))).toThrow("requires LOCKER_AZURE_ACCOUNT_KEY");
  });

  const azureMessage = (over: Record<string, string | undefined>) => {
    try { assertLockerSelection(azureEnv(over)); } catch (e) { return (e as Error).message; }
    return "no_throw";
  };

  test("azure: LOCKER_BUCKET must be an Azure container name, never echoed", () => {
    for (const name of ["ab", "a".repeat(64), "Sessions", "-sessions", "sessions-", "ses--sions", "ses_sions", "ses.sions", " sessions"]) {
      expect([name, azureMessage({ LOCKER_BUCKET: name })]).toEqual([name, "96bulls: LOCKER_PROVIDER=azure requires LOCKER_BUCKET to be an Azure container name"]);
    }
    for (const name of ["abc", "a".repeat(63), "session-index-2", "0sessions9"]) expect([name, azureMessage({ LOCKER_BUCKET: name })]).toEqual([name, "no_throw"]);
  });

  test("azure: the account key must be padded base64 (length a multiple of 4), never echoed", () => {
    const refused = "96bulls: LOCKER_PROVIDER=azure requires LOCKER_AZURE_ACCOUNT_KEY (base64)";
    for (const key of ["abc", "abcde", "abcd=", "ab=c", "abc==", "a===", "secretkey!!!", "====", ACCOUNT_KEY.slice(0, -1)]) {
      const message = azureMessage({ LOCKER_AZURE_ACCOUNT_KEY: key });
      expect([key, message]).toEqual([key, refused]);
    }
    for (const key of ["abcd", "abc=", "ab==", ACCOUNT_KEY, Buffer.alloc(64, 7).toString("base64")]) expect([key, azureMessage({ LOCKER_AZURE_ACCOUNT_KEY: key })]).toEqual([key, "no_throw"]);
  });

  test("azure: LOCKER_ENDPOINT is https, or http to loopback only, with no userinfo, query or fragment", () => {
    const refused = "96bulls: azure LOCKER_ENDPOINT must be an https URL (http only to loopback) with no credentials, query or fragment";
    for (const endpoint of [
      "http://azurite:10000/devstoreaccount1", "http://10.0.0.5:10000", "http://169.254.169.254", "http://[::2]:10000",
      "http://128.0.0.1", "ftp://127.0.0.1", "not a url", "https://user:pw@example.blob.core.windows.net",
      "https://user@example.blob.core.windows.net", "http://u@127.0.0.1:10000", "https://example.blob.core.windows.net/?sig=secret",
      "https://example.blob.core.windows.net/?", "https://example.blob.core.windows.net/#frag", "http://127.0.0.1:10000/#",
    ]) {
      const message = azureMessage({ LOCKER_ENDPOINT: endpoint });
      expect([endpoint, message]).toEqual([endpoint, refused]);
    }
    for (const endpoint of [
      "https://example.blob.core.windows.net", "https://azurite.internal:10000/devstoreaccount1", "http://127.0.0.1:10000/devstoreaccount1",
      "http://127.8.9.10:10000", "http://localhost:10000/devstoreaccount1", "http://LOCALHOST:10000", "http://[::1]:10000/devstoreaccount1",
    ]) expect([endpoint, azureMessage({ LOCKER_ENDPOINT: endpoint })]).toEqual([endpoint, "no_throw"]);
  });

  test("the vendored pipeline routes refuse fs and azure instead of reading them as not configured", () => {
    withRoot((root) => {
      for (const env of [fsEnv({ LOCKER_FS_ROOT: root }), azureEnv({})]) {
        const saved = Object.fromEntries(Object.keys(env).map((k) => [k, process.env[k]]));
        Object.assign(process.env, env);
        try { expect(() => vendorBindings()).toThrow(`LOCKER_PROVIDER=${env.LOCKER_PROVIDER} is not available to the vendored pipeline routes`); }
        finally { for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; } }
      }
    });
  });
});
