/** Short-lived (broker-minted) S3 credentials for neon-s3-sync and neon-s3-erasure. Synthetic values only. */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, symlinkSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  DEFAULT_MIN_REMAINING_MS, EXIT_CREDENTIALS_RENEW, ShortLivedCredentialError, expiringSoon, fileCredentialSource,
  minRemainingMs, parseCredentialDocument,
} from "../short-lived-credentials.ts";
import { CredentialRefusal, s3Credentials } from "../stdin-credentials.ts";

const T0 = Date.parse("2026-11-27T22:00:00Z");
const v1 = (over: Record<string, unknown> = {}) => ({
  Version: 1, AccessKeyId: "ASIASYNTHETIC0001", SecretAccessKey: "secret-synthetic-A", SessionToken: "token-synthetic-A",
  Expiration: "2026-11-27T22:45:00Z", ...over,
});
const code = (f: () => unknown) => { try { f(); } catch (e) { return (e as { code?: string; message?: string }).code ?? (e as Error).message; } return "no-throw"; };

describe("parseCredentialDocument", () => {
  test("credential_process v1: all five fields, with the expiry", () => {
    const c = parseCredentialDocument(v1(), "f");
    expect(c).toEqual({ accessKeyId: "ASIASYNTHETIC0001", secretAccessKey: "secret-synthetic-A", sessionToken: "token-synthetic-A", expiresAt: new Date("2026-11-27T22:45:00Z") });
  });
  test("v1 refuses a missing SessionToken or Expiration, another Version, an extra key, a non-UTC or bad time", () => {
    const { SessionToken: _t, ...noToken } = v1();
    const { Expiration: _e, ...noExp } = v1();
    expect(code(() => parseCredentialDocument(noToken, "f"))).toBe("short_lived_shape_invalid.f");
    expect(code(() => parseCredentialDocument(noExp, "f"))).toBe("short_lived_expiration_invalid.f");
    expect(code(() => parseCredentialDocument(v1({ Version: 2 }), "f"))).toBe("short_lived_version_unsupported.f");
    expect(code(() => parseCredentialDocument(v1({ Region: "x" }), "f"))).toBe("short_lived_shape_invalid.f");
    for (const bad of ["2026-11-27T22:45:00", "2026-11-27T22:45:00+01:00", "tomorrow", 1795800000, "2026-13-40T99:99:99Z"])
      expect(code(() => parseCredentialDocument(v1({ Expiration: bad }), "f"))).toBe("short_lived_expiration_invalid.f");
    expect(parseCredentialDocument(v1({ Expiration: "2026-11-27T22:45:00.123+00:00" }), "f").expiresAt).toEqual(new Date("2026-11-27T22:45:00.123Z"));
  });
  test("the static shape still parses, with or without an expiration; an extra key refuses", () => {
    expect(parseCredentialDocument({ accessKeyId: "a", secretAccessKey: "s" }, "f")).toEqual({ accessKeyId: "a", secretAccessKey: "s" });
    expect(parseCredentialDocument({ accessKeyId: "a", secretAccessKey: "s", sessionToken: "t", expiration: "2026-11-27T22:30:00Z" }, "f").expiresAt).toEqual(new Date("2026-11-27T22:30:00Z"));
    expect(code(() => parseCredentialDocument({ accessKeyId: "a", secretAccessKey: "s", region: "x" }, "f"))).toBe("short_lived_shape_invalid.f");
    expect(code(() => parseCredentialDocument({ accessKeyId: "", secretAccessKey: "s" }, "f"))).toBe("short_lived_shape_invalid.f");
    for (const bad of [null, [], "x", 1]) expect(code(() => parseCredentialDocument(bad, "f"))).toBe("short_lived_shape_invalid.f");
  });
  test("refusals never carry a value", () => {
    try { parseCredentialDocument(v1({ Expiration: "secret-synthetic-A" }), "f"); } catch (e) {
      expect(e).toBeInstanceOf(ShortLivedCredentialError);
      expect(String((e as Error).message)).not.toContain("secret-synthetic");
    }
  });
});

test("minRemainingMs: default 10 min, seconds bounded 120..3000", () => {
  expect(minRemainingMs(undefined)).toBe(DEFAULT_MIN_REMAINING_MS);
  expect(minRemainingMs("")).toBe(600_000);
  expect(minRemainingMs("120")).toBe(120_000);
  expect(minRemainingMs("3000")).toBe(3_000_000);
  for (const bad of ["119", "3001", "-5", "1e3", "10m", "abc"]) expect(code(() => minRemainingMs(bad))).toBe("short_lived_min_remaining_invalid");
  expect(EXIT_CREDENTIALS_RENEW).toBe(75);
});

test("expiringSoon: only with an expiry, strictly under the minimum", () => {
  const at = new Date(T0 + 600_000);
  expect(expiringSoon({ expiresAt: at }, 600_000, T0)).toBe(false);
  expect(expiringSoon({ expiresAt: at }, 600_000, T0 + 1)).toBe(true);
  expect(expiringSoon({}, 600_000, T0)).toBe(false);
});

describe("fileCredentialSource", () => {
  test("the provider re-reads the file, so a renewed mint (a repointed symlink) takes effect", async () => {
    const dir = mkdtempSync(join(tmpdir(), "slc-"));
    try {
      const a = join(dir, "req-a.json"), b = join(dir, "req-b.json"), cur = join(dir, "current.json");
      writeFileSync(a, JSON.stringify(v1()), { mode: 0o600 });
      writeFileSync(b, JSON.stringify(v1({ AccessKeyId: "ASIASYNTHETIC0002", SessionToken: "token-synthetic-B", Expiration: "2026-11-27T23:30:00Z" })), { mode: 0o600 });
      symlinkSync(a, cur);
      let now = T0;
      const src = fileCredentialSource(cur, { minMs: 600_000, now: () => now, cacheMs: 30_000 });
      expect((await src.provider()).accessKeyId).toBe("ASIASYNTHETIC0001");
      expect(src.expiring()).toBe(false);
      now = Date.parse("2026-11-27T22:36:00Z"); // 9 min left on A
      expect(src.expiring()).toBe(true);
      unlinkSync(cur); symlinkSync(b, cur); // the supervisor renews and switches the reference
      expect((await src.provider()).accessKeyId).toBe("ASIASYNTHETIC0002");
      expect(src.expiring()).toBe(false);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
  test("expiring() reads at most once per cacheMs; the provider always reads", async () => {
    let reads = 0, now = T0;
    const src = fileCredentialSource("/x", { minMs: 600_000, now: () => now, cacheMs: 30_000, read: () => (reads++, JSON.stringify(v1())) });
    src.expiring(); src.expiring(); now += 29_999; src.expiring();
    expect(reads).toBe(1);
    now += 1; src.expiring();
    expect(reads).toBe(2);
    await src.provider(); await src.provider();
    expect(reads).toBe(4);
  });
  test("a static key (no expiry, e.g. through a FIFO) is read exactly once and never expires", async () => {
    let reads = 0, now = T0;
    const src = fileCredentialSource("/fifo", { minMs: 600_000, now: () => now, read: () => (reads++, JSON.stringify({ accessKeyId: "a", secretAccessKey: "s" })) });
    expect((await src.provider()).accessKeyId).toBe("a");
    now += 86_400_000;
    expect(src.expiring()).toBe(false);
    await src.provider();
    expect(reads).toBe(1);
  });
  test("an unreadable or malformed file refuses by code", () => {
    expect(code(() => fileCredentialSource("/nope", { minMs: 1, read: () => { throw new Error("ENOENT secret-path"); } }).current())).toBe("short_lived_file_unreadable.S3X_CREDS_FILE");
    expect(code(() => fileCredentialSource("/x", { minMs: 1, read: () => "{not json" }).current())).toBe("short_lived_shape_invalid.S3X_CREDS_FILE");
  });
});

describe("s3Credentials (the erasure's stdin eraser key)", () => {
  test("accepts the broker's v1 object as is, with its expiry, and the static shape with an optional expiration", () => {
    expect(s3Credentials(v1(), "eraser").expiresAt).toEqual(new Date("2026-11-27T22:45:00Z"));
    expect(s3Credentials({ accessKeyId: "a", secretAccessKey: "s", sessionToken: "t" }, "eraser")).toEqual({ accessKeyId: "a", secretAccessKey: "s", sessionToken: "t" });
  });
  test("any malformed key still refuses with credentials_invalid.<name>", () => {
    for (const bad of [v1({ SessionToken: "" }), v1({ Extra: 1 }), { accessKeyId: "a" }, { accessKeyId: "a", secretAccessKey: "s", other: 1 }, null]) {
      let err: unknown;
      try { s3Credentials(bad, "eraser"); } catch (e) { err = e; }
      expect(err).toBeInstanceOf(CredentialRefusal);
      expect((err as Error).message).toBe("credentials_invalid.eraser");
    }
  });
});

describe("renewal contract (review of c1d5bf61, finding 2)", () => {
  test("a source that started as broker v1 refuses a static replacement (no expiry) and never freezes", async () => {
    let doc: unknown = v1(), reads = 0, now = T0;
    const src = fileCredentialSource("/cur", { minMs: 600_000, now: () => now, cacheMs: 30_000, read: () => (reads++, JSON.stringify(doc)) });
    expect((await src.provider()).expiresAt).toEqual(new Date("2026-11-27T22:45:00Z"));
    doc = { accessKeyId: "a", secretAccessKey: "s" }; // a downgrade: same path, static, no expiry
    let err: unknown; try { await src.provider(); } catch (e) { err = e; }
    expect((err as ShortLivedCredentialError).code).toBe("short_lived_downgrade_refused.S3X_CREDS_FILE");
    now += 30_000;
    expect(code(() => src.expiring())).toBe("short_lived_downgrade_refused.S3X_CREDS_FILE");
    const before = reads; now += 30_000; code(() => src.expiring());
    expect(reads).toBeGreaterThan(before); // still re-reading: not frozen
    doc = v1({ AccessKeyId: "ASIASYNTHETIC0003", Expiration: "2026-11-27T23:40:00Z" }); // a proper renewal recovers
    expect((await src.provider()).accessKeyId).toBe("ASIASYNTHETIC0003");
  });
  test("v1 refuses a static replacement that does carry an expiration (shape pinned too)", async () => {
    let doc: unknown = v1();
    const src = fileCredentialSource("/cur", { minMs: 1, read: () => JSON.stringify(doc) });
    await src.provider();
    doc = { accessKeyId: "a", secretAccessKey: "s", sessionToken: "t", expiration: "2026-11-27T23:00:00Z" };
    let err: unknown; try { await src.provider(); } catch (e) { err = e; }
    expect((err as ShortLivedCredentialError).code).toBe("short_lived_downgrade_refused.S3X_CREDS_FILE");
  });
  test("v1 -> v1 rotations keep working; an initially static source stays read-once", async () => {
    let doc: unknown = v1(), reads = 0;
    const src = fileCredentialSource("/cur", { minMs: 1, read: () => (reads++, JSON.stringify(doc)) });
    for (const id of ["ASIASYNTHETIC0010", "ASIASYNTHETIC0011", "ASIASYNTHETIC0012"]) { doc = v1({ AccessKeyId: id }); expect((await src.provider()).accessKeyId).toBe(id); }
    let sreads = 0;
    const st = fileCredentialSource("/fifo", { minMs: 1, read: () => (sreads++, JSON.stringify({ accessKeyId: "a", secretAccessKey: "s" })) });
    await st.provider(); await st.provider(); st.expiring();
    expect(sreads).toBe(1);
  });
});
