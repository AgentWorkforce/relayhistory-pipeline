import { describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { credentialSource, podIdentityCredentials } from "../locker.ts";

describe("locker credentials", () => {
  test("static keys win when both are set", () => {
    expect(credentialSource({ LOCKER_ACCESS_KEY_ID: "a", LOCKER_SECRET_ACCESS_KEY: "b", AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://x" })).toBe("static");
  });
  test("pod identity when no static keys", () => {
    expect(credentialSource({ AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.170.23/v1/credentials" })).toBe("pod-identity");
  });
  test("no credentials is an error, not a silent anonymous client", () => {
    expect(() => credentialSource({})).toThrow();
  });
  test("pod identity reads the token file and maps the response", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-"));
    writeFileSync(join(dir, "token"), "tok-123\n");
    let seenAuth: string | null = null;
    const fake = (async (_u: string | URL | Request, init?: RequestInit) => {
      seenAuth = new Headers(init?.headers).get("authorization");
      return new Response(JSON.stringify({ AccessKeyId: "AK", SecretAccessKey: "SK", Token: "ST", Expiration: "2030-01-01T00:00:00Z" }));
    }) as unknown as typeof fetch;
    const c = await podIdentityCredentials({ AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.170.23/v1/credentials", AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: join(dir, "token") }, fake);
    expect(seenAuth).toBe("tok-123");
    expect(c).toEqual({ accessKeyId: "AK", secretAccessKey: "SK", sessionToken: "ST", expiresAt: new Date("2030-01-01T00:00:00Z") });
  });
});
