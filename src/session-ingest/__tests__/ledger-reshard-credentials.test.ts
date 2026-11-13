import { describe, expect, test } from "bun:test";
import { reshardCredentials } from "../../../scripts/ledger-reshard-revisions.ts";

const TRUSTED = "http://169.254.170.23/v1/credentials";
const body = { AccessKeyId: "AKIAPOD", SecretAccessKey: "pod-secret", Token: "pod-token", Expiration: "2099-01-01T00:00:00Z" };

function fakeFetch(calls: string[], status = 200): typeof fetch {
  return (async (url: string | URL | Request, init?: RequestInit) => {
    calls.push(`${String(url)}|${(init?.headers as Record<string, string> | undefined)?.authorization ?? ""}`);
    return new Response(JSON.stringify(body), { status });
  }) as typeof fetch;
}

describe("reshard credentials", () => {
  test("podIdentity returns a provider the store calls, fetched from the trusted endpoint with the token", async () => {
    const calls: string[] = [];
    const creds = await reshardCredentials("podIdentity", { AWS_CONTAINER_CREDENTIALS_FULL_URI: TRUSTED, AWS_CONTAINER_AUTHORIZATION_TOKEN: "tok" }, fakeFetch(calls));
    expect(typeof creds).toBe("function");
    expect(calls).toEqual([`${TRUSTED}|tok`]);
    const again = await (creds as () => Promise<{ accessKeyId: string; sessionToken?: string }>)();
    expect(again.accessKeyId).toBe("AKIAPOD");
    expect(again.sessionToken).toBe("pod-token");
    expect(calls.length).toBe(2);
  });

  test("podIdentity ignores static AWS_* keys", async () => {
    const calls: string[] = [];
    const creds = await reshardCredentials("podIdentity",
      { AWS_CONTAINER_CREDENTIALS_FULL_URI: TRUSTED, AWS_ACCESS_KEY_ID: "STATIC", AWS_SECRET_ACCESS_KEY: "s" }, fakeFetch(calls));
    expect(typeof creds).toBe("function");
  });

  test("podIdentity refuses an untrusted endpoint before sending anything", async () => {
    const calls: string[] = [];
    await expect(reshardCredentials("podIdentity", { AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://evil.example/creds", AWS_CONTAINER_AUTHORIZATION_TOKEN: "tok" }, fakeFetch(calls)))
      .rejects.toThrow("credentials_endpoint_untrusted");
    expect(calls).toEqual([]);
  });

  test("podIdentity refuses a missing endpoint and maps a failing one to a fixed code", async () => {
    await expect(reshardCredentials("podIdentity", {}, fakeFetch([]))).rejects.toThrow("credentials_missing");
    await expect(reshardCredentials("podIdentity", { AWS_CONTAINER_CREDENTIALS_FULL_URI: TRUSTED }, fakeFetch([], 500)))
      .rejects.toThrow("credentials_unavailable");
  });

  test("keys mode is unchanged: static AWS_* env, session token optional, never fetches", async () => {
    const calls: string[] = [];
    expect(await reshardCredentials("keys", { AWS_ACCESS_KEY_ID: "AK", AWS_SECRET_ACCESS_KEY: "SK" }, fakeFetch(calls)))
      .toEqual({ accessKeyId: "AK", secretAccessKey: "SK" });
    expect(await reshardCredentials("keys", { AWS_ACCESS_KEY_ID: "AK", AWS_SECRET_ACCESS_KEY: "SK", AWS_SESSION_TOKEN: "ST", AWS_CONTAINER_CREDENTIALS_FULL_URI: TRUSTED }, fakeFetch(calls)))
      .toEqual({ accessKeyId: "AK", secretAccessKey: "SK", sessionToken: "ST" });
    await expect(reshardCredentials("keys", {}, fakeFetch(calls))).rejects.toThrow("credentials_missing");
    expect(calls).toEqual([]);
  });
});
