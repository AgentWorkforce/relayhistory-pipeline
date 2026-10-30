import { describe, expect, test } from "bun:test";
import { MigrationAssertionClient } from "../relayhistory-export-auth.ts";

const ok = (body: Record<string, unknown> = {}) =>
  new Response(
    JSON.stringify({ accessToken: "jwt", accessTokenExpiresAt: "2026-10-31T00:01:00Z", tokenType: "Bearer", ...body }),
    { status: 201 },
  );

function client(fetcher: typeof fetch, overrides = {}) {
  return new MigrationAssertionClient({
    baseUrl: "https://api.relayauth.dev",
    grantId: "mgr_abc",
    apiKey: async () => "key-secret",
    fetcher,
    ...overrides,
  });
}

describe("migration assertion client", () => {
  test("mints a fresh single-use assertion per upload with a new nonce", async () => {
    const calls: { url: string; init: RequestInit }[] = [];
    const fetcher = (async (url: string, init: RequestInit) => {
      calls.push({ url, init });
      return ok();
    }) as unknown as typeof fetch;
    const c = client(fetcher);
    const first = await c.mint("user_1", "ws_1");
    const second = await c.mint("user_1", "ws_1");
    expect(calls).toHaveLength(2); // never cached
    expect(first.nonce).not.toBe(second.nonce);
    expect(calls[0]!.url).toBe("https://api.relayauth.dev/v1/tokens/migration-assertion");
    expect(calls[0]!.init.headers).toMatchObject({ "x-api-key": "key-secret" });
    expect(JSON.parse(String(calls[0]!.init.body))).toEqual({
      grantId: "mgr_abc",
      sourceUserId: "user_1",
      workspaceId: "ws_1",
      nonce: first.nonce,
      expiresIn: 60,
    });
    expect(first.token).toBe("jwt");
  });

  test("refuses unsafe configuration", () => {
    const fetcher = (async () => ok()) as unknown as typeof fetch;
    expect(() => client(fetcher, { baseUrl: "http://relayauth.example" })).toThrow("https");
    expect(() => client(fetcher, { baseUrl: "https://u:p@relayauth.example" })).toThrow("credentials");
    expect(() => client(fetcher, { expiresInSeconds: 300 })).toThrow("10 to 60");
    expect(() => client(fetcher, { grantId: "bad grant" })).toThrow("grantId");
    expect(() => client(fetcher, { baseUrl: "http://127.0.0.1:8787" })).not.toThrow();
  });

  test("classifies refusals and rejects malformed responses", async () => {
    const status = (code: number) => (async () => new Response("{}", { status: code })) as unknown as typeof fetch;
    await expect(client(status(403)).mint("u", "w")).rejects.toMatchObject({ code: "relayauth_http_403", retryable: false });
    await expect(client(status(503)).mint("u", "w")).rejects.toMatchObject({ retryable: true });
    const bad = (async () => ok({ tokenType: "MAC" })) as unknown as typeof fetch;
    await expect(client(bad).mint("u", "w")).rejects.toThrow("invalid assertion");
    await expect(client(bad).mint("bad user", "w")).rejects.toThrow("malformed");
  });
});
