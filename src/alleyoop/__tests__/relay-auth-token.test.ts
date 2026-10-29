import { describe, expect, test } from "bun:test";
import { RelayAuthAssertionProvider, secretCommand } from "../relay-auth-token.ts";

describe("RelayAuth assertion provider", () => {
  test("mints an exact-origin assertion and caches it only until the refresh window", async () => {
    let now = Date.parse("2026-10-29T12:00:00Z");
    const requests: Array<{ url: string; init: RequestInit }> = [];
    let keyReads = 0;
    const provider = new RelayAuthAssertionProvider({
      baseUrl: "https://relayauth.example",
      historyOrigin: "https://sessions-dev.agentrelay.com",
      orgId: "org_kind",
      workspaceId: "ws_kind",
      sponsorId: "user_kind",
      apiKey: async () => {
        keyReads += 1;
        return "rak_test_only_not_a_real_key";
      },
      now: () => now,
      fetcher: async (url, init) => {
        requests.push({ url: String(url), init: init! });
        const sequence = requests.length;
        return Response.json({
          accessToken: `assertion-${sequence}`,
          accessTokenExpiresAt: new Date(now + 60_000).toISOString(),
          tokenType: "Bearer",
        }, { status: 201 });
      },
    });

    expect(await provider.get()).toBe("assertion-1");
    expect(await provider.get()).toBe("assertion-1");
    expect(keyReads).toBe(1);
    expect(requests).toHaveLength(1);
    expect(requests[0]!.url).toBe("https://relayauth.example/v1/tokens/relayhistory-assertion");
    expect(new Headers(requests[0]!.init.headers).get("x-api-key")).toBe("rak_test_only_not_a_real_key");
    expect(JSON.parse(String(requests[0]!.init.body))).toEqual({
      orgId: "org_kind",
      workspaceId: "ws_kind",
      sponsorId: "user_kind",
      scopes: ["rth:sync", "rth:read"],
      expiresIn: 60,
      historyOrigin: "https://sessions-dev.agentrelay.com",
    });

    now += 46_000;
    expect(await provider.get()).toBe("assertion-2");
    expect(keyReads).toBe(2);
  });

  test("coalesces concurrent mints and invalidates after a 401", async () => {
    let mints = 0;
    const provider = new RelayAuthAssertionProvider({
      baseUrl: "https://relayauth.example/",
      historyOrigin: "https://history.example",
      orgId: "org",
      workspaceId: "workspace",
      sponsorId: "user",
      apiKey: async () => "rak_test_only_not_a_real_key",
      fetcher: async () => {
        mints += 1;
        await Promise.resolve();
        return Response.json({
          accessToken: `assertion-${mints}`,
          accessTokenExpiresAt: new Date(Date.now() + 60_000).toISOString(),
          tokenType: "Bearer",
        }, { status: 201 });
      },
    });

    expect(await Promise.all([provider.get(), provider.get()])).toEqual(["assertion-1", "assertion-1"]);
    expect(mints).toBe(1);
    expect(provider.invalidate()).toBe(true);
    expect(await provider.get()).toBe("assertion-2");
  });

  test("does not expose a refusal body or the API key", async () => {
    const provider = new RelayAuthAssertionProvider({
      baseUrl: "https://relayauth.example",
      historyOrigin: "https://history.example",
      orgId: "org",
      workspaceId: "workspace",
      sponsorId: "user",
      apiKey: async () => "rak_super_secret",
      fetcher: async () => new Response("server leaked rak_super_secret", { status: 403 }),
    });

    await expect(provider.get()).rejects.toMatchObject({ code: "relayauth_http_403", retryable: false });
    await expect(provider.get()).rejects.not.toThrow(/rak_super_secret|server leaked/);
  });
});

describe("RelayAuth API key command", () => {
  test("uses a JSON argv array without shell parsing", async () => {
    const read = secretCommand(JSON.stringify([process.execPath, "-e", "process.stdout.write('rak_from_command\\n')"]));
    expect(await read()).toBe("rak_from_command");
  });

  test("rejects shell strings and secret-shaped multiline output", async () => {
    expect(() => secretCommand("op read secret")).toThrow(/JSON argv array/);
    const read = secretCommand(JSON.stringify([process.execPath, "-e", "process.stdout.write('one two')"]));
    await expect(read()).rejects.toMatchObject({ code: "relayauth_api_key_source_failed" });
  });
});
