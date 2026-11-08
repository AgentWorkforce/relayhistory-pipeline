import { describe, expect, test } from "bun:test";
import { createAmpTransportRoutes } from "../routes.ts";
import { AMP_CLI_VERSION } from "../provider.ts";
import { failure, type AmpResult } from "../process.ts";
const token = "fixture-internal-transport-secret-only";
const key = "sgamp_fixture_only_not_a_real_token";
const threadId = "T-fixture-thread";
const path = "/v1/internal/amp/operation";
const body = (operation: unknown) => ({
  v: 1,
  cliVersion: AMP_CLI_VERSION,
  key,
  operation,
});
const request = (data: unknown, auth: string | undefined = token) => ({
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    ...(auth ? { Authorization: `Bearer ${auth}` } : {}),
  },
  body: JSON.stringify(data),
});
const success = (): AmpResult => ({
  v: 1,
  cliVersion: AMP_CLI_VERSION,
  ok: true,
  value: { accepted: true },
});
describe("persistent Amp endpoint", () => {
  test("requires the dedicated platform credential before touching any provider key", async () => {
    let calls = 0;
    const app = createAmpTransportRoutes({
      token: () => token,
      run: async () => {
        calls++;
        return success();
      },
    });
    for (const auth of ["", "user-upload-jwt", "another-secret"])
      expect(
        (await app.request(path, request(body({ kind: "discover" }), auth)))
          .status,
      ).toBe(401);
    expect(calls).toBe(0);
    expect(
      (
        await createAmpTransportRoutes({ token: () => undefined }).request(
          path,
          request(body({ kind: "discover" })),
        )
      ).status,
    ).toBe(503);
  });
  test("reuses the host for different keys and operations, passing only fixed structured commands", async () => {
    const seen: unknown[] = [];
    const app = createAmpTransportRoutes({
      token: () => token,
      run: async (key, operation) => {
        seen.push({ key, operation });
        return success();
      },
    });
    for (const operation of [
      { kind: "discover" },
      { kind: "export", threadId },
      {
        kind: "inject",
        threadId: `https://ampcode.com/threads/${threadId}`,
        text: "literal $(text)",
      },
    ]) {
      const res = await app.request(path, request(body(operation)));
      expect(res.status).toBe(200);
      expect(res.headers.get("cache-control")).toBe("no-store");
      expect(await res.json()).toEqual(success());
    }
    expect(seen).toHaveLength(3);
    expect(seen[2]).toEqual({
      key,
      operation: { kind: "inject", threadId, text: "literal $(text)" },
    });
  });
  test("rejects arbitrary commands, extra options, mismatched contracts, malformed keys and oversized bodies", async () => {
    let calls = 0;
    const app = createAmpTransportRoutes({
      token: () => token,
      run: async () => {
        calls++;
        return success();
      },
    });
    const invalid = [
      body({ kind: "exec", command: "uname" }),
      body({ kind: "discover", command: "uname" }),
      body({ kind: "inject", threadId, text: "" }),
      body({ kind: "inject", threadId, text: "a\0b" }),
      body({ kind: "export", threadId: "T-bad; echo no" }),
      { ...body({ kind: "discover" }), key: "no" },
      { ...body({ kind: "discover" }), cliVersion: "old" },
    ];
    for (const data of invalid)
      expect((await app.request(path, request(data))).status).toBe(400);
    expect(
      (
        await app.request(
          path,
          request(
            body({ kind: "inject", threadId, text: "x".repeat(100 * 1024) }),
          ),
        )
      ).status,
    ).toBe(413);
    expect(calls).toBe(0);
  });
  test("bounds concurrent processes, rejects before executing when full, and frees the slot on failure", async () => {
    const releases: Array<() => void> = [];
    let calls = 0;
    const app = createAmpTransportRoutes({
      token: () => token,
      run: async () => {
        calls++;
        await new Promise<void>((r) => releases.push(r));
        throw new Error(key);
      },
    });
    const pending = Array.from({ length: 4 }, () =>
      app.request(path, request(body({ kind: "discover" }))),
    );
    while (releases.length !== 4) await new Promise((r) => setTimeout(r, 1));
    const busy = await app.request(
      path,
      request(body({ kind: "inject", threadId, text: "hello" })),
    );
    expect(busy.status).toBe(429);
    expect(await busy.json()).toEqual(
      failure("rejected", "amp_transport_busy"),
    );
    expect(calls).toBe(4);
    releases.forEach((r) => r());
    const results = await Promise.all(pending);
    for (const res of results) expect(await res.text()).not.toContain(key);
    const again = app.request(
      path,
      request(body({ kind: "inject", threadId, text: "hello" })),
    );
    while (releases.length !== 5) await new Promise((r) => setTimeout(r, 1));
    releases[4]!();
    expect(await (await again).json()).toEqual(
      failure("unknown", "amp_process_failed"),
    );
  });
});
