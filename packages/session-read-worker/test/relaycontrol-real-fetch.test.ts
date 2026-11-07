/**
 * The relaycontrol introspector through workerd's REAL global fetch (no stub),
 * against the synthetic endpoint in test/global-setup.ts. A stubbed fetch
 * cannot see workerd's own RequestInit rules: it rejects `redirect: "error"`,
 * which once made every Worker introspection a 503. Synthetic ids and secret.
 */
import { env } from "cloudflare:workers";
import { describe, expect, test } from "vitest";
import {
  createRelaycontrolIntrospector,
  RelaycontrolUnavailableError,
} from "@rhc/auth/relaycontrol.ts";

const SECRET = "synthetic-relaycontrol-secret";
const AUD = "https://read.test.example";
const TOKEN = "sh_at_synthetic-real-fetch";
const server = new URL((env as unknown as { RELAYAUTH_JWKS_URL: string }).RELAYAUTH_JWKS_URL).origin;

function introspector(base: string, secret = SECRET) {
  return createRelaycontrolIntrospector({ url: `${server}${base}`, secret, log: () => {} });
}

async function unavailableReason(promise: Promise<unknown>) {
  try {
    await promise;
  } catch (error) {
    return error instanceof RelaycontrolUnavailableError ? error.reason : `other: ${String(error)}`;
  }
  return "admitted";
}

describe("relaycontrol introspection over workerd's real fetch", () => {
  test("admits a desktop-user token (the fetch init is accepted by workerd)", async () => {
    const auth = await introspector("/rc-ok").verifyRelaycontrolToken(TOKEN, { aud: AUD });
    expect(auth).toMatchObject({ userId: "u-real", orgId: "org-real", workspaceId: "ws-real", scopes: ["rth:read"] });
  });

  test("never follows a redirect: a 3xx is unavailable (503)", async () => {
    expect(await unavailableReason(introspector("/rc-redirect").verifyRelaycontrolToken(TOKEN, { aud: AUD }))).toBe("status_302");
  });

  test("a wrong service secret (relaycontrol 401) is unavailable, not a token refusal", async () => {
    expect(await unavailableReason(introspector("/rc-ok", "synthetic-wrong").verifyRelaycontrolToken(TOKEN, { aud: AUD }))).toBe("introspect_unauthorized");
  });

  test("an oversized response is refused unparsed", async () => {
    expect(await unavailableReason(introspector("/rc-large").verifyRelaycontrolToken(TOKEN, { aud: AUD }))).toBe("response_too_large");
  });
});
