import { describe, expect, test } from "bun:test";
import { requireServiceAccountActAs } from "../iam-preflight.ts";

const projectId = "agentrelay-96bulls-soc2-2610";
const email = `awf-gcp-dev-nodes@${projectId}.iam.gserviceaccount.com`;
const credential = { bearer: "ya29.test-boundary-token" };

describe("GKE node identity actAs preflight", () => {
  test("uses testIamPermissions and requires iam.serviceAccounts.actAs", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const proof = await requireServiceAccountActAs(projectId, email, credential, async (url, init) => {
      calls.push({ url: String(url), init });
      return Response.json({ permissions: ["iam.serviceAccounts.actAs"] });
    });
    expect(proof).toEqual({ proof: "iam.serviceAccounts.actAs", serviceAccountEmail: email });
    expect(calls[0]!.url).toBe(
      `https://iam.googleapis.com/v1/projects/${projectId}/serviceAccounts/${encodeURIComponent(email)}:testIamPermissions`,
    );
    expect(JSON.parse(String(calls[0]!.init?.body))).toEqual({ permissions: ["iam.serviceAccounts.actAs"] });
    expect(calls[0]!.url).not.toContain(credential.bearer);
    expect(String(calls[0]!.init?.body)).not.toContain(credential.bearer);
    expect(new Headers(calls[0]!.init?.headers).get("authorization")).toBe(`Bearer ${credential.bearer}`);
  });

  test("fails closed when the caller lacks serviceAccountUser/actAs", async () => {
    await expect(
      requireServiceAccountActAs(projectId, email, credential, async () => Response.json({ permissions: [] })),
    ).rejects.toThrow(/roles\/iam\.serviceAccountUser/);
    await expect(
      requireServiceAccountActAs(projectId, email, credential, async () => new Response("denied", { status: 403 })),
    ).rejects.toThrow(/could not verify/);
    let caught: Error | undefined;
    try {
      await requireServiceAccountActAs(projectId, email, credential, async () => {
        throw new Error(`transport leaked ${credential.bearer}`);
      });
    } catch (error) {
      caught = error as Error;
    }
    expect(caught?.message).toMatch(/could not verify/);
    expect(caught?.message).not.toContain(credential.bearer);
  });
});
