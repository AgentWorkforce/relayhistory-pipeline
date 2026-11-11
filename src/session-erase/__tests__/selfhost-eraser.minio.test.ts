/** Uses the disposable, loopback-only versioned MinIO fixture in pg-proof. Never a production store. */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { signRequest } from "../../../packages/object-store/src/sigv4.ts";
import { encodeKeyPath } from "../../../packages/object-store/src/crypto.ts";
import { createS3Eraser } from "../../migration/neon-s3-erasure/eraser.ts";
import { eraseDiscoveredSessionBytes } from "../selfhost-eraser.ts";

const endpoint = process.env.FRONTIER_TEST_S3_ENDPOINT;
const bucket = process.env.FRONTIER_TEST_S3_BUCKET;
const accessKeyId = process.env.FRONTIER_TEST_S3_ACCESS_KEY;
const secretAccessKey = process.env.FRONTIER_TEST_S3_SECRET_KEY;
const enabled = !!(endpoint && bucket && accessKeyId && secretAccessKey);
if (endpoint) {
  const u = new URL(endpoint);
  if (u.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname)
    || u.username || u.password || u.pathname !== "/" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}

describe.skipIf(!enabled)("automatic session bytes on versioned MinIO", () => {
  test("removes all versions and delete markers across dates, unindexed machines and generations", async () => {
    const root = `eraseproof${crypto.randomUUID().replaceAll("-", "")}`;
    const credentials = { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey! };
    const config = { endpoint: endpoint!, bucket: bucket!, region: "us-east-1", credentials };
    const key = { orgId: "org", workspaceId: "ws", source: "claude", sessionId: "session" };
    const base = `${root}/org/`;
    const doomed = [
      `${base}v1/ws=ws/m=not-indexed/h=claude/d=2025/01/01/s=session/raw/one`,
      `${base}v1/ws=ws/m=other-machine/h=claude/d=2026/11/11/s=session/manifest.json`,
      `${base}upload-generations/g=01234567-1234-4234-8234-0123456789ab/v1/ws=ws/m=not-indexed/h=claude/d=2026/11/11/s=session/publications/p/manifest.json`,
      `${base}live-sessions/v1/ws=ws/m=not-indexed/h=claude/s=session.json`,
    ];
    const neighbour = doomed[0]!.replace("s=session/", "s=session-other/");
    const s3 = async (method: string, suffix: string, body = "") => {
      const url = new URL(`${endpoint}/${bucket}/${suffix.startsWith("?") ? suffix : encodeKeyPath(suffix)}`);
      const headers = new Headers();
      await signRequest({ method, url, headers, payloadSha256: createHash("sha256").update(body).digest("hex"), region: config.region, credentials });
      return fetch(url, { method, headers, ...(body ? { body } : {}), signal: AbortSignal.timeout(10_000) });
    };
    const eraser = createS3Eraser({ ...config, fetch: (async (url, init) => {
      const parsed = new URL(String(url));
      // The eraser's transport cannot read or write transcript content, or issue marker-only deletes.
      expect((init?.method === "GET" && parsed.searchParams.has("versions"))
        || (init?.method === "DELETE" && parsed.searchParams.has("versionId"))).toBe(true);
      return fetch(url, init);
    }) as typeof fetch });
    try {
      for (const k of [...doomed, neighbour]) {
        expect((await s3("PUT", k, "synthetic one")).ok).toBe(true);
        expect((await s3("PUT", k, "synthetic two")).ok).toBe(true);
      }
      // A real delete marker hides an old version without removing its bytes.
      expect((await s3("DELETE", doomed[0]!)).ok).toBe(true);
      const result = await eraseDiscoveredSessionBytes(eraser, key, { keyRoot: root, bucket: bucket!, log: () => {} });
      expect(result).toMatchObject({ verified: true, transcriptObjects: 3, pointerObjects: 1, versionsDeleted: 9 });
      for (const k of doomed) expect((await s3("GET", k)).status).toBe(404);
      expect((await s3("GET", neighbour)).status).toBe(200);
      const inventory = await s3("GET", `?versions&prefix=${encodeURIComponent(base)}`);
      expect(inventory.ok).toBe(true);
      const xml = await inventory.text();
      for (const k of doomed) expect(xml).not.toContain(`<Key>${k}</Key>`);
      expect(xml).toContain(`<Key>${neighbour}</Key>`);
      expect((await eraseDiscoveredSessionBytes(eraser, key, { keyRoot: root, bucket: bucket!, log: () => {} })).versionsDeleted).toBe(0);
    } finally {
      // Only this run's unique synthetic prefix, including its neighbour control.
      const cleanup = createS3Eraser(config);
      await cleanup.eraseStep(`${root}/`, { scope: root, authorize: async () => {} });
    }
  }, 60_000);
});
