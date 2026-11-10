import type { GetObject } from "../../object-store/src/types.ts";
import { expect, it } from "vitest";
import { decompress } from "fzstd";
import { canonicalHandle } from "../src/canonical.ts";
import {
  computeContentSha256,
  manifestKey,
  rawChunkKey,
  objectKey,
  type ObjectStore,
  type ChunkEntry,
  type ManifestV1,
} from "../../object-store/src/index.ts";
import { sha256Hex } from "../../object-store/src/crypto.ts";
import type {
  CanonicalDeps,
  CanonicalPointer,
} from "../../../src/session-read-api/canonical.ts";
interface Scenario {
  name: string;
  large?: boolean;
  manifestVersion: number;
  family?: string;
  chunkVersion?: number | string;
  ceiling?: string | number;
  status: number;
}
const scenarios: Scenario[] = [
  { name: "main-writer chunk8 retains legacy default", manifestVersion: 1, chunkVersion: "8", status: 200 },
  { name: "main-writer chunk8 satisfies explicit8", manifestVersion: 1, chunkVersion: "8", ceiling: "8", status: 200 },
  { name: "legacy small", manifestVersion: 1, status: 200 },
  { name: "legacy legally large replay", large: true, manifestVersion: 1, status: 200 },
  { name: "future manifest retains legacy default", manifestVersion: 10, chunkVersion: 10, status: 200 },
  { name: "version9 metadata-free legacy default", manifestVersion: 9, status: 200 },
  { name: "future manifest refused by explicit8", manifestVersion: 10, chunkVersion: 10, ceiling: "8", status: 503 },
  { name: "foreign family retains legacy default", family: "foreign-family", manifestVersion: 1, status: 200 },
  { name: "foreign family refused by explicit8", family: "foreign-family", manifestVersion: 1, ceiling: "8", status: 503 },
  { name: "future manifest with explicit reader capability", manifestVersion: 10, chunkVersion: 10, ceiling: "10", status: 200 },
  { name: "mixed-version chunks retain legacy default", manifestVersion: 1, chunkVersion: 10, status: 200 },
  { name: "mixed-version chunks refused by explicit8", manifestVersion: 1, chunkVersion: 10, ceiling: "8", status: 503 },
  { name: "mixed-version chunks with explicit reader capability", manifestVersion: 1, chunkVersion: 10, ceiling: "10", status: 200 },
  { name: "future manifest requires chunk metadata", manifestVersion: 10, ceiling: "10", status: 503 },
  { name: "chunk above configured ceiling", manifestVersion: 1, chunkVersion: 11, ceiling: "10", status: 503 },
  { name: "chunk version must be canonical decimal", manifestVersion: 1, chunkVersion: "1e1", ceiling: "10", status: 503 },
  { name: "number-typed Worker binding refuses", manifestVersion: 1, ceiling: 8, status: 503 },
  { name: "empty Worker binding refuses", manifestVersion: 1, ceiling: "", status: 503 },
  { name: "invalid Worker capability binding refuses", manifestVersion: 1, ceiling: " 10", status: 503 },
];
it.each(scenarios)(
  "canonical read in workerd: $name",
  async ({ large = false, manifestVersion, family = "relayhistory-scrub", chunkVersion, ceiling, status }) => {
    const at = "2026-11-01T10:00:00.000Z",
      location = {
        prefix: "96bulls/org_1",
        workspaceId: "ws_1",
        machineId: "srv_bb",
        harness: "claude",
        sessionId: "sess_1",
        startDate: "2026-11-01",
      };
    const stored = Uint8Array.from(
      atob(
        large
          ? "KLUv/YBY7AFAALwEAHJJHh5wN9UBEFFN2ciI4hdfstH3XdU/Avt8khTep/7/OwMDBBqd5v+PDlQgHI6TyTBAweglk9lyAAUrcRon/g/+9KcfxLlxjmFAR87kuBRTZRXR9xQmAnE9TZwzubmFq5zHnkP0OY1XtVXsc07j2FRR1dxCqNCZzNouvAQJADj/eBZpCTFVuK9mQpqt28RYZ0aYAxYu9QQCABB3AgAQdwIAEHcCABB3AgAQdwIAEHcCABB3AgAQdwIAEHcCABB3AgAQdwIAEHcCABB3AgAQdwIAEHfMBQByiyMfYEnbBgCdWXThp2ZTvGyIrNAgeBCNWkNyc6DEIFAQJ01B288gIiI6IIEAgsO5fCep2xWXPxRLLCjlZ26AiACIiZh4Cg4a5xgDCNEv31Q6dFlFKLO2gtAUq3J++cW2d0F8MEPoWmSZ6aJpse3WXntwhWGx+GED6eSXPzYporo1+Zm3pVpvt/rlNcH4Z54QDgCA/ZADcHcRTGAGYdqr6Z5XYtocTSdfxM1g4lBVLlaNt5sXBCnABgIAEHcCABB3AgAQdwIAEHcCABB3AgAQdwIAEHcCABB3AgAQdwIAEHcCABB3AgAQdwIAEHcCABB3AgAQd9UBACQDdyIsICJyb2xlIjogImFzc2lzdGFudCIsICJtZXNzYWdlX2lkIjogIm1zZy1yMiJ9fQoBALcqEAI="
          : "KLUv/WC4AIUFAMKKIR5Aa+IGOCuf1yrVJUa/qLgL4yjwKcgk8tKIBHPc6ZqLqUI2h0bjzVJzMqef40K75SpmaRtgUbG6wIB9ciZhBgQKEuWcExD3Gecr6aIhtZMf3F3AnSsG5RClIAjMmZ93QguPRIW7yE2h02LTn+dRdq0q3JMnYomQqydvhgcipsmM/PPFUnFGDwDAqQrpja9JyDADRrA3nJF0y02wb+ZAmeG0ikbzNpxDXmblg0kG",
      ),
      (c) => c.charCodeAt(0),
    );
    // Fixed compressor fixture; generate raw from its decoded canonical envelope.
    const raw = decompress(stored),
      sha = await sha256Hex(raw);
    const chunk: ChunkEntry = {
      seq: 0,
      epoch: 0,
      start_offset: 0,
      bytes: raw.byteLength,
      stored_bytes: stored.byteLength,
      sha256: sha,
      sha256_8: sha.slice(0, 8),
      stored_sha256: await sha256Hex(stored),
      stored_raw_bytes: raw.byteLength,
      raw_sha256: sha,
      compression: "zstd",
      records: 2,
    };
    const manifest = {
      schema_v: 1,
      layout: "v1",
      trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
      revision: 1,
      created_at: at,
      updated_at: at,
      session: {
        workspace_id: "ws_1",
        machine_id: "srv_bb",
        harness: "claude",
        session_id: "sess_1",
        started_at: at,
      },
      writer: { delivery_instance_id: "srv_bb", plugin_version: "test" },
      raw: {
        owner: "srv_bb",
        state: "open",
        updated_at: at,
        format: "ndjson",
        compression: "zstd",
        chunk_count: 1,
        raw_bytes: raw.byteLength,
        stored_bytes: stored.byteLength,
        content_sha256: await computeContentSha256([chunk]),
        epoch: 0,
        epochs: [
          {
            epoch: 0,
            opened_at: at,
            reason: "initial",
            first_seq: 0,
            opened_by: "srv_bb",
          },
        ],
        next_offset: raw.byteLength,
        chunks: [chunk],
        scrub: { ruleset: family, version: manifestVersion },
      },
      extensions: {
        "96bulls": { user_id: "user_1", canonical_session_id: "sess:1" },
      },
    } as ManifestV1;
    const objects = new Map<string, Uint8Array>([
      [String(rawChunkKey(location, 0, 0, sha, "ndjson")), stored],
      [
        String(objectKey(manifestKey(location))),
        new TextEncoder().encode(JSON.stringify(manifest)),
      ],
    ]);
    const body = (key: unknown) => {
      const value = objects.get(String(key));
      if (!value) throw new Error("unexpected object");
      return new Response(value).body!;
    };
    const store = {
      get: async (key: unknown) => body(key),
      getObject: async (key: unknown): Promise<GetObject> => ({
        body: body(key),
        meta: {
          key: objectKey(String(key)),
          size: objects.get(String(key))!.byteLength,
          lastModified: new Date(at),
          version: { eTag: "fixture" },
          metadata: String(key).endsWith(".zst") && chunkVersion !== undefined
            ? { "scrub-ruleset": "relayhistory-scrub", "scrub-version": String(chunkVersion) }
            : {},
        },
      }),
    } as unknown as ObjectStore;
    const pointer: CanonicalPointer = {
      org_id: "org_1",
      workspace_id: "ws_1",
      harness: "claude",
      session_id: "sess:1",
      machine_id: "srv_bb",
      user_id: "user_1",
      storage_provider: "s3",
      bucket: "bucket",
      manifest_key: manifestKey(location),
      content_sha256: manifest.raw.content_sha256,
      raw_format: "ndjson",
      revision: 1,
      deleted_at: null,
    };
    const deps: CanonicalDeps = {
      store,
      provider: "s3",
      bucket: "bucket",
      prefix: {
        PIPELINE_ORG_FROM_PREFIX: "true",
        PIPELINE_KEY_PREFIX: "96bulls",
      },
      pointers: async () => [pointer],
      decompress: (bytes, max) => decompress(bytes, new Uint8Array(max)),
    };
    const token = [
      btoa("{}"),
      btoa(
        JSON.stringify({
          role: "session_directory_search_worker",
          app_metadata: { org_id: "org_1", workspace_ids: ["ws_1"] },
        }),
      ),
      "fixture",
    ].join(".");
    const result = await canonicalHandle(
      new Request(
        "https://reader.invalid/v1/canonical/sessions/sess%3A1?workspace=ws_1&source=claude&limit=1" +
          (large ? "&view=replay" : ""),
        { headers: { authorization: "Bearer " + token } },
      ),
      { CANONICAL_READ_ENABLED: "true", ...(ceiling === undefined ? {} : { READER_MAX_RULESET: ceiling as string }) },
      fetch,
      deps,
    );
    expect(result.status).toBe(status);
    const payload = await result.json();
    if (status !== 200) {
      expect(payload).toMatchObject({ error: { code: "session_read_unavailable" } });
      expect(result.headers.get("X-Session-Read-Reason")).toBe((ceiling === " 10" || ceiling === "" || typeof ceiling === "number") ? "canonical_unavailable" : "canonical_projection_incomplete");
      expect(payload).not.toHaveProperty("rows");
      expect(JSON.stringify(payload)).not.toContain("worker transcript");
      return;
    }
    expect(payload).toMatchObject({
      sessionId: "sess:1",
      rows: [
        {
          table: "convergence_events",
          row: {
            content: large
              ? "w".repeat(2 * 1024 * 1024 + 1)
              : "worker transcript",
            session_id: "sess:1",
          },
        },
      ],
    });
  },
);
