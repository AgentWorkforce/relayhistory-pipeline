import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  chunkKeys,
  computeContentSha256,
  manifestKey,
  needsTransform,
  normalizedIsCurrent,
  parseManifest,
  pointerFields,
  quarantined,
  scrubbedForPointer,
  sha256HexForTest as sha256Hex,
  type ManifestV1,
} from "./helpers.js";

const loc = {
  workspaceId: "ws_7f3a2c",
  machineId: "mach-linuyc-desktop",
  harness: "claude",
  startDate: "2026-10-25",
  sessionId: "4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718",
};

async function chunk(seq: number, epoch: number, start: number, bytes: number) {
  const sha = await sha256Hex(`chunk-${seq}`);
  return {
    seq,
    epoch,
    start_offset: start,
    bytes,
    stored_bytes: Math.ceil(bytes / 5),
    sha256: sha,
    sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(`stored-${seq}`),
    compression: "zstd" as const,
    records: 10,
  };
}

/** The spec's example, with real digests so the hash chain verifies. */
async function exampleManifest(): Promise<ManifestV1> {
  const chunks = [
    await chunk(0, 0, 0, 4194304),
    await chunk(1, 0, 4194304, 4194304),
    await chunk(2, 0, 8388608, 1048576),
  ];
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
    revision: 3,
    created_at: "2026-10-25T18:02:11.204Z",
    updated_at: "2026-10-25T18:41:07.990Z",
    session: {
      workspace_id: loc.workspaceId,
      machine_id: loc.machineId,
      harness: loc.harness,
      session_id: loc.sessionId,
      started_at: "2026-10-25T18:01:58.000Z",
    },
    writer: {
      delivery_instance_id: "dest_objstore_01",
      plugin_version: "0.1.0",
    },
    raw: {
      owner: "dest_objstore_01",
      state: "open",
      updated_at: "2026-10-25T18:41:07.990Z",
      format: "jsonl",
      compression: "zstd",
      chunk_count: 3,
      raw_bytes: chunks.reduce((a, c) => a + c.bytes, 0),
      stored_bytes: chunks.reduce((a, c) => a + c.stored_bytes, 0),
      content_sha256: await computeContentSha256(chunks),
      next_offset: 9437184,
      epoch: 0,
      epochs: [
        {
          epoch: 0,
          opened_at: "2026-10-25T18:02:11.204Z",
          reason: "initial",
          first_seq: 0,
          opened_by: "dest_objstore_01",
        },
      ],
      chunks,
    },
  };
}

describe("manifest v1", () => {
  it("accepts a valid manifest and derives pointer fields and chunk keys", async () => {
    const m = await exampleManifest();
    const parsed = await parseManifest(JSON.stringify(m), manifestKey(loc));
    expect(pointerFields(parsed)).toMatchObject({
      manifestRevision: 3,
      rawBytes: 9437184,
      chunkCount: 3,
      rawEpoch: 0,
      contentSha256: m.raw.content_sha256,
    });
    const keys = chunkKeys(parsed).map((c) => c.key);
    expect(keys[1]).toBe(
      `v1/ws=ws_7f3a2c/m=mach-linuyc-desktop/h=claude/d=2026/10/25/s=4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718/raw/0000000001-4194304-${m.raw.chunks[1]!.sha256_8}.jsonl.zst`,
    );
  });

  it("an empty chunk list hashes to 64 zeros", async () => {
    expect(await computeContentSha256([])).toBe("0".repeat(64));
  });

  it("keeps unknown fields (forward compatibility)", async () => {
    const m = { ...(await exampleManifest()), future_field: 1 };
    await expect(parseManifest(m)).resolves.toMatchObject({ future_field: 1 });
  });

  const corruptions: [string, (m: ManifestV1) => void, RegExp][] = [
    ["chunk_count", (m) => (m.raw.chunk_count = 2), /raw\.chunk_count/],
    ["raw_bytes", (m) => (m.raw.raw_bytes += 1), /raw\.raw_bytes/],
    [
      "gap in offsets",
      (m) => (m.raw.chunks[2]!.start_offset += 1),
      /start_offset/,
    ],
    ["seq not dense", (m) => (m.raw.chunks[1]!.seq = 5), /seq/],
    ["sha256_8", (m) => (m.raw.chunks[0]!.sha256_8 = "00000000"), /sha256_8/],
    [
      "hash chain",
      (m) => (m.raw.content_sha256 = "f".repeat(64)),
      /content_sha256/,
    ],
    ["next_offset", (m) => (m.raw.next_offset = 1), /next_offset/],
    ["epoch decreases", (m) => (m.raw.chunks[0]!.epoch = 1), /epoch/],
    ["epoch counter without a record", (m) => (m.raw.epoch = 1), /raw\.epoch/],
    [
      "second initial epoch",
      (m) => m.raw.epochs.push({ ...m.raw.epochs[0]!, epoch: 1 }),
      /reason|raw\.epoch/,
    ],
  ];
  for (const [name, corrupt, pattern] of corruptions) {
    it(`rejects: ${name}`, async () => {
      const m = await exampleManifest();
      corrupt(m);
      await expect(parseManifest(m)).rejects.toThrow(pattern);
    });
  }

  it("accepts a new epoch restarting at offset 0", async () => {
    const m = await exampleManifest();
    const c3 = await chunk(3, 1, 0, 100);
    m.raw.chunks.push(c3);
    m.raw.chunk_count = 4;
    m.raw.raw_bytes += 100;
    m.raw.stored_bytes += c3.stored_bytes;
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-25T18:50:00.000Z",
      reason: "rewritten",
      first_seq: 3,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 100;
    m.raw.content_sha256 = await computeContentSha256(m.raw.chunks);
    const parsed = await parseManifest(m);
    expect(chunkKeys(parsed, { currentEpochOnly: true })).toHaveLength(1);
  });

  it("accepts a migration backfill epoch after live delivery", async () => {
    const m = await exampleManifest();
    const c3 = await chunk(3, 1, 0, 100);
    m.raw.chunks.push(c3);
    m.raw.chunk_count = 4;
    m.raw.raw_bytes += 100;
    m.raw.stored_bytes += c3.stored_bytes;
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-25T18:50:00.000Z",
      reason: "backfill",
      first_seq: 3,
      opened_by: "migration:mgr_export",
    });
    m.raw.next_offset = 100;
    m.raw.content_sha256 = await computeContentSha256(m.raw.chunks);
    await expect(parseManifest(m)).resolves.toMatchObject({
      raw: { epoch: 1, epochs: [{ reason: "initial" }, { reason: "backfill" }] },
    });
  });

  it("opens an epoch with no chunks yet (source emptied)", async () => {
    const m = await exampleManifest();
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-25T18:50:00.000Z",
      reason: "truncated",
      first_seq: 3,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 0;
    await expect(parseManifest(m)).resolves.toBeTruthy();
  });

  it("derives effective state and normalized freshness from both writers' sections", async () => {
    const m = await exampleManifest();
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      state: "active",
      lifecycle: "active",
      normalizedVersion: null,
      normalizedStale: false,
    });
    m.server = {
      updated_at: "2026-10-27T00:00:00.000Z",
      normalized: {
        version: 2,
        source_content_sha256: "0".repeat(64),
        source_epoch: 0,
        built_at: "2026-10-26T00:00:00.000Z",
      },
      lifecycle: "abandoned",
      lifecycle_at: "2026-11-08T00:00:00.000Z",
    };
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      state: "abandoned",
      normalizedVersion: 2,
      normalizedStale: true,
    });
    m.server.normalized!.source_content_sha256 = m.raw.content_sha256;
    m.raw.state = "closed";
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      state: "closed",
      lifecycle: "abandoned",
      normalizedStale: false,
    });
  });

  it("treats normalized output from the previous epoch as stale", async () => {
    const m = await exampleManifest();
    m.server = {
      updated_at: "2026-10-26T00:00:00.000Z",
      normalized: {
        version: 1,
        source_content_sha256: m.raw.content_sha256,
        source_epoch: 0,
        built_at: "2026-10-26T00:00:00.000Z",
      },
      lifecycle: "active",
      lifecycle_at: "2026-10-26T00:00:00.000Z",
    };
    expect(pointerFields(await parseManifest(m)).normalizedStale).toBe(false);
    // Open an empty epoch: same digest, newer epoch.
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-26T01:00:00.000Z",
      reason: "truncated",
      first_seq: 3,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 0;
    const parsed = await parseManifest(m);
    expect(normalizedIsCurrent(parsed)).toBe(false);
    expect(pointerFields(parsed).normalizedStale).toBe(true);
  });

  it("checks first_seq of an epoch that has no chunks yet", async () => {
    const m = await exampleManifest();
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-26T01:00:00.000Z",
      reason: "truncated",
      first_seq: 99,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 0;
    await expect(parseManifest(m)).rejects.toThrow(
      /raw\.epochs\[1\]\.first_seq/,
    );
    m.raw.epochs[1]!.first_seq = 3;
    await expect(parseManifest(m)).resolves.toBeTruthy();
  });

  it("validates session.repo and exposes it on the pointer row", async () => {
    const m = await exampleManifest();
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      gitRemoteUrl: null,
      gitRootCommit: null,
    });
    for (const good of [
      "intranet/team/repo",
      "[::1]/x/y",
      "gitlab.example.com/Group/Sub/Repo",
    ]) {
      await expect(
        parseManifest({
          ...m,
          session: { ...m.session, repo: { remote_url: good } },
        }),
      ).resolves.toBeTruthy();
    }
    m.session.repo = {
      remote_url: "github.com/agentworkforce/relayhistory",
      root_commit: "a".repeat(40),
    };
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      gitRemoteUrl: "github.com/agentworkforce/relayhistory",
      gitRootCommit: "a".repeat(40),
    });
    for (const bad of [
      { remote_url: "https://github.com/x/y" },
      { remote_url: "git@github.com:x/y" },
      { remote_url: "GitHub.com/x/y" },
      { remote_url: "github.com:22/x/y" },
      { remote_url: "github.com/" },
      { remote_url: "github.com/x/y?token=abc" },
      { remote_url: "github.com/x/y#frag" },
      { remote_url: "github.com/x:y" },
      { remote_url: "github.com/x/y/" },
      { remote_url: "github.com" },
      { remote_url: "./src" },
      { remote_url: "../src" },
      { root_commit: "ABC" },
      { root_commit: "a".repeat(41) },
    ]) {
      await expect(
        parseManifest({ ...m, session: { ...m.session, repo: bad } }),
      ).rejects.toThrow(/session\.repo/);
    }
  });

  it("accepts a Rust-produced manifest with its committed digest", async () => {
    // Built by relayhistory's Rust manifest helpers; the expected digest is
    // a literal so a change to the hash chain's byte layout cannot pass by
    // being recomputed with the same code.
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const m = await parseManifest(fixture.manifest, fixture.key);
    expect(m.raw.content_sha256).toBe(
      "2f99d12c09716e5f9198013ced2e0ba6392db02031520137ff5b6daf3c53fbb2",
    );
    expect(await computeContentSha256(m.raw.chunks)).toBe(
      "2f99d12c09716e5f9198013ced2e0ba6392db02031520137ff5b6daf3c53fbb2",
    );
    expect(pointerFields(m)).toMatchObject({
      chunkCount: 3,
      rawEpoch: 1,
      normalizedStale: false,
    });
  });

  it("accepts a Rust-produced scrubbed manifest", async () => {
    // relayhistory#551: record_scrub before apply_chunks.
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1-scrubbed.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const m = await parseManifest(fixture.manifest, fixture.key);
    expect(m.raw.content_sha256).toBe(
      "a97482a47131f0a1a196ee3fbfb1eeee7bef52f48d7c9c52a91772049da67e86",
    );
    expect(m.raw.scrub).toEqual({ ruleset: "relayhistory-scrub", version: 1 });
    expect(m.raw.chunks.map((c) => c.stored_raw_bytes)).toEqual([10, 7]);
    expect(pointerFields(m).scrubbed).toBe(true);
    expect(quarantined(m)).toBe(false);
    expect(needsTransform(m)).toBe(true);
  });

  it("quarantines unscrubbed manifests and never transforms them", async () => {
    const m = await parseManifest(await exampleManifest());
    expect(quarantined(m)).toBe(true);
    expect(needsTransform(m)).toBe(false);
    expect(pointerFields(m).scrubbed).toBe(false);
  });

  it("keeps a pointer row quarantined once it saw unscrubbed chunks", async () => {
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1-scrubbed.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const scrubbed = await parseManifest(fixture.manifest, fixture.key);
    const unscrubbed = await parseManifest(await exampleManifest());
    const prev = (
      scrubbed: boolean,
      chunkCount: number,
      scrubQuarantined = false,
    ) => ({ scrubbed, chunkCount, scrubQuarantined });
    const on = { scrubbed: true, scrubQuarantined: false };
    const stuck = { scrubbed: false, scrubQuarantined: true };
    expect(scrubbedForPointer(null, scrubbed)).toEqual(on);
    // A first revision that is unscrubbed with chunks is flagged at once.
    expect(scrubbedForPointer(null, unscrubbed)).toEqual(stuck);
    // Marker written with or before the first chunk: allowed.
    expect(scrubbedForPointer(prev(false, 0), scrubbed)).toEqual(on);
    // Chunks were seen without the marker: stays quarantined.
    expect(scrubbedForPointer(prev(false, 3), scrubbed)).toEqual(stuck);
    // A scrubbed row whose manifest loses the marker is quarantined again.
    expect(scrubbedForPointer(prev(true, 3), unscrubbed)).toEqual(stuck);
    // The flag survives a revision that reset the chunk count.
    expect(scrubbedForPointer(prev(false, 0, true), scrubbed)).toEqual(stuck);
    // A row that already had chunks when the column was added (false, flag unset).
    expect(scrubbedForPointer(prev(false, 4), scrubbed)).toEqual(stuck);
  });

  it("requires stored fields on every chunk once raw.scrub is set", async () => {
    const m = await exampleManifest();
    const scrub = { ruleset: "relayhistory-scrub", version: 1 };
    const stored = await Promise.all(
      m.raw.chunks.map(async (c) => ({
        ...c,
        stored_raw_bytes: c.bytes,
        raw_sha256: await sha256Hex(`scrubbed-${c.seq}`),
      })),
    );
    const ok = await parseManifest({
      ...m,
      raw: { ...m.raw, scrub, chunks: stored },
    });
    expect(pointerFields(ok).scrubbed).toBe(true);

    const missing = stored.map((c, i) =>
      i === 1 ? { ...c, raw_sha256: undefined } : c,
    );
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: missing } }),
    ).rejects.toThrow(/raw\.chunks\[1\]\.raw_sha256: is required/);
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: m.raw.chunks } }),
    ).rejects.toThrow(/raw\.chunks\[0\]\.stored_raw_bytes: is required/);
    for (const bad of [
      { ruleset: "", version: 1 },
      { ruleset: "relayhistory-scrub", version: 0 },
    ])
      await expect(
        parseManifest({ ...m, raw: { ...m.raw, scrub: bad, chunks: stored } }),
      ).rejects.toThrow(/raw\.scrub/);
    const zero = stored.map((c) => ({ ...c, stored_raw_bytes: 0 }));
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: zero } }),
    ).rejects.toThrow(/stored_raw_bytes/);
    const badHex = stored.map((c) => ({ ...c, raw_sha256: "ABC" }));
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: badHex } }),
    ).rejects.toThrow(/raw_sha256/);
  });

  it("rejects a manifest whose session does not match its key", async () => {
    const m = await exampleManifest();
    await expect(
      parseManifest(m, manifestKey({ ...loc, harness: "codex" })),
    ).rejects.toThrow(/session\.harness/);
  });

  it("refuses a newer schema_v instead of guessing", async () => {
    const m = { ...(await exampleManifest()), schema_v: 2 };
    await expect(parseManifest(m)).rejects.toMatchObject({
      code: "unsupported",
    });
  });

  it("rejects schema violations", async () => {
    const m = await exampleManifest();
    await expect(
      parseManifest({ ...m, trace_id: "not-a-uuid" }),
    ).rejects.toThrow(/trace_id/);
    await expect(parseManifest("{not json")).rejects.toMatchObject({
      code: "invalid_argument",
    });
  });
});
