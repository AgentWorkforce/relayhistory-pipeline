import { afterEach, describe, expect, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import { MANIFEST_EPOCHS_CAPABILITY, objectKey, type ManifestV1 } from "../../shared/vendor.ts";
import { MANIFEST_READERS } from "../manifests.ts";
import { envelopeRange, sourceResumeOffset } from "../courier.ts";
import { courierWorld } from "./courier-harness.ts";

type World = Awaited<ReturnType<typeof courierWorld>>;
const worlds: World[] = [];
const world = async (...args: Parameters<typeof courierWorld>) => {
  const w = await courierWorld(...args);
  worlds.push(w);
  return w;
};
afterEach(async () => {
  await Promise.all(worlds.splice(0).map((w) => w.close()));
});

const line = (n: number) => JSON.stringify({ type: "user", n, timestamp: new Date(1_761_730_000_000 + n).toISOString() });

/** Live chunks' record count: folding hides a double upload, this does not. */
const uploadedRecords = (m: ManifestV1) => {
  const backfill = m.raw.epochs.find((e) => e.reason === "backfill")?.epoch;
  return m.raw.chunks.filter((c) => c.epoch !== backfill).reduce((n, c) => n + (c.records ?? 0), 0);
};

const settle = async (w: World) => {
  for (let i = 0; i < 4; i += 1) await w.run();
};

describe("courier and exporter races (C1-C3)", () => {
  test("C1: concurrent exporter and courier passes land every line once", async () => {
    for (let round = 0; round < 5; round += 1) {
      const w = await world({ format: "ndjson" });
      await w.write(line(0));
      await w.run();
      const courier = (async () => {
        for (let n = 1; n <= 8; n += 1) {
          await w.write(line(n));
          await w.run();
        }
      })();
      const [, exported] = await Promise.all([courier, w.exporter(w.exportText("pre-t0"))]);
      expect(exported.status).toBe("committed");
      await settle(w);

      const { manifest, lines, records } = await w.liveRecords();
      expect(manifest.raw.owner).toBe(w.machineId);
      expect(manifest.raw.epochs.filter((e) => e.reason === "backfill")).toHaveLength(1);
      expect(records.some((r) => r.record_id === "pre-t0")).toBe(true);
      expect(lines.toSorted()).toEqual(Array.from({ length: 9 }, (_, n) => line(n)).toSorted());
      expect(uploadedRecords(manifest)).toBe(9);
      expect((await w.state()).sessions["claude:session-a"].needs_rebase).toBeFalsy();
    }
  });

  test("C1: an exporter commit in the courier's read-to-write window costs one retry", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.write(line(1));
    w.hooks.beforeManifest = async () => {
      w.hooks.beforeManifest = undefined;
      await w.exporter(w.exportText("pre-t0"));
    };
    await w.run();
    expect((await w.state()).sessions["claude:session-a"].needs_rebase).toBe(true);
    await w.run();
    await w.run();
    const { manifest, lines } = await w.liveRecords();
    expect(lines).toEqual([line(0), line(1)]);
    expect(uploadedRecords(manifest)).toBe(2);
    expect(manifest.raw.epochs.map((e) => e.reason)).toEqual(["initial", "backfill", "resumed"]);
  });

  test("C2: a lost response replayed after an exporter commit never uploads twice", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.write(line(1));
    // The write commits, its response is lost, and the exporter lands next.
    w.hooks.afterManifest = async () => {
      w.hooks.afterManifest = undefined;
      await w.exporter(w.exportText("pre-t0"));
      return Response.json({ error: { code: "storage_unavailable" } }, { status: 503 });
    };
    await w.run();
    await settle(w);
    const { manifest, lines } = await w.liveRecords();
    expect(lines).toEqual([line(0), line(1)]);
    expect(uploadedRecords(manifest)).toBe(2);
    expect(manifest.raw.epochs.map((e) => e.reason)).toEqual(["initial", "backfill"]);
  });

  test("C3: after a backfill the next write resumes the live offsets", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    const before = (await w.stored())!.manifest;
    await w.exporter(w.exportText("pre-t0"));
    await w.run();
    expect((await w.stored())!.manifest.raw.epoch).toBe(1);
    await w.write(line(1));
    await settle(w);

    const { manifest } = await w.liveRecords();
    const tail = manifest.raw.chunks.at(-1)!;
    expect(manifest.raw.epochs[2]).toMatchObject({ reason: "resumed", resumes_epoch: 0, opened_by: w.machineId });
    expect(tail.epoch).toBe(2);
    expect(tail.start_offset).toBe(before.raw.next_offset);
    expect(sourceResumeOffset(manifest)).toBe(Buffer.byteLength(`${line(0)}\n${line(1)}\n`));
  });

  test("a jsonl session's backfill is refused and the courier carries on", async () => {
    const w = await world({ format: "jsonl" });
    await w.write(line(0));
    await w.run();
    await expect(w.exporter(w.exportText("pre-t0"), 1)).rejects.toThrow(/ndjson/);
    await w.write(line(1));
    await settle(w);
    expect((await w.stored())!.manifest.raw).toMatchObject({ format: "jsonl", chunk_count: 2, epoch: 0 });
  });
});

describe("courier edits the stored manifest (C4-C7)", () => {
  async function serverWrites(w: World, edit: (m: ManifestV1 & Record<string, unknown>) => void) {
    const { manifest, version } = (await w.stored())!;
    const next = structuredClone(manifest) as ManifestV1 & Record<string, unknown>;
    edit(next);
    next.revision += 1;
    await w.store.put(objectKey(w.key), new TextEncoder().encode(JSON.stringify(next)), { ifMatch: version, contentType: "application/json" });
  }

  test("C4: the server section, session fields and unknown fields survive", async () => {
    const w = await world({ format: "jsonl" });
    await w.write(line(0));
    await w.run();
    const at = "2026-10-30T00:00:00.000Z";
    await serverWrites(w, (m) => {
      m.server = { updated_at: at, normalized: null, lifecycle: "idle", lifecycle_at: at };
      m.future = { keep: true };
      m.session.parent_session_id = "parent";
      m.extensions = { ...m.extensions, "server.note": 1 };
    });
    await w.write(line(1));
    await settle(w);
    const m = (await w.stored())!.manifest as ManifestV1 & Record<string, unknown>;
    expect(m.raw.chunk_count).toBe(2);
    expect(m.server?.lifecycle).toBe("idle");
    expect(m.future).toEqual({ keep: true });
    expect(m.session.parent_session_id).toBe("parent");
    expect(m.extensions?.["server.note"]).toBe(1);
  });

  test("C5: ndjson output is a stream the transform reads", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.write("not json at all");
    await w.run();
    await w.exporter(w.exportText("pre-t0"));
    const { records, lines } = await w.liveRecords();
    expect(lines.toSorted()).toEqual([line(0), "not json at all"].toSorted());
    expect(records.find((r) => r.record_id === "pre-t0")).toBeDefined();
    const live = records.filter((r) => r.kind === "harness_line");
    expect(new Set(live.map((r) => r.record_id)).size).toBe(2);
    expect(live.every((r) => r.origin_id === `machine:${w.machineId}`)).toBe(true);
  });

  test("C5: sealing the same range twice gives the same records", () => {
    const bytes = Buffer.from(`${line(0)}\n${line(1)}\n`);
    const ids = { machineId: "m", sessionId: "s", startedAt: "2026-10-30T00:00:00.000Z" };
    expect(envelopeRange({ start: 40, bytes }, ids).equals(envelopeRange({ start: 40, bytes }, ids))).toBe(true);
    expect(envelopeRange({ start: 40, bytes }, ids).equals(envelopeRange({ start: 41, bytes }, ids))).toBe(false);
  });

  test("C6: state from before this version rebases first, and never rebuilds epoch 0", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.exporter(w.exportText("pre-t0"));
    const state = await w.state();
    delete state.sessions["claude:session-a"].base;
    await writeFile(w.statePath, JSON.stringify(state));
    await w.write(line(1));
    const posts = w.counts.manifestPosts;
    await w.run();
    expect(w.counts.manifestPosts).toBe(posts + 1);
    const m = (await w.stored())!.manifest;
    expect(m.raw.epochs.map((e) => e.reason)).toEqual(["initial", "backfill", "resumed"]);
    expect(m.raw.chunk_count).toBe(3);
  });

  test("C7: a raw stream another writer owns stops the courier", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await serverWrites(w, (m) => {
      m.raw.owner = "another-instance";
      m.writer.delivery_instance_id = "another-instance";
    });
    await w.write(line(1));
    await w.run();
    await w.run();
    const { chunkPosts, manifestPosts } = w.counts;
    await w.write(line(2));
    await w.run();
    await w.run();
    expect(w.counts).toEqual({ chunkPosts, manifestPosts });
    expect((await w.state()).sessions["claude:session-a"].fenced).toBe(true);
    expect((await w.stored())!.manifest.raw.owner).toBe("another-instance");
  });
});

describe("activation gate (G1) and old couriers (D1)", () => {
  const refused = { code: "activation_required" };

  test("G1: refuses while any reader lacks the capability, and when unconfigured", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    const all = w.env.MANIFEST_READER_CAPABILITIES!;
    for (const reader of MANIFEST_READERS) {
      w.env.MANIFEST_READER_CAPABILITIES = all.split(",").filter((p) => !p.startsWith(`${reader}=`)).join(",");
      await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(refused);
    }
    w.env.MANIFEST_READER_CAPABILITIES = all.replace(/-v2/g, "-v1");
    await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(refused);
    delete w.env.MANIFEST_READER_CAPABILITIES;
    await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(refused);
    expect((await w.stored())!.manifest.raw.epochs).toHaveLength(1);

    w.env.MANIFEST_READER_CAPABILITIES = all;
    expect(await w.exporter(w.exportText("pre-t0"))).toMatchObject({ status: "committed" });
  });

  test("G1: refuses a session whose writer lacks the capability", async () => {
    const w = await world({ format: "ndjson" });
    w.hooks.legacyWriter = true;
    await w.write(line(0));
    await w.run();
    await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(refused);
  });

  test("the courier advertises it on every write", async () => {
    const w = await world({ format: "jsonl" });
    await w.write(line(0));
    await w.run();
    expect((await w.stored())!.manifest.writer.capabilities).toEqual([MANIFEST_EPOCHS_CAPABILITY]);
  });

  test("D1: an old courier keeps delivering while its backfill waits, and an upgrade releases it", async () => {
    const w = await world({ format: "ndjson" });
    w.hooks.legacyWriter = true;
    await w.write(line(0));
    await w.run();
    for (let n = 1; n <= 3; n += 1) {
      await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(refused);
      await w.write(line(n));
      await w.run();
    }
    const old = (await w.stored())!.manifest;
    expect(old.raw.chunk_count).toBe(4);
    expect(old.raw.epochs).toHaveLength(1);

    w.hooks.legacyWriter = false;
    await w.write(line(4));
    await w.run();
    expect(await w.exporter(w.exportText("pre-t0"))).toMatchObject({ status: "committed" });
    await w.write(line(5));
    await settle(w);
    const { lines, manifest } = await w.liveRecords();
    expect(lines).toHaveLength(6);
    expect(uploadedRecords(manifest)).toBe(6);
  });
});
