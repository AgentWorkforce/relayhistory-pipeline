import { afterEach, describe, expect, test } from "bun:test";
import { computeContentSha256, objectKey, parseManifest, readAll, type ManifestV1 } from "../../shared/vendor.ts";
import { courierWorld, EXPORTER, MARKER } from "./courier-harness.ts";

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

const line = (n: number) => JSON.stringify({ type: "user", n });
const settle = async (w: World) => {
  for (let i = 0; i < 4; i += 1) await w.run();
};
const liveCount = (m: ManifestV1) => m.raw.chunks.filter((c) => c.epoch > 0).reduce((n, c) => n + (c.records ?? 0), 0);

/** A takeover by another writer instance, built on `base` with sealed chunks. */
async function takeover(w: World, base: ManifestV1, owner: string, text: string, edit?: (m: ManifestV1) => void) {
  const m = structuredClone(base);
  const epoch = m.raw.epoch + 1;
  const { entry } = await w.seal(text, m.raw.chunk_count, epoch, m.trace_id);
  m.revision += 1;
  m.writer = { delivery_instance_id: owner, plugin_version: "test", capabilities: ["manifest-epochs-v2"] };
  m.raw.owner = owner;
  m.raw.epochs.push({ epoch, opened_at: m.updated_at, reason: "adopted", first_seq: m.raw.chunk_count, opened_by: owner });
  m.raw.epoch = epoch;
  m.raw.chunks.push(entry);
  m.raw.chunk_count += 1;
  m.raw.raw_bytes += entry.bytes;
  m.raw.stored_bytes += entry.stored_bytes;
  m.raw.next_offset = entry.bytes;
  m.raw.content_sha256 = await computeContentSha256(m.raw.chunks);
  edit?.(m);
  return m;
}

describe("handover from a cold export (H1-H5)", () => {
  test("H4: the client takes over and uploads everything it accumulated", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    for (let n = 0; n < 3; n += 1) await w.write(line(n));
    await settle(w);
    const { manifest, lines, records } = await w.liveRecords();
    expect(manifest.raw.owner).toBe(w.machineId);
    expect(manifest.writer.delivery_instance_id).toBe(w.machineId);
    expect(manifest.raw.epochs.map((e) => e.reason)).toEqual(["initial", "adopted"]);
    expect(manifest.raw.chunks[0]).toEqual(cold.raw.chunks[0]!);
    expect(manifest.raw.chunks[1]).toMatchObject({ epoch: 1, start_offset: 0 });
    expect(manifest.trace_id).toBe(cold.trace_id);
    expect(manifest.extensions?.["client.relayhistory_export"]).toEqual(MARKER);
    expect(lines.toSorted()).toEqual([line(0), line(1), line(2)].toSorted());
    expect(records.some((r) => r.record_id === "pre-t0")).toBe(true);

    await w.write(line(3));
    await settle(w);
    const after = await w.liveRecords();
    expect(after.lines).toHaveLength(4);
    expect(liveCount(after.manifest)).toBe(4);
    expect(after.manifest.raw.epochs).toHaveLength(2);
  });

  test("H1: racing takeovers on one version: one wins, one adopted epoch", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    const { version } = (await w.stored())!;
    const a = await takeover(w, cold, "instance-a", '{"from":"a"}\n');
    const b = await takeover(w, cold, "instance-b", '{"from":"b"}\n');
    const results = await Promise.all([
      w.post({ key: w.key, manifest: a, previousVersion: version }),
      w.post({ key: w.key, manifest: b, previousVersion: version }),
    ]);
    expect(results.map((r) => r.status).toSorted()).toEqual([202, 409]);
    const m = (await w.stored())!.manifest;
    expect(m.raw.epochs.filter((e) => e.reason === "adopted")).toHaveLength(1);
    expect(["instance-a", "instance-b"]).toContain(m.raw.owner);
  });

  test("H1: a courier that loses the takeover is fenced and writes nothing more", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    await w.write(line(0));
    await w.run(); // first contact: 409, then the rebase sees the cold export
    await w.write(line(1));
    w.hooks.beforeManifest = async () => {
      w.hooks.beforeManifest = undefined;
      const { version } = (await w.stored())!;
      const res = await w.post({ key: w.key, manifest: await takeover(w, cold, "instance-b", '{"from":"b"}\n'), previousVersion: version });
      expect(res.status).toBe(202);
    };
    await settle(w);
    expect((await w.state()).sessions["claude:session-a"].fenced).toBe(true);
    const m = (await w.stored())!.manifest;
    expect(m.raw.owner).toBe("instance-b");
    expect(m.raw.chunk_count).toBe(2);
  });

  test("H2: an exporter retry racing the takeover is a no-op in both orders", async () => {
    for (let round = 0; round < 6; round += 1) {
      const w = await world();
      const text = w.exportText("pre-t0");
      await w.coldExport(text);
      await w.write(line(0));
      const [, exported] = await Promise.all([settle(w), w.exporter(text)]);
      expect(exported.status).toBe("already_present");
      await settle(w);
      const { manifest } = await w.liveRecords();
      expect(manifest.raw.epochs.map((e) => e.reason)).toEqual(["initial", "adopted"]);
      expect(await w.exporter(text)).toMatchObject({ status: "already_present" });
      await expect(w.exporter(w.exportText("different"))).rejects.toMatchObject({ code: "precondition_failed" });
    }
  });

  test("H3: refused takeovers", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    const { version } = (await w.stored())!;
    const owner = "instance-a";
    const refused: [string, (m: ManifestV1) => void][] = [
      ["owner is not the writer", (m) => void (m.raw.owner = "someone-else")],
      ["no adopted epoch", (m) => void (m.raw.epochs[1]!.reason = "rewritten")],
      ["wrong opened_by", (m) => void (m.raw.epochs[1]!.opened_by = "someone-else")],
      ["trace changed", (m) => void (m.trace_id = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19")],
      ["export marker dropped", (m) => void (m.extensions = {})],
    ];
    for (const [name, edit] of refused) {
      const res = await w.post({ key: w.key, manifest: await takeover(w, cold, owner, '{"x":1}\n', edit), previousVersion: version });
      expect([name, res.status]).toEqual([name, 409]);
    }
    const exportedChunk = await takeover(w, cold, owner, '{"x":1}\n');
    exportedChunk.raw.chunks.splice(0, 1);
    exportedChunk.raw.chunks.forEach((c, i) => (c.seq = i));
    expect((await w.post({ key: w.key, manifest: exportedChunk, previousVersion: version })).status).toBe(400);

    const asExporter = await takeover(w, cold, owner, '{"x":1}\n');
    expect((await w.post({ key: w.key, manifest: asExporter, previousVersion: version }, "migration")).status).toBe(409);
    expect((await w.stored())!.manifest.raw.owner).toBe(EXPORTER);

    const won = await takeover(w, cold, owner, '{"x":1}\n');
    expect((await w.post({ key: w.key, manifest: won, previousVersion: version })).status).toBe(202);
    const second = await takeover(w, won, "instance-b", '{"y":1}\n');
    const { version: now } = (await w.stored())!;
    expect((await w.post({ key: w.key, manifest: second, previousVersion: now })).status).toBe(400);
  });

  test("H5: a client on another machine id never meets the export", async () => {
    const cold = await world({ machineId: "unknown-origin-7" });
    await cold.coldExport(cold.exportText("pre-t0"));
    const client = await world({ format: "ndjson", machineId: "machine-a", root: cold.rootDir });
    await client.write(line(0));
    await settle(client);
    expect((await client.stored())!.manifest.raw.owner).toBe("machine-a");
    expect((await cold.stored())!.manifest.raw.owner).toBe(EXPORTER);
  });
});

describe("gate and old couriers on a cold export (G1, D1)", () => {
  const held = async (w: World) => {
    const key = objectKey(w.key.replace(/manifest\.json$/, `parked/${w.machineId}/manifest.json`));
    return parseManifest(new TextDecoder().decode(await readAll(await w.store.get(key))));
  };

  test("G1 + D1: a closed gate parks the takeover, and opening it completes it", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    const open = w.env.MANIFEST_READER_CAPABILITIES!;
    delete w.env.MANIFEST_READER_CAPABILITIES;
    await w.write(line(0));
    await settle(w);
    await w.write(line(1));
    await settle(w);
    expect((await w.stored())!.manifest).toEqual(cold);
    expect((await held(w)).raw.chunks.length).toBeGreaterThan(1);

    w.env.MANIFEST_READER_CAPABILITIES = open;
    await w.write(line(2));
    await settle(w);
    const { manifest, lines } = await w.liveRecords();
    expect(manifest.raw.owner).toBe(w.machineId);
    expect(lines).toHaveLength(3);
    expect(liveCount(manifest)).toBe(3);
  });

  test("D1: an old courier never stalls on a cold export, and an upgrade delivers everything", async () => {
    const w = await world({ format: "ndjson" });
    const cold = await w.coldExport(w.exportText("pre-t0"));
    w.hooks.legacyWriter = true;
    for (let n = 0; n < 3; n += 1) {
      await w.write(line(n));
      await w.run();
    }
    await w.run();
    const state = (await w.state()).sessions["claude:session-a"];
    expect(state.pending).toBeFalsy();
    expect(state.fenced).toBeFalsy();
    expect((await w.stored())!.manifest).toEqual(cold);
    expect((await held(w)).writer.capabilities).toBeUndefined();

    // Nothing reaches readers while parked, and a restart's check still passes.
    const { lines } = await w.liveRecords();
    expect(lines).toEqual([]);

    w.hooks.legacyWriter = false;
    await w.write(line(3));
    await settle(w);
    const after = await w.liveRecords();
    expect(after.manifest.raw.owner).toBe(w.machineId);
    expect(after.lines).toHaveLength(4);
    expect(liveCount(after.manifest)).toBe(4);
  });
});
