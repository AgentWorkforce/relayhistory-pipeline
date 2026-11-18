/** The snapshot loader: hashes, deleteBy, file names, bounds and strict fields. */
import { describe, expect, test } from "bun:test";
import { symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { loadSnapshot, SnapshotError } from "../replay/snapshot.ts";
import { sha256Hex } from "../replay/util.ts";
import { at, FIXTURE_DIR, session, tmp, writeSnapshot } from "./helpers.ts";

describe("loadSnapshot", () => {
  test("loads the committed fixture", async () => {
    const s = await loadSnapshot(FIXTURE_DIR, { now: new Date("2026-11-18T00:00:00Z") });
    expect(s.sessions).toHaveLength(13);
    expect(s.manifest.window).toEqual({ since: at("09:00"), until: at("13:00") });
    expect(s.manifestSha256).toMatch(/^[0-9a-f]{64}$/);
  });

  test("refuses a file whose sha256 does not match the manifest", async () => {
    const dir = writeSnapshot(tmp(), [session("s1")]);
    writeFileSync(join(dir, "sessions.jsonl"), `${JSON.stringify(session("s1", { projectId: "other" }))}\n`);
    await expect(loadSnapshot(dir)).rejects.toThrow("does not match its manifest sha256");
  });

  test("refuses past deleteBy, before reading any session", async () => {
    const dir = writeSnapshot(tmp(), [session("s1")], { deleteBy: "2026-11-10T00:00:00.000Z" });
    writeFileSync(join(dir, "sessions.jsonl"), "not json and not the hashed bytes\n");
    await expect(loadSnapshot(dir, { now: new Date("2026-11-10T00:00:01Z") })).rejects.toThrow("past its deleteBy");
    // Exactly at deleteBy is still allowed (the corrupt file then fails its hash).
    await expect(loadSnapshot(dir, { now: new Date("2026-11-10T00:00:00Z") })).rejects.toThrow("does not match");
  });

  test("refuses unsafe or missing manifest files", async () => {
    const cases: Array<[Record<string, unknown>, string]> = [
      [{ files: { "../sessions.jsonl": "0".repeat(64), "sessions.jsonl": "0".repeat(64) } }, "invalid file name"],
      [{ files: { ".hidden": "0".repeat(64), "sessions.jsonl": "0".repeat(64) } }, "invalid file name"],
      [{ files: { "other.jsonl": "0".repeat(64) } }, "must list sessions.jsonl"],
      [{ files: { "sessions.jsonl": "ABC" } }, "sha256 hex"],
      [{ format: "session-intel-snapshot/2" }, "format must be"],
      [{ window: { since: at("13:00"), until: at("09:00") } }, "since < until"],
      [{ deleteBy: "2026-11-01" }, "ISO-8601"],
      [{ extra: true }, "unknown field"],
    ];
    for (const [over, message] of cases) {
      const dir = writeSnapshot(tmp(), [session("s1")], over);
      await expect(loadSnapshot(dir)).rejects.toThrow(message);
    }
    const body = `${JSON.stringify(session("s1"))}\n`;
    const missing = writeSnapshot(tmp(), [], { files: { "claims.jsonl": "0".repeat(64), "sessions.jsonl": sha256Hex(body) } }, body);
    await expect(loadSnapshot(missing)).rejects.toThrow("claims.jsonl is missing");
  });

  test("refuses a symlinked file", async () => {
    const real = writeSnapshot(tmp(), [session("s1")]);
    const dir = tmp();
    writeSnapshot(dir, [session("s1")]);
    const { rmSync } = await import("node:fs");
    rmSync(join(dir, "sessions.jsonl"));
    symlinkSync(join(real, "sessions.jsonl"), join(dir, "sessions.jsonl"));
    await expect(loadSnapshot(dir)).rejects.toThrow("regular file");
  });

  test("validates each session line strictly", async () => {
    const cases: Array<[unknown, string]> = [
      [{ ...session("s1"), transcript: "body" }, "unknown field"],
      [session("s1", { writes: [{ path: "a.ts", ts: at("08:00"), eventId: "e", kind: "k" }] }), "between createdAt and lastActivityAt"],
      [session("s1", { writes: [{ path: "a".repeat(5_000), ts: at("10:00"), eventId: "e", kind: "k" }] }), "path must be"],
      [session("s1", { lastActivityAt: at("08:00") }), "before createdAt"],
      [session("s1", { harness: "bad harness" }), "harness must match"],
      [session("s1", { outcomes: [{ key: "abc", ts: at("10:00"), eventId: "e", reverted: "yes" }] }), "reverted must be a boolean"],
      [session("s1", { claims: [{ at: at("10:00"), kind: "wish", text: "x", confidence: 0.5, evidenceIds: [] }] }), "kind must be intent"],
      [session("s1", { claims: [{ at: at("10:00"), kind: "intent", text: "x", confidence: 2, evidenceIds: [] }] }), "confidence"],
      [session("s1", { usage: [{ ts: at("10:00"), eventId: "u", tokens: -1, costUsdMicros: null }] }), "non-negative integer"],
    ];
    for (const [line, message] of cases) {
      const dir = writeSnapshot(tmp(), [line]);
      const err = await loadSnapshot(dir).catch((e) => e);
      expect(err).toBeInstanceOf(SnapshotError);
      expect(String(err.message)).toContain(message);
      expect(String(err.message)).toContain("line 1");
    }
    const dup = writeSnapshot(tmp(), [session("s1"), session("s1", { workspaceId: "ws_other" })]);
    await expect(loadSnapshot(dup)).rejects.toThrow("repeats a (harness, sessionId)");
  });

  test("a summary without a timeline is visible only from lastActivityAt; a timeline is sorted", async () => {
    const dir = writeSnapshot(tmp(), [
      session("s1", { shortSummary: "plain summary" }),
      session("s2", { shortSummary: { timeline: [{ at: at("11:00"), text: "later" }, { at: at("10:00"), text: "earlier" }] } }),
    ]);
    const s = await loadSnapshot(dir);
    expect(s.sessions[0]!.summaryTimeline).toEqual([{ at: at("12:00"), text: "plain summary" }]);
    expect(s.sessions[1]!.summaryTimeline.map((e) => e.text)).toEqual(["earlier", "later"]);
  });
});
