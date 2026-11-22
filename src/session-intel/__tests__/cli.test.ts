/** Actual offline CLI: synthetic captures only, no credentials or endpoint. */
import { afterAll, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main as importMain } from "../import/cli.ts";
import { main, replayDetectors } from "../replay/cli.ts";
import { ACTION_SCOPED_DETECTOR } from "../replay/action-scoped.ts";
import { parseReplayResult } from "../replay/replay.ts";
import { sha256Hex } from "../replay/util.ts";

const dirs: string[] = [], now = new Date("2026-11-02T00:00:00.000Z");
const temp = () => { const dir = mkdtempSync(join(tmpdir(), "si-action-cli-")); dirs.push(dir); return dir; };
afterAll(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });
const io = () => ({ now, out: () => {}, err: () => {} });
const at = (m: string) => `2026-11-01T09:${m}:00.000Z`;

test("default replay CLI outputs match the pinned golden byte for byte (findings as at 74914c24; source hashes re-pinned)", async () => {
  const out = join(temp(), "out"), fixture = new URL("./fixtures/snapshot", import.meta.url).pathname;
  expect(await main(["replay", "--snapshot", fixture, "--out", out, "--allow-repo-out"], io())).toBe(0);
  // Produced by running the baseline CLI against this same synthetic fixture and fixed clock. Re-pinned for 062220's
  // writer flag: it edits coordination-advisor sources, so only the detector's sourceSha256 (and the hash over it) moved.
  const expected = { "alerts.json": "c8af973122cd4313e064f97835631bf157abacd18bb9451d46db25dd86927c55", "report.json": "bba12068a02546fc6aad39fa6df0a040b6c41762ad66eb414aef3a8cfbdff9fe", "report.md": "c8a34a83deee57598bc1d84dbee4eb884b5e2badd8f0c6e77395f9e0da5ae356" };
  for (const [file, digest] of Object.entries(expected)) expect(sha256Hex(readFileSync(join(out, file)))).toBe(digest);
  expect(replayDetectors(undefined).map((d) => d.name)).toEqual(["coordination-advisor"]);
});

test("explicit CLI consumes the actual /2 importer output with late action scope", async () => {
  const base = temp(), input = join(base, "input.jsonl"), map = join(base, "roots.json"), capture = join(base, "capture");
  writeFileSync(input, [
    { sessionId: "one", ts: at("01"), kind: "write", cwd: "/fixture/repo", files: ["supabase/migrations/0042_one.sql"] },
    { sessionId: "two", ts: at("02"), kind: "write", cwd: "/fixture/repo", files: ["supabase/migrations/0042_two.sql"] },
    { sessionId: "one", ts: at("20"), kind: "end" },
  ].map((e) => JSON.stringify(e)).join("\n"));
  writeFileSync(map, JSON.stringify({ format: "session-intel-repository-map/1", roots: [{ directory: "/fixture/repo", repository: "example.test/team/repo", observedAt: at("10") }] }), { mode: 0o600 });
  const args = ["--format", "generic", "--in", input, "--salt-stdin", "--delete-by", "2026-11-10T00:00:00.000Z", "--action-repository-paths"];
  const warnings: string[] = [];
  const captureIo = () => ({ ...io(), err: (line: string) => { warnings.push(line); }, stdin: (async function* () { yield "synthetic-offline-cli-salt"; })() });
  expect(await importMain([...args, "--repository-map", map, "--out", capture], captureIo())).toBe(0);
  expect(warnings.some((line) => line.includes("version-2 action repository paths require the explicit offline action-scoped-outcomes adapter"))).toBe(true);
  expect(warnings.some((line) => line.includes("until its separate adapter update"))).toBe(false);
  const out = join(base, "out");
  expect(await main(["replay", "--snapshot", capture, "--out", out, "--detectors", ACTION_SCOPED_DETECTOR, "--interval-ms", "60000"], io())).toBe(0);
  const result = parseReplayResult(JSON.parse(readFileSync(join(out, "alerts.json"), "utf8")));
  expect(result.alerts).toHaveLength(1); expect(result.incomplete).toEqual([]);
  expect(result.alerts[0]!.alertAt).toBe(at("10")); expect(result.alerts[0]!.sentAt).toBe(at("11"));
  const rows = readFileSync(join(capture, "sessions.jsonl"), "utf8").trim().split("\n").map((l) => JSON.parse(l));
  expect(result.alerts[0]!.pair).toEqual(rows.map((r) => `${r.harness}:${r.sessionId}`).sort());
  expect(JSON.stringify(result.alerts)).not.toContain("/fixture/repo");
  const unknown = join(base, "unknown"), gaps = join(base, "gaps");
  expect(await importMain([...args, "--out", unknown], captureIo())).toBe(0);
  expect(await main(["replay", "--snapshot", unknown, "--out", gaps, "--detectors", ACTION_SCOPED_DETECTOR], io())).toBe(1);
  expect(parseReplayResult(JSON.parse(readFileSync(join(gaps, "alerts.json"), "utf8"))).incomplete).toContain(`${ACTION_SCOPED_DETECTOR} rejected=2`);
});

test("action-scoped mode refuses mixed session-scoped detectors and legacy captures", async () => {
  expect(() => replayDetectors("unknown-fixture")).toThrow("coordination-advisor, conflict-rule, action-scoped-outcomes");
  expect(() => replayDetectors(`${ACTION_SCOPED_DETECTOR},coordination-advisor`)).toThrow("separate run");
  const fixture = new URL("./fixtures/snapshot", import.meta.url).pathname;
  await expect(main(["replay", "--snapshot", fixture, "--out", join(temp(), "out"), "--detectors", ACTION_SCOPED_DETECTOR], io())).rejects.toThrow("provenance");
});
