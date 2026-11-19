/** Test helpers: the committed synthetic fixture and throwaway snapshots in a temp directory. */
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { heldoutHash12 } from "../replay/score.ts";
import { sha256Hex } from "../replay/util.ts";

export const FIXTURE_DIR = join(import.meta.dir, "fixtures", "snapshot");
export const FIXTURE_LABELS = join(import.meta.dir, "fixtures", "labels.json");
export const at = (hm: string) => `2026-11-01T${hm}:00.000Z`;
export const tmp = (prefix = "si-") => mkdtempSync(join(tmpdir(), prefix));

export const session = (sessionId: string, over: Record<string, unknown> = {}) => ({
  orgId: "org_fixture", workspaceId: "ws_fixture", harness: "claude", sessionId, projectId: "proj-x", repo: "fixture-org/x",
  branch: null, title: null, userId: null, taskRef: null, parentSessionIds: [], createdAt: at("09:00"), lastActivityAt: at("12:00"),
  shortSummary: null, writes: [], commits: [], outcomes: [], usage: [], ...over,
});

/** Writes manifest.json and sessions.jsonl with correct hashes, unless `manifest` overrides them. */
export function writeSnapshot(dir: string, sessions: unknown[], manifest: Record<string, unknown> = {}, body?: string): string {
  const text = body ?? sessions.map((s) => JSON.stringify(s)).join("\n") + "\n";
  writeFileSync(join(dir, "sessions.jsonl"), text);
  writeFileSync(join(dir, "manifest.json"), JSON.stringify({
    format: "session-intel-snapshot/1", createdAt: "2026-11-02T00:00:00.000Z", deleteBy: "2099-01-01T00:00:00.000Z",
    window: { since: at("09:00"), until: at("13:00") }, files: { "sessions.jsonl": sha256Hex(text) }, ...manifest,
  }));
  return dir;
}

/** A held-out report's file name for the replay in `alertsPath`: `report.heldout.<hash12>[.forced-<n>].<ext>`. */
export function heldoutName(alertsPath: string, ext: "json" | "md" = "json", forced?: number): string {
  const r = JSON.parse(readFileSync(alertsPath, "utf8")) as { detectors: Array<{ hash: string }> };
  return `report.heldout.${heldoutHash12(r)}${forced === undefined ? "" : `.forced-${forced}`}.${ext}`;
}
