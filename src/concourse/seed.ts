/**
 * Concourse seed: a fake HOME with transcript files, indexed into a fresh
 * history.db by the real `ai-hist sync` (Rust CLI from vendor/relayhistory).
 * No real sessions are read. Fixtures come from the ai-hist test corpus; one
 * extra session carries shape-only fake secrets so the smoke test can prove
 * Technical Foul scrubbed them before they reached the Locker.
 */
import { initScrubEngine } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { access, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { log } from "../shared/log.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const FIXTURES = join(ROOT, "vendor/relayhistory/crates/ai-hist/tests/fixtures/claude");
const DEFAULT_BIN = join(ROOT, "vendor/relayhistory/target/release/ai-hist");

/** Shape-only fakes. None is a real credential; each matches a scrub rule. */
export const PLANTED_SECRETS = {
  awsKey: "AKIAIOSFODNN7EXAMPLE",
  openaiKey: "sk-proj-96bullsFakeKey0000000000000000000000000000",
  slackToken: "xoxb-1234567890-1234567890123-96bullsFakeToken0000",
  email: "secret.person@example.com",
  pem: "-----BEGIN RSA PRIVATE KEY-----\\nMIIEpAIBAAKCAQEA96bullsFakeKeyBodyNotARealKey000000000000000000\\n-----END RSA PRIVATE KEY-----",
} as const;

/** Phrases that must only ever appear inside the Locker and HoopDb, never in telemetry. */
export const PLANTED_CONTENT_MARKER = "ORANGE-ELEPHANT-96BULLS-CONTENT-MARKER";

export interface SeedResult {
  home: string;
  dbPath: string;
  sessions: Array<{ sessionId: string; file: string; planted: boolean }>;
}

function withSessionId(jsonl: string, sessionId: string, cwd: string): string {
  return jsonl
    .split("\n")
    .filter((l) => l.trim())
    .map((line) => {
      try {
        const rec = JSON.parse(line) as Record<string, unknown>;
        if ("sessionId" in rec) rec.sessionId = sessionId;
        if ("cwd" in rec) rec.cwd = cwd;
        return JSON.stringify(rec);
      } catch {
        return line;
      }
    })
    .join("\n") + "\n";
}

function plantedSession(sessionId: string, cwd: string, at = new Date("2026-10-20T10:00:00.000Z")): string {
  const t = (s: number) => new Date(at.getTime() + s * 1000).toISOString();
  const content = [
    `Please rotate these before we ship. ${PLANTED_CONTENT_MARKER}`,
    `AWS_ACCESS_KEY_ID=${PLANTED_SECRETS.awsKey}`,
    `OPENAI_API_KEY=${PLANTED_SECRETS.openaiKey}`,
    `SLACK_BOT_TOKEN=${PLANTED_SECRETS.slackToken}`,
    `contact ${PLANTED_SECRETS.email} when done`,
    PLANTED_SECRETS.pem.replace(/\\n/g, "\n"),
  ].join("\n");
  const lines = [
    { type: "permission-mode", permissionMode: "default", sessionId },
    { parentUuid: null, isSidechain: false, promptId: "p-1", type: "user", message: { role: "user", content }, uuid: "u-user-1", timestamp: t(0), cwd, sessionId, version: "2.1.96" },
    { parentUuid: "u-user-1", isSidechain: false, message: { model: "claude-sonnet-4-6", id: "msg_planted_1", type: "message", role: "assistant", content: [{ type: "text", text: "I will not echo credentials. Rotating them now." }, { type: "tool_use", id: "tu_1", name: "Bash", input: { command: "echo rotate" } }], stop_reason: "tool_use", stop_sequence: null, usage: { input_tokens: 120, output_tokens: 40 } }, uuid: "u-asst-1", timestamp: t(5), cwd, sessionId, type: "assistant", version: "2.1.96" },
    { parentUuid: "u-asst-1", isSidechain: false, type: "user", message: { role: "user", content: [{ type: "tool_result", tool_use_id: "tu_1", content: "rotate" }] }, uuid: "u-user-2", timestamp: t(6), cwd, sessionId, version: "2.1.96" },
    { parentUuid: "u-user-2", isSidechain: false, message: { model: "claude-sonnet-4-6", id: "msg_planted_2", type: "message", role: "assistant", content: [{ type: "text", text: "Done. Keys rotated." }], stop_reason: "end_turn", stop_sequence: null, usage: { input_tokens: 200, output_tokens: 12 } }, uuid: "u-asst-2", timestamp: t(9), cwd, sessionId, type: "assistant", version: "2.1.96" },
  ];
  return lines.map((l) => JSON.stringify(l)).join("\n") + "\n";
}

export async function seedConcourse(opts: { dir: string; fixtures?: string[]; bin?: string; fresh?: boolean }): Promise<SeedResult> {
  const home = join(opts.dir, "home");
  const dbPath = join(opts.dir, "history.db");
  if (opts.fresh) await rm(opts.dir, { recursive: true, force: true });
  const cwd = "/workspace/demo";
  const project = join(home, ".claude", "projects", "-workspace-demo");
  await mkdir(project, { recursive: true });
  const names = opts.fixtures ?? ["simple-turn.jsonl", "files-touched.jsonl", "compact-boundary.jsonl"];
  const sessions: SeedResult["sessions"] = [];
  for (const name of names) {
    const sessionId = randomUUID();
    const file = join(project, `${sessionId}.jsonl`);
    await writeFile(file, withSessionId(await readFile(join(FIXTURES, name), "utf8"), sessionId, cwd));
    sessions.push({ sessionId, file, planted: false });
  }
  const plantedId = randomUUID();
  const plantedFile = join(project, `${plantedId}.jsonl`);
  await writeFile(plantedFile, plantedSession(plantedId, cwd));
  sessions.push({ sessionId: plantedId, file: plantedFile, planted: true });

  await syncConcourse({ home, dbPath, bin: opts.bin });
  log.info("concourse seeded", { count: sessions.length });
  return { home, dbPath, sessions };
}

/** Adds one more planted session to an existing seed and re-syncs Concourse. Used by the smoke test. */
export async function addPlantedSession(opts: { dir: string; bin?: string }): Promise<{ sessionId: string; file: string }> {
  const home = join(opts.dir, "home");
  const dbPath = join(opts.dir, "history.db");
  const project = join(home, ".claude", "projects", "-workspace-demo");
  await mkdir(project, { recursive: true });
  const sessionId = randomUUID();
  const file = join(project, `${sessionId}.jsonl`);
  await writeFile(file, plantedSession(sessionId, "/workspace/demo", new Date(Date.now() - 60_000)));
  await syncConcourse({ home, dbPath, bin: opts.bin });
  return { sessionId, file };
}

async function syncConcourse(opts: { home: string; dbPath: string; bin?: string }): Promise<void> {
  const bin = opts.bin ?? process.env.AI_HIST_BIN ?? DEFAULT_BIN;
  await access(bin).catch(() => {
    throw new Error(`96bulls: ai-hist binary not found at ${bin}; run \`cargo build --release -p ai-hist-cli\` in vendor/relayhistory or set AI_HIST_BIN`);
  });
  const { out, err, code } = await new Promise<{ out: string; err: string; code: number }>((resolve) => {
    const proc = spawn(bin, ["--db", opts.dbPath, "sync", "--local", "--no-source-connectors"], {
      env: { ...process.env, HOME: opts.home, XDG_DATA_HOME: join(opts.home, ".local/share"), AI_HIST_DB: opts.dbPath },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    proc.stdout.on("data", (d) => (out += d));
    proc.stderr.on("data", (d) => (err += d));
    proc.on("close", (c) => resolve({ out, err, code: c ?? 1 }));
  });
  if (code !== 0) {
    log.error("ai-hist sync failed", { error_code: `exit_${code}` });
    throw new Error(`ai-hist sync failed (exit ${code}): ${err.slice(0, 500) || out.slice(0, 500)}`);
  }
}

if (import.meta.main) {
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  await initScrubEngine();
  const dir = process.argv[2] ?? join(ROOT, ".96bulls/local/concourse");
  const r = await seedConcourse({ dir, fresh: process.argv.includes("--fresh") });
  console.log(JSON.stringify({ dbPath: r.dbPath, sessions: r.sessions.map((s) => s.sessionId) }, null, 2));
}
