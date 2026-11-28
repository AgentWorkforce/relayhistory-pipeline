/** Reads commits from a local git mirror. The parser is pure; `readCommits` is the only I/O. */
import { spawn } from "node:child_process";
import type { Commit, FileChange } from "./types.ts";

const RS = "\x1e";
const US = "\x1f";
/** One record per commit: fields separated by US, then the `--name-status` lines. */
export const GIT_LOG_FORMAT = `${RS}%H${US}%an${US}%aI${US}%P${US}%s${US}%b${US}`;

const STATUS = new Set(["A", "M", "D", "R", "C", "T"]);

export function parseGitLog(raw: string): Commit[] {
  const out: Commit[] = [];
  for (const record of raw.split(RS)) {
    if (!record.trim()) continue;
    const parts = record.split(US);
    if (parts.length < 7) throw new Error("git log record has too few fields");
    const [sha, author, authoredAt, parents, subject, body] = parts as [string, string, string, string, string, string];
    const files: FileChange[] = [];
    for (const line of parts.slice(6).join(US).split("\n")) {
      if (!line.trim()) continue;
      const fields = line.split("\t");
      const code = fields[0]?.[0];
      if (!code || !STATUS.has(code) || fields.length < 2) continue;
      // Renames and copies list old and new paths; the new path is the one written.
      files.push({ status: code as FileChange["status"], path: fields[fields.length - 1]! });
    }
    out.push({ sha, author, authoredAt: new Date(authoredAt).toISOString(), parents: parents.split(" ").filter(Boolean), subject, body: body.trim(), files });
  }
  return out;
}

/** `git log --all` over a bare mirror. `--no-renames` keeps a blobless mirror from fetching blobs. */
export function readCommits(gitDir: string, since: string, until: string): Promise<Commit[]> {
  const args = ["--git-dir", gitDir, "log", "--all", "--no-renames", "--name-status",
    `--since=${since}`, `--until=${until}`, `--format=${GIT_LOG_FORMAT}`];
  return new Promise((resolve, reject) => {
    const child = spawn("git", args, { stdio: ["ignore", "pipe", "pipe"] });
    const chunks: Buffer[] = [];
    let err = "";
    child.stdout.on("data", (c: Buffer) => chunks.push(c));
    child.stderr.on("data", (c: Buffer) => { err += c.toString(); });
    child.on("error", reject);
    child.on("close", (code) => code === 0
      ? resolve(parseGitLog(Buffer.concat(chunks).toString("utf8")))
      : reject(new Error(`git log exited ${code}: ${err.slice(0, 400)}`)));
  });
}
