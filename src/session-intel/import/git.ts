/**
 * Git actions read from a shell command and its output. Only the action and a sha leave here: the
 * command, the commit message and the output are never stored.
 */

export interface GitOps {
  /** The command runs `git commit`. */
  commit: boolean;
  /** One entry per `git revert`: the reverted sha when the command names one, else null. */
  reverts: (string | null)[];
}

const SHA = /^[0-9a-f]{7,40}$/iu;
/** Global options that take a separate argument (`git -C <dir> commit`). */
const ARG_OPTIONS = new Set(["-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path", "--config-env"]);
const PREFIXES = new Set(["sudo", "command", "time", "exec", "nohup", "env"]);

/**
 * A rough tokenizer: segments split on `&&`, `||`, `;`, `|` and newlines, words on whitespace, quotes
 * stripped. A `git commit` quoted inside another command's argument can be miscounted; that is accepted.
 */
export function gitOps(command: string): GitOps {
  const ops: GitOps = { commit: false, reverts: [] };
  if (typeof command !== "string" || command.length > 1_000_000) return ops;
  for (const segment of command.split(/&&|\|\||[;|\n(){}]/u)) {
    const words = segment.trim().split(/\s+/u).map((w) => w.replace(/^['"]+|['"]+$/gu, "")).filter(Boolean);
    let i = 0;
    while (i < words.length && (PREFIXES.has(words[i]!) || /^[A-Za-z_][A-Za-z0-9_]*=/u.test(words[i]!))) i++;
    const exe = words[i];
    if (!exe || !(exe === "git" || exe.endsWith("/git"))) continue;
    i++;
    while (i < words.length && words[i]!.startsWith("-")) {
      if (ARG_OPTIONS.has(words[i]!)) i++;
      i++;
    }
    const sub = words[i];
    const args = words.slice(i + 1);
    if (sub === "commit" && !args.includes("--dry-run")) ops.commit = true;
    else if (sub === "revert") {
      if (args.some((a) => a === "--abort" || a === "--quit" || a === "--skip")) continue;
      const shas = args.filter((a) => SHA.test(a));
      if (shas.length) for (const s of shas) ops.reverts.push(s.toLowerCase());
      else ops.reverts.push(null);
    }
  }
  return ops;
}

/** The sha of git's commit summary line (`[main 1a2b3c4] ...`, `[main (root-commit) 1a2b3c4] ...`), or null. */
export function commitShaFromOutput(output: string): string | null {
  if (typeof output !== "string") return null;
  const m = /^\[[^\]\n]*?\s([0-9a-f]{7,40})\]/mu.exec(output.slice(0, 64 * 1024));
  return m ? m[1]!.toLowerCase() : null;
}
