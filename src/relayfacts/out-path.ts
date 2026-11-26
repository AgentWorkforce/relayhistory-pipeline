/**
 * Output paths for the relayfacts tools. Their output holds quotes and metadata from real sessions, so it must
 * never land inside this (public) repository. The path is checked before anything is created: its nearest
 * existing ancestor is resolved through symlinks, so a refused path leaves nothing behind.
 */
import { existsSync, mkdirSync, realpathSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

export const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../..", import.meta.url))));

/** The path as it will resolve once created: the nearest existing ancestor's real path plus the rest. */
function prospective(path: string): string {
  let head = resolve(path);
  const tail: string[] = [];
  while (!existsSync(head)) {
    const parent = dirname(head);
    if (parent === head) break;
    tail.unshift(basename(head));
    head = parent;
  }
  return join(realpathSync(head), ...tail);
}

function inside(real: string, root: string): boolean {
  return real === root || real.startsWith(root + sep);
}

/** A directory outside the repository, created if needed; throws (creating nothing) for a path inside it. */
export function outsideDir(path: string | undefined, root: string = REPO_ROOT): string {
  if (!path) throw new Error("--out is required");
  if (inside(prospective(path), root)) throw new Error("--out must be outside this repository");
  mkdirSync(path, { recursive: true });
  const real = realpathSync(path);
  if (inside(real, root)) throw new Error("--out must be outside this repository");
  return real;
}

/** A file path whose directory is outside the repository (the directory is created if needed). */
export function outsideFile(path: string | undefined, root: string = REPO_ROOT): string {
  if (!path) throw new Error("--out is required");
  return join(outsideDir(dirname(resolve(path)), root), basename(path));
}
