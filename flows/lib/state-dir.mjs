// Where the flows keep their state: one directory per flow under a shared
// root. By default the root is .relayflow in the checkout (git-ignored); with
// RELAYFLOW_STATE_DIR set, it is that directory instead, so a run that gives
// each of its flows the same value keeps all their reports in one place, out
// of the checkout. A relative value is taken from the repository root, where
// the flows run. A root inside the checkout must be git-ignored: the reviews
// and CI refuse a tree with new files nobody staged.
//
//   node flows/lib/state-dir.mjs <flow>      prints the flow's directory
//   node flows/lib/state-dir.mjs             prints the root
//
// The flows read it through a command rather than process.env, so a resumed
// run replays the directory it started with.
import { execFileSync } from "node:child_process";
import { existsSync, realpathSync } from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

export const DEFAULT_ROOT = ".relayflow";

/** The state root: `.relayflow` unless RELAYFLOW_STATE_DIR names another, made absolute from `cwd`. */
export function stateRoot(env = process.env, cwd = process.cwd()) {
  const given = env.RELAYFLOW_STATE_DIR?.trim();
  if (!given) return DEFAULT_ROOT;
  return isAbsolute(given) ? resolve(given) : resolve(cwd, given);
}

/** One flow's directory under the root. */
export function stateDir(flow, env = process.env, cwd = process.cwd()) {
  if (!flow || /[/\\]|^\.\.?$/.test(flow)) throw new Error(`not a flow name: ${JSON.stringify(flow)}`);
  const root = stateRoot(env, cwd);
  return root === DEFAULT_ROOT ? `${DEFAULT_ROOT}/${flow}` : join(root, flow);
}

// git reports the checkout by its real path (/private/var on macOS), so the
// root is compared by its real path too, as far as it exists yet.
const real = (path) => (existsSync(path) || dirname(path) === path ? realpathSync(path) : join(real(dirname(path)), basename(path)));

/** Throw when a RELAYFLOW_STATE_DIR root lies inside the checkout at `cwd` and git does not ignore it. */
export function checkRoot(root, cwd = process.cwd()) {
  if (root === DEFAULT_ROOT) return;
  const git = (...args) => execFileSync("git", ["-C", cwd, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  let top;
  try {
    top = git("rev-parse", "--show-toplevel");
  } catch {
    return;
  }
  const inside = relative(realpathSync(top), real(resolve(cwd, root)));
  if (inside === ".." || inside.startsWith(`..${sep}`) || isAbsolute(inside)) return;
  try {
    // The trailing slash lets a `dir/` pattern match a directory not made yet.
    if (!inside) throw new Error("the checkout itself");
    git("check-ignore", "-q", "--no-index", `${inside}/`);
  } catch {
    throw new Error(`RELAYFLOW_STATE_DIR ${root} is inside the checkout and not git-ignored; put it outside the checkout or ignore it`);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    const root = stateRoot();
    checkRoot(root);
    const [flow] = process.argv.slice(2);
    console.log(flow ? stateDir(flow) : root);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
