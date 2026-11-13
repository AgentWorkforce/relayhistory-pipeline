// The lanes run the change's own code before anyone approved it: hold lane.sh to running it without the
// caller's credentials (no token variables, a scratch HOME, no git credential helper, credential CLIs shadowed)
// and with what it does need (a synthetic git identity for the sandbox snapshot).
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

const LANE = new URL("./lane.sh", import.meta.url).pathname;
const SECRETS = { GH_TOKEN: "x", GITHUB_TOKEN: "x", JEV_TOKEN: "x", NPM_TOKEN: "x", AWS_SECRET_ACCESS_KEY: "x", BL_API_KEY: "x", BL_WORKSPACE: "x", SSH_AUTH_SOCK: "/tmp/agent" };
// Run as a caller would, even when this test itself runs inside a lane.
const { RH_LANE_HOME: _home, RH_LANE_PID: _pid, ...caller } = process.env;

function envCheck(args = [], extra = {}) {
  const r = spawnSync("bash", [LANE, "env-check", ...args], { encoding: "utf8", env: { ...caller, ...SECRETS, ...extra } });
  assert.equal(r.status, 0, r.stderr);
  return new Set(r.stdout.split("\n").filter(Boolean));
}

test("a lane sees no token variables, a scratch HOME, no git credential helper and shadowed credential CLIs", () => {
  const names = envCheck();
  for (const leaked of Object.keys(SECRETS)) assert.ok(!names.has(leaked), `${leaked} reached the lane`);
  assert.ok(names.has("PATH"), "PATH is kept");
  assert.ok(names.has("HOME_IS_SCRATCH=yes"), [...names].join("\n"));
  assert.ok(names.has("GIT_HELPER=none"), [...names].join("\n"));
  assert.ok(names.has("GH_SHADOWED=yes"), "gh by name is the lane's refusing stub");
});

test("a lane has a synthetic git identity, so the sandbox snapshot can commit-tree under the scrub", () => {
  const names = envCheck();
  assert.ok(names.has("GIT_IDENT=yes"), [...names].join("\n"));
  for (const v of ["GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL"]) assert.ok(names.has(v), v);
});

test("only the sandbox e2e gets the Blaxel credentials", () => {
  assert.ok(envCheck(["e2e", "--target", "sandbox"]).has("BL_API_KEY"));
  assert.ok(!envCheck(["e2e", "--target", "local"]).has("BL_API_KEY"));
  assert.ok(!envCheck(["shard", "1"]).has("BL_API_KEY"));
});

test("an inherited or forged RH_LANE_HOME / RH_LANE_PID neither skips the scrub nor is deleted", () => {
  const names = envCheck([], { RH_LANE_HOME: "/tmp/not-the-lanes", RH_LANE_PID: "1" });
  assert.ok(!names.has("GH_TOKEN"), "the scrub ran");
  assert.ok(names.has("HOME_IS_SCRATCH=yes"));
});
