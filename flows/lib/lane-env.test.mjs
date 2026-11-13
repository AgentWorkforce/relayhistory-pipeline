// The lanes run the change's own code before anyone approved it: hold lane.sh to running it without the
// caller's credentials (no token variables, a scratch HOME, no git credential helper).
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

test("a lane sees no token variables, a scratch HOME and no git credential helper", () => {
  // Run as a caller would, even when this test itself runs inside a lane.
  const { RH_LANE_HOME: _inLane, ...caller } = process.env;
  const r = spawnSync("bash", [new URL("./lane.sh", import.meta.url).pathname, "env-check"], {
    encoding: "utf8",
    env: { ...caller, GH_TOKEN: "x", GITHUB_TOKEN: "x", JEV_TOKEN: "x", NPM_TOKEN: "x", AWS_SECRET_ACCESS_KEY: "x", BL_API_KEY: "x", SSH_AUTH_SOCK: "/tmp/agent" },
  });
  assert.equal(r.status, 0, r.stderr);
  const names = new Set(r.stdout.split("\n").filter(Boolean));
  for (const leaked of ["GH_TOKEN", "GITHUB_TOKEN", "JEV_TOKEN", "NPM_TOKEN", "AWS_SECRET_ACCESS_KEY", "BL_API_KEY", "SSH_AUTH_SOCK"]) {
    assert.ok(!names.has(leaked), `${leaked} reached the lane`);
  }
  assert.ok(names.has("PATH"), "PATH is kept");
  assert.ok(names.has("HOME_IS_SCRATCH=yes"), r.stdout);
  assert.ok(names.has("GIT_HELPER=none"), r.stdout);
});

test("only the sandbox e2e gets the Blaxel credentials", () => {
  const src = new URL("./lane.sh", import.meta.url);
  const text = spawnSync("cat", [src.pathname], { encoding: "utf8" }).stdout;
  assert.match(text, /\*" e2e "\*"--target sandbox"\*\) keep\+=\(BL_API_KEY BL_WORKSPACE\)/);
});
