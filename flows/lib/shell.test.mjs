import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";

import { shellWord } from "./shell.ts";

test("shellWord passes any string through /bin/sh unchanged", () => {
  for (const value of ["plain", "it's", "a b", "$HOME", "`id`", "; rm -rf /", "line\nbreak", "'", "", "\\'\"$("]) {
    assert.equal(execFileSync("/bin/sh", ["-c", `printf %s ${shellWord(value)}`], { encoding: "utf8" }), value);
  }
});
