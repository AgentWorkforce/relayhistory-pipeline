import { describe, expect, test } from "bun:test";
import { MAX_OBSERVED_PATHS, observedPaths } from "../observed-paths.ts";

describe("observedPaths", () => {
  test("reads a git status --short listing, renames as their new path, with or without a branch header", () => {
    expect(observedPaths(" M src/a.ts\nA  src/b.ts\n?? notes/c.md\nR  old/x.ts -> new/x.ts\n D gone.ts\n")).toEqual(["src/a.ts", "src/b.ts", "notes/c.md", "new/x.ts", "gone.ts"]);
    expect(observedPaths("## main...origin/main [ahead 1]\n M src/a.ts")).toEqual(["src/a.ts"]);
    expect(observedPaths("MM src/a.ts\r\nUU src/b.ts\r\n")).toEqual(["src/a.ts", "src/b.ts"]);
    expect(observedPaths("RM a.ts -> b.ts\nC  x.ts -> y.ts\n R c.ts -> d.ts\n A e.ts\nAA f.ts\n!! g.log")).toEqual(["b.ts", "y.ts", "d.ts", "e.ts", "f.ts", "g.log"]);
  });
  test("reads a git diff --stat block ending in its summary", () => {
    expect(observedPaths(" src/a.ts        | 12 +++++++-----\n docs/b.md       |  3 +++\n assets/logo.png | Bin 10 -> 20 bytes\n 3 files changed, 10 insertions(+), 5 deletions(-)\n"))
      .toEqual(["src/a.ts", "docs/b.md", "assets/logo.png"]);
    expect(observedPaths(" src/a.ts | 1 +\n 1 file changed, 1 insertion(+)")).toEqual(["src/a.ts"]);
  });
  test("--stat lines whose name is ambiguous (rename notation, braces) or quoted are skipped; the rest still count", () => {
    const block = [
      " keep.ts                                      |  2 ++",
      " old.ts => new.ts                             |  0",
      " src/{old => new}/a.ts                        |  0",
      " deep/{x.ts => y.ts}                          |  0",
      " lib/{ => sub}/b.ts                           |  0",
      " lib/{sub => }/c.ts                           |  0",
      " logo.bin                                     | Bin 6 -> 9 bytes",
      " 7 files changed, 2 insertions(+)",
    ].join("\n");
    // Git prints a file literally named "old.ts => new.ts" the same way, so no rename line names a path.
    expect(observedPaths(block)).toEqual(["keep.ts", "logo.bin"]);
    expect(observedPaths(' "q\\"uote.txt" | 1 +\n plain.txt     | 1 +\n 2 files changed, 2 insertions(+)')).toEqual(["plain.txt"]);
  });
  test("status lines with quoted paths are skipped; the rest of the listing still counts", () => {
    expect(observedPaths(' M "a b.txt"\n M ok.ts')).toEqual(["ok.ts"]);
    expect(observedPaths('R  "a b" -> "c d"\n?? x.ts')).toEqual(["x.ts"]);
    expect(observedPaths(' M "unterminated\n M ok.ts')).toEqual([]);
  });
  test("anything that is not exactly such a listing yields nothing", () => {
    for (const text of [
      "I changed src/a.ts and src/b.ts.",
      " M src/a.ts\nnow running the tests...",
      "build log\n M src/a.ts",
      " src/a.ts | 12 +++\n",                       // stat lines without the summary
      " 1 file changed, 1 insertion(+)",             // a summary alone
      " synthetic confidential prose | Bin arbitrary log text\n 1 file changed", // binary payload must be git's
      " a.ts | 3 +++\n 2 files changed, 3 insertions(+)",                        // file count must match
      " a.ts | 3 +++\n 1 file changed, 2 insertions(+)",                         // counts must add up
      " a.ts | 1 +\n longer.ts | 1 +\n 2 files changed, 2 insertions(+)",       // pipes must line up
      ' M "path with spaces.ts"',                    // quoted paths are not read
      "   this sentence is indented by three spaces", // two spaces are not a status code
      "   - first point\n   - second point",           // nor is an indented list
      " M this line is prose, not a path",          // unquoted paths never hold whitespace
      "?M src/a.ts", "!  src/a.ts", "XY src/a.ts",    // codes git never prints
      " M src/a.ts\n   indented prose after it",     // one bad line spoils the listing
      "?? old -> synthetic-token",                   // the arrow only follows a rename or copy code
      "UU old -> synthetic-token", " M a.ts -> b.ts", "A  a.ts -> b.ts",
      "RR synthetic-token", "R  synthetic-token", "C  only-one-path", // a rename or copy always names both
      "MA src/a.ts", "DM src/a.ts", "UM src/a.ts", "D  a -> b",         // codes git never prints
      "", "   \n  ", null, undefined,
    ]) expect(observedPaths(text as string)).toEqual([]);
  });
  test("is bounded: paths, path length and input size", () => {
    const many = Array.from({ length: 100 }, (_, i) => ` M src/f${i}.ts`).join("\n");
    expect(observedPaths(many)).toHaveLength(MAX_OBSERVED_PATHS);
    expect(observedPaths(` M ${"a/".repeat(300)}x.ts`)).toEqual([]);
    expect(observedPaths(" M src/a.ts\n".repeat(30_000))).toEqual([]); // over the content bound
    expect(observedPaths(" M src/a.ts\n M src/a.ts")).toEqual(["src/a.ts"]);
    expect(observedPaths(" M -rf\n M src/.../x.ts")).toEqual([]);
  });
});
