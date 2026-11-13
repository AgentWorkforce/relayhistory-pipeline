// Gate for a reviewer's findings file: it must parse and match the shape the
// review flows ask for, so a reviewer that only talked never passes. Findings
// about a file the change does not touch are out of scope: they are moved to
// "dropped" (with the reason) and the file is rewritten, so one stray minor
// finding narrows the review instead of failing the whole fleet. Paths are
// compared repository-relative (`./x` and absolute paths are accepted), and a
// blocker or major that would be dropped fails the gate instead: a review must
// never turn green because a serious finding named its file oddly.
//
//   node flows/lib/check-findings.mjs <findings.json> <files.txt>
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { isAbsolute, relative } from "node:path";

const SEVERITIES = new Set(["blocker", "major", "minor", "nit"]);
const [path, filesPath] = process.argv.slice(2);
const fail = (why) => {
  console.error(`${path}: ${why}`);
  process.exit(1);
};

let report;
try {
  report = JSON.parse(readFileSync(path, "utf8"));
} catch (error) {
  fail(`not readable JSON (${error.message})`);
}
if (typeof report?.reviewer !== "string" || !Array.isArray(report.findings)) {
  fail('needs a "reviewer" string and a "findings" array');
}
report.findings.forEach((finding, i) => {
  if (!SEVERITIES.has(finding?.severity)) fail(`finding ${i} has severity ${JSON.stringify(finding?.severity)}`);
  if (!Number.isInteger(finding.line) || finding.line < 1) fail(`finding ${i} needs "line" as a line number`);
  for (const key of ["file", "title", "failure_scenario"]) {
    if (typeof finding[key] !== "string" || !finding[key].trim()) fail(`finding ${i} is missing "${key}"`);
  }
});

let root = process.cwd();
try {
  root = execFileSync("git", ["rev-parse", "--show-toplevel"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
} catch {
  // Not in a checkout: absolute paths are taken relative to the working directory.
}
let renamed = false;
for (const finding of report.findings) {
  const file = (isAbsolute(finding.file) ? relative(root, finding.file) : finding.file).replace(/^(\.\/)+/, "");
  renamed ||= file !== finding.file;
  finding.file = file;
}

const changed = new Set(readFileSync(filesPath, "utf8").split("\n").filter(Boolean));
const outside = report.findings.filter((finding) => !changed.has(finding.file));
const serious = outside.filter((finding) => finding.severity === "blocker" || finding.severity === "major");
if (serious.length) {
  fail(
    `${serious.length} blocker/major finding(s) name a file the change does not touch (${serious.map((f) => f.file).join(", ")}); ` +
      "report each at the changed line that reaches it, using a path from the changed-file list",
  );
}
if (outside.length) {
  report.findings = report.findings.filter((finding) => changed.has(finding.file));
  report.dropped = [
    ...(Array.isArray(report.dropped) ? report.dropped : []),
    ...outside.map((finding) => ({ ...finding, dropped: "the change does not touch this file" })),
  ];
}
if (outside.length || renamed) writeFileSync(path, `${JSON.stringify(report, null, 2)}\n`);
console.log(`${path}: ${report.findings.length} finding(s)${outside.length ? `, ${outside.length} out of scope dropped` : ""}`);
