// `flows check` accepts any string as a wallclock budget, but the runtime
// reads only one number and one unit ("90m", not "1h30m") and refuses the
// rest when the run starts. Catch that before a run does.
import { readdirSync, readFileSync } from "node:fs";

const bad = [];
for (const file of readdirSync(new URL("..", import.meta.url)).filter((name) => name.endsWith(".flow.ts"))) {
  for (const [, value] of readFileSync(new URL(`../${file}`, import.meta.url), "utf8").matchAll(/wallclock:\s*"([^"]*)"/g)) {
    if (!/^\d+(ms|s|m|h|d)$/.test(value)) bad.push(`${file}: wallclock "${value}"`);
  }
}
if (bad.length) {
  console.error(`budgets the runtime refuses (use one unit, such as "150m"):\n${bad.join("\n")}`);
  process.exit(1);
}
