import { checkHyperdriveConfig } from "./hyperdrive-config.ts";

const id = process.env.HYPERDRIVE_ID?.trim();
const caId = process.env.EXPECTED_CA_ID?.trim();
if (!id || !caId) {
  console.error(`hyperdrive check: missing expected field${!id && !caId ? "s" : ""} ${[!id && "HYPERDRIVE_ID", !caId && "EXPECTED_CA_ID"].filter(Boolean).join(",")}`);
  process.exit(1);
}

let text = "";
process.stdin.setEncoding("utf8");
for await (const chunk of process.stdin) text += chunk;

let body: unknown;
try {
  body = JSON.parse(text);
} catch {
  console.error("hyperdrive check: response was not valid JSON");
  process.exit(1);
}

const report = checkHyperdriveConfig(body, { id, caId });
console.log(JSON.stringify({ checks: report.checks }));
if (!report.ok) {
  const failed = Object.entries(report.checks).filter(([, ok]) => !ok).map(([field]) => field);
  console.error(`hyperdrive check failed: ${failed.join(",")}`);
  process.exit(1);
}
