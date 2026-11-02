import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const CONFIG = new URL("../wrangler.jsonc", import.meta.url);
const DOMAIN = "sessions-read.agentrelay.com";
const parsed = ts.parseConfigFileTextToJson(fileURLToPath(CONFIG), readFileSync(CONFIG, "utf8"));

if (parsed.error) {
  throw new Error(`wrangler.jsonc is not valid JSONC: ${ts.flattenDiagnosticMessageText(parsed.error.messageText, "\n")}`);
}

const config = parsed.config as {
  workers_dev?: unknown;
  routes?: Array<{ pattern?: unknown; custom_domain?: unknown }>;
};

if (config.workers_dev !== false) {
  throw new Error("wrangler.jsonc must keep workers_dev=false; same-account Worker fetches through workers.dev can fail with Cloudflare 1042");
}

const customDomain = config.routes?.filter((route) => route.pattern === DOMAIN && route.custom_domain === true) ?? [];
if (customDomain.length !== 1) {
  throw new Error(`wrangler.jsonc must contain exactly one custom-domain route for ${DOMAIN}`);
}

console.log(`deploy config: workers.dev disabled; custom domain ${DOMAIN}`);
