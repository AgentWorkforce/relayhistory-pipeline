import { readFile, writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

export function deploymentConfig(template: Record<string, unknown>, options: {
  image?: string; source?: string; account?: string; hostname?: string;
}) {
  const { image, source, account, hostname } = options;
  const match = /^registry\.cloudflare\.com\/([a-f0-9]{32})\/96bulls-services@sha256:[a-f0-9]{64}$/.exec(image ?? "");
  if (!/^[a-f0-9]{32}$/.test(account ?? "") || !match || match[1] !== account ||
      !/^[a-f0-9]{40}$/.test(source ?? ""))
    throw new Error("Supply the verified image digest, matching operator account and approved full source SHA.");
  if (!hostname || hostname.length > 253 || hostname.split(".").length < 2 ||
      !hostname.split(".").every(label => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label)))
    throw new Error("Supply the approved bare custom-domain hostname.");
  if (!Array.isArray(template.containers) || template.containers.length !== 1)
    throw new Error("Expected exactly one reviewed native container.");
  return { ...template, account_id: account,
    routes: [{ pattern: hostname, custom_domain: true }],
    containers: [{ ...template.containers[0], image }], vars: { SOURCE_REVISION: source } };
}

async function main() {
  const root = new URL("../", import.meta.url);
  const config = deploymentConfig(JSON.parse(await readFile(new URL("wrangler.template.json", root), "utf8")), {
    image: process.argv[2], source: process.argv[3], account: process.env.CLOUDFLARE_ACCOUNT_ID,
    hostname: process.env.AMP_TRANSPORT_HOSTNAME,
  });
  await writeFile(new URL("wrangler.generated.json", root), JSON.stringify(config, null, 2) + "\n");
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
