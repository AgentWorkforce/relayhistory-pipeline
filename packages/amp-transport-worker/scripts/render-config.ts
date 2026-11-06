import { readFile, writeFile } from "node:fs/promises";
const root = new URL("../", import.meta.url);
const image = process.argv[2];
const source = process.argv[3];
if (!/^registry\.cloudflare\.com\/f7232cb80f6fab86a95426302af243e4\/96bulls-services@sha256:[a-f0-9]{64}$/.test(image ?? "") ||
    !/^[a-f0-9]{40}$/.test(source ?? ""))
  throw new Error("Supply the verified Cloudflare image digest and its approved full source SHA.");
const config = JSON.parse(await readFile(new URL("wrangler.template.json", root), "utf8"));
config.containers[0].image = image;
config.vars = { SOURCE_REVISION: source };
await writeFile(new URL("wrangler.generated.json", root), JSON.stringify(config, null, 2) + "\n");
