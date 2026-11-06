import { expect, test } from "bun:test";
import { deploymentConfig } from "./render-config.ts";
const account = "0".repeat(32);
const template = { name: "native-test", containers: [{ class_name: "AmpTransport", max_instances: 1, image: "UNUSABLE" }] };
const approved = { account, image: `registry.cloudflare.com/${account}/96bulls-services@sha256:${"a".repeat(64)}`,
  source: "b".repeat(40), hostname: "amp-transport.example.com" };

test("operator deployment values are required and cannot silently use another account", () => {
  const config = deploymentConfig(template, approved);
  expect(config.account_id).toBe(account);
  expect(config.containers[0]).toEqual({ class_name: "AmpTransport", max_instances: 1, image: approved.image });
  expect(config.routes).toEqual([{ pattern: approved.hostname, custom_domain: true }]);
  expect(config.vars.SOURCE_REVISION).toBe(approved.source);
  expect(template.containers[0].image).toBe("UNUSABLE");
  for (const change of [ { account: undefined }, { account: "1".repeat(32) }, { source: "main" },
    { image: approved.image.replace("@sha256:" + "a".repeat(64), ":latest") },
    { hostname: undefined }, { hostname: "https://transport.example.com" }, { hostname: "../other" },
    { hostname: "bad..example.com" }, { hostname: "transport.example.com:443" } ])
    expect(() => deploymentConfig(template, { ...approved, ...change })).toThrow();
});

test("the production template supplies an explicit valid container application name", async () => {
  const production = await Bun.file(new URL("../wrangler.template.json", import.meta.url)).json();
  const config = deploymentConfig(production, approved);
  expect(config.name).toBe("96bulls-amp-transport");
  expect(config.containers[0]).toMatchObject({
    name: "amp-transport", class_name: "AmpTransport", instance_type: "basic", max_instances: 1,
  });
  // Containers reject application names beginning with a digit, even though
  // the Worker name may begin with one. Do not inherit the Worker name.
  expect(config.containers[0].name).toMatch(/^[a-z][a-z0-9-]*$/);
  expect(config.containers[0].image).toBe(approved.image);
});
