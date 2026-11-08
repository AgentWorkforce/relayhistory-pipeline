// Image-build entry point only. Runtime requests never install dependencies.
import { spawnSync } from "node:child_process";
import { mkdirSync, symlinkSync } from "node:fs";
import { AMP_CLI_VERSION } from "../src/amp/provider.ts";

if (process.platform !== "linux" || !["x64", "arm64"].includes(process.arch))
  throw new Error("Amp transport image requires Linux x64 or arm64");
const root = "/opt/relay-amp";
const nativePackage = `@ampcode/cli-linux-${process.arch}`;
mkdirSync(root, { recursive: true });
// Install Amp's pinned native package directly: the image needs neither npm,
// Node, nor wrapper postinstall scripts at request time.
const installed = spawnSync(
  process.execPath,
  [
    "add",
    "--cwd",
    root,
    "--ignore-scripts",
    "--exact",
    `${nativePackage}@${AMP_CLI_VERSION}`,
  ],
  { stdio: "inherit" },
);
if (installed.status !== 0)
  throw new Error("Amp CLI image installation failed");
mkdirSync(`${root}/node_modules/.bin`, { recursive: true });
const binary = `${root}/node_modules/.bin/amp`;
symlinkSync(`../${nativePackage}/amp`, binary);
const version = spawnSync(binary, ["--version"], {
  encoding: "utf8",
  timeout: 30_000,
});
if (
  version.status !== 0 ||
  version.stdout.trim().split(/\s/)[0] !== AMP_CLI_VERSION
)
  throw new Error("Pinned Amp CLI cannot run in this image");
console.log(`Installed Amp CLI ${AMP_CLI_VERSION}`);
