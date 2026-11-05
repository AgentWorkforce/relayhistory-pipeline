/** Synthetic immutable previous-producer fixture; no production key/SQL/IO. */
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
const root = process.env.HISTORY_SUMMARY_PRODUCER_ROOT,
  head = "09cc699627862c958510572152b416f5d873e769",
  out = process.argv[2];
if (!root || !out)
  throw Error(
    "Explicit frozen producer root and NEW output directory required",
  );
if (
  execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: root,
    encoding: "utf8",
  }).trim() !== head
)
  throw Error("Producer HEAD must be frozen09cc");
if (
  execFileSync("git", ["status", "--porcelain"], {
    cwd: root,
    encoding: "utf8",
  }).trim()
)
  throw Error("Producer checkout must be clean");
if (existsSync(out)) throw Error("Output already exists");
mkdirSync(out, { recursive: true });
const from = (file: string) => import(pathToFileURL(resolve(root!, file)).href);
const { createFsStore } = await from("packages/object-store/src/fs.ts");
const { readAll } = await from("packages/object-store/src/index.ts");
const { prepareFullModelResults } = await from(
  "src/bob-costas/full-model-artifacts.ts",
);
const { masterKeyring, parseMasterKeys } = await from("src/shared/keyring.ts");
const { initialFullModelCheckpoint } = await from(
  "src/bob-costas/full-model-work.ts",
);
const { fullModelSourceBinding } = await from(
  "src/bob-costas/full-model-input.ts",
);
const files = [
  "src/bob-costas/full-model-artifacts.ts",
  "src/bob-costas/summary-objects.ts",
  "src/shared/history-source-authority.ts",
  "src/shared/keyring.ts",
  "packages/object-store/src/layout.ts",
];
const sources = Object.fromEntries(
  files.map((file) => {
    const bytes = readFileSync(root + "/" + file);
    if (
      !bytes.equals(
        execFileSync("git", ["show", head + ":" + file], { cwd: root }),
      )
    )
      throw Error("frozen crypto source changed:" + file);
    return [file, createHash("sha256").update(bytes).digest("hex")];
  }),
);
const id = "12345678-1234-1234-1234-123456789abc",
  identity = {
    orgId: "org_fixture",
    workspaceId: "00000000-0000-4000-8000-000000000001",
    source: "claude",
    sessionId: "native:summary-fixture",
  };
const captured = {
  identity,
  source: { epoch: "9007199254740993", fingerprint: "a".repeat(64) },
  authorityId: id,
  maskSha256: "b".repeat(64),
  recordCount: "2",
  schemaGeneration: "c".repeat(64),
  derivedAnchorKey:
    "96bulls/org_fixture/upload-generations/g=" +
    id +
    "/v1/ws=" +
    identity.workspaceId +
    "/m=history_scope/h=claude/d=2026/11/01/s=logical/publications/" +
    id +
    "/manifest.json",
};
const keyring = await masterKeyring(
    parseMasterKeys("reader-fixture:" + "d".repeat(64)),
  ),
  store = createFsStore({ root: out + "/objects" });
const checkpoint = initialFullModelCheckpoint(
  fullModelSourceBinding(captured),
  "e".repeat(64),
);
checkpoint.inputComplete = true;
checkpoint.units = "1";
checkpoint.calls = "1";
const result = await prepareFullModelResults(
  {
    db: {
      query: async () => ({ rows: [{ result: { registered: true } }] }),
    } as never,
    store,
    keyring,
    captured,
    workId: id,
    configSha256: "e".repeat(64),
    sourceBinding: checkpoint.sourceBinding,
    keyId: "hkdf1:reader-fixture",
  },
  {
    checkpoint,
    model: "fixture-model",
    builtAt: "2026-11-04T00:00:00.000Z",
    signal: AbortSignal.timeout(10000),
    conclusion: {
      short: "FIXTURE SHORT full source 😀",
      long: "FIXTURE LONG full-source evidence, exact Unicode \ud800 and completed observations.",
      state: "done",
      stateReason: "encrypted suggestion only",
    },
  },
);
const objects: Record<string, unknown> = {},
  claims: Record<string, unknown> = {};
for (const pointer of result.pointers) {
  const object = await store.getObject(pointer.objectKey as never),
    body = await readAll(object.body),
    file = pointer.kind + ".cipher";
  writeFileSync(out + "/" + file, body);
  objects[pointer.objectKey!] = {
    file,
    sha256: pointer.objectSha256,
    bytes: body.length,
    meta: object.meta,
  };
  claims[pointer.kind] = {
    version: 1,
    kind: pointer.kind,
    exists: true,
    required: true,
    ready: true,
    identity,
    source: captured.source,
    authority: {
      authorityId: id,
      maskSha256: captured.maskSha256,
      recordCount: captured.recordCount,
      derivedAnchorKey: captured.derivedAnchorKey,
      schemaGeneration: captured.schemaGeneration,
    },
    storage: { provider: "s3", bucket: "fixture-bucket", region: "us-east-1" },
    pointer,
    state: { state: "active", classifiedAt: "2026-11-04T00:00:00.000000Z" },
  };
}
writeFileSync(
  out + "/fixture.json",
  JSON.stringify(
    {
      protocol: "owned-history-summary-crypto-fixture-v1",
      sourceHead: head,
      sources,
      fixtureOnly: true,
      keyId: "reader-fixture",
      keyNote:
        "Synthetic only: test derives reader-fixture with 64 repetitions of hex d; no production secret or key material file.",
      claims,
      objects,
    },
    null,
    2,
  ) + "\n",
);
console.log(
  JSON.stringify({
    sourceHead: head,
    objects: Object.keys(objects).length,
    fixtureOnly: true,
  }),
);
