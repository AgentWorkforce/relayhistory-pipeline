/** OFFLINE proof entrypoint only: fake CF operation, real private transport and native recovery sink. */
import { runPrivateBootstrap } from "../private-state-bootstrap-stdio.ts";
import { prepareOnePasswordSink } from "../private-state-bootstrap.ts";
const accountId = "f7232cb80f6fab86a95426302af243e4";
globalThis.fetch = (() => { throw new Error("offline proof: network forbidden"); }) as unknown as typeof fetch;
await runPrivateBootstrap({
  input: process.stdin,
  write: (line) => { process.stdout.write(line); },
  close: () => { process.stdin.destroy(); },
}, [], async (_argv, port) => {
  const generation = "a".repeat(64);
  const reserved = await prepareOnePasswordSink({
    accountId, secretsStoreId: "d".repeat(32), onePasswordVaultId: "6hmhqxvcbcmhh24yahdflbbkge",
  } as any, generation, port);
  const url = "https://alchemy-state-store-96bulls.synthetic.workers.dev";
  const receipt = await reserved.sink({
    protocol: "private-state-bootstrap-v1", status: "created", generation, accountId, url,
    authToken: "c".repeat(64),
    encodedBootstrapState: Object.fromEntries([
      "StateStoreAuthTokenValue", "Api", "StateStoreSecrets", "AlchemyStateStoreToken",
      "StateStoreEncryptionKeyValue", "StateStoreEncryptionKey",
    ].map((key) => [key, { synthetic: true }])),
  });
  return { operation: "created-control-store", generation, url, credentialItemId: receipt.itemId,
    controlResources: 6, ingestApplied: false, edgePreview: false };
});
