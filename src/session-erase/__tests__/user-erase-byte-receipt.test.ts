/**
 * The operator user erase's byte-phase receipt (src/session-erase/user-erase-byte-receipt.ts): create-only, one
 * destination per request, shape-checked, ids hashed. CODE ONLY: the erase job does not write it yet.
 */
import { afterEach, expect, test } from "bun:test";
import { objectKey } from "../../shared/vendor.ts";
import { scratchStore } from "../../session-ingest/__tests__/support.ts";
import { readUserEraseByteReceipt, sameDestination, userEraseByteReceipt, userEraseByteReceiptKey, validStoreDestination, validUserEraseByteReceipt,
  writeUserEraseByteReceipt } from "../user-erase-byte-receipt.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const fresh = () => { const s = scratchStore(); cleanups.push(s.done); return s.store; };
const REQ = { requestId: "req-1", orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const AT = { bucket: "locker-bucket", region: "us-east-1", endpoint: null, keyRoot: "96bulls" };

test("written create-only; a retry with the same content passes; another destination for the request conflicts", async () => {
  const store = fresh();
  const r = userEraseByteReceipt(REQ, AT, ["96bulls/org-a/v1/ws=ws-a/m=srv_x/"], 1);
  await writeUserEraseByteReceipt(store, r, "req-1");
  await writeUserEraseByteReceipt(store, { ...r, verifiedAtMs: 2 }, "req-1");
  expect(await readUserEraseByteReceipt(store, "96bulls", "req-1")).toEqual(r);
  await expect(writeUserEraseByteReceipt(store, userEraseByteReceipt(REQ, { ...AT, region: "eu-west-1" }, [], 3), "req-1")).rejects.toThrow("user_erase_byte_receipt_conflict");
  expect(await readUserEraseByteReceipt(store, "96bulls", "req-2")).toBeNull();
  for (const id of ["req-1", "org-a", "ws-a", "user-1"]) expect(JSON.stringify(r)).not.toContain(id);
  expect(userEraseByteReceiptKey("96bulls", "req-1")).not.toContain("req-1");
});

test("shape checks: receipts and destinations", async () => {
  const r = userEraseByteReceipt(REQ, AT, [], 1);
  expect(validUserEraseByteReceipt(r)).toBe(true);
  for (const bad of [{ ...r, extra: 1 }, { ...r, destination: { ...r.destination, bucket: "Bad_Bucket" } }, { ...r, prefixes: "x" }, { ...r, verifiedAtMs: 0 }])
    expect(validUserEraseByteReceipt(bad)).toBe(false);
  const store = fresh();
  await store.put(objectKey(userEraseByteReceiptKey("96bulls", "req-1")), new TextEncoder().encode("{\"v\":1}"));
  await expect(readUserEraseByteReceipt(store, "96bulls", "req-1")).rejects.toThrow();
  const d = { keyRoot: "96bulls", main: { bucket: "locker-bucket", region: "us-east-1", endpoint: null }, ledger: null };
  expect(validStoreDestination(d)).toBe(true);
  expect(validStoreDestination({ ...d, ledger: undefined })).toBe(false);
  expect(sameDestination(d, { ...d, main: { ...d.main, endpoint: "https://x" } })).toBe(false);
  expect(sameDestination(d, { ...d, ledger: { bucket: "ledger-bucket", region: "us-east-1", endpoint: null } })).toBe(false);
});
