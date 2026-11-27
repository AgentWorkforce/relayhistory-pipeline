/** neon-s3-erasure exit status (review of c1d5bf61, finding 1): an expiry stop is 75, never success. */
import { expect, test } from "bun:test";
import { runStatus, stalePassStatus } from "../neon-s3-erasure/main.ts";
import { EXIT_CREDENTIALS_RENEW } from "../short-lived-credentials.ts";

const code = (f: () => unknown) => { try { return f(); } catch (e) { return (e as { code?: string }).code ?? String(e); } };

test("stale-prefix pass: completion 0, expiry mid-pass 75 (ok or not), a genuine failed erase refuses", () => {
  expect(stalePassStatus({ ok: true }, false)).toBe(0);
  expect(stalePassStatus({ ok: false }, true)).toBe(EXIT_CREDENTIALS_RENEW); // stopped part-way on expiry
  expect(stalePassStatus({ ok: true }, true)).toBe(EXIT_CREDENTIALS_RENEW); // the stop landed after the last item
  expect(code(() => stalePassStatus({ ok: false }, false))).toBe("stale_prefix_pass_stopped");
  expect(code(() => stalePassStatus({ ok: true, refused: "stale_prefix_lease_held" }, true))).toBe("stale_prefix_lease_held");
});

test("full job: 75 whenever it stopped on an expiring key, else the job's own status", () => {
  expect(runStatus(0, false)).toBe(0);
  expect(runStatus(0, true)).toBe(EXIT_CREDENTIALS_RENEW);
});
