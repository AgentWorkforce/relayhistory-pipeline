/** READ_API_CANONICAL_DEADLINE_MS (service-canonical.ts): self-host canonical read deadline. */
import { describe, expect, test } from "bun:test";
import { eventMessageIdDeps } from "../lambda.ts";
import { CANONICAL_DEADLINE_MS_MAX, CANONICAL_DEADLINE_MS_MIN, canonicalDeadlineMs, serviceCanonicalDeps } from "../service-canonical.ts";

const deadline = (v: string | undefined) => canonicalDeadlineMs(v === undefined ? {} : { READ_API_CANONICAL_DEADLINE_MS: v });
const MESSAGE = "session read api: READ_API_CANONICAL_DEADLINE_MS must be a whole number of milliseconds in 3000..60000";

describe("canonicalDeadlineMs", () => {
  test("unset or empty: undefined, so the routes keep their 3000 default", () => {
    expect(CANONICAL_DEADLINE_MS_MIN).toBe(3000);
    expect(CANONICAL_DEADLINE_MS_MAX).toBe(60000);
    expect(deadline(undefined)).toBeUndefined();
    expect(deadline("")).toBeUndefined();
  });

  test.each([["3000", 3000], ["30000", 30000], ["60000", 60000]])("%p is accepted as %p", (raw, want) => {
    expect(deadline(raw)).toBe(want);
  });

  test.each([
    "2999", "60001", "1", "0", "-1", "abc", "30000.5", " 30000", "30000 ", "+30000", "030000", "3e4", "0x7530",
    "30s", "30_000", " ", "Infinity", "NaN", "99999999999999999999",
  ])("%j fails startup, naming the variable and range without echoing the value", (raw) => {
    let message = "";
    try { deadline(raw); } catch (e) { message = (e as Error).message; }
    expect(message).toBe(MESSAGE);
  });
});

describe("serviceCanonicalDeps applies the deadline to every canonical dep", () => {
  const env = {
    LOCKER_BUCKET: "bucket",
    LOCKER_ENDPOINT: "http://127.0.0.1:9",
    LOCKER_REGION: "us-east-1",
    LOCKER_ACCESS_KEY_ID: "synthetic-access-key",
    LOCKER_SECRET_ACCESS_KEY: "synthetic-secret-key",
    PIPELINE_KEY_PREFIX: "96bulls",
    PIPELINE_ORG_FROM_PREFIX: "true",
  };
  const run = (async () => { throw new Error("no query at startup"); }) as never;

  test.each([[undefined], [""]])("READ_API_CANONICAL_DEADLINE_MS=%p: no dep carries deadlineMs (byte-identical)", async (raw) => {
    const deps = (await serviceCanonicalDeps(raw === undefined ? env : { ...env, READ_API_CANONICAL_DEADLINE_MS: raw }, run))!;
    for (const dep of [deps.timing, deps.turns, deps.catalog, deps.events]) expect("deadlineMs" in dep).toBe(false);
    expect("deadlineMs" in eventMessageIdDeps({ READ_API_EVENT_MESSAGE_IDS: "true" }, deps.turns).eventMessageIds!).toBe(false);
  });

  test("30000 reaches timing, turns, catalog, events and (through turns) message-id reads", async () => {
    const deps = (await serviceCanonicalDeps({ ...env, READ_API_CANONICAL_DEADLINE_MS: "30000" }, run))!;
    expect(deps.timing.deadlineMs).toBe(30000);
    expect(deps.turns.deadlineMs).toBe(30000);
    expect(deps.catalog.deadlineMs).toBe(30000);
    expect(deps.events.deadlineMs).toBe(30000);
    expect(eventMessageIdDeps({ READ_API_EVENT_MESSAGE_IDS: "true" }, deps.turns).eventMessageIds!.deadlineMs).toBe(30000);
  });

  test.each(["2999", "60001", "abc", "30000.5", "-1", " 30000"])("%p refuses startup", async (raw) => {
    await expect(serviceCanonicalDeps({ ...env, READ_API_CANONICAL_DEADLINE_MS: raw }, run)).rejects.toThrow(MESSAGE);
  });

  test("LOCKER_BUCKET unset: nothing is wired, so the deadline is not read (as the scope knobs)", async () => {
    expect(await serviceCanonicalDeps({ READ_API_CANONICAL_DEADLINE_MS: "bad" }, run)).toBeUndefined();
  });
});
