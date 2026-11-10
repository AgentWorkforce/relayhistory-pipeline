/** The read worker's pg Client 'error' log decision (src/pg-close.ts): the benign
 * "This socket has been closed" after `end()` logs at info; anything else keeps the
 * error line. Pure logic over a fake client; no database is needed. */
import type pg from "pg";
import { expect, test } from "vitest";
import { endPg, isPgEnding, pgClientErrorLog, type PgClientErrorLog } from "../src/pg-close.ts";

/** A fake EventEmitter-like client wired the way connectPg wires its 'error' listener. */
function fakeClient() {
  const handlers: ((err: unknown) => void)[] = [];
  const lines: PgClientErrorLog[] = [];
  const client = {
    ended: false,
    on(_event: "error", fn: (err: unknown) => void) { handlers.push(fn); },
    emit(err: unknown) { for (const fn of handlers) fn(err); },
    async end() { client.ended = true; },
  };
  const asPg = client as unknown as pg.Client;
  client.on("error", (err) => lines.push(pgClientErrorLog(isPgEnding(asPg), err)));
  return { client, asPg, lines };
}

const socketClosed = () => new Error("This socket has been closed");

test("a code-less error after the ending mark logs one info line and no error line", async () => {
  const { client, asPg, lines } = fakeClient();
  await endPg(asPg);
  expect(client.ended).toBe(true);
  client.emit(socketClosed());
  expect(lines).toEqual([{ level: "info", message: "read worker pg close after end", error_code: "post_end_socket_close" }]);
  expect(lines.filter((l) => l.level === "error")).toHaveLength(0);
});

test("a code-less error before the ending mark keeps the error line with pg_error", () => {
  const { client, asPg, lines } = fakeClient();
  expect(isPgEnding(asPg)).toBe(false);
  client.emit(socketClosed());
  expect(lines).toEqual([{ level: "error", message: "read worker pg error", error_code: "pg_error" }]);
});

test("an error with a SQLSTATE code after the ending mark keeps the error line with that code", async () => {
  const { client, asPg, lines } = fakeClient();
  await endPg(asPg);
  client.emit(Object.assign(new Error("relation does not exist"), { code: "42P01" }));
  expect(lines).toEqual([{ level: "error", message: "read worker pg error", error_code: "42P01" }]);
});

test("the ending mark is per client", async () => {
  const a = fakeClient();
  const b = fakeClient();
  await endPg(a.asPg);
  b.client.emit(socketClosed());
  expect(b.lines).toEqual([{ level: "error", message: "read worker pg error", error_code: "pg_error" }]);
});
