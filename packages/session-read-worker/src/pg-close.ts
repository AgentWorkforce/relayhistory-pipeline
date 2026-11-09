/**
 * The benign post-end socket close of a read worker pg client. After `end()`
 * closes the Workers socket, pg-cloudflare's pending read rejects with the
 * runtime's "This socket has been closed" (an Error with no `.code`); pg only
 * swallows ECONNRESET/EPIPE while ending, so the Client re-emits it as 'error'.
 * A client is marked ending before `end()`; a code-less error on a marked
 * client logs at info. Anything else (an error before `end()`, or one carrying
 * a SQLSTATE or errno code) keeps the error line unchanged.
 */
import type pg from "pg";

const endingClients = new WeakSet<pg.Client>();

/** Marks the client as ending, then ends it. */
export function endPg(client: pg.Client): Promise<void> {
  endingClients.add(client);
  return client.end();
}

export function isPgEnding(client: pg.Client): boolean {
  return endingClients.has(client);
}

export interface PgClientErrorLog {
  level: "info" | "error";
  message: string;
  error_code: string;
}

/** The log line for a pg Client 'error' event. */
export function pgClientErrorLog(isEnding: boolean, err: unknown): PgClientErrorLog {
  const code = (err as { code?: string } | null | undefined)?.code;
  if (isEnding && code === undefined) {
    return { level: "info", message: "read worker pg close after end", error_code: "post_end_socket_close" };
  }
  return { level: "error", message: "read worker pg error", error_code: code ?? "pg_error" };
}
