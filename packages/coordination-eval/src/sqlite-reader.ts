import { Database } from "bun:sqlite";
import { DatabaseSync } from "node:sqlite";

interface ReadStatement<Row, Params extends (string | number)[]> {
  get(...params: Params): Row | null | undefined;
  all(...params: Params): Row[];
}

interface ReadOnlySqlite {
  readonly inTransaction: boolean;
  exec(sql: string): void;
  function(name: string, callback: () => number): void;
  query<Row, Params extends (string | number)[]>(
    sql: string,
  ): ReadStatement<Row, Params>;
  close(): void;
}

/** Bun 1.4.2 supports scalar functions through node:sqlite; bun:sqlite added
 * them in 1.4.4. Both paths keep the source read-only and enforce the SQL budget. */
export function openReadOnlySqlite(path: string): ReadOnlySqlite {
  const db =
    typeof Database.prototype.function === "function"
      ? new Database(path, { readonly: true, strict: true })
      : new DatabaseSync(path, { readOnly: true });
  return {
    get inTransaction() {
      return db instanceof Database ? db.inTransaction : db.isTransaction;
    },
    exec: (sql) => db.exec(sql),
    function(name, callback) {
      if (db instanceof Database) db.function(name, callback);
      else db.function(name, callback);
    },
    query<Row, Params extends (string | number)[]>(
      sql: string,
    ): ReadStatement<Row, Params> {
      const statement =
        db instanceof Database ? db.query(sql) : db.prepare(sql);
      return {
        get: (...params: Params) =>
          statement.get(...params) as Row | null | undefined,
        all: (...params: Params) => statement.all(...params) as Row[],
      };
    },
    close: () => db.close(),
  };
}
