import { readFileSync } from "node:fs";
import { describe, expect, it } from "bun:test";
import { privateSqlQueryTarget } from "../private-sql-query-target.ts";

const accepted = {
  host: "cloud-sql-proxy.96bulls.svc",
  port: 5432,
  database: "bulls96",
  role: "bulls96_app",
};

describe("private SQL query target", () => {
  it("accepts the in-cluster proxy without a password or server address", () => {
    expect(privateSqlQueryTarget(accepted)).toEqual(accepted);
  });

  it("accepts a reported server address inside 10.80.0.0/20", () => {
    expect(privateSqlQueryTarget({ ...accepted, serverAddress: "10.80.0.3" })).toEqual({
      ...accepted,
      serverAddress: "10.80.0.3",
    });
    expect(privateSqlQueryTarget({ ...accepted, serverAddress: "10.80.15.255" }).serverAddress).toBe(
      "10.80.15.255",
    );
  });

  it("rejects a password, URL, userinfo, or public host", () => {
    expect(() => privateSqlQueryTarget({ ...accepted, password: "" })).toThrow("rejects a password");
    expect(() => privateSqlQueryTarget({ ...accepted, password: "secret" })).toThrow("rejects a password");
    expect(() =>
      privateSqlQueryTarget({ ...accepted, host: "postgres://bulls96_app:secret@10.80.0.3:5432/bulls96" }),
    ).toThrow("rejects a URL");
    expect(() => privateSqlQueryTarget({ ...accepted, database: "bulls96?sslmode=disable" })).toThrow(
      "rejects a URL",
    );
    expect(() => privateSqlQueryTarget({ ...accepted, host: "10.80.0.3" })).toThrow(
      "cloud-sql-proxy.96bulls.svc",
    );
    expect(() => privateSqlQueryTarget({ ...accepted, port: 5433 })).toThrow("port must be 5432");
  });

  it("rejects a server address outside the private SQL range", () => {
    expect(() => privateSqlQueryTarget({ ...accepted, serverAddress: "10.80.16.0" })).toThrow(
      "10.80.0.0/20",
    );
    expect(() => privateSqlQueryTarget({ ...accepted, serverAddress: "172.20.0.2" })).toThrow(
      "10.80.0.0/20",
    );
    expect(() => privateSqlQueryTarget({ ...accepted, serverAddress: "10.80.0.3/32" })).toThrow(
      "IPv4",
    );
    expect(() => privateSqlQueryTarget({ ...accepted, serverAddress: "::ffff:10.80.0.3" })).toThrow(
      "IPv4",
    );
  });

  it("is not imported by the Alchemy stack", () => {
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    expect(stack).not.toContain("privateSqlQueryTarget");
    expect(stack).not.toContain("private-sql-query-target");
  });
});
