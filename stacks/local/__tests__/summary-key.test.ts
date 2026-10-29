import { describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SERVICES_CONTAINER_UID, summaryKeyAccess, summaryKeyPath } from "../topology.ts";

describe("local summary master key custody", () => {
  it("lives in the runtime tmpfs, not .96bulls/", () => {
    expect(summaryKeyPath("local", 1000)).toMatch(/\/96bulls\/local\/summary-master-keys$/);
    expect(summaryKeyPath("local", 1000)).not.toContain(".96bulls");
  });

  it("is never loosened: 0600, plus one read ACL for the container uid only when the host uid differs", () => {
    expect(SERVICES_CONTAINER_UID).toBe(1000);
    expect(summaryKeyAccess(1000)).toEqual({ mode: 0o600 });
    expect(summaryKeyAccess(0)).toEqual({ mode: 0o600, aclReadForUid: 1000 });
    expect(summaryKeyAccess(1001)).toEqual({ mode: 0o600, aclReadForUid: 1000 });
  });

  it("the ACL mechanism grants exactly u:<uid>:r and keeps group/other at none", () => {
    if (spawnSync("setfacl", ["--version"]).status !== 0) return; // no acl tools on this host
    const f = join(mkdtempSync(join(tmpdir(), "sk-")), "keys");
    writeFileSync(f, "x\n", { mode: 0o600 });
    chmodSync(f, 0o600);
    const access = summaryKeyAccess(4242, 4343); // host uid != container uid
    const r = spawnSync("setfacl", ["-m", `u:${access.aclReadForUid}:r`, f]);
    if (r.status !== 0) return; // filesystem without ACL support: the CLI fails closed there
    const acl = spawnSync("getfacl", ["-n", "-p", f], { encoding: "utf8" }).stdout;
    expect(acl).toContain("user:4343:r--");
    expect(acl).toMatch(/other::---/);
    expect(statSync(f).mode & 0o007).toBe(0);
  });
});
