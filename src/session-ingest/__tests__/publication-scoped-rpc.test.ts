import { expect, test } from "bun:test";
import type { SemanticBoundedSql } from "../../search-features/semantic-copy-postgres.ts";
import { scopedPublicationCycle, scopedPublicationRpc, type PublicationScopeBinding } from "../publication-scoped-rpc.ts";

const stage = "11111111-1111-1111-1111-111111111111";
const binding: PublicationScopeBinding = {
  admissionId: stage, planSha256: "b".repeat(64), frontierSha256: "c".repeat(64),
  tenant: {orgId:"org",workspaceId:"ws",userId:"user",originId:"origin"},
  target: {keyRoot:"96bulls",bucket:"fixture",region:"us-east-1",provider:"s3",generation:"a".repeat(64)},
};
const signal = new AbortController().signal;
function harness(reply: (name: string) => unknown) {
  const calls: { name: string; payload: Record<string,unknown> }[] = [];
  const db: SemanticBoundedSql = {
    async query(){throw Error("unbounded");},
    async queryBounded<T>(sql: string, args: unknown[] | undefined) {
      const name = sql.match(/^SELECT sessions\.ingest_publication_(\w+)\(\$1::jsonb\) AS result$/)?.[1];
      if (!name) throw Error("unexpected statement");
      calls.push({name,payload:JSON.parse(String(args?.[0]))});
      return {rows:[{result:reply(name)} as T]};
    },
  };
  return {db,calls};
}
const deps = {target:binding.target, store:()=>{ throw Error("unexpected store"); }};
test("pause, scoped discovery, rollback and refused missing contract", async () => {
  const {db,calls} = harness(() => ({stageTokens:[]}));
  expect((await scopedPublicationCycle(db,binding,deps)).enabled).toBe(false);
  expect(calls).toHaveLength(0);
  expect((await scopedPublicationCycle(db,binding,deps,{enabled:true})).examined).toBe(0);
  expect(calls).toEqual([{name:"scoped_candidates_v1",payload:{binding,limit:1,generation:binding.target.generation,
    keyRoot:"96bulls",region:"us-east-1",bucket:"fixture"}}]);
  expect((await scopedPublicationCycle(db,binding,deps,{enabled:false})).enabled).toBe(false);
  expect(calls).toHaveLength(1);
  const missing=harness(()=>{ throw Object.assign(Error("missing wrapper"),{code:"42883"}); });
  await expect(scopedPublicationCycle(missing.db,binding,deps,{enabled:true})).rejects.toThrow("missing wrapper");
  expect(missing.calls.map(c=>c.name)).toEqual(["scoped_candidates_v1"]);
});
test("refuse a foreign target, excess candidates, unselected stage and lease-free retry", async () => {
  const {db,calls}=harness(()=>({stageTokens:[stage,stage]}));
  expect(()=>scopedPublicationRpc(db,{...binding.target,bucket:"foreign"},binding)).toThrow("target");
  const rpc=scopedPublicationRpc(db,binding.target,binding);
  await expect(rpc.claim(stage,signal)).rejects.toThrow("unselected");
  await expect(rpc.candidates(2,signal)).rejects.toThrow("candidate_limit");
  await expect(rpc.candidates(1,signal)).rejects.toThrow("candidate_result");
  await expect(rpc.defer(stage,"40001",signal)).rejects.toThrow("lease_free_defer");
  expect(calls.map(c=>c.name)).toEqual(["scoped_candidates_v1"]);
});
