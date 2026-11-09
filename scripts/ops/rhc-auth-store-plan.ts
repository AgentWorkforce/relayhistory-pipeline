/** Offline only. Non-secret JSON on stdin; stdout is an unqualified policy proposal. */
import { buildRhcAuthStorePlan, canonicalPolicyJson, RhcAuthStorePolicyError } from "../../stacks/aws/rhc-auth-store-policy.ts";
export async function runRhcAuthStorePlan(args: string[], input: AsyncIterable<Uint8Array | string>, output: (text: string) => void): Promise<number> {
  try {
    if (args.length) {
      if (args.length === 1 && args[0] === "--help") { output("Offline C auth policy plan: non-secret JSON on stdin. No execute/apply mode.\n"); return 0; }
      throw new RhcAuthStorePolicyError("arguments");
    }
    const chunks: Buffer[] = []; let bytes = 0;
    for await (const chunk of input) {
      const b = Buffer.from(chunk); bytes += b.byteLength;
      if (bytes > 65_536) throw new RhcAuthStorePolicyError("input_bound");
      chunks.push(b);
    }
    let value: unknown;
    try { value = JSON.parse(Buffer.concat(chunks).toString("utf8")); }
    catch { throw new RhcAuthStorePolicyError("input_json"); }
    output(canonicalPolicyJson(buildRhcAuthStorePlan(value)) + "\n");
    return 0;
  } catch (e) {
    const code = e instanceof RhcAuthStorePolicyError ? e.code : "input_read";
    output(JSON.stringify({ status: "refused", error: code }) + "\n"); return 1;
  }
}
if (import.meta.main) process.exitCode = await runRhcAuthStorePlan(process.argv.slice(2), process.stdin, text => process.stdout.write(text));
