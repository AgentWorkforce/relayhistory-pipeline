/** Independent-process harness: open a state path with the FROZEN main 341dd5a9 SyncState (state.ts, verbatim). */
import { SyncState } from "./state.ts";
try {
  new SyncState(process.argv[2]!);
  process.stdout.write(JSON.stringify({ opened: true }) + "\n");
} catch (e) {
  process.stdout.write(JSON.stringify({ opened: false, message: String((e as Error).message) }) + "\n");
  process.exitCode = 1;
}
