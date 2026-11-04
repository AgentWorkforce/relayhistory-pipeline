/** Disposable proof entry, never an infrastructure handler. Runs the packaged
 * finite-duration consumer under Node against owned fixture ports. */
import { openHoopDb } from "../shared/db.ts";
import { lockerStore, lockerManifestReader } from "../shared/locker.ts";
import { keyringFromEnv } from "../shared/keyring.ts";
import { derivedTick } from "./derived-tick.ts";

if (process.argv.includes("--run-owned-fixture")) {
  const db = openHoopDb();
  try {
    const result = await derivedTick({ db, store: lockerStore().store,
      reader: lockerManifestReader(), keyring: await keyringFromEnv(), remainingMs: () => 300_000 });
    console.log(`BUNDLED_DERIVED_RESULT=${JSON.stringify(result)}`);
  } finally { await db.end(); }
}
