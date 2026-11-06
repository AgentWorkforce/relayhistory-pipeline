import { describe, expect, test } from "bun:test";
import { assertNoNeonEdge, NEON_EXIT_ENV, neonExitProfile, NeonExitRefusal } from "../neon-exit.ts";

describe("SESSIONS_NEON_EXIT profile", () => {
  test("unset is off (the legacy posture)", () => {
    expect(NEON_EXIT_ENV).toBe("SESSIONS_NEON_EXIT");
    expect(neonExitProfile({})).toEqual({ enabled: false });
    expect(neonExitProfile({ SESSIONS_NEON_EXIT: undefined })).toEqual({ enabled: false });
  });

  test('"false" is off, "true" is on', () => {
    expect(neonExitProfile({ SESSIONS_NEON_EXIT: "false" }).enabled).toBe(false);
    expect(neonExitProfile({ SESSIONS_NEON_EXIT: "true" }).enabled).toBe(true);
  });

  test("any other value throws at startup, naming the variable and never echoing the value", () => {
    for (const value of ["", "1", "0", "TRUE", "False", " true", "yes", "on", true, 1, null]) {
      let message = "";
      try { neonExitProfile({ SESSIONS_NEON_EXIT: value }); } catch (error) { message = (error as Error).message; }
      expect(message).toBe('96bulls: SESSIONS_NEON_EXIT must be unset, "true" or "false"');
    }
  });

  test("the decision is frozen", () => {
    for (const env of [{}, { SESSIONS_NEON_EXIT: "true" }]) {
      const profile = neonExitProfile(env);
      expect(Object.isFrozen(profile)).toBe(true);
      expect(() => { (profile as { enabled: boolean }).enabled = !profile.enabled; }).toThrow();
    }
  });

  test("assertNoNeonEdge: off is a no-op; on refuses the first present edge, naming variable, consumer and setting", () => {
    const facts = { "SETTING_A is set": false, "SETTING_B is set": true, "SETTING_C is set": true };
    expect(() => assertNoNeonEdge(neonExitProfile({}), "the consumer", facts)).not.toThrow();
    expect(() => assertNoNeonEdge(neonExitProfile({ SESSIONS_NEON_EXIT: "true" }), "the consumer", { "SETTING_A is set": false })).not.toThrow();
    let error: unknown;
    try { assertNoNeonEdge(neonExitProfile({ SESSIONS_NEON_EXIT: "true" }), "the consumer", facts); } catch (e) { error = e; }
    expect(error).toBeInstanceOf(NeonExitRefusal);
    const refusal = error as NeonExitRefusal;
    expect(refusal.code).toBe("neon_exit_refused");
    expect(refusal.message).toBe("96bulls: SESSIONS_NEON_EXIT=true refuses the consumer: SETTING_B is set");
    expect([refusal.consumer, refusal.setting]).toEqual(["the consumer", "SETTING_B is set"]);
  });
});
