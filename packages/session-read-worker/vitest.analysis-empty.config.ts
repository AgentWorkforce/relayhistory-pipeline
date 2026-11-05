import base from "./vitest.analysis-cache.config.ts";
export default {
  ...base,
  test: { ...base.test, include: ["test/analysis-empty.proof.ts"] },
};
