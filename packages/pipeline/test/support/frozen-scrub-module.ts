/**
 * A frozen rule set's `lib/scrub.ts` (a `test/fixtures/scrub-ruleset-*.ts` module) as the
 * live raw-chunks segmenter imports it, for suites that `vi.doMock` scrub.js to a frozen
 * source and load the current raw-chunks.ts on top of it.
 *
 * Since rule set 9's records path change, the segmenter also imports `isPublicSelectorKey`
 * and `scrubContainedSegment`, which the frozen sources predate. A frozen rule set has no
 * public selector context, so the key test is always false: every public flag stays off,
 * `scrubContainedSegment` is never reached (it throws if it is), and the segmenter runs
 * exactly as it did against that source. The fixture files themselves stay byte for byte.
 */
export function frozenScrubModule<T extends object>(frozen: T) {
  return {
    ...frozen,
    isPublicSelectorKey: (_key: string) => false,
    scrubContainedSegment: (_value: string): [string, boolean] => {
      throw new Error("frozen scrub rule set has no public selector containers");
    },
  };
}
