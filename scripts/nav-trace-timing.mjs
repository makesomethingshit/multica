/**
 * MUL-7095: timing-sample validity shared by the runner and its unit pin.
 *
 * A trace is timing-usable only when its full content matches the common
 * spec assertions (ordering invariant and populated+initialized sample),
 * the sampler loop stayed healthy, and the scenario process exited zero.
 *
 * `collect` mode (base side) relaxes the ACCEPTANCE defects only:
 * the spec itself skips the blank-frame and retained-surface assertions
 * there, so the known-bad base still exits 0 in `collect` mode
 * (measured: 5/5 repeats, nav-fix-r6). A non-zero exit is therefore never
 * that expected miss, and it invalidates the sample in BOTH modes —
 * exactly like a sampler-loop failure.
 *
 * @param entry runner repeat entry `{ scenario_exit, trace }`
 * @returns `true` when the sample must NOT count for the guardrail.
 */
export const invalidForTiming = (entry) => {
  const trace = entry?.trace;
  if (!trace || trace.status !== "ok") return true;
  // Measurement-infrastructure invariant (BOTH modes): any sampler error
  // invalidates the timing sample.
  if (!Array.isArray(trace.samplerErrors) || trace.samplerErrors.length > 0)
    return true;
  // Common spec assertions (both modes): the full ordering invariant,
  // a populated+initialized sample, and both navigation timings.
  if (typeof trace.clickT !== "number") return true;
  if (typeof trace.firstDetailCommitT !== "number") return true;
  if (typeof trace.firstHostT !== "number") return true;
  if (typeof trace.firstPopulatedT !== "number") return true;
  if (typeof trace.clickToCommitMs !== "number" || trace.clickToCommitMs <= 0)
    return true;
  if (trace.firstHostT < trace.firstDetailCommitT) return true;
  if (trace.firstPopulatedT < trace.firstHostT) return true;
  if (typeof trace.clickToPopulatedMs !== "number") return true;
  if (trace.clickToPopulatedMs < trace.clickToCommitMs) return true;
  if (
    !Array.isArray(trace.samples) ||
    !trace.samples.some((s) => s.populated && s.editorInitialized === true)
  )
    return true;
  // Non-zero exit is not a usable sample in either mode. `collect` mode
  // already skips the known-bad base's acceptance assertions and that base
  // exits 0, so any non-zero exit is a failure outside the recorded status
  // (assertion, teardown, worker) and must never turn a partial run into a
  // measured sample.
  return entry.scenario_exit !== 0;
};
