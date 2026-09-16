import assert from "node:assert/strict";
import test from "node:test";

import { invalidForTiming } from "./nav-trace-timing.mjs";

// MUL-7095: pin sampler-error and exit-code handling across the
// collect/accept split (Case 1~5 from the follow-up spec).
const validTrace = () => ({
  status: "ok",
  acceptance: "accept",
  clickT: 100,
  firstDetailCommitT: 200,
  firstHostT: 200,
  firstPopulatedT: 200,
  clickToCommitMs: 100,
  clickToPopulatedMs: 100,
  samplerErrors: [],
  samples: [{ populated: true, editorInitialized: true }],
});

test("Case 1: empty samplerErrors keeps the existing valid semantics", () => {
  assert.equal(invalidForTiming({ scenario_exit: 0, trace: validTrace() }), false);
});

test("Case 2: one sampler error invalidates even with perfect timing", () => {
  const trace = validTrace();
  trace.samplerErrors = [{ t: 100, message: "boom" }];
  assert.equal(invalidForTiming({ scenario_exit: 0, trace }), true);
});

test("Case 3: collect mode does not forgive a non-zero exit", () => {
  const trace = validTrace();
  trace.acceptance = "collect";
  assert.equal(invalidForTiming({ scenario_exit: 1, trace }), true);
});

// The known-bad base is still measured: it exits 0 in collect mode because
// the spec skips the blank-frame and retained-surface assertions there
// (measured 5/5, nav-fix-r6). Only that, never a failed process.
test("Case 4: the known-bad base stays usable when its collect run exits 0", () => {
  const trace = validTrace();
  trace.acceptance = "collect";
  trace.blankFrames = 2;
  trace.retainedSurface = false;
  assert.equal(invalidForTiming({ scenario_exit: 0, trace }), false);
});

test("Case 5: accept mode with a non-zero exit stays unusable", () => {
  assert.equal(
    invalidForTiming({ scenario_exit: 1, trace: validTrace() }),
    true,
  );
});
