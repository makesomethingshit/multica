import assert from "node:assert/strict";
import test from "node:test";
import { BackendManager } from "./manager.mjs";

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const request = { state: { file: "x" }, question: "Inspect this file?", options: { yes: "relevant", no: "unrelated" }, mode: "choice", backend: "auto" };

class FakeBackend {
  constructor(stats, fail = false) {
    this.stats = stats;
    this.fail = fail;
    this.alive = false;
  }
  async start() { this.stats.starts++; this.alive = true; }
  async health() { return this.alive; }
  async decide() {
    this.stats.decisions++;
    if (this.fail) throw new Error("crashed");
    return { choice: "yes", probabilities: { yes: 0.8, no: 0.2 } };
  }
  async stop() { this.stats.stops++; this.alive = false; }
}

test("serializes concurrent calls, reuses one backend, and idles it out", async () => {
  const stats = { starts: 0, stops: 0, decisions: 0 };
  const manager = new BackendManager({
    env: { DECISION_GATE_DEFAULT_BACKEND: "laya", DECISION_GATE_IDLE_TIMEOUT_SECONDS: "0.02" },
    backendFactory: () => new FakeBackend(stats),
  });
  const results = await Promise.all([manager.decide(request), manager.decide(request)]);
  assert.equal(results[0].choice, "yes");
  assert.equal(results[1].choice, "yes");
  assert.equal(stats.starts, 1);
  assert.equal(stats.decisions, 2);
  await wait(60);
  assert.equal(stats.stops, 1);
});

test("restarts once, then reports Codex fallback", async () => {
  const stats = { starts: 0, stops: 0, decisions: 0 };
  const manager = new BackendManager({
    env: { DECISION_GATE_DEFAULT_BACKEND: "laya", DECISION_GATE_IDLE_TIMEOUT_SECONDS: "1" },
    backendFactory: () => new FakeBackend(stats, true),
  });
  const result = await manager.decide(request);
  assert.deepEqual(result, { status: "unavailable", fallback: "codex", reason: "backend_failed", backend: "laya" });
  assert.equal(stats.starts, 2);
  assert.equal(stats.stops, 2);
  await manager.shutdown();
});
