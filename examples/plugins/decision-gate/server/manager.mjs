import { createDeciderBackend } from "./backends/decider.mjs";
import { BackendError } from "./backends/base.mjs";
import { createLlamaCppBackend } from "./backends/llama_cpp.mjs";
import { createLayaBackend } from "./backends/laya.mjs";
import { createVonBackend } from "./backends/von.mjs";

const BACKENDS = new Set(["laya", "von", "llama_cpp", "decider"]);

const defaultFactory = (name, options) => ({ laya: createLayaBackend, von: createVonBackend, llama_cpp: createLlamaCppBackend, decider: createDeciderBackend }[name](options));

function numberEnv(env, name, fallback) {
  const value = Number(env[name] ?? fallback);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function normalizeProbabilities(probabilities, optionIds) {
  if (Array.isArray(probabilities)) {
    probabilities = Object.fromEntries(optionIds.map((id, index) => [id, Number(probabilities[index] ?? 0)]));
  }
  if (!probabilities || typeof probabilities !== "object") return {};
  const values = Object.fromEntries(optionIds.map((id) => {
    const value = Number(probabilities[id] ?? 0);
    return [id, Number.isFinite(value) ? Math.max(0, value) : 0];
  }));
  const total = Object.values(values).reduce((sum, value) => sum + value, 0);
  if (total <= 0) return {};
  return Object.fromEntries(Object.entries(values).map(([id, value]) => [id, value / total]));
}

function normalizeResult(raw, request, backend, latencyMs) {
  const answer = raw?.answers?.decision ?? raw?.answer ?? raw;
  const optionIds = Object.keys(request.options);
  const probabilities = normalizeProbabilities(answer?.probabilities ?? answer?.distribution ?? raw?.probabilities, optionIds);
  let choice = answer?.choice ?? raw?.choice;
  if (!choice && typeof answer?.score === "number") {
    choice = optionIds[Math.max(0, Math.min(optionIds.length - 1, Math.round(answer.score)))];
  }
  if (!choice || !optionIds.includes(choice)) {
    if (Object.keys(probabilities).length === 0) throw new BackendError("backend returned no valid option");
    choice = optionIds.reduce((best, id) => probabilities[id] > probabilities[best] ? id : best, optionIds[0]);
  }
  const confidence = Number(answer?.confidence ?? raw?.confidence ?? probabilities[choice]);
  return {
    choice,
    confidence: Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : null,
    probabilities,
    backend,
    latency_ms: Math.max(0, Math.round(latencyMs)),
  };
}

export class BackendManager {
  constructor({ env = process.env, backendFactory = defaultFactory, logger = () => {} } = {}) {
    this.env = env;
    this.backendFactory = backendFactory;
    this.logger = logger;
    this.defaultBackend = env.DECISION_GATE_DEFAULT_BACKEND ?? "laya";
    this.idleTimeoutMs = numberEnv(env, "DECISION_GATE_IDLE_TIMEOUT_SECONDS", 60) * 1000;
    this.startupTimeoutMs = numberEnv(env, "DECISION_GATE_STARTUP_TIMEOUT_SECONDS", 30) * 1000;
    this.shutdownGraceMs = numberEnv(env, "DECISION_GATE_SHUTDOWN_GRACE_SECONDS", 3) * 1000;
    this.current = null;
    this.idleTimer = null;
    this.serial = Promise.resolve();
  }

  enqueue(operation) {
    const result = this.serial.then(operation, operation);
    this.serial = result.catch(() => {});
    return result;
  }

  decide(request) {
    return this.enqueue(() => this.#decide(request));
  }

  async #decide(request) {
    const requested = request.backend === "auto" || !request.backend ? this.defaultBackend : request.backend;
    if (!BACKENDS.has(requested)) throw new BackendError(`unsupported backend ${requested}`);
    try {
      return await this.#run(request, requested);
    } catch (firstError) {
      this.logger("backend_restart", { backend: requested, reason: String(firstError?.message ?? firstError).slice(0, 160) });
      await this.#stopCurrent();
      try {
        return await this.#run(request, requested);
      } catch (secondError) {
        this.logger("backend_unavailable", { backend: requested, reason: String(secondError?.message ?? secondError).slice(0, 160) });
        await this.#stopCurrent();
        return {
          status: "unavailable",
          fallback: "codex",
          reason: "backend_failed",
          backend: requested,
        };
      }
    }
  }

  async #run(request, backendName) {
    await this.#ensure(backendName);
    const started = performance.now();
    const raw = await this.current.backend.decide(request, this.startupTimeoutMs);
    this.#touch();
    return normalizeResult(raw, request, backendName, performance.now() - started);
  }

  async #ensure(name) {
    if (this.current?.name === name && await this.current.backend.health()) return;
    await this.#stopCurrent();
    const backend = this.backendFactory(name, {
      startupTimeoutMs: this.startupTimeoutMs,
      shutdownGraceMs: this.shutdownGraceMs,
      logger: this.logger,
    });
    await backend.start();
    this.current = { name, backend };
    this.logger("backend_ready", { backend: name });
  }

  #touch() {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      this.enqueue(async () => {
        if (this.current) {
          this.logger("idle_timeout", { backend: this.current.name });
          await this.#stopCurrent();
        }
      });
    }, this.idleTimeoutMs);
    this.idleTimer.unref?.();
  }

  async #stopCurrent() {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = null;
    if (!this.current) return;
    const current = this.current;
    this.current = null;
    await current.backend.stop();
    this.logger("backend_stopped", { backend: current.name });
  }

  status() {
    return {
      backend: this.current?.name ?? null,
      resident: Boolean(this.current),
    };
  }

  shutdown() {
    return this.enqueue(() => this.#stopCurrent());
  }

  // The normal signal path awaits graceful shutdown. The process-exit path
  // cannot await promises, so kill the direct backend child synchronously.
  forceStop() {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = null;
    this.current?.backend?.child?.kill("SIGTERM");
  }
}

export { BACKENDS, normalizeResult };
