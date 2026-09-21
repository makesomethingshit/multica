import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { setTimeout as sleep } from "node:timers/promises";

export class BackendError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = "BackendError";
  }
}

export function jsonArgs(value, fallback = []) {
  if (!value) return fallback;
  const parsed = JSON.parse(value);
  if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== "string")) {
    throw new Error("backend argument configuration must be a JSON string array");
  }
  return parsed;
}

function childEnv(extra = {}) {
  return { ...process.env, ...extra };
}

async function stopChild(child, exitPromise, graceMs) {
  if (!child || child.exitCode !== null) return;
  child.kill("SIGTERM");
  await Promise.race([exitPromise, sleep(graceMs)]);
  if (child.exitCode === null) child.kill("SIGKILL");
  await exitPromise.catch(() => {});
}

// A newline-delimited worker keeps model memory outside the MCP process. The
// same class is used by every process backend, so swapping models cannot add a
// second lifecycle implementation.
export class JsonlProcessBackend {
  constructor({ command, args = [], env = {}, cwd, startupTimeoutMs = 30_000, shutdownGraceMs = 3_000, logger = () => {} }) {
    this.command = command;
    this.args = args;
    this.env = env;
    this.cwd = cwd;
    this.startupTimeoutMs = startupTimeoutMs;
    this.shutdownGraceMs = shutdownGraceMs;
    this.logger = logger;
    this.child = null;
    this.exitPromise = null;
    this.reader = null;
    this.nextId = 1;
    this.pending = new Map();
  }

  async health() {
    return this.child !== null && this.child.exitCode === null && !this.child.killed;
  }

  async start() {
    if (await this.health()) return;
    if (!this.command) throw new BackendError("backend command is not configured");

    const child = spawn(this.command, this.args, {
      cwd: this.cwd,
      env: childEnv(this.env),
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    });
    this.child = child;
    this.exitPromise = new Promise((resolve) => child.once("exit", resolve));

    let readyResolve;
    let readyReject;
    const ready = new Promise((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    let readySettled = false;
    const failReady = (error) => {
      if (!readySettled) {
        readySettled = true;
        readyReject(error);
      }
    };
    const resolveReady = () => {
      if (!readySettled) {
        readySettled = true;
        readyResolve();
      }
    };

    child.once("error", (error) => failReady(new BackendError(`backend process failed: ${error.message}`, { cause: error })));
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => this.logger("backend_stderr", { text: String(chunk).trim().slice(-500) }));
    this.reader = createInterface({ input: child.stdout });
    this.reader.on("line", (line) => {
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        this.logger("backend_protocol_error", { reason: "invalid_json" });
        return;
      }
      if (message.ready === true) {
        resolveReady();
        return;
      }
      const waiter = this.pending.get(String(message.id));
      if (!waiter) return;
      this.pending.delete(String(message.id));
      clearTimeout(waiter.timer);
      if (message.error) waiter.reject(new BackendError(String(message.error)));
      else waiter.resolve(message.result ?? message);
    });
    child.once("exit", (code, signal) => {
      const error = new BackendError(`backend exited${code === null ? ` by ${signal}` : ` with code ${code}`}`);
      failReady(error);
      for (const waiter of this.pending.values()) {
        clearTimeout(waiter.timer);
        waiter.reject(error);
      }
      this.pending.clear();
    });

    try {
      await Promise.race([
        ready,
        sleep(this.startupTimeoutMs).then(() => {
          throw new BackendError(`backend did not become ready within ${this.startupTimeoutMs}ms`);
        }),
      ]);
    } catch (error) {
      await this.stop();
      throw error;
    }
  }

  async decide(request, timeoutMs = this.startupTimeoutMs) {
    if (!(await this.health())) throw new BackendError("backend is not alive");
    const id = String(this.nextId++);
    const child = this.child;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new BackendError(`backend decision timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      child.stdin.write(`${JSON.stringify({ id, ...request })}\n`, (error) => {
        if (!error) return;
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new BackendError(`could not send decision to backend: ${error.message}`, { cause: error }));
      });
    });
  }

  async stop() {
    const child = this.child;
    if (!child) return;
    this.reader?.close();
    this.reader = null;
    await stopChild(child, this.exitPromise ?? Promise.resolve(), this.shutdownGraceMs);
    this.child = null;
    this.exitPromise = null;
  }
}

export class HttpBackend {
  constructor({ command, args = [], env = {}, cwd, baseUrl, healthPath = "/health", decisionPath = "/decision", startupTimeoutMs = 30_000, shutdownGraceMs = 3_000, logger = () => {}, transform = (request) => request }) {
    this.command = command;
    this.args = args;
    this.env = env;
    this.cwd = cwd;
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.healthUrl = `${this.baseUrl}${healthPath}`;
    this.decisionUrl = `${this.baseUrl}${decisionPath}`;
    this.startupTimeoutMs = startupTimeoutMs;
    this.shutdownGraceMs = shutdownGraceMs;
    this.logger = logger;
    this.transform = transform;
    this.child = null;
    this.exitPromise = null;
  }

  async health() {
    if (this.child && (this.child.exitCode !== null || this.child.killed)) return false;
    try {
      const response = await fetch(this.healthUrl, { signal: AbortSignal.timeout(2_000) });
      return response.ok;
    } catch {
      return false;
    }
  }

  async start() {
    if (await this.health()) return;
    if (!this.command) throw new BackendError("backend command is not configured");
    this.child = spawn(this.command, this.args, {
      cwd: this.cwd,
      env: childEnv(this.env),
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    this.exitPromise = new Promise((resolve) => this.child.once("exit", resolve));
    this.child.stderr.setEncoding("utf8");
    this.child.stderr.on("data", (chunk) => this.logger("backend_stderr", { text: String(chunk).trim().slice(-500) }));

    const deadline = Date.now() + this.startupTimeoutMs;
    while (Date.now() < deadline) {
      if (await this.health()) return;
      await sleep(100);
    }
    await this.stop();
    throw new BackendError(`backend did not become healthy within ${this.startupTimeoutMs}ms`);
  }

  async decide(request, timeoutMs = this.startupTimeoutMs) {
    const response = await fetch(this.decisionUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(this.transform(request)),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const body = await response.text();
    if (!response.ok) throw new BackendError(`backend returned HTTP ${response.status}`);
    try {
      return JSON.parse(body);
    } catch (error) {
      throw new BackendError("backend returned invalid JSON", { cause: error });
    }
  }

  async stop() {
    const child = this.child;
    if (!child) return;
    await stopChild(child, this.exitPromise ?? Promise.resolve(), this.shutdownGraceMs);
    this.child = null;
    this.exitPromise = null;
  }
}
