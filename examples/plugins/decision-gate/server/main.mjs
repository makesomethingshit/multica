import { createServer as createHttpServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { BackendManager } from "./manager.mjs";

const MAX_REQUEST_BYTES = 1 << 20;
const MCP_PROTOCOL_VERSION = "2025-03-26";
const TOOL = {
  name: "decision",
  description: "Make one small, bounded, closed-option judgment. Treat the result as evidence, never truth. Never use for final architecture, root-cause, merge, blocker, or destructive decisions.",
  inputSchema: {
    type: "object",
    required: ["state", "question", "options", "mode"],
    properties: {
      state: { oneOf: [{ type: "string" }, { type: "object" }] },
      question: { type: "string" },
      options: { type: "object", additionalProperties: { type: "string" } },
      mode: { type: "string", enum: ["choice", "boolean", "score"] },
      backend: { type: "string", enum: ["auto", "laya", "von", "llama_cpp", "decider"] },
    },
    additionalProperties: false,
  },
};

function logEvent(event, fields = {}) {
  console.error(`[decision-gate] ${event}`, JSON.stringify(fields));
}

function authorized(request, env) {
  const expected = env.DECISION_GATE_TOKEN;
  if (!expected) return true;
  const provided = request.headers.authorization?.replace(/^Bearer\s+/i, "") ?? "";
  const left = Buffer.from(provided);
  const right = Buffer.from(expected);
  return left.length === right.length && timingSafeEqual(left, right);
}

export function validateRequest(request) {
  if (!request || typeof request !== "object" || Array.isArray(request)) throw new Error("request must be an object");
  if (!(typeof request.state === "string" || (request.state && typeof request.state === "object" && !Array.isArray(request.state)))) throw new Error("state must be a string or object");
  if (typeof request.question !== "string" || request.question.trim() === "" || request.question.length > 2_000) throw new Error("question must be a non-empty string of at most 2000 characters");
  if (!request.options || typeof request.options !== "object" || Array.isArray(request.options)) throw new Error("options must be an object");
  const optionIds = Object.keys(request.options);
  if (optionIds.length < 2 || optionIds.length > 20) throw new Error("options must contain between 2 and 20 candidates");
  if (optionIds.some((id) => !/^[a-zA-Z0-9_-]{1,64}$/.test(id) || typeof request.options[id] !== "string" || request.options[id].length > 500)) throw new Error("options must map short ids to short string descriptions");
  if (!["choice", "boolean", "score"].includes(request.mode)) throw new Error("mode must be choice, boolean, or score");
  if (request.backend !== undefined && !["auto", "laya", "von", "llama_cpp", "decider"].includes(request.backend)) throw new Error("backend is unsupported");
  if (JSON.stringify(request.state).length > 128_000) throw new Error("state is too large; send only relevant evidence");
}

async function readBody(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > MAX_REQUEST_BYTES) throw new Error("request is too large");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function reply(response, id, result) {
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify({ jsonrpc: "2.0", id: id ?? null, result }));
}

function errorReply(response, id, code, message) {
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify({ jsonrpc: "2.0", id: id ?? null, error: { code, message } }));
}

export function createMcpServer({ manager, env = process.env } = {}) {
  const selectedManager = manager ?? new BackendManager({ env, logger: logEvent });
  const mcpPath = env.DECISION_GATE_MCP_PATH ?? "/mcp";
  return async (request, response) => {
    if (request.url === "/health" && request.method === "GET") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ ok: true, ...selectedManager.status() }));
      return;
    }
    if (request.url !== mcpPath || request.method !== "POST") {
      response.writeHead(404);
      response.end();
      return;
    }
    if (!authorized(request, env)) {
      response.writeHead(401, { "content-type": "text/plain" });
      response.end("credential rejected");
      return;
    }

    let rpc;
    try {
      rpc = JSON.parse(await readBody(request));
      if (!rpc || typeof rpc !== "object" || Array.isArray(rpc) || rpc.jsonrpc !== "2.0" || typeof rpc.method !== "string") {
        throw new Error("request is not valid JSON-RPC 2.0");
      }
    } catch (error) {
      errorReply(response, null, -32700, error.message);
      return;
    }
    if (rpc.method === "notifications/initialized") {
      response.writeHead(202);
      response.end();
      return;
    }
    if (rpc.method === "initialize") {
      reply(response, rpc.id, {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: { tools: {} },
        serverInfo: { name: "multica-decision-gate", version: "0.1.0" },
      });
      return;
    }
    if (rpc.method === "ping") {
      reply(response, rpc.id, {});
      return;
    }
    if (rpc.method === "tools/list") {
      reply(response, rpc.id, { tools: [TOOL] });
      return;
    }
    if (rpc.method !== "tools/call") {
      errorReply(response, rpc.id, -32601, `method not found: ${rpc.method}`);
      return;
    }

    const params = rpc.params ?? {};
    if (params.name !== TOOL.name) {
      errorReply(response, rpc.id, -32602, `unknown tool ${params.name}`);
      return;
    }
    try {
      validateRequest(params.arguments);
      const result = await selectedManager.decide(params.arguments);
      reply(response, rpc.id, {
        content: [{ type: "text", text: JSON.stringify(result) }],
        structuredContent: result,
      });
    } catch (error) {
      errorReply(response, rpc.id, -32602, error instanceof Error ? error.message : String(error));
    }
  };
}

export async function startServer({ env = process.env } = {}) {
  const manager = new BackendManager({ env, logger: logEvent });
  const handler = createMcpServer({ manager, env });
  const certPath = env.TLS_CERT ?? "dev-cert.pem";
  const keyPath = env.TLS_KEY ?? "dev-key.pem";
  const server = env.MCP_ALLOW_HTTP === "1"
    ? createHttpServer(handler)
    : createHttpsServer({ cert: readFileSync(certPath), key: readFileSync(keyPath) }, handler);
  const host = env.HOST ?? "127.0.0.1";
  const port = Number(env.PORT ?? 8789);
  await new Promise((resolve) => server.listen(port, host, resolve));
  logEvent("ready", { host, port, path: env.DECISION_GATE_MCP_PATH ?? "/mcp" });

  const shutdown = async () => {
    await manager.shutdown();
    await new Promise((resolve) => server.close(resolve));
  };
  if (env.DECISION_GATE_KILL_ON_MCP_EXIT !== "0") process.once("exit", () => manager.forceStop());
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
  return { server, manager, shutdown };
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  startServer().catch((error) => {
    console.error(`[decision-gate] startup failed: ${error.message}`);
    process.exitCode = 1;
  });
}
