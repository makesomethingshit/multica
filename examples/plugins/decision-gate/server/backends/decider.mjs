import { HttpBackend, jsonArgs } from "./base.mjs";

export function createDeciderBackend(options) {
  const port = Number(process.env.DECIDER_PORT ?? 18082);
  const baseUrl = process.env.DECIDER_URL ?? `http://127.0.0.1:${port}`;
  return new HttpBackend({
    command: process.env.DECIDER_COMMAND ?? "decider",
    args: jsonArgs(process.env.DECIDER_ARGS_JSON, ["serve", "--port", String(port)]),
    baseUrl,
    healthPath: process.env.DECIDER_HEALTH_PATH ?? "/health",
    decisionPath: process.env.DECIDER_DECISION_PATH ?? "/decision",
    ...options,
  });
}
