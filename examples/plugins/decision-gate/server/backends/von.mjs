import { HttpBackend, jsonArgs } from "./base.mjs";

export function createVonBackend(options) {
  const port = Number(process.env.VON_PORT ?? 18080);
  const baseUrl = process.env.VON_URL ?? `http://127.0.0.1:${port}`;
  return new HttpBackend({
    command: process.env.VON_COMMAND ?? "von",
    args: jsonArgs(process.env.VON_ARGS_JSON, ["serve", "--port", String(port)]),
    baseUrl,
    healthPath: process.env.VON_HEALTH_PATH ?? "/health",
    decisionPath: "/v1/systemone",
    transform: (request) => ({
      state: request.state,
      questions: {
        decision: {
          type: request.mode === "score" ? "score" : "choice",
          instructions: request.question,
          criteria: request.options,
        },
      },
    }),
    ...options,
  });
}
