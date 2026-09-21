import { HttpBackend, jsonArgs } from "./base.mjs";

function promptFor(request) {
  const state = typeof request.state === "string" ? request.state : JSON.stringify(request.state);
  const options = Object.entries(request.options).map(([id, description]) => `- ${id}: ${description}`).join("\n");
  return `${state}\n\nQuestion: ${request.question}\nOptions:\n${options}\nAnswer with one option id only:`;
}

function fromCompletion(body, optionIds) {
  if (body.choice && body.probabilities) return body;
  const top = body.choices?.[0]?.logprobs?.content?.[0]?.top_logprobs;
  if (!Array.isArray(top)) throw new Error("llama.cpp did not return candidate token probabilities");
  const logProbabilities = Object.fromEntries(top.map((entry) => [entry.token.trim(), Math.exp(entry.logprob)]));
  const probabilities = Object.fromEntries(optionIds.map((id) => [id, logProbabilities[id] ?? 0]));
  const total = Object.values(probabilities).reduce((sum, value) => sum + value, 0);
  if (total <= 0) throw new Error("llama.cpp returned no probability for the supplied options");
  for (const id of optionIds) probabilities[id] /= total;
  const choice = optionIds.reduce((best, id) => probabilities[id] > probabilities[best] ? id : best, optionIds[0]);
  return { choice, probabilities, confidence: probabilities[choice] };
}

export function createLlamaCppBackend(options) {
  const port = Number(process.env.LLAMA_CPP_PORT ?? 18081);
  const baseUrl = process.env.LLAMA_CPP_URL ?? `http://127.0.0.1:${port}`;
  const model = process.env.LLAMA_CPP_MODEL;
  const defaultArgs = model ? ["-m", model, "--host", "127.0.0.1", "--port", String(port)] : [];
  const backend = new HttpBackend({
    command: process.env.LLAMA_CPP_COMMAND ?? "llama-server",
    args: jsonArgs(process.env.LLAMA_CPP_ARGS_JSON, defaultArgs),
    baseUrl,
    healthPath: process.env.LLAMA_CPP_HEALTH_PATH ?? "/health",
    decisionPath: "/completion",
    transform: (request) => ({
      prompt: promptFor(request),
      n_predict: 1,
      n_probs: Object.keys(request.options).length,
      temperature: 0,
    }),
    ...options,
  });
  const request = backend.decide.bind(backend);
  backend.decide = async (input, timeoutMs) => fromCompletion(await request(input, timeoutMs), Object.keys(input.options));
  return backend;
}
