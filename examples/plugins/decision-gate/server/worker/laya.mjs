import { createInterface } from "node:readline";

const { Laya } = await import("@receptron/laya");
const laya = await Laya.load({
  ...(process.env.LAYA_MODEL_DIR ? { modelDir: process.env.LAYA_MODEL_DIR } : {}),
  ...(process.env.LAYA_CACHE ? { cacheDir: process.env.LAYA_CACHE } : {}),
});

function send(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function criteriaFor(request) {
  if (request.mode === "score") return Object.keys(request.options);
  return request.options;
}

function typeFor(request) {
  return request.mode === "score" ? "score" : "choice";
}

send({ ready: true });
const reader = createInterface({ input: process.stdin });
for await (const line of reader) {
  if (!line.trim()) continue;
  let request;
  try {
    request = JSON.parse(line);
    const result = await laya.systemOne(request.state, {
      decision: {
        type: typeFor(request),
        instructions: request.question,
        criteria: criteriaFor(request),
      },
    });
    const answer = result.answers?.decision ?? {};
    send({
      id: request.id,
      result: {
        choice: answer.choice,
        score: answer.score,
        probabilities: answer.probabilities ?? answer.distribution,
      },
    });
  } catch (error) {
    send({ id: request?.id, error: error instanceof Error ? error.message : String(error) });
  }
}

await laya.close?.();
