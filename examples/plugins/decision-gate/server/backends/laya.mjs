import { fileURLToPath } from "node:url";
import path from "node:path";
import { JsonlProcessBackend } from "./base.mjs";

const workerPath = fileURLToPath(new URL("../worker/laya.mjs", import.meta.url));

export function createLayaBackend(options) {
  const env = {};
  if (process.env.LAYA_MODEL_DIR) env.LAYA_MODEL_DIR = process.env.LAYA_MODEL_DIR;
  if (process.env.LAYA_CACHE) env.LAYA_CACHE = process.env.LAYA_CACHE;
  return new JsonlProcessBackend({
    command: process.env.LAYA_NODE ?? process.execPath,
    args: [workerPath],
    env,
    cwd: process.env.LAYA_CWD ? path.resolve(process.env.LAYA_CWD) : undefined,
    ...options,
  });
}
