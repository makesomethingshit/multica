---
name: decision-gate
description: Use the local Decision Gate MCP tool for small, bounded, closed-option judgments such as relevance filtering, routing, triage, retry/skip choices, taxonomy, and candidate prioritization. Do not use it for final architecture, root-cause, merge, blocker, or destructive decisions.
---

# Decision Gate

Use the `decision` MCP tool only when the question is narrow and the candidate
set is explicit. The tool is a filter, not a second large reasoning model.

## Good uses

- Decide whether a file or finding is worth inspecting.
- Route evidence to a subsystem.
- Classify a change into a predefined taxonomy.
- Prioritize candidate locations.
- Decide whether evidence is strong enough to investigate further.
- Choose a retry, skip, or continue option when the choices are already defined.

## Never delegate

Do not use Decision Gate for:

- final merge or blocker decisions;
- architectural correctness;
- root-cause conclusions or multi-step causal reasoning;
- judgments without an explicit candidate set;
- actions whose failure could delete data, deploy code, change permissions, or
  otherwise cause destructive impact.

## Call procedure

1. Formulate one narrow question.
2. Include only relevant state: an issue excerpt, diff hunk, symbol names, or
   concise invariant. Do not send an entire repository or long PR.
3. Supply mutually understandable option ids and descriptions.
4. Call `decision` with `mode: "choice"`, `"boolean"`, or `"score"` and leave
   `backend` as `"auto"` unless a comparison is intentional.
5. Treat `confidence` and `probabilities` as model evidence, not truth.
6. Verify the surviving candidate against repository evidence yourself.
7. If the result is unavailable or confidence is low, continue with your own
   reasoning. Do not call repeatedly until the tool gives the desired answer.

The tool may return `{ "status": "unavailable", "fallback": "codex" }`. That is
an expected safe result: keep working without the local backend.
