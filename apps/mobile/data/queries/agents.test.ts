import { describe, expect, it, vi } from "vitest";
import type { Agent } from "@multica/core/types";

import { agentListOptions } from "./agents";

vi.mock("@/data/api", () => ({ api: {} }));

describe("agentListOptions", () => {
  it("polls while any agent has projected runtime availability", () => {
    const options = agentListOptions("ws-1");
    const interval = options.refetchInterval;
    expect(typeof interval).toBe("function");
    if (typeof interval !== "function") return;

    const queryState = (data: Agent[]) =>
      interval({ state: { status: "success", data } } as never);

    expect(queryState([{ runtime_availability: "online" } as Agent])).toBe(30_000);
    expect(queryState([{ runtime_availability: "unstable" } as Agent])).toBe(30_000);
    expect(queryState([{ runtime_availability: "offline" } as Agent])).toBe(30_000);
    expect(queryState([{ runtime_availability: undefined } as Agent])).toBe(false);
    expect(queryState([])).toBe(false);
  });
});
