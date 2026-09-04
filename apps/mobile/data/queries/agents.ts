import { queryOptions } from "@tanstack/react-query";
import { api } from "@/data/api";

export const agentListOptions = (wsId: string | null) =>
  queryOptions({
    queryKey: ["agents", wsId] as const,
    queryFn: ({ signal }) => api.listAgents({ signal }),
    enabled: !!wsId,
    refetchInterval: (query) =>
      query.state.data?.some(
        (agent) =>
          !agent.archived_at &&
          agent.runtime_availability !== undefined,
      )
        ? 30_000
        : false,
  });
