import { describe, expect, it, vi } from "vitest";
import { render, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useLayoutEffect, useState } from "react";
import { ContentEditor } from "./content-editor";
import { MARKDOWN_CHUNK_THRESHOLD } from "./utils/parse-markdown-chunked";

vi.mock("../i18n", () => ({
  useT: () => ({ t: () => "" }),
}));

const chunkedDescription = Array.from(
  { length: 500 },
  (_, index) => `Chunked description paragraph ${index}.`,
).join("\n\n");

describe("ContentEditor initial readiness (real editor)", () => {
  it("does not remove its fallback until a chunked initial body reaches the editor DOM", async () => {
    const fallbackRemovalDomLengths: number[] = [];
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    function Host() {
      const [ready, setReady] = useState(false);

      useLayoutEffect(() => {
        if (ready) {
          fallbackRemovalDomLengths.push(
            document.querySelector(".ProseMirror")?.textContent?.length ?? 0,
          );
        }
      }, [ready]);

      return (
        <>
          {!ready && <div>Cached long description</div>}
          <div className={ready ? undefined : "hidden"}>
            <ContentEditor
              value={chunkedDescription}
              onReady={() => setReady(true)}
            />
          </div>
        </>
      );
    }

    expect(chunkedDescription.length).toBeGreaterThan(MARKDOWN_CHUNK_THRESHOLD);

    render(
      <QueryClientProvider client={queryClient}>
        <Host />
      </QueryClientProvider>,
    );

    await waitFor(
      () => expect(fallbackRemovalDomLengths).toHaveLength(1),
      { timeout: 5_000 },
    );
    expect(fallbackRemovalDomLengths[0]).toBeGreaterThan(0);
  });
});
