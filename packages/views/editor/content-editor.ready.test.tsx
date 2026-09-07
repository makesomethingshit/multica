import { describe, expect, it, vi } from "vitest";
import { render, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createRef, useLayoutEffect, useState } from "react";
import { ContentEditor, type ContentEditorRef } from "./content-editor";
import { MARKDOWN_CHUNK_THRESHOLD } from "./utils/parse-markdown-chunked";

vi.mock("../i18n", () => ({
  useT: () => ({ t: () => "" }),
}));

const chunkedDescription = Array.from(
  { length: 500 },
  (_, index) => `Chunked description paragraph ${index}.`,
).join("\n\n");
const shortDescription = "Short description.";

describe("ContentEditor initial readiness (real editor)", () => {
  it("signals once and leaves an empty editor usable", async () => {
    const onReady = vi.fn();
    const ref = createRef<ContentEditorRef>();
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <ContentEditor ref={ref} value="" onReady={onReady} />
      </QueryClientProvider>,
    );

    await waitFor(() => expect(onReady).toHaveBeenCalledTimes(1));
    expect(ref.current).not.toBeNull();
    expect(ref.current!.getMarkdown()).toBe("");
    expect(document.querySelector(".ProseMirror")).toHaveAttribute("contenteditable", "true");
    expect(() => ref.current!.focus()).not.toThrow();
  });

  it("signals once after short initial content reaches the editor DOM", async () => {
    const onReadyDomContents: string[] = [];
    const onReady = vi.fn(() => {
      onReadyDomContents.push(document.querySelector(".ProseMirror")?.textContent ?? "");
    });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    expect(shortDescription.length).toBeLessThan(MARKDOWN_CHUNK_THRESHOLD);

    render(
      <QueryClientProvider client={queryClient}>
        <ContentEditor value={shortDescription} onReady={onReady} />
      </QueryClientProvider>,
    );

    await waitFor(() => expect(onReady).toHaveBeenCalledTimes(1));
    expect(onReadyDomContents).toEqual([shortDescription]);
  });

  it("does not remove its fallback until a chunked initial body reaches the editor DOM", async () => {
    const fallbackRemovalDomLengths: number[] = [];
    const onReady = vi.fn();
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
              onReady={() => {
                onReady();
                setReady(true);
              }}
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
    expect(onReady).toHaveBeenCalledTimes(1);
    expect(fallbackRemovalDomLengths[0]).toBeGreaterThan(0);
  });
});
