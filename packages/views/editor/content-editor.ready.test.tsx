import { describe, expect, it, vi } from "vitest";
import { render, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createRef, useLayoutEffect, useState } from "react";
import { ContentEditor, type ContentEditorRef } from "./content-editor";
import { ReadonlyContent } from "./readonly-content";
import { MARKDOWN_CHUNK_THRESHOLD } from "./utils/parse-markdown-chunked";

vi.mock("../i18n", () => ({
  useT: () => ({ t: () => "" }),
}));

const chunkedDescriptionParagraphCount = 500;
const chunkedDescription = Array.from(
  { length: chunkedDescriptionParagraphCount },
  (_, index) => `Chunked description paragraph ${index}.`,
).join("\n\n");
const shortDescription = "Short description.";
const mixedDescription = [
  "# Actual renderer heading",
  "",
  "- First list item",
  "- Second list item",
  "",
  "| Name | Value |",
  "| --- | --- |",
  "| renderer | ready |",
  "",
  "```ts",
  "const ready = true;",
  "```",
].join("\n");
const imageDescription = "![Actual renderer image](https://example.test/issue-description.png)";

function ActualRendererTransitionHost({
  issueId,
  value,
  onReady,
}: {
  issueId: string;
  value: string;
  onReady?: (issueId: string) => void;
}) {
  const [ready, setReady] = useState(false);

  useLayoutEffect(() => {
    setReady(false);
  }, [issueId]);

  return (
    <div data-testid="actual-renderer-transition">
      {!ready && (
        <ReadonlyContent content={value} className="actual-renderer-transition-fallback" />
      )}
      <div className={ready ? undefined : "hidden"}>
        <ContentEditor
          key={issueId}
          value={value}
          onReady={() => {
            onReady?.(issueId);
            setReady(true);
          }}
        />
      </div>
    </div>
  );
}

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
    const fallbackRemovalSnapshots: Array<{
      textLength: number;
      paragraphCount: number;
      firstParagraph: string;
      lastParagraph: string;
    }> = [];
    const onReady = vi.fn();
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    function Host() {
      const [ready, setReady] = useState(false);

      useLayoutEffect(() => {
        if (ready) {
          const editor = document.querySelector(".ProseMirror");
          const paragraphs = Array.from(editor?.querySelectorAll("p") ?? []);
          fallbackRemovalSnapshots.push({
            textLength: editor?.textContent?.length ?? 0,
            paragraphCount: paragraphs.length,
            firstParagraph: paragraphs[0]?.textContent ?? "",
            lastParagraph: paragraphs.at(-1)?.textContent ?? "",
          });
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
      () => expect(fallbackRemovalSnapshots).toHaveLength(1),
      { timeout: 5_000 },
    );
    expect(onReady).toHaveBeenCalledTimes(1);
    expect(fallbackRemovalSnapshots[0]).toEqual({
      textLength: expect.any(Number),
      paragraphCount: chunkedDescriptionParagraphCount,
      firstParagraph: "Chunked description paragraph 0.",
      lastParagraph: "Chunked description paragraph 499.",
    });
    expect(fallbackRemovalSnapshots[0]?.textLength).toBeGreaterThan(0);
  });

  describe("actual ReadonlyContent to ContentEditor transitions", () => {
    const transitionCases = [
      {
        name: "short body",
        value: shortDescription,
        assertReadonly: (host: HTMLElement) => {
          const fallback = host.querySelector("[data-rich-content]");
          expect(fallback?.querySelector("p")?.textContent).toBe(shortDescription);
        },
        assertEditor: (host: HTMLElement) => {
          expect(host.querySelector(".ProseMirror p")?.textContent).toBe(shortDescription);
        },
      },
      {
        name: "long body",
        value: chunkedDescription,
        assertReadonly: (host: HTMLElement) => {
          const paragraphs = Array.from(
            host.querySelectorAll("[data-rich-content] p"),
          );
          expect(paragraphs).toHaveLength(chunkedDescriptionParagraphCount);
          expect(paragraphs[0]?.textContent).toBe("Chunked description paragraph 0.");
          expect(paragraphs.at(-1)?.textContent).toBe("Chunked description paragraph 499.");
        },
        assertEditor: (host: HTMLElement) => {
          const paragraphs = Array.from(host.querySelectorAll(".ProseMirror p"));
          expect(paragraphs).toHaveLength(chunkedDescriptionParagraphCount);
          expect(paragraphs[0]?.textContent).toBe("Chunked description paragraph 0.");
          expect(paragraphs.at(-1)?.textContent).toBe("Chunked description paragraph 499.");
        },
      },
      {
        name: "heading, list, table, and code body",
        value: mixedDescription,
        assertReadonly: (host: HTMLElement) => {
          const fallback = host.querySelector("[data-rich-content]");
          expect(fallback?.querySelector("h1")?.textContent).toBe("Actual renderer heading");
          expect(fallback?.querySelectorAll("ul > li")).toHaveLength(2);
          expect(fallback?.querySelector(".tableWrapper table")).not.toBeNull();
          expect(fallback?.querySelector("pre code")?.textContent).toContain(
            "const ready = true;",
          );
        },
        assertEditor: (host: HTMLElement) => {
          const editor = host.querySelector(".ProseMirror");
          expect(editor?.querySelector("h1")?.textContent).toBe("Actual renderer heading");
          expect(editor?.querySelectorAll("ul > li")).toHaveLength(2);
          expect(editor?.querySelector("table")).not.toBeNull();
          expect(editor?.querySelector("pre code")?.textContent).toContain(
            "const ready = true;",
          );
        },
      },
      {
        name: "image body",
        value: imageDescription,
        assertReadonly: (host: HTMLElement) => {
          const image = host.querySelector<HTMLImageElement>(
            "[data-rich-content] img.image-content",
          );
          expect(image?.alt).toBe("Actual renderer image");
          expect(image?.src).toBe("https://example.test/issue-description.png");
        },
        assertEditor: (host: HTMLElement) => {
          const image = host.querySelector<HTMLImageElement>(".ProseMirror img.image-content");
          expect(image?.alt).toBe("Actual renderer image");
          expect(image?.src).toBe("https://example.test/issue-description.png");
        },
      },
    ];

    it.each(transitionCases)(
      "keeps actual renderer DOM through the $name handoff",
      async ({ value, assertReadonly, assertEditor }) => {
        const onReady = vi.fn(() => {
          const host = document.querySelector<HTMLElement>(
            '[data-testid="actual-renderer-transition"]',
          );
          expect(host).not.toBeNull();
          assertReadonly(host!);
        });
        const queryClient = new QueryClient({
          defaultOptions: { queries: { retry: false } },
        });

        render(
          <QueryClientProvider client={queryClient}>
            <ActualRendererTransitionHost issueId="issue-a" value={value} onReady={onReady} />
          </QueryClientProvider>,
        );

        await waitFor(
          () => {
            expect(onReady).toHaveBeenCalledTimes(1);
            expect(
              document.querySelector('[data-testid="actual-renderer-transition"] [data-rich-content]'),
            ).toBeNull();
          },
          { timeout: 5_000 },
        );

        const host = document.querySelector<HTMLElement>(
          '[data-testid="actual-renderer-transition"]',
        );
        expect(host).not.toBeNull();
        assertEditor(host!);
      },
    );

    it("uses the current body when the same issue re-enters", async () => {
      const onReady = vi.fn(() => {
        const host = document.querySelector<HTMLElement>(
          '[data-testid="actual-renderer-transition"]',
        );
        expect(host?.querySelector("[data-rich-content] p")?.textContent).toBe(shortDescription);
      });
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
      });
      const renderHost = (visible: boolean) => (
        <QueryClientProvider client={queryClient}>
          {visible && (
            <ActualRendererTransitionHost
              issueId="issue-a"
              value={shortDescription}
              onReady={onReady}
            />
          )}
        </QueryClientProvider>
      );

      const { rerender } = render(renderHost(true));
      await waitFor(() => expect(onReady).toHaveBeenCalledTimes(1));
      rerender(renderHost(false));
      rerender(renderHost(true));

      await waitFor(() => expect(onReady).toHaveBeenCalledTimes(2));
      const host = document.querySelector<HTMLElement>(
        '[data-testid="actual-renderer-transition"]',
      );
      expect(host?.querySelector("[data-rich-content]")).toBeNull();
      expect(host?.querySelector(".ProseMirror p")?.textContent).toBe(shortDescription);
    });

    it("uses A, B, then A content across issue changes", async () => {
      const issueBodies = {
        "issue-a": "Issue A actual renderer body.",
        "issue-b": "Issue B actual renderer body.",
      };
      const readyBodies: Array<{ issueId: string; body: string }> = [];
      const onReady = vi.fn((issueId: string) => {
        const host = document.querySelector<HTMLElement>(
          '[data-testid="actual-renderer-transition"]',
        );
        const body = host?.querySelector("[data-rich-content] p")?.textContent ?? "";
        readyBodies.push({ issueId, body });
        expect(body).toBe(issueBodies[issueId as keyof typeof issueBodies]);
      });
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
      });
      const renderHost = (issueId: keyof typeof issueBodies) => (
        <QueryClientProvider client={queryClient}>
          <ActualRendererTransitionHost
            issueId={issueId}
            value={issueBodies[issueId]}
            onReady={onReady}
          />
        </QueryClientProvider>
      );

      const { rerender } = render(renderHost("issue-a"));
      await waitFor(() => expect(onReady).toHaveBeenCalledTimes(1));
      rerender(renderHost("issue-b"));
      await waitFor(() => expect(onReady).toHaveBeenCalledTimes(2));
      rerender(renderHost("issue-a"));
      await waitFor(() => expect(onReady).toHaveBeenCalledTimes(3));

      expect(readyBodies).toEqual([
        { issueId: "issue-a", body: issueBodies["issue-a"] },
        { issueId: "issue-b", body: issueBodies["issue-b"] },
        { issueId: "issue-a", body: issueBodies["issue-a"] },
      ]);
      const host = document.querySelector<HTMLElement>(
        '[data-testid="actual-renderer-transition"]',
      );
      expect(host?.querySelector("[data-rich-content]")).toBeNull();
      expect(host?.querySelector(".ProseMirror p")?.textContent).toBe(issueBodies["issue-a"]);
    });
  });
});
