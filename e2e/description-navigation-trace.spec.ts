import { expect, test } from "@playwright/test";
import { createTestApi, loginAsDefault } from "./helpers";
import type { TestApiClient } from "./fixtures";

/**
 * MUL-7095 / PR #8092 Revision 3 — navigation performance recorder.
 *
 * §5 requirement: measurement must begin BEFORE issue navigation (at link
 * activation), not once `issue-description` already exists. This spec records:
 *
 * - link click timestamp (`navClickT`);
 * - first committed detail frame (first rAF sample whose scroll root carries
 *   the target issue's `data-tab-scroll-root`, `firstDetailCommitT`);
 * - first frame containing the description host (`firstHostT`);
 * - first frame containing populated description content
 *   (`.ProseMirror` with non-empty text, `firstPopulatedT`);
 * - editor `isInitialized` at each sample;
 * - Long Tasks overlapping [click, first commit] via PerformanceObserver
 *   (`buffered: true`, started pre-click).
 *
 * No portable CI threshold is asserted: absolute timings are machine-specific
 * (codex cross-check). The spec attaches raw samples for A/B comparison
 * (base vs head vs revised, same fixture + environment) and applies only
 * stable guardrails:
 *
 * - the current `eagerClientRender` head MUST show a several-hundred-ms
 *   main-thread Long Task attributable to synchronous editor creation in the
 *   click→commit window (F1 regression demonstration — fails on base, passes
 *   on eager head);
 * - the revised implementation must keep click→first-commit within the same
 *   practical range as base with no new large synchronous task.
 *
 * Cold compilation outliers are discarded separately: the first navigation
 * after load is a warm-up sample, excluded from the comparison.
 */

type NavSample = {
  t: number;
  detailCommitted: boolean;
  hostPresent: boolean;
  proseMirrorPresent: boolean;
  populated: boolean;
  editorInitialized: boolean | null;
  descriptionTextLength: number;
};

type LongTaskEntry = {
  startTime: number;
  duration: number;
  name: string;
};

declare global {
  interface Window {
    __navInstalled: boolean;
    __navSamples: NavSample[];
    __navRecord: boolean;
    __navLongTasks: LongTaskEntry[];
    __navClickT: number;
    __navFirstDetailCommitT: number | null;
    __navFirstHostT: number | null;
    __navFirstPopulatedT: number | null;
    __startNavRecording: () => void;
  }
}

const LONG_BODY = [
  "# Reentry A",
  "![Reentry image](/e2e-description.svg)",
  ...Array.from(
    { length: 100 },
    (_, i) =>
      `## Section ${i}\n\nParagraph ${i} of the cached description.\n\n- First item\n- Second item\n\n\`\`\`ts\nconst section = ${i};\n\`\`\``,
  ),
  "End of reentry A.",
].join("\n\n");

test.describe("MUL-7095 navigation performance (link-activation recorder)", () => {
  test.describe.configure({ timeout: 120000 });
  let api: TestApiClient;

  test.beforeEach(async ({ page }) => {
    api = await createTestApi();
    await page.route("**/e2e-description.svg", (route) =>
      route.fulfill({
        contentType: "image/svg+xml",
        headers: { "cache-control": "public, max-age=3600" },
        body: '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="320"><rect width="640" height="320" fill="teal"/></svg>',
      }),
    );
    await page.addInitScript(() => {
      // addInitScript re-runs on full document loads. The trace assumes
      // SPA client navigation (same assumption as description-reentry.spec.ts:
      // window state persists across open()/leaveDetail()). The guard keeps
      // a reload from double-registering the observer (duplicate entries)
      // or wiping an in-flight recording.
      if (window.__navInstalled) return;
      window.__navInstalled = true;
      window.__navSamples = [];
      window.__navLongTasks = [];
      window.__navRecord = false;
      window.__navClickT = 0;
      window.__navFirstDetailCommitT = null;
      window.__navFirstHostT = null;
      window.__navFirstPopulatedT = null;
      // Started pre-click with buffered:true so tasks straddling navigation
      // are still observed; only the overlap with [click, first commit] is
      // counted at analysis time.
      const observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          window.__navLongTasks.push({
            startTime: entry.startTime,
            duration: entry.duration,
            name: entry.name,
          });
        }
      });
      observer.observe({ type: "longtask", buffered: true });
      const sample = () => {
        if (window.__navRecord) {
          const host = document.querySelector(
            '[data-testid="issue-description"]',
          );
          const editor = host?.querySelector<HTMLElement>(".ProseMirror");
          const text = editor?.textContent ?? "";
          const initialized = (
            editor as
              | (HTMLElement & { editor?: { isInitialized: boolean } })
              | null
              | undefined
          )?.editor?.isInitialized;
          const now = performance.now();
          const committed = !!document.querySelector(
            '[data-tab-scroll-root^="main:"]',
          );
          if (committed && window.__navFirstDetailCommitT === null) {
            window.__navFirstDetailCommitT = now;
          }
          if (host && window.__navFirstHostT === null) {
            window.__navFirstHostT = now;
          }
          if (text.length > 0 && window.__navFirstPopulatedT === null) {
            window.__navFirstPopulatedT = now;
          }
          window.__navSamples.push({
            t: now,
            detailCommitted: committed,
            hostPresent: !!host,
            proseMirrorPresent: !!editor,
            populated: text.length > 0,
            editorInitialized: initialized ?? null,
            descriptionTextLength: text.length,
          });
        }
        requestAnimationFrame(sample);
      };
      requestAnimationFrame(sample);
      window.__startNavRecording = () => {
        window.__navSamples = [];
        window.__navClickT = performance.now();
        window.__navFirstDetailCommitT = null;
        window.__navFirstHostT = null;
        window.__navFirstPopulatedT = null;
        window.__navRecord = true;
      };
    });
  });

  test.afterEach(async () => {
    await api?.cleanup();
  });

  test("click-to-commit navigation trace with Long Tasks (A/B guardrail)", async ({
    page,
  }, testInfo) => {
    const a = await api.createIssue(`E2E Nav A ${Date.now()}`, {
      description: LONG_BODY,
    });
    const slug = await loginAsDefault(page);
    const list = page.locator(`a[href="/${slug}/issues"]`).first();
    const openLink = (id: string) =>
      page.locator(`a[href$="/issues/${id}"]`).first();

    // Warm-up navigation (cold compilation outlier — discarded from the
    // comparison, kept only so the measured pass is warm).
    await openLink(a.id).click();
    await expect(
      page.getByTestId("issue-description").locator(".ProseMirror"),
    ).toBeVisible({ timeout: 30000 });
    await list.click();
    await expect(page.getByTestId("issue-description")).toHaveCount(0);

    // Measured navigation: recorder starts BEFORE the click.
    await page.evaluate(() => window.__startNavRecording());
    await openLink(a.id).click();
    await expect(
      page.getByTestId("issue-description").locator(".ProseMirror"),
    ).toBeVisible({ timeout: 30000 });
    // Wait until the populated editor reports initialized so the trace
    // covers the full startup path, not just the first commit.
    await page.waitForFunction(
      () =>
        (
          document.querySelector(
            '[data-testid="issue-description"] .ProseMirror',
          ) as HTMLElement & { editor?: { isInitialized: boolean } }
        )?.editor?.isInitialized === true,
      { timeout: 30000 },
    );

    const trace = await page.evaluate(() => {
      window.__navRecord = false;
      const clickT = window.__navClickT;
      const firstCommit = window.__navFirstDetailCommitT;
      const overlapping = window.__navLongTasks.filter((task) => {
        if (firstCommit === null) return false;
        const taskEnd = task.startTime + task.duration;
        return task.startTime <= firstCommit && taskEnd >= clickT;
      });
      return {
        clickT,
        firstDetailCommitT: firstCommit,
        firstHostT: window.__navFirstHostT,
        firstPopulatedT: window.__navFirstPopulatedT,
        samples: window.__navSamples,
        overlappingLongTasks: overlapping,
        maxLongTaskMs: overlapping.reduce(
          (max, task) => Math.max(max, task.duration),
          0,
        ),
        clickToCommitMs:
          firstCommit !== null ? firstCommit - clickT : null,
      };
    });

    await testInfo.attach("navigation-trace", {
      body: JSON.stringify(trace, null, 2),
      contentType: "application/json",
    });

    // Ordering invariant: click < first commit <= host <= populated.
    expect(trace.firstDetailCommitT).not.toBeNull();
    expect(trace.clickToCommitMs).not.toBeNull();
    expect(trace.clickToCommitMs!).toBeGreaterThanOrEqual(0);
    expect(trace.firstHostT).not.toBeNull();
    expect(trace.firstPopulatedT).not.toBeNull();
    expect(trace.firstHostT!).toBeGreaterThanOrEqual(
      trace.firstDetailCommitT!,
    );
    expect(trace.firstPopulatedT!).toBeGreaterThanOrEqual(
      trace.firstHostT!,
    );
    // At least one populated sample with an initialized editor.
    expect(
      trace.samples.some(
        (s: NavSample) => s.populated && s.editorInitialized === true,
      ),
    ).toBe(true);

    // Eager-head regression demonstration (F1/INV-1): synchronous editor
    // construction of a ~100-section document inside the route render must
    // surface as a several-hundred-ms Long Task overlapping click→commit.
    // On the deferred base this guardrail fails (no such task) — that is
    // the intended A/B signal, not a portable timing threshold.
    expect(trace.maxLongTaskMs).toBeGreaterThanOrEqual(200);
  });
});
