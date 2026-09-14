import { expect, test, type Page } from "@playwright/test";
import { createTestApi, loginAsDefault } from "./helpers";
import type { TestApiClient } from "./fixtures";

/**
 * MUL-7095 / PR #8092 Revision 3 — navigation performance recorder.
 *
 * §0-A requirement: measurement must begin BEFORE issue navigation (at link
 * activation), not once the description host already exists. This spec
 * records, per navigation:
 *
 * - link click timestamp (`navClickT`);
 * - first committed detail frame for the TARGET issue (first rAF sample with
 *   `t >= clickT` whose `[data-tab-scroll-root]` exactly equals
 *   `main:<targetId>`, `firstDetailCommitT`). Matching the target id (not a
 *   `main:` prefix) and requiring `t >= clickT` keeps a stale root from a
 *   pre-click frame from passing as this navigation's commit.
 * - first frame containing the target issue's description surface, at or
 *   after this navigation's commit (`firstHostT`);
 * - first frame containing populated description content (`.ProseMirror`
 *   with non-empty text, at or after commit, `firstPopulatedT`);
 * - editor `isInitialized` at each sample;
 * - Long Tasks clipped to [click, first commit]: each overlapping entry
 *   contributes only its intersection (`overlapMs`), exported as both max
 *   (`maxOverlapMs`) and sum (`totalOverlapMs`).
 *
 * No portable timing threshold is asserted: absolute numbers are
 * machine-specific. The spec writes the raw trace to a JSON attachment
 * (`navigation-trace`) and, when `NAV_TRACE_REPORT_PATH` is set, to that
 * file for ref-to-ref A/B runners. Ordering is the only in-spec invariant:
 * `clickT < firstDetailCommitT <= firstHostT <= firstPopulatedT`, plus one
 * populated initialized sample. A/B verdicts are relative guardrails
 * applied outside this spec via scripts/nav-trace-compare.mjs (base vs
 * head vs revised, same fixture + environment).
 *
 * Selectors are base/main/head compatible: the commit root
 * (`[data-tab-scroll-root]`) exists on all three refs, and the description
 * surface is found via the drop-zone container (head: the same element
 * carrying `data-testid="issue-description"`; base/main: its structurally
 * identical container without the testid). Nothing here keys on
 * `data-testid="issue-description"`, so the same spec runs unchanged on
 * every ref.
 *
 * Cold compilation outliers are discarded separately: the first navigation
 * after load is a warm-up sample, excluded from the measured pass.
 */

type NavSample = {
  t: number;
  commitRoot: string | null;
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

type OverlapTask = LongTaskEntry & {
  overlapStart: number;
  overlapEnd: number;
  overlapMs: number;
};

declare global {
  interface Window {
    __navInstalled: boolean;
    __navSamples: NavSample[];
    __navRecord: boolean;
    __navLongTasks: LongTaskEntry[];
    __navClickT: number;
    __navTargetId: string;
    __navFirstDetailCommitT: number | null;
    __navFirstHostT: number | null;
    __navFirstPopulatedT: number | null;
    __startNavRecording: (targetId: string) => void;
  }
}

/**
 * Base/main have no `data-testid="issue-description"` (added on the PR head
 * at issue-detail.tsx:3023). The container is still locatable on all refs:
 * it is the drop-zone div wrapping the description editor's `.ProseMirror`,
 * inside the detail scroll root. This mirrors that host div (same position
 * relative to `.ProseMirror`) instead of keying on the head-only testid.
 */
function descriptionSurface(page: Page, issueId?: string) {
  const root = issueId
    ? `[data-tab-scroll-root="main:${issueId}"]`
    : '[data-tab-scroll-root^="main:"]';
  return page
    .locator(`${root} .ProseMirror`)
    .first()
    .locator("xpath=ancestor::div[contains(@class,'relative')][contains(@class,'mt-5')][1]");
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
      // SPA client navigation (window state persists across open()/list
      // trips). The guard keeps a reload from double-registering the
      // observer (duplicate entries) or wiping an in-flight recording.
      if (window.__navInstalled) return;
      window.__navInstalled = true;
      window.__navSamples = [];
      window.__navLongTasks = [];
      window.__navRecord = false;
      window.__navClickT = 0;
      window.__navTargetId = "";
      window.__navFirstDetailCommitT = null;
      window.__navFirstHostT = null;
      window.__navFirstPopulatedT = null;
      // Started pre-click with buffered:true so tasks straddling navigation
      // are still observed; only the intersection with [click, first commit]
      // is counted at analysis time.
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
          const now = performance.now();
          // Target-locked root: the exact `main:<targetId>` element, not
          // the first scroll root in the document. The list's `list` root
          // or another issue's `main:<other>` root must never satisfy
          // commit, host or populated — cross-issue isolation for all
          // three marks alike.
          const want = `main:${window.__navTargetId}`;
          const targetRoot =
            Array.from(
              document.querySelectorAll<HTMLElement>("[data-tab-scroll-root]"),
            ).find(
              (candidate) =>
                candidate.getAttribute("data-tab-scroll-root") === want,
            ) ?? null;
          const rootValue =
            targetRoot?.getAttribute("data-tab-scroll-root") ?? null;
          // Pre-click frames (now < clickT) never count, even if a rAF
          // fired between flag-on and the actual click.
          const committed =
            now >= window.__navClickT && targetRoot !== null;
          // Head carries data-testid="issue-description" on this div;
          // base/main do not, so resolve from the editor upward instead —
          // always inside the TARGET root, never a stale sibling.
          const editor =
            targetRoot?.querySelector<HTMLElement>(".ProseMirror");
          const host =
            editor?.closest<HTMLElement>("div.relative.mt-5") ?? null;
          const text = editor?.textContent ?? "";
          const initialized = (
            editor as
              | (HTMLElement & { editor?: { isInitialized: boolean } })
              | null
              | undefined
          )?.editor?.isInitialized;
          if (committed && window.__navFirstDetailCommitT === null) {
            window.__navFirstDetailCommitT = now;
          }
          // Host/populated lock to the target commit: only samples at or
          // after this navigation's commit may set them, so a stale
          // surface from another issue can never satisfy them first.
          if (committed && host && window.__navFirstHostT === null) {
            window.__navFirstHostT = now;
          }
          if (
            committed &&
            text.length > 0 &&
            window.__navFirstPopulatedT === null
          ) {
            window.__navFirstPopulatedT = now;
          }
          window.__navSamples.push({
            t: now,
            commitRoot: rootValue,
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
      window.__startNavRecording = (targetId: string) => {
        window.__navSamples = [];
        window.__navTargetId = targetId;
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

  test("click-to-commit navigation trace with clipped Long Tasks", async ({
    page,
  }, testInfo) => {
    const a = await api.createIssue(`E2E Nav A ${Date.now()}`, {
      description: LONG_BODY,
    });
    const slug = await loginAsDefault(page);
    const list = page.locator(`a[href="/${slug}/issues"]`).first();
    const openLink = (id: string) =>
      page.locator(`a[href$="/issues/${id}"]`).first();
    const surface = () => descriptionSurface(page, a.id);
    const editorReady = () =>
      page.waitForFunction(
        (targetId: string) => {
          const root = document.querySelector<HTMLElement>(
            `[data-tab-scroll-root="main:${targetId}"]`,
          );
          return (
            (
              root?.querySelector(".ProseMirror") as HTMLElement & {
                editor?: { isInitialized: boolean };
              }
            )?.editor?.isInitialized === true
          );
        },
        a.id,
        { timeout: 30000 },
      );

    // Warm-up navigation (cold compilation outlier — discarded from the
    // comparison, kept only so the measured pass is warm).
    await openLink(a.id).click();
    await expect(surface().locator(".ProseMirror")).toBeVisible({
      timeout: 30000,
    });
    await list.click();
    await expect(surface()).toHaveCount(0);

    // Measured navigation: recorder starts BEFORE the click, target-locked.
    await page.evaluate((id: string) => window.__startNavRecording(id), a.id);
    await openLink(a.id).click();
    await expect(surface().locator(".ProseMirror")).toBeVisible({
      timeout: 30000,
    });
    // Wait until the populated editor reports initialized so the trace
    // covers the full startup path, not just the first commit.
    await editorReady();

    const trace = await page.evaluate(() => {
      window.__navRecord = false;
      const clickT = window.__navClickT;
      const targetId = window.__navTargetId;
      const firstCommit = window.__navFirstDetailCommitT;
      // Clip each overlapping task to [click, firstCommit]: only the
      // intersection counts toward max/sum, so a task straddling the
      // window edge is never billed for work outside it.
      const overlapping: OverlapTask[] = [];
      if (firstCommit !== null) {
        for (const task of window.__navLongTasks) {
          const overlapStart = Math.max(task.startTime, clickT);
          const overlapEnd = Math.min(task.startTime + task.duration, firstCommit);
          if (overlapEnd > overlapStart) {
            overlapping.push({
              ...task,
              overlapStart,
              overlapEnd,
              overlapMs: overlapEnd - overlapStart,
            });
          }
        }
      }
      return {
        targetId,
        clickT,
        firstDetailCommitT: firstCommit,
        firstHostT: window.__navFirstHostT,
        firstPopulatedT: window.__navFirstPopulatedT,
        samples: window.__navSamples,
        overlappingLongTasks: overlapping,
        maxOverlapMs: overlapping.reduce(
          (max, task) => Math.max(max, task.overlapMs),
          0,
        ),
        totalOverlapMs: overlapping.reduce(
          (sum, task) => sum + task.overlapMs,
          0,
        ),
        clickToCommitMs: firstCommit !== null ? firstCommit - clickT : null,
      };
    });

    const report = { ...trace, status: "ok", fixture: "navigation-trace-v1" };
    await testInfo.attach("navigation-trace", {
      body: JSON.stringify(report, null, 2),
      contentType: "application/json",
    });
    const reportPath = process.env.NAV_TRACE_REPORT_PATH;
    if (reportPath) {
      const { writeFileSync } = await import("node:fs");
      writeFileSync(reportPath, JSON.stringify(report, null, 2));
    }

    // Strict ordering invariant: click < first commit <= host <= populated.
    expect(trace.firstDetailCommitT).not.toBeNull();
    expect(trace.clickToCommitMs).not.toBeNull();
    expect(trace.clickToCommitMs!).toBeGreaterThan(0);
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

    // No absolute timing assertion here: A/B verdicts are relative
    // guardrails over the attached raw traces (base vs head vs revised,
    // same fixture + environment), not a portable millisecond threshold.
  });
});
