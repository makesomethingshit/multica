import { expect, test } from "@playwright/test";
import { createTestApi, loginAsDefault, reloadAppPage } from "./helpers";
import type { TestApiClient } from "./fixtures";

type Frame = {
  issue: string;
  text: string;
  height: number;
  imageHeight: number;
  scroll: number;
  pageY: number;
  readonly: boolean;
  ready: boolean;
  imageLoaded: boolean;
};

declare global {
  interface Window {
    descriptionFrames: Frame[];
    recordDescription: boolean;
    releaseEditorCreates: () => void;
  }
}

const body = [
  "# Reentry A",
  "![Reentry image](/e2e-description.svg)",
  ...Array.from({ length: 100 }, (_, i) => `## Section ${i}\n\nParagraph ${i} of the cached description.\n\n- First item\n- Second item\n\n\`\`\`ts\nconst section = ${i};\n\`\`\``),
  "End of reentry A.",
].join("\n\n");

test.describe("#8083 description initialization", () => {
  test.describe.configure({ timeout: 120000 });
  let api: TestApiClient;

  test.beforeEach(async ({ page }) => {
    api = await createTestApi();
    await page.route("**/e2e-description.svg", route => route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="320"><rect width="640" height="320" fill="teal"/></svg>',
    }));
    await page.addInitScript(() => {
      window.descriptionFrames = [];
      window.recordDescription = false;
      function sample() {
        const host = document.querySelector('[data-testid="issue-description"]');
        const editor = host?.querySelector<HTMLElement>(".ProseMirror");
        const image = host?.querySelector("img");
        if (window.recordDescription && host) {
          const scroll = host.closest<HTMLElement>("[data-tab-scroll-root]");
          window.descriptionFrames.push({
            issue: scroll?.dataset.tabScrollRoot ?? "",
            text: editor?.textContent ?? "",
            height: host.getBoundingClientRect().height,
            imageHeight: image?.getBoundingClientRect().height ?? 0,
            scroll: scroll?.scrollTop ?? 0,
            pageY: window.scrollY,
            readonly: !!host.querySelector("[data-rich-content]"),
            ready: !!(editor as HTMLElement & { editor?: { isInitialized: boolean } } | null)?.editor?.isInitialized,
            imageLoaded: !!image?.complete && image.naturalWidth > 0,
          });
        }
        requestAnimationFrame(sample);
      }
      requestAnimationFrame(sample);
    });
  });

  test.afterEach(async () => { await api?.cleanup(); });

  test("cached detail/list re-entry has one populated surface and stable geometry", async ({ page }, testInfo) => {
    const a = await api.createIssue(`E2E Reentry A ${Date.now()}`, { description: body });
    const b = await api.createIssue(`E2E Reentry B ${Date.now()}`, { description: "Reentry B distinct description." });
    const slug = await loginAsDefault(page);
    const description = page.getByTestId("issue-description");
    const list = page.locator(`a[href="/${slug}/issues"]`).first();
    const open = async (id: string) => {
      await page.locator(`a[href$="/issues/${id}"]`).first().click();
      await expect(description.locator(".ProseMirror")).toBeVisible({ timeout: 30000 });
    };
    // Warm both query data and the image, just as re-entry does in #8083.
    await open(a.id);
    await expect(description.locator("img")).toBeVisible();
    await description.locator("img").evaluate((img: HTMLImageElement) => img.decode());
    await list.click();
    await open(b.id);
    await list.click();

    const visits: Frame[][] = [];
    for (const issue of [a, a, b, a, b, a]) {
      await expect(description).toHaveCount(0);
      await page.evaluate(() => { window.descriptionFrames = []; window.recordDescription = true; });
      await open(issue.id);
      await page.waitForFunction(() => window.descriptionFrames.length >= 3 && window.descriptionFrames.some(frame => frame.ready));
      // Sampling begins before navigation and continues through readiness.
      const frames = await page.evaluate(() => { window.recordDescription = false; return window.descriptionFrames; });
      visits.push(frames);
      const expected = issue.id === a.id ? "Reentry A" : "Reentry B";
      for (const frame of frames) {
        expect(frame.issue).toContain(issue.id);
        expect(frame.text).toContain(expected);
        expect(frame.readonly).toBe(false);
        expect(frame.height).toBeCloseTo(frames[0]!.height, 1);
        expect(frame.imageHeight).toBeCloseTo(frames[0]!.imageHeight, 1);
        expect(frame.scroll).toBe(frames[0]!.scroll);
        expect(frame.pageY).toBe(frames[0]!.pageY);
      }
      await list.click();
    }
    await testInfo.attach("description-frames", { body: JSON.stringify(visits, null, 2), contentType: "application/json" });
  });

  test("cold image loading preserves the initial description geometry", async ({ page }, testInfo) => {
    const issue = await api.createIssue(`E2E Cold Image ${Date.now()}`, { description: body });
    let release!: () => void;
    const response = new Promise<void>(resolve => { release = resolve; });
    await page.route("**/e2e-description.svg", async route => {
      await response;
      await route.fulfill({
        contentType: "image/svg+xml",
        body: '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="320"><rect width="640" height="320" fill="teal"/></svg>',
      });
    });
    try {
      await loginAsDefault(page);
      await page.evaluate(() => { window.descriptionFrames = []; window.recordDescription = true; });
      await page.locator(`a[href$="/issues/${issue.id}"]`).first().click();
      const host = page.getByTestId("issue-description");
      await expect(host.locator(".ProseMirror")).toBeVisible({ timeout: 30000 });
      await page.waitForFunction(() => window.descriptionFrames.length >= 3);
      await testInfo.attach("cold-image-before", { body: await page.screenshot(), contentType: "image/png" });
      release();
      await page.waitForFunction(() => window.descriptionFrames.filter(frame => frame.imageLoaded).length >= 3);
      const frames = await page.evaluate(() => { window.recordDescription = false; return window.descriptionFrames; });
      await testInfo.attach("cold-image-frames", { body: JSON.stringify(frames), contentType: "application/json" });
      expect(frames.some(frame => !frame.imageLoaded)).toBe(true);
      for (const frame of frames) {
        expect(frame.text).toContain("Reentry A");
        expect(frame.imageHeight).toBeGreaterThan(0);
        expect(frame.height).toBeCloseTo(frames[0]!.height, 1);
        expect(frame.imageHeight).toBeCloseTo(frames[0]!.imageHeight, 1);
        expect(frame.scroll).toBe(frames[0]!.scroll);
        expect(frame.pageY).toBe(frames[0]!.pageY);
      }
      await testInfo.attach("cold-image-after", { body: await page.screenshot(), contentType: "image/png" });
      await host.locator(".image-toolbar button").first().click();
      await expect(page.getByRole("dialog")).toBeVisible();
      await page.keyboard.press("Escape");
    } finally {
      release();
    }
  });

  test("first edit and file drop survive startup and save on immediate navigation", async ({ page }) => {
    const issue = await api.createIssue(`E2E Startup ${Date.now()}`, { description: body });
    // Hold only Tiptap's existing create task until both inputs arrive. A
    // fixed delay would race slower browsers or the first route compilation.
    await page.addInitScript(() => {
      const schedule = window.setTimeout;
      const pending: (() => void)[] = [];
      window.releaseEditorCreates = () => { pending.splice(0).forEach(release => release()); };
      window.setTimeout = ((callback: TimerHandler, delay?: number, ...args: unknown[]) => {
        if (typeof callback === "function" && /\.emit\(["']create["']/.test(callback.toString())) {
          const timer = schedule(callback, 60000, ...args);
          pending.push(() => { clearTimeout(timer); callback(...args); });
          return timer;
        }
        return schedule(callback, delay, ...args);
      }) as typeof window.setTimeout;
    });
    const slug = await loginAsDefault(page);
    await reloadAppPage(page);
    await page.locator(`a[href$="/issues/${issue.id}"]`).first().click();
    const host = page.getByTestId("issue-description");
    const editor = host.locator(".ProseMirror");
    await expect(editor).toBeVisible();
    expect(await editor.evaluate(el => (el as HTMLElement & { editor: { isInitialized: boolean } }).editor.isInitialized)).toBe(false);
    await editor.locator("h1").click();
    await page.keyboard.insertText("FIRSTEDIT ");
    await host.evaluate(el => {
      const data = new DataTransfer();
      data.items.add(new File(["first drop"], "first-drop.txt", { type: "text/plain" }));
      el.dispatchEvent(new DragEvent("drop", { bubbles: true, cancelable: true, dataTransfer: data }));
    });
    expect(await editor.evaluate(el => (el as HTMLElement & { editor: { isInitialized: boolean } }).editor.isInitialized)).toBe(false);
    await page.evaluate(() => window.releaseEditorCreates());
    await page.waitForFunction(() => (document.querySelector('[data-testid="issue-description"] .ProseMirror') as HTMLElement & { editor: { isInitialized: boolean } })?.editor.isInitialized);
    await expect(editor).toContainText("FIRSTEDIT");
    await expect(editor).toContainText("first-drop.txt");
    await expect(editor.locator('[data-uploading]')).toHaveCount(0);
    await page.locator(`a[href="/${slug}/issues"]`).first().click();
    await page.locator(`a[href$="/issues/${issue.id}"]`).first().click();
    await expect(page.getByTestId("issue-description")).toContainText("FIRSTEDIT");
    await expect(page.getByTestId("issue-description")).toContainText("first-drop.txt");
  });
});
