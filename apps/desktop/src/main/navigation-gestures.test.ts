// @vitest-environment node
import type { BrowserWindow } from "electron";
import { describe, expect, it, vi } from "vitest";
import { NAVIGATION_GESTURE_CHANNEL } from "../shared/navigation-gestures";
import { installNavigationGestures } from "./navigation-gestures";

function makeWindow() {
  let swipeHandler:
    | ((event: unknown, direction: string) => void)
    | undefined;
  let appCommandHandler:
    | ((event: unknown, command: string) => void)
    | undefined;

  const win = {
    on: vi.fn(
      (event: string, handler: (event: unknown, direction: string) => void) => {
        if (event === "swipe") swipeHandler = handler;
        if (event === "app-command") appCommandHandler = handler;
        return win;
      },
    ),
    webContents: {
      send: vi.fn(),
    },
  };

  return {
    win: win as unknown as BrowserWindow,
    send: win.webContents.send,
    emitSwipe: (direction: string) => swipeHandler?.({}, direction),
    emitAppCommand: (command: string) => appCommandHandler?.({}, command),
  };
}

describe("installNavigationGestures", () => {
  it("registers macOS swipe navigation", () => {
    const { win, send, emitSwipe } = makeWindow();

    installNavigationGestures(win, "darwin");

    emitSwipe("right");
    expect(send).toHaveBeenCalledWith(NAVIGATION_GESTURE_CHANNEL, "back");

    emitSwipe("left");
    expect(send).toHaveBeenCalledWith(NAVIGATION_GESTURE_CHANNEL, "forward");
  });

  it("ignores non-horizontal swipe directions", () => {
    const { win, send, emitSwipe } = makeWindow();

    installNavigationGestures(win, "darwin");
    emitSwipe("up");

    expect(send).not.toHaveBeenCalled();
  });

  it("does not register on non-mac platforms", () => {
    const { win, send, emitSwipe } = makeWindow();

    installNavigationGestures(win, "linux");
    emitSwipe("right");

    expect(send).not.toHaveBeenCalled();
  });

  it("maps Windows browser app commands to navigation gestures", () => {
    const { win, send, emitSwipe, emitAppCommand } = makeWindow();

    installNavigationGestures(win, "win32");

    // Windows uses native app commands, not the macOS swipe event.
    emitSwipe("right");
    emitAppCommand("browser-backward");
    emitAppCommand("browser-forward");

    expect(send).toHaveBeenNthCalledWith(1, NAVIGATION_GESTURE_CHANNEL, "back");
    expect(send).toHaveBeenNthCalledWith(
      2,
      NAVIGATION_GESTURE_CHANNEL,
      "forward",
    );
    expect(send).toHaveBeenCalledTimes(2);
  });

  it("ignores unknown Windows app commands", () => {
    const { win, send, emitAppCommand } = makeWindow();

    installNavigationGestures(win, "win32");
    emitAppCommand("browser-refresh");

    expect(send).not.toHaveBeenCalled();
  });
});
