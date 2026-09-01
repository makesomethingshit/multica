import type { BrowserWindow } from "electron";
import {
  NAVIGATION_GESTURE_CHANNEL,
  navigationGestureFromSwipe,
} from "../shared/navigation-gestures";

export function installNavigationGestures(
  win: BrowserWindow,
  platform: NodeJS.Platform = process.platform,
): void {
  if (platform === "darwin") {
    win.on("swipe", (_event, direction) => {
      const gesture = navigationGestureFromSwipe(direction);
      if (!gesture) return;
      win.webContents.send(NAVIGATION_GESTURE_CHANNEL, gesture);
    });
    return;
  }

  if (platform !== "win32") return;

  win.on("app-command", (_event, command) => {
    if (command === "browser-backward") {
      win.webContents.send(NAVIGATION_GESTURE_CHANNEL, "back");
      return;
    }
    if (command === "browser-forward") {
      win.webContents.send(NAVIGATION_GESTURE_CHANNEL, "forward");
    }
  });
}
