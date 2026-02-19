import { activeWindow as getActiveWin } from 'get-windows';
import { execSync } from 'child_process';
import logger from '../utils/logger.js';

// Browser-specific AppleScript to get active tab title (no Screen Recording needed)
const BROWSER_TAB_SCRIPTS = {
  'Google Chrome': 'tell application "Google Chrome" to get title of active tab of front window',
  'Chromium': 'tell application "Chromium" to get title of active tab of front window',
  'Google Chrome Canary': 'tell application "Google Chrome Canary" to get title of active tab of front window',
  'Brave Browser': 'tell application "Brave Browser" to get title of active tab of front window',
  'Microsoft Edge': 'tell application "Microsoft Edge" to get title of active tab of front window',
  'Vivaldi': 'tell application "Vivaldi" to get title of active tab of front window',
  'Opera': 'tell application "Opera" to get title of active tab of front window',
  'Safari': 'tell application "Safari" to get name of front document',
  'Safari Technology Preview': 'tell application "Safari Technology Preview" to get name of front document',
};

/**
 * Get the currently active window info (app name + window title + optional URL).
 * Uses get-windows for app name, URL, and title.
 * Falls back to browser-specific AppleScript for tab titles when Screen Recording
 * permission is not granted (get-windows returns empty title without it).
 */
export async function getActiveWindow() {
  try {
    const win = await getActiveWin();

    if (!win) {
      return { appName: 'unknown', windowTitle: 'unknown', url: null };
    }

    const appName = win.owner?.name || 'unknown';
    let windowTitle = win.title || '';
    const url = win.url || null;

    // If title is empty (Screen Recording permission not granted),
    // try browser-specific AppleScript as fallback
    if (!windowTitle && process.platform === 'darwin') {
      const browserScript = BROWSER_TAB_SCRIPTS[appName];
      if (browserScript) {
        try {
          windowTitle = execSync(
            `osascript -e '${browserScript}' 2>/dev/null`,
            { timeout: 1500, encoding: 'utf-8' }
          ).trim();
        } catch (e) {
          // AppleScript failed — use URL domain as last resort
        }
      }

      // Last resort: use URL domain as title
      if (!windowTitle && url) {
        try {
          windowTitle = new URL(url).hostname;
        } catch (e) {
          windowTitle = url;
        }
      }
    }

    return {
      appName,
      windowTitle: windowTitle || 'unknown',
      url
    };
  } catch (error) {
    logger.error('Failed to get active window', { error: error.message });
    return { appName: 'unknown', windowTitle: 'unknown', url: null };
  }
}

/**
 * Check if the system is idle (screen locked or no recent input).
 * Mac only for now — uses HIDIdleTime.
 */
export function isSystemIdle(idleThresholdMs = 300000) {
  try {
    if (process.platform === 'darwin') {
      const idleTime = execSync(
        'ioreg -c IOHIDSystem | awk \'/HIDIdleTime/ {print $NF; exit}\'',
        { timeout: 2000, encoding: 'utf-8' }
      ).trim();
      const idleMs = parseInt(idleTime, 10) / 1000000; // nanoseconds to ms
      return idleMs > idleThresholdMs;
    }
    // Windows: could use GetLastInputInfo, but skip for now
    return false;
  } catch (error) {
    logger.error('Failed to check idle state', { error: error.message });
    return false;
  }
}
