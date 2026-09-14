import { activeWindow as getActiveWin } from 'get-windows';
import { exec } from 'child_process';
import { promisify } from 'util';
import os from 'os';
import logger from '../utils/logger.js';

const execAsync = promisify(exec);

// Cache idle state — ioreg is expensive, only re-run every 4s
let _idleCache = { idle: false, ts: 0 };
const IDLE_CACHE_TTL = 4000;

// Cache active window — osascript is expensive, only re-run every 2s
let _windowCache = { result: null, ts: 0 };
const WINDOW_CACHE_TTL = 2000;

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

// ── Open-document path probes (authoritative — the app knows its own file) ──
// Per-app AppleScript to get the path of the front document. One-time TCC
// automation-consent prompt per app; runs in the background monitor (5s loop)
// so the 1-2s probe latency is invisible to the user. Fail soft → null.
const DOCUMENT_PATH_SCRIPTS = {
  'TextEdit': 'tell application "TextEdit" to get path of document 1',
  'Preview': 'tell application "Preview" to get path of document 1',
  'Pages': 'tell application "Pages" to get path of document 1',
  'Keynote': 'tell application "Keynote" to get path of document 1',
  'Numbers': 'tell application "Numbers" to get path of document 1',
  'Microsoft PowerPoint': 'tell application "Microsoft PowerPoint" to get path of active presentation',
  'Microsoft Word': 'tell application "Microsoft Word" to get path of active document',
  'Microsoft Excel': 'tell application "Microsoft Excel" to get path of active workbook',
  // Editors that expose `document 1` — may need per-app verification
  'Visual Studio Code': 'tell application "Visual Studio Code" to get path of document 1',
  'Code': 'tell application "Code" to get path of document 1',
  'Cursor': 'tell application "Cursor" to get path of document 1',
  'Windsurf': 'tell application "Windsurf" to get path of document 1',
  'Zed': 'tell application "Zed" to get path of document 1',
  'Sublime Text': 'tell application "Sublime Text" to get path of front window\'s document',
  'BBEdit': 'tell application "BBEdit" to get path of document 1',
  'Nova': 'tell application "Nova" to get path of document 1',
};

/**
 * Probe the active app for its open document path via AppleScript.
 * Authoritative — the app reports its own file. One-time TCC prompt per app.
 * @param {string} appName
 * @returns {Promise<string|null>} absolute file path or null
 */
async function _probeDocumentPath(appName) {
  if (!appName || process.platform !== 'darwin') return null;
  const script = DOCUMENT_PATH_SCRIPTS[appName];
  if (!script) return null;
  try {
    const { stdout } = await execAsync(`osascript -e '${script.replace(/'/g, '\'\\\'\'')}' 2>/dev/null`, {
      timeout: 2000,
    });
    const p = stdout.trim();
    // AppleScript returns 'missing value' or empty when no doc is open
    if (!p || p === 'missing value' || p === '') return null;
    return p;
  } catch (e) {
    // TCC denied, app not scriptable, or no document — fail soft
    return null;
  }
}

// Per-filename resolution cache — avoids re-running mdfind/find for the same
// title on every monitor tick. TTL 60s; unresolved titles (null) also cached
// so "Untitled" doesn't trigger a find every 5s.
const _pathResolveCache = new Map();
const PATH_CACHE_TTL = 60000;

/**
 * Infer the open file path from the window title + mdfind (no TCC needed).
 * Parses the filename from the title (split on ` — ` / ` - `), then resolves
 * the full path via Spotlight. Returns null if no filename-like token is found
 * or mdfind returns nothing.
 *
 * @param {string} windowTitle
 * @param {string} appName
 * @param {object} [opts] — { deep: boolean } — when true, fall back to a bounded
 *   `find` over common roots (including hidden dirs like ~/.devin that Spotlight
 *   doesn't index) if mdfind misses. ~3s cost; only use at request time, not in
 *   the monitor tick loop.
 * @returns {Promise<string|null>}
 */
export async function _inferPathFromTitle(windowTitle, appName, opts = {}) {
  if (!windowTitle || windowTitle === 'unknown') return null;
  // Strip common app-name suffixes: "file.png — Preview", "file.js - Visual Studio Code"
  // Take the side that looks like a filename (has a dot + extension).
  const parts = windowTitle.split(/\s+[—-]\s+/);
  let filename = null;
  for (const part of parts) {
    let trimmed = part.trim();
    // Strip leading dirty markers (●, •, *) — VS Code prefixes unsaved files
    trimmed = trimmed.replace(/^[●•*]\s*/, '');
    // Has an extension and no path separators → candidate filename
    if (/\.\w{1,10}$/.test(trimmed) && !/[\\/]/.test(trimmed)) {
      filename = trimmed;
      break;
    }
  }
  if (!filename) return null;

  // Check cache — key includes deep flag so a shallow (mdfind-only) miss does
  // NOT mask a request-time deep find (they resolve different root sets).
  const cacheKey = `${filename}|${opts.deep === true ? 'deep' : 'shallow'}`;
  const cached = _pathResolveCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < PATH_CACHE_TTL) {
    return cached.path;
  }

  const result = await _resolveFilename(filename, opts.deep === true);
  _pathResolveCache.set(cacheKey, { path: result, ts: Date.now() });
  return result;
}

/**
 * Resolve a filename to a full path via mdfind (fast, Spotlight-indexed) then
 * optionally a bounded find (slow, covers hidden dirs Spotlight doesn't index).
 * @param {string} filename
 * @param {boolean} deep — run find fallback if mdfind misses
 * @returns {Promise<string|null>}
 */
async function _resolveFilename(filename, deep) {
  // ── Tier 1: mdfind (Spotlight) — fast, covers most user dirs ──
  try {
    const { stdout } = await execAsync(`mdfind -name ${JSON.stringify(filename)} 2>/dev/null | head -5`, {
      timeout: 2000,
    });
    const lines = stdout.trim().split('\n').filter(Boolean);
    if (lines.length > 0) {
      if (lines.length === 1) return lines[0];
      // Multiple matches — pick the most recently modified (stat -f %m)
      return await _pickMostRecent(lines);
    }
  } catch (e) { /* mdfind failed — fall through */ }

  if (!deep) return null;

  // ── Tier 2: bounded find — covers ~/.devin, ~/.thinkdrop, ~/.config ──
  // Spotlight doesn't index hidden directories, so files in ~/.devin/plans/
  // (e.g. Devin plan files) are invisible to mdfind. ~3s cost; request-time only.
  try {
    const home = process.env.HOME || os.homedir();
    const roots = [
      `${home}/.devin`,
      `${home}/.thinkdrop`,
      `${home}/.config`,
      `${home}/Desktop`,
      `${home}/Documents`,
      `${home}/Downloads`,
    ].filter(Boolean);
    // Prune node_modules/.git/dist to avoid scanning huge dirs
    const findCmd = `find ${roots.map(r => JSON.stringify(r)).join(' ')} ` +
      '\\( -name node_modules -o -name .git -o -name dist \\) -prune ' +
      `-o -name ${JSON.stringify(filename)} -print 2>/dev/null | head -10`;
    const { stdout } = await execAsync(findCmd, { timeout: 5000 });
    const lines = stdout.trim().split('\n').filter(Boolean);
    if (lines.length > 0) {
      if (lines.length === 1) return lines[0];
      return await _pickMostRecent(lines);
    }
    logger.debug(`[activeWindow] Deep find found no matches for "${filename}"`);
  } catch (e) {
    logger.debug(`[activeWindow] Deep find failed for "${filename}": ${e.message} (code=${e.code}, killed=${e.killed})`);
  }

  return null;
}

/**
 * Pick the most recently modified file from a list of paths.
 * @param {string[]} lines
 * @returns {Promise<string>}
 */
async function _pickMostRecent(lines) {
  let best = lines[0];
  let bestMtime = 0;
  for (const line of lines) {
    try {
      const { stdout: mtimeOut } = await execAsync(`stat -f %m ${JSON.stringify(line)} 2>/dev/null`, { timeout: 1000 });
      const mtime = parseInt(mtimeOut.trim(), 10) || 0;
      if (mtime > bestMtime) { bestMtime = mtime; best = line; }
    } catch (_) { /* skip */ }
  }
  return best;
}

/**
 * Get the currently active window info (app name + window title + optional URL).
 * Uses get-windows for app name, URL, and title.
 * Falls back to browser-specific AppleScript for tab titles when Screen Recording
 * permission is not granted (get-windows returns empty title without it).
 */
/**
 * Pure AppleScript fallback for macOS — no native binary needed.
 * Returns { appName, windowTitle }.
 */
async function getActiveWindowAppleScript() {
  const script = `
    tell application "System Events"
      set frontApp to name of first application process whose frontmost is true
    end tell
    set appTitle to ""
    try
      tell application frontApp
        set appTitle to name of front window
      end tell
    end try
    return frontApp & "|" & appTitle
  `;
  try {
    const { stdout: raw } = await execAsync(`osascript -e '${script.replace(/\n/g, ' ')}' 2>/dev/null`, {
      timeout: 2000
    });
    const result = raw.trim();
    const [appName, windowTitle] = result.split('|');
    const cleanAppName = appName?.trim() || 'unknown';

    // Try browser-specific tab title
    const browserScript = BROWSER_TAB_SCRIPTS[cleanAppName];
    let title = windowTitle?.trim() || '';
    if (!title && browserScript) {
      try {
        const { stdout } = await execAsync(`osascript -e '${browserScript}' 2>/dev/null`, { timeout: 1500 });
        title = stdout.trim();
      } catch (e) { /* ignore */ }
    }

    // ── Resolve open document path (same layered strategy as the primary path) ──
    let filePath = null;
    if (!browserScript && !['Electron', 'ThinkDrop'].includes(cleanAppName)) {
      filePath = await _probeDocumentPath(cleanAppName);
      if (!filePath) {
        filePath = await _inferPathFromTitle(title, cleanAppName);
      }
    }

    return { appName: cleanAppName, windowTitle: title || 'unknown', url: null, bounds: null, filePath };
  } catch (e) {
    return { appName: 'unknown', windowTitle: 'unknown', url: null, bounds: null, filePath: null };
  }
}

export async function getActiveWindow() {
  const now = Date.now();
  if (_windowCache.result && now - _windowCache.ts < WINDOW_CACHE_TTL) {
    return _windowCache.result;
  }
  try {
    const win = await getActiveWin();

    if (!win) {
      return { appName: 'unknown', windowTitle: 'unknown', url: null, bounds: null, filePath: null };
    }

    const appName = win.owner?.name || 'unknown';
    let windowTitle = win.title || '';
    const url = win.url || null;
    const bounds = win.bounds || null;

    // If title is empty (Screen Recording permission not granted),
    // try browser-specific AppleScript as fallback (async — non-blocking)
    if (!windowTitle && process.platform === 'darwin') {
      const browserScript = BROWSER_TAB_SCRIPTS[appName];
      if (browserScript) {
        try {
          const { stdout } = await execAsync(`osascript -e '${browserScript}' 2>/dev/null`, { timeout: 1500 });
          windowTitle = stdout.trim();
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

    // ── Resolve open document path (layered: AppleScript primary, mdfind fallback) ──
    // Skip for browsers (URL is the relevant context, not a file path) and the
    // ThinkDrop overlay itself. Run only the probe matching the resolved appName.
    let filePath = null;
    if (appName && appName !== 'unknown' && !BROWSER_TAB_SCRIPTS[appName] && !['Electron', 'ThinkDrop'].includes(appName)) {
      filePath = await _probeDocumentPath(appName);
      if (!filePath) {
        filePath = await _inferPathFromTitle(windowTitle, appName);
      }
    }

    const result = { appName, windowTitle: windowTitle || 'unknown', url, bounds, filePath };
    _windowCache = { result, ts: Date.now() };
    return result;
  } catch (error) {
    // get-windows binary failed — fall back to AppleScript on macOS
    if (process.platform === 'darwin') {
      const result = await getActiveWindowAppleScript();
      _windowCache = { result, ts: Date.now() };
      return result;
    }
    logger.error('Failed to get active window', { error: error.message });
    return { appName: 'unknown', windowTitle: 'unknown', url: null, bounds: null, filePath: null };
  }
}

/**
 * Check if the system is idle (screen locked or no recent input).
 * Async — uses ioreg via exec (non-blocking). Cached for IDLE_CACHE_TTL ms.
 */
export async function isSystemIdle(idleThresholdMs = 300000) {
  const now = Date.now();
  if (now - _idleCache.ts < IDLE_CACHE_TTL) {
    return _idleCache.idle;
  }
  try {
    if (process.platform === 'darwin') {
      const { stdout } = await execAsync(
        'ioreg -c IOHIDSystem | awk \'/HIDIdleTime/ {print $NF; exit}\'',
        { timeout: 2000 }
      );
      const idleMs = parseInt(stdout.trim(), 10) / 1000000;
      const idle = idleMs > idleThresholdMs;
      _idleCache = { idle, ts: now };
      return idle;
    }
    return false;
  } catch (error) {
    logger.error('Failed to check idle state', { error: error.message });
    return false;
  }
}
