import { activeWindow as getActiveWin, openWindows } from 'get-windows';
import { exec, execSync } from 'child_process';
import { promisify } from 'util';
import os from 'os';
import path from 'path';
import fs from 'fs';
import logger from '../utils/logger.js';
import { getOCRService } from './ocrService.js';

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

// Browser-specific AppleScript to get the active tab URL. Uses the Automation
// (Apple Events) TCC grant — a separate permission from Accessibility (which
// get-windows needs for win.url) and from Screen Recording. macOS prompts once
// per browser and remembers denial, after which calls fail fast → Tier 2.
const BROWSER_URL_SCRIPTS = {
  'Google Chrome': 'tell application "Google Chrome" to get URL of active tab of front window',
  'Chromium': 'tell application "Chromium" to get URL of active tab of front window',
  'Google Chrome Canary': 'tell application "Google Chrome Canary" to get URL of active tab of front window',
  'Brave Browser': 'tell application "Brave Browser" to get URL of active tab of front window',
  'Microsoft Edge': 'tell application "Microsoft Edge" to get URL of active tab of front window',
  'Vivaldi': 'tell application "Vivaldi" to get URL of active tab of front window',
  'Opera': 'tell application "Opera" to get URL of active tab of front window',
  'Safari': 'tell application "Safari" to get URL of front document',
  'Safari Technology Preview': 'tell application "Safari Technology Preview" to get URL of front document',
};

// Per-app URL probe cache. Resolved URLs cache briefly (user may switch tabs);
// failures cache only briefly so a transient OCR miss is retried promptly.
const _urlProbeCache = new Map();
const URL_PROBE_OK_TTL = 3000;
const URL_PROBE_FAIL_TTL = 8000;

/** True when the app is a browser we know how to URL-probe. */
export function isBrowserApp(appName) {
  return !!BROWSER_URL_SCRIPTS[appName];
}

/**
 * Normalize a raw URL-ish string into a usable URL, or null.
 * @param {string} raw
 * @returns {string|null}
 */
export function _normalizeUrl(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (!s || s === 'missing value') return null;
  if (/^https?:\/\/\S+/i.test(s)) {
    // Validate the RAW host — new URL() would canonicalize "4.2" → "4.0.0.2"
    // (a valid IPv4), hiding that the token was a star rating, not an address.
    const m = s.match(/^https?:\/\/([^/:?#]+)/i);
    return (m && _isPlausibleHost(m[1])) ? s : null;
  }
  if (/^localhost(:\d+)?(\/\S*)?$/i.test(s)) return `http://${s}`;
  if (_DOMAIN_TOKEN_RE.test(s) || _IPV4_TOKEN_RE.test(s)) return `https://${s}`;
  return null;
}

// Plausible navigable host: localhost, valid IPv4, or labels ending in an
// alpha TLD. Rejects OCR junk like "4.2" (ratings) and "1.61.1" (versions).
function _isPlausibleHost(h) {
  if (!h) return false;
  if (h === 'localhost') return true;
  if (/^(?:\d{1,3}\.){3}\d{1,3}$/.test(h)) {
    return h.split('.').every(o => +o <= 255);
  }
  return /(?:^|\.)[a-z]{2,63}$/i.test(h) && h.includes('.');
}

/**
 * Tier 1 — probe the browser's front-tab URL via AppleScript (Automation grant).
 * Fail-open: returns null on denial/timeout/app-closed.
 * @param {string} appName
 * @returns {Promise<string|null>}
 */
/**
 * Ask Electron main (the overlay control server, 127.0.0.1:3010) to run the
 * browser URL AppleScript — ThinkDrop.app is the TCC responsible process, so
 * the consent prompt actually renders.
 * @returns {Promise<string|false|null>} url on hit; false when the endpoint
 *   answered but the script failed (don't retry locally — same TCC result);
 *   null when the server is unreachable (caller falls back to local osascript)
 */
async function _probeUrlViaOverlayServer(appName) {
  try {
    const res = await fetch('http://127.0.0.1:3010/browser-url', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ app: appName }),
      signal: AbortSignal.timeout(3000),
    });
    const data = await res.json().catch(() => null);
    // 404 / unparseable → the running app predates the endpoint: treat as
    // unreachable so the caller falls back to local osascript.
    if (!data || res.status === 404 || data.error === 'Not Found') return null;
    if (data.ok && data.url) {
      logger.info(`[activeWindow] url-probe applescript ${appName}: hit ${data.url} (via ThinkDrop.app)`);
      return data.url;
    }
    logger.info(`[activeWindow] url-probe applescript ${appName}: miss via ThinkDrop.app (${String(data.error || 'empty').slice(0, 100)})`);
    return false;
  } catch (_) {
    return null; // overlay server unreachable — Electron not running
  }
}

export async function probeBrowserUrl(appName) {
  if (!appName || process.platform !== 'darwin') return null;
  const script = BROWSER_URL_SCRIPTS[appName];
  if (!script) return null;
  const cached = _urlProbeCache.get(appName);
  if (cached && Date.now() - cached.ts < (cached.url ? URL_PROBE_OK_TTL : URL_PROBE_FAIL_TTL)) {
    return cached.url;
  }
  // Prefer running the AppleScript under ThinkDrop.app (Electron main's
  // /browser-url endpoint): the TCC responsible process is the bundled app,
  // so the Automation consent dialog actually renders and is remembered.
  // This service is a detached node process — local osascript gets a silent
  // -1743 (automation-denied) with no prompt ever shown.
  const viaApp = await _probeUrlViaOverlayServer(appName);
  if (typeof viaApp === 'string' && viaApp) {
    _urlProbeCache.set(appName, { url: viaApp, ts: Date.now() });
    return viaApp;
  }
  if (viaApp === false) {
    // Endpoint answered but the AS failed under ThinkDrop.app too (denied or
    // no scriptable window) — a local retry would hit the same wall. Cache
    // the miss and let T2 handle it.
    _urlProbeCache.set(appName, { url: null, ts: Date.now() });
    return null;
  }
  let url = null;
  let failReason = null;
  try {
    // stderr kept (no 2>/dev/null) so failures are diagnosable in the log:
    // -1743 = Automation denied, -1719 = no window on this space (e.g. the
    // browser window is fullscreen, which AS can't enumerate), timeout = TCC
    // prompt likely waiting unseen.
    const { stdout } = await execAsync(`osascript -e '${script.replace(/'/g, '\'\\\'\'')}'`, { timeout: 1500 });
    url = _normalizeUrl(stdout);
    if (!url) failReason = `empty/non-url stdout: ${String(stdout).trim().slice(0, 80)}`;
  } catch (e) {
    failReason = (e.killed || /timeout/i.test(e.message)) ? 'timeout'
      : /-1743/.test(e.message) ? 'automation-denied'
        : /-1719|Invalid index/.test(e.message) ? 'no-scriptable-window (fullscreen/other-space)'
          : e.message.slice(0, 120);
  }
  logger.info(`[activeWindow] url-probe applescript ${appName}: ${url ? `hit ${url}` : `miss (${failReason})`}`);
  _urlProbeCache.set(appName, { url, ts: Date.now() });
  return url;
}

/**
 * Resolve an app's main on-screen window via get-windows openWindows() — the
 * authoritative rect + CGWindowID. getActiveWin()'s bounds are occasionally
 * degenerate (toolbar-only rects), so prefer this for capture cropping.
 * Bounds/owner come from CGWindowList — available without extra permissions.
 * @param {string} appName
 * @returns {Promise<{id:number|null,x:number,y:number,w:number,h:number}|null>}
 */
export async function getMainWindowInfo(appName) {
  if (!appName || process.platform !== 'darwin') return null;
  try {
    const wins = await openWindows();
    const candidates = (wins || []).filter(w =>
      w.owner?.name === appName && w.bounds?.width > 0 && w.bounds?.height > 0);
    if (!candidates.length) return null;
    const w = candidates.reduce((a, b) =>
      (b.bounds.width * b.bounds.height > a.bounds.width * a.bounds.height ? b : a));
    return { id: w.id ?? null, x: w.bounds.x, y: w.bounds.y, w: w.bounds.width, h: w.bounds.height };
  } catch (_) { /* get-windows failed — fail soft */ }
  return null;
}

// Shape gate for _normalizeUrl — the last-resort validator. The PRIMARY
// validator is history membership (resolveUrlCandidateViaHistory): a token
// only becomes a page URL when it exists in the browser's own record. Shape
// alone can never distinguish "4.2" (star rating) from a domain — existence
// can. Alpha TLD ≥2 keeps bare numerics ("4.2", "1.61.1") out of the fallback.
const _DOMAIN_TOKEN_RE = /^(?:[a-z0-9-]+\.)+[a-z]{2,63}(:\d+)?(\/\S*)?$/i;
const _IPV4_TOKEN_RE = /^(?:\d{1,3}\.){3}\d{1,3}(:\d+)?(\/\S*)?$/;
// Characters OCR commonly invents around omnibox text (› breadcrumbs, ellipses…)
const _TOKEN_STRIP_RE = /^[^\w]+|[^\w/:.?&=%#@~+-]+$/g;

// Loose URL-ish GENERATOR — a token qualifies as a candidate if it has a dot
// plus a letter, or an explicit scheme/www/localhost/IPv4 marker. Deliberately
// permissive: generation is not validation — history membership decides what's
// real. "4.2" (dot, no letter) doesn't even qualify as a candidate.
function _looksUrlish(tok) {
  if (/\s/.test(tok)) return false;
  return /^https?:\/\/\S+/i.test(tok) || /^www\.\S+/i.test(tok)
      || /^localhost(:\d+)?(\/\S*)?$/i.test(tok)
      || /^(?:\d{1,3}\.){3}\d{1,3}(:\d+)?(\/\S*)?$/.test(tok)
      || (tok.includes('.') && /[a-z]/i.test(tok));
}

/**
 * Extract ranked URL-ish candidates from Tesseract word/line boxes captured
 * over the browser toolbar strip. Longer tokens score higher (a full
 * "host/path" beats a bare domain); height tiebreaks toward omnibox text.
 * Returns [{token, score, y}] sorted best-first — callers resolve each
 * against History (ground truth) rather than trusting shape.
 * @param {Array<{text:string, bbox:{x0:number,y0:number,x1:number,y1:number}}>} words
 * @returns {Array<{token:string, score:number, y:number}>}
 */
export function _extractUrlCandidates(words) {
  if (!Array.isArray(words)) return [];
  const out = [];
  for (const w of words) {
    const raw = (w?.text || '').trim().replace(_TOKEN_STRIP_RE, '');
    if (!raw || !_looksUrlish(raw)) continue;
    const height = w.bbox ? (w.bbox.y1 - w.bbox.y0) : 0;
    out.push({ token: raw, score: raw.length * 1000 + height, y: w?.bbox?.y0 ?? 0 });
  }
  return out.sort((a, b) => b.score - a.score);
}

/** Top candidate only (legacy callers). */
export function _extractUrlCandidate(words) {
  return _extractUrlCandidates(words)[0]?.token || null;
}

// Split structuredRows into positioned word tokens within the strip region.
function _rowsToWords(rows, opts = {}) {
  const yMin = opts.bounds ? (opts.bounds.y ?? 0) - 10 : -Infinity;
  const yMax = opts.bounds ? (opts.bounds.y ?? 0) + 140 : (opts.maxY ?? 200);
  const words = [];
  for (const r of rows || []) {
    if (!r || typeof r.text !== 'string') continue;
    const ry = r.y ?? 0;
    if (ry > yMax || ry < yMin) continue;
    const h = r.height || 0;
    for (const tok of r.text.split(/\s+/)) {
      if (tok) words.push({ text: tok, bbox: { x0: r.x || 0, y0: ry, x1: (r.x || 0) + (r.width || 0), y1: ry + h } });
    }
  }
  return words;
}

/**
 * Extract a URL candidate from LiteParser structuredRows ({text,x,y,width,
 * height}) — the positioned items monitorService stores per capture. Rows are
 * line-level, so each row is split into whitespace tokens sharing its box.
 * Restricted to the top strip (the address-bar region): page-body URLs must
 * not become the active-page referent.
 * @param {Array} rows
 * @param {object} [opts] — { maxY } top-strip cutoff in points (default 200),
 *   or { bounds:{y} } window bounds → strip = bounds.y .. bounds.y+140
 * @returns {string|null} raw candidate token (un-normalized)
 */
export function _extractUrlFromRows(rows, opts = {}) {
  return _extractUrlCandidate(_rowsToWords(rows, opts));
}

/** Ranked candidate list variant — for history-evidence resolution. */
export function _extractUrlCandidatesFromRows(rows, opts = {}) {
  return _extractUrlCandidates(_rowsToWords(rows, opts));
}

// Chromium-family profile roots for History DB URL expansion (Tier 3).
const _CHROMIUM_PROFILE_ROOTS = {
  'Google Chrome': 'Google/Chrome',
  'Chromium': 'Chromium',
  'Google Chrome Canary': 'Google/Chrome Canary',
  'Brave Browser': 'BraveSoftware/Brave-Browser',
  'Microsoft Edge': 'Microsoft Edge',
  'Vivaldi': 'Vivaldi',
  'Opera': 'com.operasoftware.Opera',
};

/**
 * Tier 3 — expand a possibly-truncated OCR URL to the full visited URL via the
 * browser's History SQLite DB (read-only immutable open; no lock contention).
 * Omniboxes elide the tail of long URLs ("…beef-lamb-" for "…-recipe"); the
 * History urls table holds the exact URL. Fail-open → null.
 * @param {string} partialUrl — URL candidate from OCR (or AS)
 * @param {string} appName — Chromium-family browser
 * @returns {Promise<string|null>} full URL or null
 */
export async function expandUrlViaHistory(partialUrl, appName) {
  if (!partialUrl || process.platform !== 'darwin') return null;
  const rootRel = _CHROMIUM_PROFILE_ROOTS[appName];
  if (!rootRel) return null;
  try {
    const root = path.join(os.homedir(), 'Library', 'Application Support', rootRel);
    const profiles = fs.readdirSync(root).filter(d =>
      fs.existsSync(path.join(root, d, 'History')));
    // Escape LIKE metachars + quotes in the partial before prefix-matching.
    const esc = partialUrl.replace(/'/g, '\'\'').replace(/[%_\\]/g, c => '\\' + c);
    for (const prof of profiles) {
      const db = path.join(root, prof, 'History');
      const sql = `SELECT url FROM urls WHERE url LIKE '${esc}%' ESCAPE '\\' ORDER BY last_visit_time DESC LIMIT 1`;
      try {
        const { stdout } = await execAsync(
          `sqlite3 "file:${db}?immutable=1" ${JSON.stringify(sql)}`, { timeout: 1500 });
        const hit = stdout.trim().split('\n').find(Boolean);
        if (hit && hit.length >= partialUrl.length) {
          logger.info(`[activeWindow] url-probe history-expand ${appName}: ${partialUrl} -> ${hit}`);
          return hit;
        }
      } catch (_) { /* profile DB busy/missing — try next */ }
    }
  } catch (_) { /* profile root unreadable — fail soft */ }
  return null;
}

/**
 * Host-boundary History match — a bare-domain OCR token ("seriouseats.com",
 * "www.seriouseats.com/x") resolves to the MOST RECENT visited URL on that
 * host. The %://HOST/ boundary keeps "4.2"-style junk from ever matching.
 * @param {string} token — URL-ish token (no scheme)
 * @param {string} appName — Chromium-family browser
 * @returns {Promise<string|null>}
 */
async function _hostMatchViaHistory(token, appName) {
  if (!token || process.platform !== 'darwin') return null;
  const rootRel = _CHROMIUM_PROFILE_ROOTS[appName];
  if (!rootRel) return null;
  try {
    const raw = String(token).replace(/^https?:\/\//i, '').replace(/^www\./i, '');
    const host = raw.split(/[/:?#]/)[0];
    if (!host || !host.includes('.') || !/[a-z]/i.test(host)) return null;
    const esc = host.replace(/'/g, '\'\'').replace(/[%_\\]/g, c => '\\' + c);
    const sql = 'SELECT url, last_visit_time FROM urls WHERE (' +
      `url LIKE '%://${esc}/%' ESCAPE '\\' OR url LIKE '%://${esc}:%' ESCAPE '\\' OR url LIKE '%://${esc}' ESCAPE '\\' OR '` +
      `url LIKE '%://%.${esc}/%' ESCAPE '\\' OR url LIKE '%://%.${esc}:%' ESCAPE '\\' OR url LIKE '%://%.${esc}' ESCAPE '\\'` +
      ') ORDER BY last_visit_time DESC LIMIT 1';
    const root = path.join(os.homedir(), 'Library', 'Application Support', rootRel);
    const profiles = fs.readdirSync(root).filter(d =>
      fs.existsSync(path.join(root, d, 'History')));
    let best = null, bestT = -1;
    for (const prof of profiles) {
      const db = path.join(root, prof, 'History');
      try {
        const { stdout } = await execAsync(
          `sqlite3 -separator '|' "file:${db}?immutable=1" ${JSON.stringify(sql)}`, { timeout: 1500 });
        const line = stdout.trim().split('\n').find(Boolean);
        if (!line) continue;
        const [u, t] = line.split('|');
        if (u && Number(t) > bestT) { best = u; bestT = Number(t); }
      } catch (_) { /* profile DB busy/missing — try next */ }
    }
    if (best) logger.info(`[activeWindow] url-probe history-host ${appName}: ${token} -> ${best}`);
    return best;
  } catch (_) { return null; }
}

/**
 * Evidence-based candidate resolution — a token becomes a page URL only if
 * the browser's own History contains it. Prefix-expand first (omnibox
 * truncation), then host-boundary match (bare domains → most recent visit).
 * Scheme-ful tokens skip host-match (already complete — a different page on
 * the same host must not overwrite the real URL).
 * @param {string} candidate — raw OCR/AS token
 * @param {string} appName — Chromium-family browser
 * @returns {Promise<string|null>}
 */
// Substring match — "seriouseats.com/shepherds-pie" (no scheme, has path)
// finds the stored row containing it, ordered by recency.
async function _substringMatchViaHistory(token, appName) {
  if (!token || process.platform !== 'darwin') return null;
  const rootRel = _CHROMIUM_PROFILE_ROOTS[appName];
  if (!rootRel) return null;
  try {
    const esc = String(token).replace(/'/g, '\'\'').replace(/[%_\\]/g, c => '\\' + c);
    const sql = `SELECT url, last_visit_time FROM urls WHERE url LIKE '%${esc}%' ESCAPE '\\' ORDER BY last_visit_time DESC LIMIT 1`;
    const root = path.join(os.homedir(), 'Library', 'Application Support', rootRel);
    const profiles = fs.readdirSync(root).filter(d =>
      fs.existsSync(path.join(root, d, 'History')));
    let best = null, bestT = -1;
    for (const prof of profiles) {
      const db = path.join(root, prof, 'History');
      try {
        const { stdout } = await execAsync(
          `sqlite3 -separator '|' "file:${db}?immutable=1" ${JSON.stringify(sql)}`, { timeout: 1500 });
        const line = stdout.trim().split('\n').find(Boolean);
        if (!line) continue;
        const [u, t] = line.split('|');
        if (u && Number(t) > bestT) { best = u; bestT = Number(t); }
      } catch (_) { /* profile DB busy/missing — try next */ }
    }
    if (best) logger.info(`[activeWindow] url-probe history-substr ${appName}: ${token} -> ${best}`);
    return best;
  } catch (_) { return null; }
}

export async function resolveUrlCandidateViaHistory(candidate, appName) {
  if (!candidate) return null;
  const expanded = await expandUrlViaHistory(candidate, appName);
  if (expanded) return expanded;
  if (/^https?:\/\//i.test(candidate)) return null;
  // Path-bearing bare token ("host/path") — substring keeps the path signal;
  // bare domain falls to host-boundary match (most recent visit on host).
  if (String(candidate).includes('/')) {
    const sub = await _substringMatchViaHistory(candidate, appName);
    if (sub) return sub;
  }
  return _hostMatchViaHistory(candidate, appName);
}

/**
 * Tier 3b — resolve the current page URL by matching on-screen title text
 * (tab strip, page headings, window title) against the browser's History DB
 * urls.title column. Covers the case where no URL text is on screen at all
 * (omnibox showing typed text, toolbar occluded/missing from the capture).
 * Accepts only a UNIQUE url match across all profiles — an ambiguous title
 * could belong to several pages. Fail-open → null.
 * @param {string} appName — Chromium-family browser
 * @param {string[]} titleCandidates — on-screen title-ish strings
 * @returns {Promise<string|null>}
 */
export async function findUrlInHistoryByTitle(appName, titleCandidates) {
  if (!appName || !Array.isArray(titleCandidates) || !titleCandidates.length) return null;
  if (process.platform !== 'darwin') return null;
  const rootRel = _CHROMIUM_PROFILE_ROOTS[appName];
  if (!rootRel) return null;
  try {
    const root = path.join(os.homedir(), 'Library', 'Application Support', rootRel);
    const profiles = fs.readdirSync(root).filter(d =>
      fs.existsSync(path.join(root, d, 'History')));
    for (const cand of titleCandidates) {
      const esc = String(cand).replace(/'/g, '\'\'').replace(/[%_\\]/g, c => '\\' + c);
      const hits = new Set();
      for (const prof of profiles) {
        const db = path.join(root, prof, 'History');
        const sql = `SELECT url FROM urls WHERE title LIKE '%${esc}%' ESCAPE '\\' ORDER BY last_visit_time DESC LIMIT 5`;
        try {
          const { stdout } = await execAsync(
            `sqlite3 "file:${db}?immutable=1" ${JSON.stringify(sql)}`, { timeout: 1500 });
          for (const line of stdout.trim().split('\n').filter(Boolean)) hits.add(line);
        } catch (_) { /* profile DB busy/missing — try next */ }
      }
      if (hits.size === 1) {
        const url = [...hits][0];
        logger.info(`[activeWindow] url-probe history-title ${appName}: "${String(cand).slice(0, 60)}" -> ${url}`);
        return url;
      }
      if (hits.size > 1) {
        logger.info(`[activeWindow] url-probe history-title ${appName}: "${String(cand).slice(0, 60)}" ambiguous (${hits.size} urls) — skipped`);
      }
    }
  } catch (_) { /* profile root unreadable — fail soft */ }
  return null;
}

function _titleWords(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\s*[-–—|]\s*(google chrome|chromium|microsoft edge|brave|vivaldi|opera|safari)$/i, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean);
}

/**
 * Return true when a captured title plausibly describes the live active tab.
 * A shared three-word prefix handles normal title suffixes while the 75%
 * overlap rule handles short browser-generated title variations.
 */
export function _titleMatchesLive(candidate, liveTitle) {
  if (!candidate || !liveTitle || liveTitle === 'unknown') return true;
  const a = _titleWords(candidate);
  const b = _titleWords(liveTitle);
  if (!a.length || !b.length) return false;
  const prefix = Math.min(a.length, b.length, 3);
  if (prefix >= 3 && a.slice(0, prefix).every((word, i) => word === b[i])) return true;
  const bSet = new Set(b);
  const shared = a.filter(word => bSet.has(word)).length;
  return shared / Math.max(a.length, b.length) >= 0.75;
}

/**
 * Title candidates for the History-DB match. Tab-strip rows are intentionally
 * excluded: they enumerate inactive tabs and cannot identify the active page.
 * Keep the live window title, semantic headings, and flat-text fragments.
 * @param {object|null} capture — episodic capture (structuredRows, windowTitle, text)
 * @param {string} [liveTitle] — request-time active window title
 * @returns {string[]}
 */
export function _titleCandidates(capture, liveTitle) {
  if (!capture) return [];
  const out = [];
  const push = (t, requireLiveMatch = true) => {
    if (typeof t !== 'string') return;
    const s = t.trim().replace(/\s*[-–—|]\s*(Google Chrome|Chromium|Microsoft Edge|Brave|Vivaldi|Opera|Safari)$/i, '').trim();
    if (s.length >= 8 && s.length <= 120 && /[a-zA-Z]/.test(s) && !/^\S+\.\S+$/.test(s)
        && (!requireLiveMatch || _titleMatchesLive(s, liveTitle))) out.push(s);
  };
  push(liveTitle, false);
  push(capture.windowTitle);
  for (const r of capture.structuredRows || []) {
    if (r.type === 'heading') push(r.text);
  }
  for (const frag of String(capture.text || '').split(/\s+[-–—|]{1,3}\s+/)) {
    if (out.length >= 12) break;
    push(frag);
  }
  return [...new Set(out)].sort((a, b) => b.length - a.length).slice(0, 12);
}

/**
 * Tier 3 — History-DB corroboration, always-on. T3a prefix-expands a truncated
 * omnibox candidate; T3b matches on-screen title text to urls.title. The
 * History row is canonical when the title match is unique and the on-screen
 * candidate is absent/truncated/garbled; an unrelated on-screen URL still wins
 * (it's literally what the user sees) with the disagreement logged.
 * @param {string|null} candidate — URL from T1/T2
 * @param {object|null} capture — shared stored capture (T3b candidates)
 * @param {string} appName
 * @param {string} [liveTitle] — request-time active window title
 * @returns {Promise<string|null>}
 */
export async function corroborateUrlViaHistory(candidate, capture, appName, liveTitle) {
  try {
    if (candidate) {
      // T3a: omniboxes elide long URL tails — expand via url-prefix match.
      const expanded = await expandUrlViaHistory(candidate, appName);
      if (expanded) return expanded;
    }
    const titleCandidates = _titleCandidates(capture, liveTitle);
    const titleUrl = await findUrlInHistoryByTitle(appName, titleCandidates);
    const normC = _normalizeUrl(candidate);
    // No history evidence → normalized candidate or null. Never return raw
    // junk: "4.2" (a star rating) must not flow downstream as the page URL.
    if (!titleUrl) return normC;
    if (!normC) return titleUrl;
    const normH = _normalizeUrl(titleUrl);
    if (normC === normH) return normC;
    try {
      if (normH && new URL(normC).host === new URL(normH).host) {
        // Same site, different path — either the OCR URL is garbled or the
        // title match is the current page. History's exact row wins.
        logger.info(`[activeWindow] url-probe history-title ${appName}: on-screen ${normC} vs history ${normH} — history wins (same host)`);
        return titleUrl;
      }
    } catch (_) { /* unparseable — fall through to candidate */ }
    logger.info(`[activeWindow] url-probe history-title ${appName}: disagreement — keeping on-screen ${normC} over history ${normH}`);
    return normC;
  } catch (_) {
    return _normalizeUrl(candidate) || null;
  }
}

/**
 * All on-screen windows for an app, sorted topmost-first (y asc, then larger
 * area). Iterating every window's top strip — not just the largest — is what
 * makes fullscreen browsers work: a fullscreen window's CGWindow rect IS its
 * toolbar strip (degenerate ~80px-tall rect at the top of the screen).
 * @param {string} appName
 * @returns {Promise<Array<{id:number|null,x:number,y:number,w:number,h:number}>>}
 */
export async function _getAppWindowRects(appName) {
  if (!appName || process.platform !== 'darwin') return [];
  try {
    const wins = await openWindows();
    return (wins || [])
      .filter(w => w.owner?.name === appName && w.bounds?.width > 0 && w.bounds?.height > 0)
      .map(w => ({ id: w.id ?? null, x: w.bounds.x, y: w.bounds.y, w: w.bounds.width, h: w.bounds.height }))
      .sort((a, b) => (a.y - b.y) || (b.w * b.h - a.w * a.h));
  } catch (_) { /* get-windows failed */ }
  return [];
}

let _litAvailable = null;
function _checkLit() {
  if (_litAvailable !== null) return _litAvailable;
  try {
    execSync('which lit', { timeout: 2000, stdio: 'pipe' });
    _litAvailable = true;
  } catch (_) { _litAvailable = false; }
  return _litAvailable;
}

/**
 * OCR a toolbar-strip PNG into a URL candidate. LiteParser (`lit parse`)
 * preferred — same positioned-items engine the monitor uses; falls back to
 * Tesseract word boxes via ocrService. `topOnly` restricts candidates to the
 * top of the image (full-screen captures — keeps page-body URLs from winning).
 * @param {string} imgPath
 * @param {boolean} [topOnly]
 * @returns {Promise<Array<{token:string, score:number, y:number}>>} ranked candidates
 */
async function _extractUrlCandidatesFromStripImage(imgPath, topOnly = false) {
  if (_checkLit()) {
    const out = imgPath.replace(/\.png$/, '.lit.json');
    try {
      await execAsync(`lit parse ${JSON.stringify(imgPath)} --format json -o ${JSON.stringify(out)}`, { timeout: 8000 });
      const parsed = JSON.parse(fs.readFileSync(out, 'utf8'));
      const rows = [];
      for (const page of parsed.pages || []) {
        for (const item of page.textItems || []) {
          rows.push({ text: item.text || '', x: item.x || 0, y: item.y || 0, width: item.width || 0, height: item.height || 0 });
        }
      }
      // Strip image coords are image-relative; topOnly caps at ~200px of image.
      return _extractUrlCandidatesFromRows(rows, { maxY: topOnly ? 400 : Infinity });
    } catch (_) { /* lit failed — fall through to tesseract */ }
    finally { try { fs.unlinkSync(out); } catch (_) { /* cleanup best-effort */ } }
  }
  const data = await getOCRService().extractWithBoxes(fs.readFileSync(imgPath));
  const words = data?.words || [];
  return _extractUrlCandidates(topOnly ? words.filter(w => (w?.bbox?.y0 ?? 0) < 400) : words);
}

/**
 * Resolve strip candidates against History (evidence wins over shape). If no
 * candidate has history evidence — incognito pages, brand-new sites — the
 * strict-shape top candidate survives as the last resort. Capped at 4
 * candidates per strip to bound sqlite queries.
 * @param {Array<{token:string}>} cands — ranked candidates from a strip image
 * @param {string} appName
 * @returns {Promise<string|null>}
 */
async function _resolveStripCandidates(cands, appName) {
  for (const c of (cands || []).slice(0, 4)) {
    const hit = await resolveUrlCandidateViaHistory(c.token, appName);
    if (hit) return hit;
  }
  return _normalizeUrl(cands?.[0]?.token);
}

/**
 * Tier 2b — read the address bar out of a fresh screenshot. Iterates every
 * on-screen window of the app (topmost first) and crops each window's own
 * ~130px top strip; the first strip yielding a domain token wins. No usable
 * windows → full-screen capture (regex still isolates the omnibox token).
 * @param {string} appName — known browser app
 * @param {object} [fallbackBounds] — get-windows bounds, tried after openWindows rects
 * @returns {Promise<string|null>}
 */
export async function probeBrowserUrlViaOcr(appName, fallbackBounds) {
  if (!appName || !BROWSER_URL_SCRIPTS[appName] || process.platform !== 'darwin') return null;
  const cached = _urlProbeCache.get(`${appName}|ocr`);
  if (cached && Date.now() - cached.ts < (cached.url ? URL_PROBE_OK_TTL : URL_PROBE_FAIL_TTL)) {
    return cached.url;
  }
  let url = null;
  const tmpFiles = [];
  try {
    const rects = await _getAppWindowRects(appName);
    if (fallbackBounds && fallbackBounds.width > 0 &&
        !rects.some(r => Math.abs(r.x - fallbackBounds.x) < 2 && Math.abs(r.y - fallbackBounds.y) < 2)) {
      rects.push({ id: null, x: fallbackBounds.x, y: fallbackBounds.y, w: fallbackBounds.width, h: fallbackBounds.height || 130 });
    }
    // Up to 3 window strips; each strip is already the toolbar region so no
    // inner y-filter is needed. Candidates resolve against History — a token
    // only becomes a URL if the browser's own record contains it.
    for (const r of rects.slice(0, 3)) {
      const tmp = path.join(os.tmpdir(), `td-urlstrip-${Date.now()}-${tmpFiles.length}.png`);
      tmpFiles.push(tmp);
      try {
        const stripH = Math.min(r.h || 130, 130);
        await execAsync(`screencapture -R ${Math.round(r.x)},${Math.round(r.y)},${Math.round(r.w)},${Math.round(stripH)} -x ${JSON.stringify(tmp)}`, { timeout: 2000 });
        url = await _resolveStripCandidates(await _extractUrlCandidatesFromStripImage(tmp), appName);
        if (url) { logger.info(`[activeWindow] url-probe ocr-strip ${appName}: hit ${url} (win ${r.id ?? '?'} strip y=${Math.round(r.y)})`); break; }
      } catch (_) { /* this strip failed — try next window */ }
    }
    if (!url && rects.length === 0) {
      const tmp = path.join(os.tmpdir(), `td-urlstrip-${Date.now()}-full.png`);
      tmpFiles.push(tmp);
      try {
        await execAsync(`screencapture -x ${JSON.stringify(tmp)}`, { timeout: 2000 });
        url = await _resolveStripCandidates(await _extractUrlCandidatesFromStripImage(tmp, true), appName);
      } catch (_) { /* full-screen capture failed */ }
    }
  } catch (e) {
    logger.info(`[activeWindow] url-probe ocr-strip ${appName}: error ${e.message}`);
  } finally {
    for (const tmp of tmpFiles) { try { fs.unlinkSync(tmp); } catch (_) { /* temp cleanup best-effort */ } }
  }
  if (!url) logger.info(`[activeWindow] url-probe ocr-strip ${appName}: miss`);
  _urlProbeCache.set(`${appName}|ocr`, { url, ts: Date.now() });
  return url;
}

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

    // Try browser-specific tab title + URL
    const browserScript = BROWSER_TAB_SCRIPTS[cleanAppName];
    let title = windowTitle?.trim() || '';
    let url = null;
    if (browserScript) {
      url = await probeBrowserUrl(cleanAppName);
    }
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

    return { appName: cleanAppName, windowTitle: title || 'unknown', url, bounds: null, filePath, windowId: null };
  } catch (e) {
    return { appName: 'unknown', windowTitle: 'unknown', url: null, bounds: null, filePath: null, windowId: null };
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
    let url = win.url || null;
    const bounds = win.bounds || null;
    const windowId = win.id || null;

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

    // ── URL probe (Tier 1: AppleScript) ──────────────────────────────────────
    // get-windows only fills win.url with an Accessibility grant — absent here,
    // so probe the browser's front-tab URL directly. Tier 2 (OCR address-bar
    // strip) stays request-time only in memory.getActiveAppContext — too heavy
    // for the poll loop. Throttled via _urlProbeCache.
    if (!url && BROWSER_URL_SCRIPTS[appName]) {
      url = await probeBrowserUrl(appName);
    }

    const result = { appName, windowTitle: windowTitle || 'unknown', url, bounds, filePath, windowId };
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
