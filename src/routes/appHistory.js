import express from 'express';
import { formatMCPResponse } from '../utils/helpers.js';
import { getMonitorService } from '../monitor/monitorService.js';
import { getMemoryService } from '../services/memory.js';
import logger from '../utils/logger.js';
import { getActiveWindow, _inferPathFromTitle, probeBrowserUrl, probeBrowserUrlViaOcr, _extractUrlCandidatesFromRows, _normalizeUrl, _titleMatchesLive, resolveUrlCandidateViaHistory, corroborateUrlViaHistory, isBrowserApp } from '../monitor/activeWindow.js';

const router = express.Router();

/**
 * Tier 2a — lift the URL out of a stored screen capture's LiteParser
 * structuredRows. Free (no new screenshot/OCR): the monitor already captured
 * the visible screen — the omnibox text sits in the top-region rows.
 * @param {object|null} capture — episodic capture row (structuredRows, url, ageMs)
 * @param {object} [bounds] — active-window bounds; narrows rows to its toolbar
 * @returns {string|null}
 */
/**
 * Ranked URL candidates from a stored capture: Tier-0 fill first (capture.url),
 * then toolbar-strip tokens, then — when window bounds exist — a wider
 * top-region pass for degenerate/moved-window bounds.
 */
function _urlCandidatesFromCapture(capture, bounds) {
  if (!capture) return [];
  const rows = capture.structuredRows || [];
  const cands = [];
  const seen = new Set();
  const push = (c) => { if (!seen.has(c.token)) { seen.add(c.token); cands.push(c); } };
  if (capture.url) push({ token: capture.url, score: Infinity, y: bounds?.y ?? 0 }); // Tier-0 fill first
  for (const c of _extractUrlCandidatesFromRows(rows, { bounds: bounds || null })) push(c);
  if (bounds) {
    // Bounds-filtered pass may miss (degenerate/moved-window bounds) — merge
    // the wider top-region candidates after the toolbar-zone ones.
    for (const c of _extractUrlCandidatesFromRows(rows, { maxY: 400 })) push(c);
  }
  return cands;
}

/**
 * Strict last-resort: only a well-formed URL-shaped token positioned in the
 * toolbar zone may become a URL without history evidence (incognito pages).
 * Page-body tokens (y past the toolbar) can never reach here.
 */
function _strictFallbackCandidate(cands, bounds) {
  const yMax = bounds && Number.isFinite(bounds.y) ? bounds.y + 95 : 200;
  for (const c of cands) {
    const n = _normalizeUrl(c.token);
    if (n && (c.y ?? 0) <= yMax) return n;
  }
  return null;
}

/**
 * Resolve capture candidates against History — a token becomes the page URL
 * only when the browser's own record contains it (prefix → substring → host).
 * No evidence → strict shape+position fallback → null.
 */
async function _resolveUrlCandidates(cands, appName, bounds, tag) {
  for (const c of (cands || []).slice(0, 4)) {
    const hit = await resolveUrlCandidateViaHistory(c.token, appName);
    if (hit) {
      logger.info(`[appHistory] url-probe ${tag} ${appName}: hit ${hit} (token '${c.token}')`);
      return hit;
    }
  }
  const fb = _strictFallbackCandidate(cands, bounds);
  logger.info(`[appHistory] url-probe ${tag} ${appName}: ${fb ? `fallback ${fb}` : `miss (${(cands || []).length} candidates)`}`);
  return fb;
}

/**
 * Request-time URL fill for browsers. Ladder: T1 AppleScript (exact; fails on
 * fullscreen windows) → T2a stored LiteParser rows → T2b fresh strip OCR →
 * T3 History corroboration (prefix-expand + title match). Runs here (not just
 * the poll loop) so the history-fallback path — overlay frontmost → previous-app
 * entry — still resolves the URL of the browser window the user is reading.
 * Fail-open: leaves url null.
 */
async function _ensureBrowserUrl(app) {
  if (!app || app.url || !app.appName || !isBrowserApp(app.appName)) return;
  try {
    // One capture fetch shared by T2a (omnibox rows) and T3b (title candidates).
    // 300s window: captures are diff-gated and stored sparsely — the latest
    // capture for an app IS the page the user is on, even if minutes old.
    const capture = await getMemoryService().getRecentOcr({
      appName: app.appName,
      maxAgeSeconds: 300,
      includeTainted: true,
    }).catch(() => null);

    const liveTitle = app.windowTitle && app.windowTitle !== 'unknown' ? app.windowTitle : null;
    const captureIsStale = Boolean(liveTitle && capture?.windowTitle && capture.windowTitle !== 'unknown'
      && !_titleMatchesLive(capture.windowTitle, liveTitle));
    const usableCapture = captureIsStale ? null : capture;
    if (captureIsStale) {
      logger.info(`[appHistory] url-probe ignored stale capture for ${app.appName}: capture='${capture.windowTitle}' live='${liveTitle}'`);
    }
    const captureTag = usableCapture ? 'stored-rows' : 'stored-rows-none';
    if (!usableCapture) logger.info(`[appHistory] url-probe stored-rows ${app.appName}: no usable capture <300s`);
    let url = await probeBrowserUrl(app.appName);                                  // T1
    if (!url) url = await _resolveUrlCandidates(
      _urlCandidatesFromCapture(usableCapture, app.bounds), app.appName, app.bounds, captureTag); // T2a
    if (!url) url = await probeBrowserUrlViaOcr(app.appName, app.bounds || null);  // T2b (history-resolved inside)
    url = await corroborateUrlViaHistory(url, usableCapture, app.appName, liveTitle); // T3 merge
    app.url = url;
  } catch (_) { /* probes are fail-open */ }
}

/**
 * POST /memory.getAppHistory
 *
 * Returns today's active-app history in sequential order.
 * Used by app.runner.cjs to replace URL-first navigation in the browser flow.
 *
 * Payload:
 *   maxAgeHours: number (optional, default: 24) — max age of history entries
 *
 * Response:
 *   { history: [{ appName, category, windowTitle, bounds, timestamp, url, filePath }] }
 */
router.post('/memory.getAppHistory', async (req, res, next) => {
  try {
    const { payload = {}, requestId } = req.body;
    const monSvc = getMonitorService();
    const history = monSvc.getAppHistory({
      maxAgeHours: payload.maxAgeHours || 24,
    });
    res.json(formatMCPResponse(
      'memory.getAppHistory',
      requestId,
      'ok',
      { history }
    ));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /memory.getPreviousActiveApp
 *
 * Returns the last non-overlay app the user was in before switching to ThinkDrop.
 * Internal/history fallback primitive — prefer memory.getActiveAppContext.
 *
 * Response:
 *   { app: { appName, category, windowTitle, bounds, timestamp, url, filePath } | null }
 */
router.post('/memory.getPreviousActiveApp', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const monSvc = getMonitorService();
    const app = monSvc.getPreviousActiveApp();
    res.json(formatMCPResponse(
      'memory.getPreviousActiveApp',
      requestId,
      'ok',
      { app }
    ));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /memory.getActiveAppContext
 *
 * Canonical active-app context resolver. Live-first:
 *   1. Query the current active window (getActiveWindow).
 *   2. If it is a real user app, return it (source: 'live').
 *   3. If it is ThinkDrop/Electron/voice-companion, fall back to getPreviousActiveApp
 *      (source: 'history').
 *   4. If history is empty (cold start), fall back to DB-seeded lastNonOverlayApp
 *      (source: 'db-seed') — included in getPreviousActiveApp's lastNonOverlayApp path.
 *   5. If all return null, return null — caller should ask the user.
 *
 * Returns app metadata + open file path where available. This is the single
 * call the planner makes for "the file that's open" / "close this app" /
 * "what app am I using" / etc.
 *
 * Response:
 *   { app: { appName, category, windowTitle, filePath, url, bounds, timestamp, source } | null }
 */
router.post('/memory.getActiveAppContext', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const monSvc = getMonitorService();

    // Step 1: live active window
    const live = await getActiveWindow();
    if (live && !monSvc._isSkipApp(live.appName, live.url)) {
      let filePath = live.filePath || null;
      // Deep-resolve: if the monitor's background probe didn't find a path
      // (e.g. file is in a hidden dir Spotlight doesn't index like ~/.devin),
      // try the bounded find fallback at request time.
      if (!filePath && live.windowTitle && live.windowTitle !== 'unknown') {
        try {
          filePath = await _inferPathFromTitle(live.windowTitle, live.appName, { deep: true });
        } catch (_) { /* deep find failed — keep null */ }
      }
      const app = {
        appName: live.appName,
        category: monSvc._getCategory(live.appName),
        windowTitle: live.windowTitle || '',
        filePath,
        url: live.url || null,
        bounds: live.bounds || null,
        windowId: live.windowId || null,
        timestamp: new Date().toISOString(),
        source: 'live',
      };
      await _ensureBrowserUrl(app);
      return res.json(formatMCPResponse('memory.getActiveAppContext', requestId, 'ok', { app }));
    }

    // Step 2/3: history fallback (includes DB-seed path inside getPreviousActiveApp)
    const prev = monSvc.getPreviousActiveApp();
    if (prev) {
      let filePath = prev.filePath || null;
      // Deep-resolve from history entry's window title too
      if (!filePath && prev.windowTitle && prev.windowTitle !== 'unknown') {
        try {
          filePath = await _inferPathFromTitle(prev.windowTitle, prev.appName, { deep: true });
        } catch (_) { /* deep find failed — keep null */ }
      }
      const app = { ...prev, filePath, source: 'history' };
      await _ensureBrowserUrl(app);
      return res.json(formatMCPResponse('memory.getActiveAppContext', requestId, 'ok', { app }));
    }

    // Step 4: all fallbacks exhausted
    return res.json(formatMCPResponse('memory.getActiveAppContext', requestId, 'ok', { app: null }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /memory.getAppUsageSummary
 *
 * Computes per-app durations from timestamped app-history entries.
 * Duration of entry[i] = entry[i+1].timestamp - entry[i].timestamp.
 * The last entry's duration = now - its timestamp (open-ended current interval).
 * Overlay/voice-companion entries are excluded from totals.
 *
 * Payload:
 *   maxAgeHours: number (optional, default: 24) — max age of history entries
 *
 * Response:
 *   { summary: [{ appName, category, totalMs, sessions: [{ start, end, durationMs }] }] }
 */
router.post('/memory.getAppUsageSummary', async (req, res, next) => {
  try {
    const { payload = {}, requestId } = req.body;
    const monSvc = getMonitorService();
    const history = monSvc.getAppHistory({
      maxAgeHours: payload.maxAgeHours || 24,
    });
    const now = Date.now();
    const perApp = new Map();
    for (let i = 0; i < history.length; i++) {
      const e = history[i];
      if (monSvc._isSkipApp(e.appName, e.url)) continue;
      const start = new Date(e.timestamp).getTime();
      const end = i < history.length - 1
        ? new Date(history[i + 1].timestamp).getTime()
        : now;
      const durationMs = Math.max(0, end - start);
      if (!perApp.has(e.appName)) {
        perApp.set(e.appName, {
          appName: e.appName,
          category: e.category || monSvc._getCategory(e.appName),
          totalMs: 0,
          sessions: [],
        });
      }
      const agg = perApp.get(e.appName);
      agg.totalMs += durationMs;
      agg.sessions.push({ start: e.timestamp, end: new Date(end).toISOString(), durationMs });
    }
    const summary = Array.from(perApp.values()).sort((a, b) => b.totalMs - a.totalMs);
    res.json(formatMCPResponse('memory.getAppUsageSummary', requestId, 'ok', { summary }));
  } catch (error) {
    next(error);
  }
});

export default router;
