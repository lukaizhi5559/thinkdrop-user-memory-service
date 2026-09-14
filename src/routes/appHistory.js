import express from 'express';
import { formatMCPResponse } from '../utils/helpers.js';
import { getMonitorService } from '../monitor/monitorService.js';
import { getActiveWindow, _inferPathFromTitle } from '../monitor/activeWindow.js';

const router = express.Router();

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
        timestamp: new Date().toISOString(),
        source: 'live',
      };
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
