import express from 'express';
import { formatMCPResponse } from '../utils/helpers.js';
import { getMonitorService } from '../monitor/monitorService.js';

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
 *   { history: [{ appName, category, windowTitle, bounds, timestamp, url }] }
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
 * Used by app.runner.cjs to restore the active app context at the start of an App-Flow.
 *
 * Response:
 *   { app: { appName, category, windowTitle, bounds, timestamp, url } | null }
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

export default router;
