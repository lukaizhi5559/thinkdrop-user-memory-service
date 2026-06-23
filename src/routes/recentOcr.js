import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';
import { getMonitorService } from '../monitor/monitorService.js';

const router = express.Router();
const memoryService = getMemoryService();

/**
 * POST /memory.getRecentOcr
 * 
 * Returns the most recent screen capture OCR text if it's fresh enough.
 * Designed for use by external services (e.g., screen_intelligence MCP/Skill)
 * to avoid redundant screenshot + OCR when the user-memory monitor already
 * captured one recently.
 * 
 * Payload:
 *   maxAgeSeconds: number (optional, default: 10) — max staleness in seconds
 * 
 * Response:
 *   - If fresh capture exists: { available: true, capture: { text, appName, windowTitle, ... } }
 *   - If no fresh capture:     { available: false, capture: null }
 */
router.post('/memory.getRecentOcr', async (req, res, next) => {
  try {
    const { payload = {}, context, requestId } = req.body;

    // Prefer the app the user was in before switching to the ThinkDrop overlay.
    // monitorService tracks this as lastNonOverlayApp — it's the app they were looking
    // at when they decided to ask ThinkDrop something, even if another app was
    // captured more recently in the DB.
    const monSvc = getMonitorService();
    const result = await memoryService.getRecentOcr({
      maxAgeSeconds: payload.maxAgeSeconds || 3,
      appName: payload.appName || null,
      preferAppName: monSvc.lastNonOverlayApp || null
    }, context);

    res.json(formatMCPResponse(
      'memory.getRecentOcr',
      requestId,
      'ok',
      {
        available: result !== null,
        capture: result
      }
    ));
  } catch (error) {
    next(error);
  }
});

export default router;
