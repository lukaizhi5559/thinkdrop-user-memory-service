import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

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

    const result = await memoryService.getRecentOcr({
      maxAgeSeconds: payload.maxAgeSeconds || 10
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
