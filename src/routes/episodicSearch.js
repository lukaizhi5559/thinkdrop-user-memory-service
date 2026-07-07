import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const memoryService = getMemoryService();

router.post('/episodic.search', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;

    if (!payload.query) {
      throw new Error('Missing required field: query');
    }

    const result = await memoryService.searchEpisodicMemories(payload.query, payload, context);

    res.json(formatMCPResponse(
      'episodic.search',
      requestId,
      'ok',
      result
    ));
  } catch (error) {
    next(error);
  }
});

router.post('/episodic.apps', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;
    const userId = context?.userId || payload?.userId || 'default_user';
    const startDate = payload?.startDate || null;
    const endDate = payload?.endDate || null;

    const apps = await memoryService.listDistinctApps(startDate, endDate, userId, payload);

    res.json(formatMCPResponse(
      'episodic.apps',
      requestId,
      'ok',
      { apps }
    ));
  } catch (error) {
    next(error);
  }
});

router.post('/episodic.keywords', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;
    const userId = context?.userId || payload?.userId || 'default_user';
    const startDate = payload?.startDate || null;
    const endDate = payload?.endDate || null;
    const limit = payload?.limit || 20;

    const keywords = await memoryService.listTopKeywords(startDate, endDate, userId, { limit });

    res.json(formatMCPResponse(
      'episodic.keywords',
      requestId,
      'ok',
      { keywords }
    ));
  } catch (error) {
    next(error);
  }
});

export default router;
