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

export default router;
