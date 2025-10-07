import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const memoryService = getMemoryService();

router.post('/memory.search', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;

    if (!payload.query) {
      throw new Error('Missing required field: query');
    }

    const result = await memoryService.searchMemories(payload.query, payload, context);

    res.json(formatMCPResponse(
      'memory.search',
      requestId,
      'ok',
      result
    ));
  } catch (error) {
    next(error);
  }
});

export default router;
