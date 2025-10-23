import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const memoryService = getMemoryService();

router.post('/memory.classify-conversational-query', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;

    console.log('Context:', context);

    if (!payload.query) {
      throw new Error('Missing required field: query');
    }

    const result = memoryService.classifyConversationalQuery(payload.query, { ...payload, context });

    res.json(formatMCPResponse(
      'memory.classify-conversational-query',
      requestId,
      'ok',
      result
    ));
  } catch (error) {
    next(error);
  }
});

export default router;
