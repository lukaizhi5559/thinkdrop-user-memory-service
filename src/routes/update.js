import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const memoryService = getMemoryService();

router.post('/memory.update', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;

    if (!payload.memoryId) {
      throw new Error('Missing required field: memoryId');
    }

    if (!payload.updates) {
      throw new Error('Missing required field: updates');
    }

    const result = await memoryService.updateMemory(
      payload.memoryId,
      payload.updates,
      context
    );

    res.json(formatMCPResponse(
      'memory.update',
      requestId,
      'ok',
      result
    ));
  } catch (error) {
    next(error);
  }
});

export default router;
