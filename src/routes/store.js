import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const memoryService = getMemoryService();

router.post('/memory.store', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;

    const result = await memoryService.storeMemory(payload, context);

    res.json(formatMCPResponse(
      'memory.store',
      requestId,
      'ok',
      result
    ));
  } catch (error) {
    next(error);
  }
});

export default router;
