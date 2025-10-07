import express from 'express';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const memoryService = getMemoryService();

router.post('/memory.retrieve', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;

    if (payload.memoryId) {
      // Retrieve specific memory by ID
      const memory = await memoryService.retrieveMemory(payload.memoryId, context);
      res.json(formatMCPResponse(
        'memory.retrieve',
        requestId,
        'ok',
        { memory }
      ));
    } else {
      // List memories with filters
      const result = await memoryService.listMemories(payload, context);
      res.json(formatMCPResponse(
        'memory.retrieve',
        requestId,
        'ok',
        result
      ));
    }
  } catch (error) {
    next(error);
  }
});

export default router;
