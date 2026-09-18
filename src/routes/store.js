import express from 'express';
import http from 'http';
import { getMemoryService } from '../services/memory.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const memoryService = getMemoryService();
const PERSONALITY_PORT = parseInt(process.env.PERSONALITY_SERVICE_PORT || '3012', 10);

/** Feed stored memories into the thought engine as 'memory' input candidates. */
function notifyThoughtEngine(text, memoryId) {
  try {
    const body = JSON.stringify({
      version: 'mcp.v1', service: 'personality-service', action: 'thought.input',
      payload: { type: 'memory', text, id: memoryId }, requestId: 'mem_th_' + Date.now(),
    });
    const req = http.request({
      hostname: '127.0.0.1', port: PERSONALITY_PORT, path: '/thought.input',
      method: 'POST', timeout: 3000,
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, (res) => res.resume());
    req.on('error', () => {});
    req.on('timeout', () => req.destroy());
    req.write(body);
    req.end();
  } catch (_) { /* fire-and-forget */ }
}

router.post('/memory.store', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;

    const result = await memoryService.storeMemory(payload, context);

    // Loop guard: the engine's own `remember` action stores tagged memories —
    // letting them re-enter as candidates would create a self-feeding cycle.
    if (payload?.metadata?.source !== 'thought-engine') {
      notifyThoughtEngine(payload?.text, result?.memoryId);
    }

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
