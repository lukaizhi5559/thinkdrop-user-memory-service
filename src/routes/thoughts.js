import express from 'express';
import { getThoughtService } from '../services/thoughts.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const thoughtService = getThoughtService();

/**
 * POST /thought.upsert
 * Insert or semantically reinforce a thought candidate.
 * Body: { payload: { input, summary, entityNames?, actionNames?, sourceIds?,
 *                   userId?, traceWeight?, silenceEpisode?, forceNew? },
 *         context, requestId }
 */
router.post('/thought.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.input || !payload?.summary) {
      return res.status(400).json({ error: 'Missing required fields: input, summary' });
    }
    const result = await thoughtService.upsert(payload);
    res.json(formatMCPResponse('thought.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /thought.list
 * List thoughts (default: open thoughts, live-recomputed score DESC).
 * Body: { payload: { userId?, statuses?, all?, limit?, includeExpired? }, context, requestId }
 */
router.post('/thought.list', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;
    const userId = payload?.userId || context?.userId || 'local_user';
    const result = await thoughtService.list({ ...payload, userId });
    res.json(formatMCPResponse('thought.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /thought.get
 * Fetch a single thought by id.
 * Body: { payload: { id }, context, requestId }
 */
router.post('/thought.get', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const thought = await thoughtService.get(payload.id);
    res.json(formatMCPResponse('thought.get', requestId, 'ok', { thought }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /thought.update
 * Partial update (status, summary, action, outcomeText, silenceEpisode, score)
 * and/or append a reinforcement trace (T2 judgment boosts, suppression).
 * Body: { payload: { id, updates?: {...}, trace?: { w, input?, srcIds? } },
 *         context, requestId }
 */
router.post('/thought.update', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const out = { id: payload.id };
    if (payload.trace) {
      out.trace = await thoughtService.addTrace(payload.id, payload.trace);
    }
    if (payload.updates && Object.keys(payload.updates).length) {
      out.update = await thoughtService.update(payload.id, payload.updates);
    }
    res.json(formatMCPResponse('thought.update', requestId, 'ok', out));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /thought.purge
 * Expire stale low-score thoughts and delete old expired rows.
 * Body: { payload: { userId?, ttlHours?, floorScore? }, context, requestId }
 */
router.post('/thought.purge', async (req, res, next) => {
  try {
    const { payload, context, requestId } = req.body;
    const userId = payload?.userId || context?.userId || 'local_user';
    const result = await thoughtService.purge({ ...payload, userId });
    res.json(formatMCPResponse('thought.purge', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
