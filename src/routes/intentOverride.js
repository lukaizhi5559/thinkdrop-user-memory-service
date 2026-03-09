import express from 'express';
import { getIntentOverrideService } from '../services/intentOverride.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const intentOverrideService = getIntentOverrideService();

/**
 * POST /intent_override.search
 * Find the closest stored intent correction for a prompt.
 * Body: { payload: { prompt: string, threshold?: number }, context, requestId }
 * Returns: { correctIntent, wrongIntent, examplePrompt, similarity } or null
 */
router.post('/intent_override.search', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.prompt) {
      return res.status(400).json({ error: 'Missing required field: prompt' });
    }
    const result = await intentOverrideService.search(payload.prompt, {
      threshold: payload.threshold
    });
    res.json(formatMCPResponse('intent_override.search', requestId, 'ok', { match: result }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /intent_override.upsert
 * Store a user intent correction.
 * Body: { payload: { examplePrompt, correctIntent, wrongIntent?, source? }, context, requestId }
 */
router.post('/intent_override.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.examplePrompt || !payload?.correctIntent) {
      return res.status(400).json({ error: 'Missing required fields: examplePrompt, correctIntent' });
    }
    const result = await intentOverrideService.upsert(
      payload.examplePrompt,
      payload.correctIntent,
      { wrongIntent: payload.wrongIntent, source: payload.source }
    );
    res.json(formatMCPResponse('intent_override.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /intent_override.list
 * List all stored overrides.
 * Body: { payload: { correctIntent?, limit? }, context, requestId }
 */
router.post('/intent_override.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const result = await intentOverrideService.list({
      correctIntent: payload?.correctIntent,
      limit: payload?.limit || 100
    });
    res.json(formatMCPResponse('intent_override.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /intent_override.delete
 * Delete an override by id.
 * Body: { payload: { id }, context, requestId }
 */
router.post('/intent_override.delete', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const result = await intentOverrideService.delete(payload.id);
    res.json(formatMCPResponse('intent_override.delete', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
