import express from 'express';
import { getUserConstraintsService } from '../services/userConstraints.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const constraintsService = getUserConstraintsService();

/**
 * POST /constraint.add
 * Add a new hard/soft constraint.
 * Body: { payload: { rule, scope?, blocks?, severity? }, context, requestId }
 */
router.post('/constraint.add', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.rule) {
      return res.status(400).json({ error: 'Missing required field: rule' });
    }
    const result = await constraintsService.add(payload.rule, {
      scope:    payload.scope    ?? 'global',
      blocks:   payload.blocks   ?? null,
      severity: payload.severity ?? 'hard',
      pin:      payload.pin      ?? null,
    });
    res.json(formatMCPResponse('constraint.add', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /constraint.check
 * Check whether any constraint blocks the given action patterns or message.
 * Body: { payload: { actionPatterns?: string[], message?: string, scope? }, context, requestId }
 */
router.post('/constraint.check', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!Array.isArray(payload?.actionPatterns) && !payload?.message) {
      return res.status(400).json({ error: 'Missing required field: actionPatterns (array) or message (string)' });
    }
    const result = await constraintsService.check(payload.actionPatterns ?? [], {
      scope:      payload.scope      ?? null,
      message:    payload.message    ?? null,
      pinAttempt: payload.pinAttempt ?? null,
    });
    res.json(formatMCPResponse('constraint.check', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /constraint.list
 * List all constraints, optionally filtered by scope.
 * Body: { payload: { scope? }, context, requestId }
 */
router.post('/constraint.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const constraints = await constraintsService.list({ scope: payload?.scope });
    res.json(formatMCPResponse('constraint.list', requestId, 'ok', { constraints }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /constraint.remove
 * Delete a constraint by id.
 * Body: { payload: { id }, context, requestId }
 */
router.post('/constraint.remove', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const result = await constraintsService.remove(payload.id);
    res.json(formatMCPResponse('constraint.remove', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
