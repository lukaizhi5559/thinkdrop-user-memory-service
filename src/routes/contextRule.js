import express from 'express';
import { getContextRuleService } from '../services/contextRule.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const contextRuleService = getContextRuleService();

/**
 * POST /context_rule.search
 * Fetch all rules matching context keys (hostnames or app names).
 * Body: { payload: { contextKeys: string[], contextType?: 'site'|'app' }, context, requestId }
 */
router.post('/context_rule.search', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.contextKeys || !Array.isArray(payload.contextKeys)) {
      return res.status(400).json({ error: 'Missing required field: contextKeys (array)' });
    }
    const result = await contextRuleService.search(payload.contextKeys, {
      contextType: payload.contextType
    });
    res.json(formatMCPResponse('context_rule.search', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /context_rule.upsert
 * Insert or update a rule for a context key (hostname or app name).
 * Body: { payload: { contextKey, ruleText, contextType?, category?, source? }, context, requestId }
 */
router.post('/context_rule.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.contextKey || !payload?.ruleText) {
      return res.status(400).json({ error: 'Missing required fields: contextKey, ruleText' });
    }
    const result = await contextRuleService.upsert(
      payload.contextKey,
      payload.ruleText,
      { contextType: payload.contextType, category: payload.category, source: payload.source }
    );
    res.json(formatMCPResponse('context_rule.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /context_rule.list
 * List all rules, optionally filtered by contextKey or contextType.
 * Body: { payload: { contextKey?, contextType?, limit? }, context, requestId }
 */
router.post('/context_rule.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const result = await contextRuleService.list({
      contextKey: payload?.contextKey,
      contextType: payload?.contextType,
      limit: payload?.limit || 100
    });
    res.json(formatMCPResponse('context_rule.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /context_rule.delete
 * Delete a rule by id.
 * Body: { payload: { id }, context, requestId }
 */
router.post('/context_rule.delete', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const result = await contextRuleService.delete(payload.id);
    res.json(formatMCPResponse('context_rule.delete', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
