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

/**
 * POST /context_rule.delete_by_key
 * Delete ALL rules for a given context key (stale-rule eviction).
 * Body: { payload: { contextKey }, context, requestId }
 */
router.post('/context_rule.delete_by_key', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.contextKey) {
      return res.status(400).json({ error: 'Missing required field: contextKey' });
    }
    const result = await contextRuleService.deleteByKey(payload.contextKey);
    res.json(formatMCPResponse('context_rule.delete_by_key', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /context_rule.list_all
 * List all rules grouped by context_key.
 * Body: { context, requestId }
 */
router.post('/context_rule.list_all', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const grouped = await contextRuleService.listAllGrouped();
    res.json(formatMCPResponse('context_rule.list_all', requestId, 'ok', { grouped }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /context_rule.update
 * Update a rule by ID with partial updates.
 * Body: { payload: { id, updates: { rule_text?, category?, priority?, user_note?, status? } }, context, requestId }
 */
router.post('/context_rule.update', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    if (!payload?.updates || typeof payload.updates !== 'object') {
      return res.status(400).json({ error: 'Missing required field: updates (object)' });
    }
    const result = await contextRuleService.update(payload.id, payload.updates);
    res.json(formatMCPResponse('context_rule.update', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /context_rule.delete_by_id
 * Delete a rule by ID (explicit endpoint name).
 * Body: { payload: { id }, context, requestId }
 */
router.post('/context_rule.delete_by_id', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const result = await contextRuleService.deleteById(payload.id);
    res.json(formatMCPResponse('context_rule.delete_by_id', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /context_rule.analyze_cleanup
 * Get rules for a specific context key for LLM cleanup analysis.
 * Body: { payload: { contextKey }, context, requestId }
 */
router.post('/context_rule.analyze_cleanup', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.contextKey) {
      return res.status(400).json({ error: 'Missing required field: contextKey' });
    }
    const result = await contextRuleService.cleanupDomain(payload.contextKey);
    res.json(formatMCPResponse('context_rule.analyze_cleanup', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
