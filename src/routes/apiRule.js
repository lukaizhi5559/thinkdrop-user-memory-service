import express from 'express';
import { getApiRuleService } from '../services/apiRule.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const apiRuleService = getApiRuleService();

/**
 * POST /api_rule.search
 * Fetch all rules for given service names.
 * Body: { payload: { services: string[], ruleType?: string }, requestId }
 */
router.post('/api_rule.search', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.services || !Array.isArray(payload.services)) {
      return res.status(400).json({ error: 'Missing required field: services (array)' });
    }
    const result = await apiRuleService.search(payload.services, {
      ruleType: payload.ruleType,
    });
    res.json(formatMCPResponse('api_rule.search', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api_rule.upsert
 * Insert or update a rule for a service.
 * Body: { payload: { service, ruleType, ruleText, codePattern?, fixHint?, source? }, requestId }
 */
router.post('/api_rule.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.service || !payload?.ruleType || !payload?.ruleText) {
      return res.status(400).json({ error: 'Missing required fields: service, ruleType, ruleText' });
    }
    const result = await apiRuleService.upsert(
      payload.service,
      payload.ruleType,
      payload.ruleText,
      {
        codePattern: payload.codePattern,
        fixHint: payload.fixHint,
        source: payload.source || 'system',
      }
    );
    res.json(formatMCPResponse('api_rule.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api_rule.list
 * List all rules, optionally filtered.
 * Body: { payload: { service?, ruleType?, source?, limit? }, requestId }
 */
router.post('/api_rule.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const result = await apiRuleService.list({
      service: payload?.service,
      ruleType: payload?.ruleType,
      source: payload?.source,
      limit: payload?.limit || 200,
    });
    res.json(formatMCPResponse('api_rule.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api_rule.delete
 * Delete a rule by id.
 * Body: { payload: { id }, requestId }
 */
router.post('/api_rule.delete', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const result = await apiRuleService.delete(payload.id);
    res.json(formatMCPResponse('api_rule.delete', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
