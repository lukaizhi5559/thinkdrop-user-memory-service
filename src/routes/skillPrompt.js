import express from 'express';
import { getSkillPromptService } from '../services/skillPrompt.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const skillPromptService = getSkillPromptService();

/**
 * POST /skill_prompt.search
 * Semantic search for skill prompt snippets matching a query.
 * Body: { payload: { query, topK?, minSimilarity? }, context, requestId }
 */
router.post('/skill_prompt.search', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.query) {
      return res.status(400).json({ error: 'Missing required field: query' });
    }

    const result = await skillPromptService.search(payload.query, {
      topK: payload.topK || 3,
      minSimilarity: payload.minSimilarity
    });

    res.json(formatMCPResponse('skill_prompt.search', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill_prompt.upsert
 * Insert or update a skill prompt snippet.
 * Body: { payload: { tags, promptText }, context, requestId }
 */
router.post('/skill_prompt.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.promptText) {
      return res.status(400).json({ error: 'Missing required field: promptText' });
    }

    const result = await skillPromptService.upsert(payload.tags || [], payload.promptText);

    res.json(formatMCPResponse('skill_prompt.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill_prompt.list
 * List all stored skill prompts (admin/debug).
 * Body: { payload: { limit? }, context, requestId }
 */
router.post('/skill_prompt.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const result = await skillPromptService.list(payload?.limit || 50);
    res.json(formatMCPResponse('skill_prompt.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill_prompt.delete
 * Delete a skill prompt by id.
 * Body: { payload: { id }, context, requestId }
 */
router.post('/skill_prompt.delete', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }

    const result = await skillPromptService.delete(payload.id);
    res.json(formatMCPResponse('skill_prompt.delete', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
