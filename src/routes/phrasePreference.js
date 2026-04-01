import express from 'express';
import { getPhrasePreferenceService } from '../services/phrasePreference.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const phrasePreferenceService = getPhrasePreferenceService();

/**
 * POST /phrase_preference.search
 * Find the closest stored delivery preference for a phrase.
 * Body: { payload: { phrase: string, threshold?: number }, context, requestId }
 * Returns: { delivery, service, metadata, examplePhrase, similarity } or null
 */
router.post('/phrase_preference.search', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.phrase) {
      return res.status(400).json({ error: 'Missing required field: phrase' });
    }
    const result = await phrasePreferenceService.search(payload.phrase, {
      threshold: payload.threshold,
    });
    res.json(formatMCPResponse('phrase_preference.search', requestId, 'ok', { match: result }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /phrase_preference.upsert
 * Store or overwrite a user delivery preference.
 * Body: { payload: { examplePhrase, delivery, service?, metadata?, source? }, context, requestId }
 */
router.post('/phrase_preference.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.examplePhrase || !payload?.delivery) {
      return res.status(400).json({ error: 'Missing required fields: examplePhrase, delivery' });
    }
    const result = await phrasePreferenceService.upsert(
      payload.examplePhrase,
      payload.delivery,
      { service: payload.service, metadata: payload.metadata, source: payload.source }
    );
    res.json(formatMCPResponse('phrase_preference.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /phrase_preference.list
 * List all stored preferences.
 * Body: { payload: { delivery?, service?, limit? }, context, requestId }
 */
router.post('/phrase_preference.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const result = await phrasePreferenceService.list({
      delivery: payload?.delivery,
      service: payload?.service,
      limit: payload?.limit || 100,
    });
    res.json(formatMCPResponse('phrase_preference.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /phrase_preference.delete
 * Delete a preference by id.
 * Body: { payload: { id }, context, requestId }
 */
router.post('/phrase_preference.delete', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }
    const result = await phrasePreferenceService.delete(payload.id);
    res.json(formatMCPResponse('phrase_preference.delete', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
