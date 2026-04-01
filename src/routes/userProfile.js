import express from 'express';
import { getUserProfileService } from '../services/userProfile.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const profileService = getUserProfileService();

/**
 * POST /profile.set
 * Create or update a user profile entry.
 * Body: { payload: { key, valueRef, sensitive?, service?, label? }, context, requestId }
 */
router.post('/profile.set', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.key || !payload?.valueRef) {
      return res.status(400).json({ error: 'Missing required fields: key, valueRef' });
    }
    const result = await profileService.set(payload.key, payload.valueRef, {
      sensitive: payload.sensitive ?? 0,
      service:   payload.service   ?? null,
      label:     payload.label     ?? null,
    });
    res.json(formatMCPResponse('profile.set', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /profile.get
 * Retrieve a profile entry by key.
 * Body: { payload: { key }, context, requestId }
 */
router.post('/profile.get', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.key) {
      return res.status(400).json({ error: 'Missing required field: key' });
    }
    const result = await profileService.get(payload.key);
    res.json(formatMCPResponse('profile.get', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /profile.list
 * List profile entries, optionally filtered by service.
 * Body: { payload: { service? }, context, requestId }
 */
router.post('/profile.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const entries = await profileService.list({ service: payload?.service });
    res.json(formatMCPResponse('profile.list', requestId, 'ok', { entries }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /profile.delete
 * Delete a profile entry by key.
 * Body: { payload: { key }, context, requestId }
 */
router.post('/profile.delete', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.key) {
      return res.status(400).json({ error: 'Missing required field: key' });
    }
    const result = await profileService.delete(payload.key);
    res.json(formatMCPResponse('profile.delete', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /profile.store_secret
 * Store a raw credential in the OS keychain and register its KEYTAR ref in
 * the profile table.  The raw value is NEVER echoed back in responses.
 * Body: { payload: { keytarKey, value, service?, label? }, requestId }
 */
router.post('/profile.store_secret', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.keytarKey || payload?.value === undefined || payload?.value === null) {
      return res.status(400).json({ error: 'Missing required fields: keytarKey, value' });
    }
    const result = await profileService.storeSecret(
      payload.keytarKey,
      payload.value,
      payload.service ?? null,
      payload.label   ?? null,
    );
    // Strip the raw value — only return the safe pointer
    const safe = { stored: result.stored, keytarKey: result.keytarKey, keyRef: result.keyRef };
    res.json(formatMCPResponse('profile.store_secret', requestId, 'ok', safe));
  } catch (error) {
    next(error);
  }
});

export default router;
