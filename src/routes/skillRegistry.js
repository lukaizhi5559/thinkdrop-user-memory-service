import express from 'express';
import { getSkillRegistryService } from '../services/skillRegistry.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();
const skillRegistryService = getSkillRegistryService();

/**
 * POST /skill.install
 * Install a skill from its contract .md content.
 * Body: { payload: { contractMd }, context, requestId }
 */
router.post('/skill.install', async (req, res) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.contractMd) {
      return res.status(400).json({ error: 'Missing required field: contractMd' });
    }

    const result = await skillRegistryService.install(payload.contractMd);
    res.json(formatMCPResponse('skill.install', requestId, 'ok', result));
  } catch (error) {
    res.status(400).json({
      error: error.message,
      code: 'SKILL_INSTALL_FAILED'
    });
  }
});

/**
 * POST /skill.remove
 * Remove an installed skill by name.
 * Body: { payload: { name }, context, requestId }
 */
router.post('/skill.remove', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.name) {
      return res.status(400).json({ error: 'Missing required field: name' });
    }

    const result = await skillRegistryService.remove(payload.name);
    res.json(formatMCPResponse('skill.remove', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.list
 * List all installed skills.
 * Body: { payload: { enabledOnly? }, context, requestId }
 */
router.post('/skill.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const result = await skillRegistryService.list(payload?.enabledOnly || false);
    res.json(formatMCPResponse('skill.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.get
 * Get a single skill by name (includes full contract_md).
 * Body: { payload: { name }, context, requestId }
 */
router.post('/skill.get', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.name) {
      return res.status(400).json({ error: 'Missing required field: name' });
    }

    const result = await skillRegistryService.get(payload.name);
    if (!result) {
      return res.status(404).json({ error: `Skill '${payload.name}' not found` });
    }
    res.json(formatMCPResponse('skill.get', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.listNames
 * Get all enabled skill names + descriptions (lightweight, used by parseSkill node).
 * Body: { payload: {}, context, requestId }
 */
router.post('/skill.listNames', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const results = await skillRegistryService.listNames();
    res.json(formatMCPResponse('skill.listNames', requestId, 'ok', { results, total: results.length }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.setEnabled
 * Enable or disable an installed skill.
 * Body: { payload: { name, enabled }, context, requestId }
 */
router.post('/skill.setEnabled', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.name || typeof payload?.enabled !== 'boolean') {
      return res.status(400).json({ error: 'Missing required fields: name, enabled (boolean)' });
    }

    const result = await skillRegistryService.setEnabled(payload.name, payload.enabled);
    res.json(formatMCPResponse('skill.setEnabled', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.upsert
 * Register or update a skill directly from fields (no contractMd).
 * Used by the installSkill node after the build pipeline writes the .cjs file.
 * Body: { payload: { name, description, execPath, execType, enabled? }, context, requestId }
 */
router.post('/skill.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;

    if (!payload?.name || !payload?.execPath) {
      return res.status(400).json({ error: 'Missing required fields: name, execPath' });
    }

    const result = await skillRegistryService.upsert({
      name:        payload.name,
      description: payload.description || '',
      execPath:    payload.execPath,
      execType:    payload.execType || 'node',
      enabled:     payload.enabled !== false,
      contractMd:  payload.contractMd || '',
    });
    res.json(formatMCPResponse('skill.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.health.upsert
 * Write a health record for a skill (called by skill.review agent after validation/repair).
 * Body: { payload: { skillName, status, errors?, autoRepaired? }, requestId }
 */
router.post('/skill.health.upsert', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.skillName || !payload?.status) {
      return res.status(400).json({ error: 'Missing required fields: skillName, status' });
    }
    const result = await skillRegistryService.healthUpsert({
      skillName:   payload.skillName,
      status:      payload.status,
      errors:      payload.errors || null,
      autoRepaired: Boolean(payload.autoRepaired),
    });
    res.json(formatMCPResponse('skill.health.upsert', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.health.get
 * Get the health record for a single skill by name.
 * Body: { payload: { skillName }, requestId }
 */
router.post('/skill.health.get', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    if (!payload?.skillName) {
      return res.status(400).json({ error: 'Missing required field: skillName' });
    }
    const result = await skillRegistryService.healthGet(payload.skillName);
    if (!result) {
      return res.status(404).json({ error: `No health record for skill '${payload.skillName}'` });
    }
    res.json(formatMCPResponse('skill.health.get', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /skill.health.list
 * List health records. By default returns only non-'ok' (unhealthy) skills.
 * Pass { all: true } to return all records.
 * Body: { payload: { all? }, requestId }
 */
router.post('/skill.health.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const result = await skillRegistryService.healthList({ all: Boolean(payload?.all) });
    res.json(formatMCPResponse('skill.health.list', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
